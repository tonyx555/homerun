from __future__ import annotations

import asyncio
import logging
import time as _time_mod
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import bindparam, func, inspect, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from models.database import LiveTradingOrder, TradeSignal, TraderOrder, release_conn
from services.polymarket import polymarket_client
from services.runtime_signal_queue import publish_signal_batch
from services.signal_bus import make_dedupe_key, upsert_trade_signal
from services.simulation import simulation_service
from services.strategy_sdk import StrategySDK
from services.live_execution_service import live_execution_service
from services.trader_order_verification import (
    TRADER_ORDER_VERIFICATION_LOCAL,
    append_trader_order_verification_event,
    apply_trader_order_verification,
    derive_trader_order_verification,
)
from services.trader_orchestrator_state import _dedupe_live_authority_rows, _extract_copy_source_wallet_from_payload
from utils.utcnow import utcnow
from utils.converters import safe_float
import services.trader_hot_state as hot_state

logger = logging.getLogger("position_lifecycle")

PAPER_ACTIVE_STATUSES = {"submitted", "executed", "open"}
LIVE_ACTIVE_STATUSES = {"submitted", "executed", "open"}
_FAILED_EXIT_MAX_RETRIES = 5
_FAILED_EXIT_MIN_RETRY_INTERVAL_SECONDS = 15

# ── Short-lived cache for wallet data to avoid redundant Polymarket API
# calls when multiple traders share the same execution wallet.
_WALLET_CACHE_TTL_SECONDS = 15.0
_WALLET_HISTORY_CACHE_TTL_SECONDS = 60.0
_WALLET_HISTORY_GRACE_SECONDS = 120.0
_WALLET_POSITIONS_LOAD_TIMEOUT_SECONDS = 4.0
_WALLET_HISTORY_LOAD_TIMEOUT_SECONDS = 4.0
_wallet_positions_cache: tuple[float, dict[str, dict[str, Any]]] = (0.0, {})
_wallet_positions_last_refresh_succeeded = False
_wallet_closed_positions_cache: tuple[float, dict[str, dict[str, Any]]] = (0.0, {})
_wallet_closed_positions_last_refresh_succeeded = False
_wallet_sell_trades_cache: tuple[float, dict[str, dict[str, Any]]] = (0.0, {})
_wallet_activity_cache: tuple[float, dict[str, dict[str, Any]]] = (0.0, {})
_wallet_activity_last_refresh_succeeded = False
_WALLET_SIZE_EPSILON = 1e-9
_MARK_TOUCH_INTERVAL_SECONDS = 0.5
_MAX_LIVE_EXIT_FALLBACK_MARK_AGE_SECONDS = 120.0
_LIVE_EXIT_ORDER_TIMEOUT_SECONDS = 12.0
_LIVE_EXIT_RETRY_TIMEOUT_SECONDS = 3.0
_MARKET_INFO_LOAD_TIMEOUT_SECONDS = 4.0
_ORDER_SNAPSHOT_LOAD_TIMEOUT_SECONDS = 3.0
_RECONCILE_TIMING_WARN_SECONDS = 20.0
_TERMINAL_REOPEN_LOOKBACK_HOURS = 6.0
_NONACTIVE_WALLET_REOPEN_LOOKBACK_HOURS = 24.0


def _iso_utc(value: datetime) -> str:
    dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _mark_touch_interval_seconds(params: dict[str, Any], *, mode: str) -> float:
    mode_key = str(mode or "").strip().lower()
    aliases: tuple[str, ...]
    if mode_key == "paper":
        aliases = ("paper_mark_touch_interval_seconds", "mark_touch_interval_seconds")
    else:
        aliases = ("live_mark_touch_interval_seconds", "mark_touch_interval_seconds")
    for key in aliases:
        parsed = safe_float(params.get(key))
        if parsed is None:
            continue
        return max(0.05, min(5.0, float(parsed)))
    return _MARK_TOUCH_INTERVAL_SECONDS


def _terminal_row_requires_reopen_audit(row: TraderOrder, now_naive: datetime) -> bool:
    payload = dict(getattr(row, "payload_json", None) or {})
    position_close = payload.get("position_close")
    pending_exit = payload.get("pending_live_exit")
    status_key = str(getattr(row, "status", None) or "").strip().lower()
    close_trigger = ""
    if isinstance(position_close, dict):
        close_trigger = str(position_close.get("close_trigger") or "").strip().lower()
    pending_status = ""
    if isinstance(pending_exit, dict):
        pending_status = str(pending_exit.get("status") or "").strip().lower()
    age_anchor = getattr(row, "updated_at", None) or getattr(row, "executed_at", None) or getattr(row, "created_at", None)
    if not isinstance(age_anchor, datetime):
        return False
    if age_anchor.tzinfo is not None:
        age_anchor = age_anchor.astimezone(timezone.utc).replace(tzinfo=None)
    if close_trigger in {"wallet_absent_close", "wallet_flat_override"} or pending_status == "filled":
        return age_anchor >= (now_naive - timedelta(hours=_TERMINAL_REOPEN_LOOKBACK_HOURS))
    if status_key not in {"cancelled", "failed", "rejected", "error"}:
        return False
    verification_status = str(getattr(row, "verification_status", None) or "").strip().lower()
    if verification_status in {"venue_order", "venue_fill", "wallet_position"}:
        return age_anchor >= (now_naive - timedelta(hours=_NONACTIVE_WALLET_REOPEN_LOOKBACK_HOURS))
    if str(getattr(row, "provider_clob_order_id", None) or "").strip():
        return age_anchor >= (now_naive - timedelta(hours=_NONACTIVE_WALLET_REOPEN_LOOKBACK_HOURS))
    if str(getattr(row, "provider_order_id", None) or "").strip():
        return age_anchor >= (now_naive - timedelta(hours=_NONACTIVE_WALLET_REOPEN_LOOKBACK_HOURS))
    return isinstance(payload.get("provider_reconciliation"), dict) and age_anchor >= (
        now_naive - timedelta(hours=_NONACTIVE_WALLET_REOPEN_LOOKBACK_HOURS)
    )


async def _strategy_exit_instance(session: AsyncSession, strategy_slug: str) -> Any | None:
    slug = str(strategy_slug or "").strip().lower()
    if not slug:
        return None
    from services.strategy_loader import strategy_loader

    loaded = strategy_loader.get_strategy(slug)
    if loaded is None:
        try:
            await strategy_loader.reload_from_db(slug, session)
        except Exception as exc:
            logger.debug("Failed lazy strategy reload for exit decision (%s): %s", slug, exc)
        loaded = strategy_loader.get_strategy(slug)
    if loaded is None:
        return None
    instance = getattr(loaded, "instance", None)
    if instance is None or not hasattr(instance, "should_exit"):
        return None
    return instance


async def _publish_trader_order_updates(rows: list[TraderOrder]) -> None:
    if not rows:
        return
    from services.event_bus import event_bus
    from services.trader_orchestrator_state import _serialize_order

    seen: set[str] = set()
    for row in rows:
        row_id = str(getattr(row, "id", "") or "").strip()
        if not row_id or row_id in seen:
            continue
        seen.add(row_id)
        try:
            await event_bus.publish("trader_order", _serialize_order(row))
        except Exception:
            continue


def _failed_exit_retry_delay_seconds(last_error: Any) -> int:
    error_text = str(last_error or "").strip().lower()
    if "status_code=429" in error_text or "error 1015" in error_text or "too many requests" in error_text:
        return 90
    if "status_code=425" in error_text or "service not ready" in error_text:
        return 45
    if "invalid signature" in error_text:
        return 120
    if "not enough balance / allowance" in error_text or "allowance" in error_text:
        return 8
    if (
        "vpn check failed" in error_text
        or "trading proxy unreachable" in error_text
        or "invalid username/password" in error_text
    ):
        return 90
    if (
        "below minimum" in error_text
        or "lower than minimum" in error_text
        or "exit_notional_below_min" in error_text
    ):
        return 20
    if "missing token_id or fill_size" in error_text:
        return 30
    return _FAILED_EXIT_MIN_RETRY_INTERVAL_SECONDS


def _is_allowance_error(error: Any) -> bool:
    error_text = str(error or "").strip().lower()
    return "not enough balance / allowance" in error_text or "allowance" in error_text


def _is_zero_balance_error(error: Any) -> bool:
    error_text = str(error or "").strip().lower()
    return "available_shares=0" in error_text or "balance_shares=0" in error_text


def _is_rate_limited_error(error: Any) -> bool:
    error_text = str(error or "").strip().lower()
    return "status_code=429" in error_text or "error 1015" in error_text or "too many requests" in error_text


def _is_invalid_signature_error(error: Any) -> bool:
    error_text = str(error or "").strip().lower()
    return "invalid signature" in error_text


def _bump_allowance_error_counter(pending_exit: dict[str, Any], error: Any) -> None:
    if _is_allowance_error(error):
        pending_exit["allowance_error_count"] = int(pending_exit.get("allowance_error_count", 0) or 0) + 1
        return
    pending_exit["allowance_error_count"] = 0


def _apply_failed_exit_state(pending_exit: dict[str, Any], *, error: Any, now: datetime, retry_count: int) -> None:
    error_text = str(error or "unknown")
    pending_exit["last_error"] = error_text
    pending_exit["last_attempt_at"] = _iso_utc(now)
    if _is_zero_balance_error(error_text):
        pending_exit["status"] = "blocked_no_inventory"
        pending_exit["retry_count"] = retry_count
        pending_exit["exhausted_at"] = _iso_utc(now)
        pending_exit["next_retry_at"] = None
        return
    pending_exit["status"] = "failed"
    pending_exit["retry_count"] = retry_count
    _bump_allowance_error_counter(pending_exit, error_text)
    pending_exit["next_retry_at"] = _iso_utc(now + timedelta(seconds=_failed_exit_retry_delay_seconds(error_text)))


def _take_profit_price_floor(pending_exit: dict[str, Any]) -> Optional[float]:
    """Compute the minimum price for a take-profit-limit exit.

    Returns None if the TP should be abandoned (market moved against us
    below entry price — caller should convert to a market sell).
    """
    kind = str(pending_exit.get("kind") or "").strip().lower()
    if kind != "take_profit_limit":
        return None
    close_price = safe_float(pending_exit.get("close_price"))
    entry_fill_price = safe_float(pending_exit.get("entry_fill_price"))

    current_price = safe_float(pending_exit.get("current_mid_price"))
    if current_price is not None and entry_fill_price is not None and entry_fill_price > 0.0:
        if current_price < entry_fill_price * 0.995:
            return None

    target_take_profit_pct = max(0.0, safe_float(pending_exit.get("target_take_profit_pct"), 0.0) or 0.0)
    fee_buffer_pct = max(0.0, safe_float(pending_exit.get("take_profit_fee_buffer_pct"), 0.35) or 0.35)
    floor_candidates: list[float] = []
    if close_price is not None and close_price > 0.0:
        floor_candidates.append(close_price)
    if entry_fill_price is not None and entry_fill_price > 0.0:
        floor_candidates.append(entry_fill_price * (1.0 + (target_take_profit_pct / 100.0)))
        floor_candidates.append(entry_fill_price * (1.0 + (fee_buffer_pct / 100.0)))
    if not floor_candidates:
        return None
    return min(0.999, max(0.01, max(floor_candidates)))


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _strategy_hold_blocks_default_exit(exit_decision: Any) -> bool:
    if exit_decision is None:
        return False
    action = str(getattr(exit_decision, "action", "") or "").strip().lower()
    if action != "hold":
        return False
    payload = getattr(exit_decision, "payload", None)
    if not isinstance(payload, dict):
        return False
    return _safe_bool(payload.get("skip_default_exit"), False)


def _is_rapid_close_trigger(close_trigger: Any) -> bool:
    trigger = str(close_trigger or "").strip().lower()
    if not trigger:
        return False
    normalized = " ".join(trigger.replace("_", " ").replace("-", " ").split())
    rapid_markers = (
        "rapid",
        "stop loss",
        "trailing stop",
        "adaptive backside",
        "executable notional guard",
        "executable hazard exit",
        "underwater",
        "resolution risk flatten",
        "force flatten",
        "manual mark to market",
    )
    return any(marker in normalized for marker in rapid_markers)


def _direction_outcome_index(direction: Any) -> Optional[int]:
    normalized = str(direction or "").strip().lower()
    if normalized == "buy_yes":
        return 0
    if normalized == "buy_no":
        return 1
    return None


def _opposite_direction(direction: Any) -> str:
    return StrategySDK.opposite_direction(direction, default="")


def _resolve_position_min_order_size_usd(
    *,
    trader_params: dict[str, Any],
    payload: dict[str, Any],
    mode: str,
) -> float:
    merged_params: dict[str, Any] = {}
    merged_params.update(dict(trader_params or {}))

    strategy_exit_config = payload.get("strategy_exit_config")
    if isinstance(strategy_exit_config, dict):
        merged_params.update(strategy_exit_config)

    strategy_context = payload.get("strategy_context")
    if isinstance(strategy_context, dict):
        strategy_context_params = strategy_context.get("params")
        if isinstance(strategy_context_params, dict):
            merged_params.update(strategy_context_params)

    payload_strategy_params = payload.get("strategy_params")
    if isinstance(payload_strategy_params, dict):
        merged_params.update(payload_strategy_params)

    return StrategySDK.resolve_min_order_size_usd(merged_params, mode=mode, fallback=1.0)


def _payload_strategy_exit_config(payload: dict[str, Any]) -> dict[str, Any]:
    raw = payload.get("strategy_exit_config")
    return dict(raw) if isinstance(raw, dict) else {}


def _payload_exit_param(
    payload: dict[str, Any],
    *,
    prefix_key: str,
    name: str,
    default: Any = None,
) -> Any:
    exit_config = _payload_strategy_exit_config(payload)
    prefixed_key = f"{prefix_key}_{name}"
    if prefixed_key in exit_config:
        return exit_config.get(prefixed_key)
    if name in exit_config:
        return exit_config.get(name)
    if prefix_key != "paper":
        paper_key = f"paper_{name}"
        if paper_key in exit_config:
            return exit_config.get(paper_key)
    return default


def _extract_market_token_ids(market_info: Optional[dict[str, Any]]) -> list[str]:
    if not isinstance(market_info, dict):
        return []
    token_ids = market_info.get("token_ids")
    if not isinstance(token_ids, list):
        token_ids = market_info.get("tokenIds")
    if not isinstance(token_ids, list):
        token_ids = market_info.get("clob_token_ids")
    if not isinstance(token_ids, list):
        token_ids = market_info.get("clobTokenIds")
    if not isinstance(token_ids, list):
        return []
    out: list[str] = []
    for token_id in token_ids:
        token_text = str(token_id or "").strip()
        if token_text:
            out.append(token_text)
    return out


def _extract_market_token_id(market_info: Optional[dict[str, Any]], outcome_idx: int) -> str:
    if outcome_idx not in (0, 1):
        return ""
    token_ids = _extract_market_token_ids(market_info)
    if not token_ids:
        return ""
    outcomes_raw = (market_info or {}).get("outcomes")
    if isinstance(outcomes_raw, list) and len(outcomes_raw) == len(token_ids):
        for idx, outcome_label in enumerate(outcomes_raw):
            label = str(outcome_label or "").strip().lower()
            if label == "yes" and outcome_idx == 0:
                return token_ids[idx]
            if label == "no" and outcome_idx == 1:
                return token_ids[idx]
    if len(token_ids) > outcome_idx:
        return token_ids[outcome_idx]
    return ""


def _normalize_reverse_entry(
    *,
    reverse_intent: Any,
    current_direction: str,
    current_price: Optional[float],
    market_seconds_left: Optional[float],
    market_info: Optional[dict[str, Any]],
    strategy_type: str,
    source: str,
    now: datetime,
) -> dict[str, Any] | None:
    normalized = StrategySDK.normalize_reverse_intent(
        reverse_intent,
        fallback_direction=_opposite_direction(current_direction),
        default_signal_type=f"{str(source or '').strip().lower() or 'strategy'}_reverse",
    )
    if not isinstance(normalized, dict):
        return None

    direction = str(normalized.get("direction") or "").strip().lower()
    if direction not in {"buy_yes", "buy_no"}:
        return None
    if direction == str(current_direction or "").strip().lower():
        return None

    min_seconds_left = max(0.0, safe_float(normalized.get("min_seconds_left"), 0.0) or 0.0)
    if market_seconds_left is not None and market_seconds_left < min_seconds_left:
        return None

    min_price_headroom = max(0.0, min(1.0, safe_float(normalized.get("min_price_headroom"), 0.0) or 0.0))
    if current_price is not None and current_price < min_price_headroom:
        return None

    outcome_idx = _direction_outcome_index(direction)
    if outcome_idx is None:
        return None

    entry_price = safe_float(normalized.get("entry_price"), None)
    if entry_price is None or entry_price <= 0.0 or entry_price >= 1.0:
        entry_price = _extract_market_side_price(market_info, outcome_idx)
    if (entry_price is None or entry_price <= 0.0 or entry_price >= 1.0) and current_price is not None:
        entry_price = 1.0 - current_price
    if entry_price is None or entry_price <= 0.0 or entry_price >= 1.0:
        return None

    token_id = str(normalized.get("token_id") or "").strip()
    if not token_id:
        token_id = _extract_market_token_id(market_info, outcome_idx)

    strategy_key = str(normalized.get("strategy_type") or strategy_type or "").strip().lower()
    signal_type = str(normalized.get("signal_type") or f"{source}_reverse").strip().lower()
    if not signal_type:
        signal_type = "strategy_reverse"

    expires_in_seconds = max(5.0, safe_float(normalized.get("expires_in_seconds"), 60.0) or 60.0)
    expires_at = now + timedelta(seconds=expires_in_seconds)
    out = dict(normalized)
    out["direction"] = direction
    out["entry_price"] = float(max(0.001, min(0.999, entry_price)))
    out["token_id"] = token_id
    out["strategy_type"] = strategy_key
    out["signal_type"] = signal_type
    out["source"] = str(source or "").strip().lower()
    out["expires_at"] = _iso_utc(expires_at)
    return out


def _arm_reverse_entry_from_exit(
    *,
    row: TraderOrder,
    payload: dict[str, Any],
    strategy_exit: Any,
    market_info: Optional[dict[str, Any]],
    market_seconds_left: Optional[float],
    current_price: Optional[float],
    now: datetime,
) -> dict[str, Any] | None:
    strategy_payload = getattr(strategy_exit, "payload", None)
    if not isinstance(strategy_payload, dict):
        return None
    reverse_intent = strategy_payload.get("reverse_intent")
    if not isinstance(reverse_intent, dict):
        return None

    existing = payload.get("pending_reverse_entry")
    existing = dict(existing) if isinstance(existing, dict) else {}
    existing_status = str(existing.get("status") or "").strip().lower()
    if existing_status in {"emitted", "submitted"} and str(existing.get("signal_id") or "").strip():
        return None

    prepared = _normalize_reverse_entry(
        reverse_intent=reverse_intent,
        current_direction=str(row.direction or ""),
        current_price=current_price,
        market_seconds_left=market_seconds_left,
        market_info=market_info,
        strategy_type=str(payload.get("strategy_type") or ""),
        source=str(row.source or ""),
        now=now,
    )
    if not isinstance(prepared, dict):
        return None

    attempt_index = int(safe_float(existing.get("attempt_index"), 0) or 0) + 1
    prepared["status"] = "armed"
    prepared["armed_at"] = _iso_utc(now)
    prepared["attempt_index"] = attempt_index
    prepared["source_order_id"] = str(row.id or "")
    prepared["source_signal_id"] = str(row.signal_id or "")
    prepared["trigger_reason"] = str(getattr(strategy_exit, "reason", "") or "").strip()
    payload["pending_reverse_entry"] = prepared
    return prepared


async def _emit_armed_reverse_signal(
    session: AsyncSession,
    *,
    row: TraderOrder,
    payload: dict[str, Any],
    signal_payload: dict[str, Any],
    market_info: Optional[dict[str, Any]],
    close_trigger: Optional[str],
    realized_pnl: Optional[float],
    now: datetime,
) -> tuple[str | None, str | None]:
    pending = payload.get("pending_reverse_entry")
    if not isinstance(pending, dict):
        return None, None
    status = str(pending.get("status") or "").strip().lower()
    if status not in {"armed", "failed", "ready"}:
        return None, None
    trigger_key = str(close_trigger or "").strip().lower()
    if "resolution" in trigger_key:
        pending["status"] = "skipped_resolution"
        pending["skipped_at"] = _iso_utc(now)
        payload["pending_reverse_entry"] = pending
        return None, None

    expires_at = _parse_iso_utc_naive(pending.get("expires_at"))
    now_naive = now.astimezone(timezone.utc).replace(tzinfo=None) if now.tzinfo is not None else now
    if expires_at is not None and now_naive > expires_at:
        pending["status"] = "expired"
        pending["expired_at"] = _iso_utc(now)
        payload["pending_reverse_entry"] = pending
        return None, None

    direction = str(pending.get("direction") or "").strip().lower()
    if direction not in {"buy_yes", "buy_no"}:
        pending["status"] = "failed"
        pending["last_error"] = "invalid_reverse_direction"
        pending["failed_at"] = _iso_utc(now)
        payload["pending_reverse_entry"] = pending
        return None, None
    outcome_idx = _direction_outcome_index(direction)
    if outcome_idx is None:
        pending["status"] = "failed"
        pending["last_error"] = "invalid_reverse_outcome_idx"
        pending["failed_at"] = _iso_utc(now)
        payload["pending_reverse_entry"] = pending
        return None, None

    entry_price = safe_float(pending.get("entry_price"), None)
    if entry_price is None or entry_price <= 0.0 or entry_price >= 1.0:
        entry_price = _extract_market_side_price(market_info, outcome_idx)
    if entry_price is None or entry_price <= 0.0 or entry_price >= 1.0:
        close_price = safe_float((payload.get("position_close") or {}).get("close_price"), None)
        if close_price is not None:
            entry_price = 1.0 - close_price
    if entry_price is None or entry_price <= 0.0 or entry_price >= 1.0:
        pending["status"] = "failed"
        pending["last_error"] = "missing_reverse_entry_price"
        pending["failed_at"] = _iso_utc(now)
        payload["pending_reverse_entry"] = pending
        return None, None

    token_id = str(pending.get("token_id") or "").strip()
    if not token_id:
        token_id = _extract_market_token_id(market_info, outcome_idx)
        if token_id:
            pending["token_id"] = token_id

    source = str(row.source or "").strip().lower() or "scanner"
    strategy_type = str(pending.get("strategy_type") or payload.get("strategy_type") or "").strip().lower() or None
    signal_type = str(pending.get("signal_type") or f"{source}_reverse").strip().lower()
    if not signal_type:
        signal_type = "strategy_reverse"
    confidence = safe_float(pending.get("confidence"), None)
    edge_percent = safe_float(pending.get("edge_percent"), None)
    liquidity = safe_float(
        pending.get("liquidity"),
        safe_float((signal_payload or {}).get("liquidity"), safe_float(getattr(row, "notional_usd", None), 0.0)),
    )

    payload_json = dict(signal_payload or {})
    payload_json["strategy_origin"] = str(payload_json.get("strategy_origin") or "crypto_worker")
    payload_json["reverse_entry"] = {
        "enabled": True,
        "source_order_id": str(row.id or ""),
        "source_signal_id": str(row.signal_id or ""),
        "close_trigger": trigger_key,
        "realized_pnl": realized_pnl,
        "direction": direction,
        "entry_price": float(entry_price),
        "size_multiplier": safe_float(pending.get("size_multiplier"), 1.0),
        "size_usd": safe_float(pending.get("size_usd")),
        "metadata": dict(pending.get("metadata") or {}) if isinstance(pending.get("metadata"), dict) else {},
    }
    payload_json["reverse_entry"]["emitted_at"] = _iso_utc(now)
    payload_json["selected_token_id"] = token_id or payload_json.get("selected_token_id")
    payload_json["token_id"] = token_id or payload_json.get("token_id")
    payload_json["signal_emitted_at"] = _iso_utc(now)
    payload_json["source_observed_at"] = _iso_utc(now)

    strategy_context = payload.get("strategy_context")
    strategy_context_json = dict(strategy_context) if isinstance(strategy_context, dict) else {}
    strategy_context_json["reverse_entry"] = dict(payload_json["reverse_entry"])
    strategy_context_json["source_order_id"] = str(row.id or "")
    strategy_context_json["source_signal_id"] = str(row.signal_id or "")
    strategy_context_json["selected_direction"] = direction
    if token_id:
        strategy_context_json["selected_token_id"] = token_id

    attempt_index = int(safe_float(pending.get("attempt_index"), 1) or 1)
    dedupe_key = make_dedupe_key(
        "reverse_entry",
        str(row.id or ""),
        attempt_index,
        direction,
        str(pending.get("armed_at") or ""),
    )
    source_item_id = str(pending.get("source_signal_id") or row.signal_id or row.id or "")
    market_question = (
        str(
            payload_json.get("market_question") or payload_json.get("question") or getattr(row, "market_id", "") or ""
        ).strip()
        or None
    )
    expires_at_dt = _parse_iso_utc_naive(pending.get("expires_at"))
    if expires_at_dt is None:
        expires_seconds = max(5.0, safe_float(pending.get("expires_in_seconds"), 60.0) or 60.0)
        expires_at_dt = now_naive + timedelta(seconds=expires_seconds)

    try:
        signal_row = await upsert_trade_signal(
            session,
            source=source,
            source_item_id=source_item_id,
            signal_type=signal_type,
            strategy_type=strategy_type,
            market_id=str(row.market_id or ""),
            market_question=market_question,
            direction=direction,
            entry_price=float(max(0.001, min(0.999, entry_price))),
            edge_percent=edge_percent,
            confidence=confidence,
            liquidity=liquidity,
            expires_at=expires_at_dt,
            payload_json=payload_json,
            strategy_context_json=strategy_context_json or None,
            dedupe_key=dedupe_key,
            commit=False,
        )
    except Exception as exc:
        pending["status"] = "failed"
        pending["last_error"] = str(exc)
        pending["failed_at"] = _iso_utc(now)
        payload["pending_reverse_entry"] = pending
        return None, None

    signal_id = str(getattr(signal_row, "id", "") or "").strip()
    pending["status"] = "emitted"
    pending["emitted_at"] = _iso_utc(now)
    pending["signal_id"] = signal_id
    pending["dedupe_key"] = dedupe_key
    payload["pending_reverse_entry"] = pending
    return signal_id or None, source


async def _publish_reverse_signal_batches(signal_ids_by_source: dict[str, list[str]], *, emitted_at: datetime) -> None:
    if not signal_ids_by_source:
        return
    from services.event_bus import event_bus

    emitted_at_iso = emitted_at.isoformat()
    for source_key, signal_ids in signal_ids_by_source.items():
        ids = [str(signal_id or "").strip() for signal_id in signal_ids if str(signal_id or "").strip()]
        if not ids:
            continue
        try:
            await event_bus.publish(
                "trade_signal_batch",
                {
                    "event_type": "reverse_entry",
                    "source": str(source_key or "").strip().lower(),
                    "signal_count": int(len(ids)),
                    "signal_ids": ids[:500],
                    "emitted_at": emitted_at_iso,
                    "trigger": "position_lifecycle_reverse",
                },
            )
        except Exception:
            continue
        try:
            await publish_signal_batch(
                event_type="reverse_entry",
                source=str(source_key or "").strip().lower(),
                signal_ids=ids,
                trigger="position_lifecycle_reverse",
                emitted_at=emitted_at_iso,
            )
        except Exception:
            continue


def _extract_signal_side_price(payload: dict[str, Any], outcome_idx: int) -> Optional[float]:
    side_keys = ("yes",) if outcome_idx == 0 else ("no",)
    for prefix in side_keys:
        for key in (
            f"{prefix}_price",
            f"{prefix}Price",
            f"best_{prefix}",
            f"best{prefix.title()}",
            f"{prefix}_mid",
            f"{prefix}Mid",
        ):
            parsed = safe_float(payload.get(key))
            if parsed is not None and parsed >= 0:
                return parsed

    prices = payload.get("outcome_prices")
    if not isinstance(prices, list):
        prices = payload.get("outcomePrices")
    if isinstance(prices, list) and len(prices) > outcome_idx:
        parsed = safe_float(prices[outcome_idx])
        if parsed is not None and parsed >= 0:
            return parsed
    return None


def _extract_market_side_price(market_info: Optional[dict[str, Any]], outcome_idx: int) -> Optional[float]:
    if not isinstance(market_info, dict):
        return None
    key = "yes_price" if outcome_idx == 0 else "no_price"
    parsed = safe_float(market_info.get(key))
    if parsed is not None and parsed >= 0:
        return parsed
    prices = market_info.get("outcome_prices")
    if isinstance(prices, list) and len(prices) > outcome_idx:
        parsed = safe_float(prices[outcome_idx])
        if parsed is not None and parsed >= 0:
            return parsed
    return None


def _extract_winning_outcome_index(market_info: Optional[dict[str, Any]]) -> Optional[int]:
    if not isinstance(market_info, dict):
        return None

    winner_raw = (
        market_info.get("winning_outcome")
        if market_info.get("winning_outcome") not in (None, "")
        else market_info.get("winner")
    )
    if winner_raw in (None, ""):
        return None

    outcomes_raw = market_info.get("outcomes")
    outcomes: list[str] = []
    if isinstance(outcomes_raw, list):
        outcomes = [str(item or "").strip().lower() for item in outcomes_raw if str(item or "").strip()]

    try:
        idx = int(winner_raw)
        if idx in (0, 1):
            return idx
    except Exception:
        pass

    winner_text = str(winner_raw).strip().lower()
    if winner_text == "yes":
        return 0
    if winner_text == "no":
        return 1
    if outcomes:
        for idx, label in enumerate(outcomes):
            if label == winner_text and idx in (0, 1):
                return idx
    return None


def _market_has_terminal_resolution_signal(market_info: Optional[dict[str, Any]]) -> bool:
    if not isinstance(market_info, dict):
        return False

    if _safe_bool(market_info.get("closed"), False):
        return True
    if _safe_bool(market_info.get("archived"), False):
        return True

    resolved_value = (
        market_info.get("resolved")
        if market_info.get("resolved") is not None
        else (
            market_info.get("isResolved")
            if market_info.get("isResolved") is not None
            else market_info.get("is_resolved")
        )
    )
    if _safe_bool(resolved_value, False):
        return True

    winner = market_info.get("winner")
    if winner not in (None, ""):
        return True

    winning_outcome = (
        market_info.get("winning_outcome")
        if market_info.get("winning_outcome") is not None
        else market_info.get("winningOutcome")
    )
    if winning_outcome not in (None, ""):
        return True

    raw_status = (
        market_info.get("status")
        if market_info.get("status") is not None
        else (
            market_info.get("market_status")
            if market_info.get("market_status") is not None
            else market_info.get("marketStatus")
        )
    )
    status_text = str(raw_status or "").strip().lower()
    if status_text:
        normalized = status_text.replace("_", " ").replace("-", " ")
        blocked_terms = (
            "in review",
            "review",
            "in dispute",
            "dispute",
            "final",
            "resolved",
            "settled",
            "closed",
            "expired",
            "cancelled",
            "canceled",
        )
        if any(term in normalized for term in blocked_terms):
            return True

    uma_statuses: list[str] = []
    raw_uma_status = (
        market_info.get("uma_resolution_status")
        if market_info.get("uma_resolution_status") is not None
        else market_info.get("umaResolutionStatus")
    )
    if isinstance(raw_uma_status, list):
        uma_statuses.extend(str(item or "").strip() for item in raw_uma_status if str(item or "").strip())
    elif raw_uma_status not in (None, ""):
        uma_statuses.append(str(raw_uma_status).strip())

    raw_uma_statuses = (
        market_info.get("uma_resolution_statuses")
        if market_info.get("uma_resolution_statuses") is not None
        else market_info.get("umaResolutionStatuses")
    )
    if isinstance(raw_uma_statuses, list):
        uma_statuses.extend(str(item or "").strip() for item in raw_uma_statuses if str(item or "").strip())
    elif raw_uma_statuses not in (None, ""):
        uma_statuses.append(str(raw_uma_statuses).strip())

    if uma_statuses:
        blocked_terms = (
            "proposed",
            "review",
            "dispute",
            "final",
            "resolved",
            "settled",
            "closed",
            "expired",
            "cancelled",
            "canceled",
        )
        for raw in uma_statuses:
            normalized = raw.lower().replace("_", " ").replace("-", " ")
            if any(term in normalized for term in blocked_terms):
                return True

    end_dt = polymarket_client._coerce_datetime(
        market_info.get("end_date") if market_info.get("end_date") is not None else market_info.get("endDate")
    )
    ended = end_dt is not None and end_dt <= utcnow()
    accepting_orders_value = (
        market_info.get("accepting_orders")
        if market_info.get("accepting_orders") is not None
        else market_info.get("acceptingOrders")
    )
    enable_order_book_value = (
        market_info.get("enable_order_book")
        if market_info.get("enable_order_book") is not None
        else market_info.get("enableOrderBook")
    )
    if ended and (
        _safe_bool(accepting_orders_value, True) is False
        or _safe_bool(enable_order_book_value, True) is False
    ):
        return True

    return False


def _extract_winning_outcome_index_from_prices(
    market_info: Optional[dict[str, Any]],
    *,
    market_tradable: bool,
    settle_floor: float,
) -> Optional[int]:
    if not isinstance(market_info, dict):
        return None
    if market_tradable:
        return None
    if not _market_has_terminal_resolution_signal(market_info):
        return None

    settle_floor = min(1.0, max(0.5, settle_floor))
    settle_ceiling = max(0.0, 1.0 - settle_floor)

    yes_price = safe_float(market_info.get("yes_price"))
    no_price = safe_float(market_info.get("no_price"))
    if yes_price is None or no_price is None:
        prices = market_info.get("outcome_prices")
        if isinstance(prices, list) and len(prices) >= 2:
            outcomes = market_info.get("outcomes")
            if isinstance(outcomes, list) and len(outcomes) == len(prices):
                for idx, raw_label in enumerate(outcomes):
                    label = str(raw_label or "").strip().lower()
                    parsed = safe_float(prices[idx])
                    if parsed is None:
                        continue
                    if yes_price is None and label == "yes":
                        yes_price = parsed
                    if no_price is None and label == "no":
                        no_price = parsed
            if yes_price is None:
                yes_price = safe_float(prices[0])
            if no_price is None:
                no_price = safe_float(prices[1])

    yes_price = _state_price_floor(yes_price)
    no_price = _state_price_floor(no_price)
    if yes_price is None or no_price is None:
        return None
    if yes_price >= settle_floor and no_price <= settle_ceiling:
        return 0
    if no_price >= settle_floor and yes_price <= settle_ceiling:
        return 1
    return None


def _state_price_floor(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def _status_for_close(*, pnl: float, close_trigger: Optional[str]) -> str:
    trigger = str(close_trigger or "").strip().lower()
    is_resolution = trigger in {"resolution", "resolution_inferred"}
    if is_resolution:
        return "resolved_win" if pnl >= 0 else "resolved_loss"
    return "closed_win" if pnl >= 0 else "closed_loss"


def _extract_position_state(payload: dict[str, Any]) -> dict[str, Any]:
    state = payload.get("position_state")
    return state if isinstance(state, dict) else {}


def _first_float_from_candidates(candidates: list[Any], keys: tuple[str, ...]) -> Optional[float]:
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        for key in keys:
            parsed = safe_float(candidate.get(key))
            if parsed is not None:
                return float(parsed)
    return None


def _extract_live_fill_metrics(payload: dict[str, Any]) -> tuple[float, float, Optional[float]]:
    provider_reconciliation = payload.get("provider_reconciliation")
    if not isinstance(provider_reconciliation, dict):
        provider_reconciliation = {}
    snapshot = provider_reconciliation.get("snapshot")
    if not isinstance(snapshot, dict):
        snapshot = {}
    candidates: list[Any] = [provider_reconciliation, snapshot, payload]
    filled_size = max(
        0.0,
        _first_float_from_candidates(
            candidates,
            (
                "filled_size",
                "size_matched",
                "sizeMatched",
                "matched_size",
                "filled_shares",
                "executed_size",
            ),
        )
        or 0.0,
    )
    average_fill_price = _first_float_from_candidates(
        candidates,
        (
            "average_fill_price",
            "avg_fill_price",
            "avg_price",
            "avgFillPrice",
            "matched_price",
            "price",
            "limit_price",
        ),
    )
    filled_notional = max(
        0.0,
        _first_float_from_candidates(
            candidates,
            (
                "filled_notional_usd",
                "filled_notional",
                "matched_notional",
                "matched_amount",
                "executed_notional",
            ),
        )
        or 0.0,
    )
    if filled_notional <= 0.0 and filled_size > 0.0 and average_fill_price is not None and average_fill_price > 0:
        filled_notional = filled_size * average_fill_price
    if filled_size <= 0.0 and filled_notional > 0.0 and average_fill_price is not None and average_fill_price > 0:
        filled_size = filled_notional / average_fill_price
    return filled_notional, filled_size, average_fill_price


def _provider_reconciliation_parts(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    provider_reconciliation = payload.get("provider_reconciliation")
    if not isinstance(provider_reconciliation, dict):
        provider_reconciliation = {}
    snapshot = provider_reconciliation.get("snapshot")
    if not isinstance(snapshot, dict):
        snapshot = {}
    return provider_reconciliation, snapshot


def _provider_snapshot_status(payload: dict[str, Any]) -> str:
    provider_reconciliation, snapshot = _provider_reconciliation_parts(payload)
    for candidate in (
        snapshot.get("normalized_status"),
        provider_reconciliation.get("snapshot_status"),
        provider_reconciliation.get("mapped_status"),
        snapshot.get("status"),
        snapshot.get("raw_status"),
    ):
        status = str(candidate or "").strip().lower()
        if not status:
            continue
        if status in {"live", "active", "working", "partially_filled", "partially_matched"}:
            return "open"
        if status in {"matched", "executed"}:
            return "filled"
        return status
    return ""


def _live_trading_order_snapshot(row: LiveTradingOrder) -> dict[str, Any]:
    raw_status = str(row.status or "").strip().lower()
    normalized_status = raw_status
    if raw_status in {"matched", "executed", "filled"}:
        normalized_status = "filled"
    elif raw_status in {"open", "working", "active", "live", "partially_filled", "partially_matched"}:
        normalized_status = "open"
    elif raw_status in {"pending", "placing", "queued", "submitted"}:
        normalized_status = "pending"
    elif raw_status in {"cancelled", "canceled", "expired"}:
        normalized_status = "cancelled"
    elif raw_status in {"failed", "rejected", "error"}:
        normalized_status = "failed"

    filled_size = max(0.0, safe_float(row.filled_size, 0.0) or 0.0)
    average_fill_price = safe_float(row.average_fill_price)
    limit_price = safe_float(row.price)
    filled_notional_usd = 0.0
    if filled_size > 0.0 and average_fill_price is not None and average_fill_price > 0.0:
        filled_notional_usd = filled_size * average_fill_price
    elif filled_size > 0.0 and limit_price is not None and limit_price > 0.0:
        filled_notional_usd = filled_size * limit_price

    return {
        "clob_order_id": str(row.clob_order_id or "").strip() or None,
        "normalized_status": normalized_status,
        "raw_status": raw_status or None,
        "size": safe_float(row.size),
        "filled_size": filled_size,
        "remaining_size": None,
        "average_fill_price": average_fill_price,
        "limit_price": limit_price,
        "filled_notional_usd": filled_notional_usd if filled_notional_usd > 0.0 else None,
        "raw": {
            "status": raw_status or None,
            "side": str(row.side or "").strip().upper() or None,
            "token_id": str(row.token_id or "").strip() or None,
            "updated_at": _iso_utc(row.updated_at if row.updated_at is not None else utcnow()),
        },
    }


def _provider_snapshot_remaining_size(payload: dict[str, Any]) -> Optional[float]:
    provider_reconciliation, snapshot = _provider_reconciliation_parts(payload)
    remaining_size = safe_float(
        snapshot.get("remaining_size"),
        safe_float(provider_reconciliation.get("remaining_size"), None),
    )
    if remaining_size is not None:
        return max(0.0, float(remaining_size))
    requested_size = _first_float_from_candidates(
        [snapshot, provider_reconciliation, payload],
        ("size", "original_size", "requested_size", "shares", "requested_shares"),
    )
    filled_size = _first_float_from_candidates(
        [snapshot, provider_reconciliation, payload],
        ("filled_size", "size_matched", "matched_size", "filled_shares", "executed_size"),
    )
    if requested_size is None:
        return None
    return max(0.0, float(requested_size) - max(0.0, float(filled_size or 0.0)))


def _provider_entry_order_still_working(payload: dict[str, Any]) -> bool:
    snapshot_status = _provider_snapshot_status(payload)
    if snapshot_status not in {"open", "submitted", "pending"}:
        return False
    remaining_size = _provider_snapshot_remaining_size(payload)
    return remaining_size is None or remaining_size > _WALLET_SIZE_EPSILON


def _extract_live_token_id(payload: dict[str, Any]) -> str:
    token_id = str(payload.get("token_id") or payload.get("selected_token_id") or "").strip()
    if token_id:
        return token_id
    provider_reconciliation = payload.get("provider_reconciliation")
    if isinstance(provider_reconciliation, dict):
        snapshot = provider_reconciliation.get("snapshot")
        if isinstance(snapshot, dict):
            token_id = str(snapshot.get("asset_id") or snapshot.get("asset") or snapshot.get("token_id") or "").strip()
            if token_id:
                return token_id
    return ""


def _extract_condition_outcome_key(payload: dict[str, Any], direction: str) -> str:
    """Build a conditionId:outcomeIndex fallback key for wallet position lookup.

    Polymarket can change a market's token_ids (token versioning) while the
    stored order still references the original.  The conditionId + outcomeIndex
    pair is stable across versions, so it serves as a reliable fallback when
    exact token_id lookup fails.
    """
    # Try live_market.condition_id first (set during live execution)
    cid = ""
    live_market = payload.get("live_market")
    if isinstance(live_market, dict):
        cid = str(live_market.get("condition_id") or live_market.get("conditionId") or "").strip()
    # Fall back to provider reconciliation snapshot
    if not cid:
        pr = payload.get("provider_reconciliation")
        if isinstance(pr, dict):
            snapshot = pr.get("snapshot")
            if isinstance(snapshot, dict):
                raw = snapshot.get("raw")
                if isinstance(raw, dict):
                    cid = str(raw.get("market") or raw.get("condition_id") or raw.get("conditionId") or "").strip()
    if not cid:
        return ""
    # Determine outcome index from direction
    dir_lower = str(direction or "").strip().lower()
    if dir_lower in ("buy_no", "no", "sell_yes"):
        outcome_idx = 1
    elif dir_lower in ("buy_yes", "yes", "sell_no"):
        outcome_idx = 0
    else:
        # Try to get from leg metadata
        leg = payload.get("leg")
        if isinstance(leg, dict):
            outcome = str(leg.get("outcome") or "").strip().lower()
            if outcome in ("no", "down"):
                outcome_idx = 1
            elif outcome in ("yes", "up"):
                outcome_idx = 0
            else:
                return ""
        else:
            return ""
    return f"{cid}:{outcome_idx}"


def _extract_wallet_settlement_price(wallet_position: Optional[dict[str, Any]]) -> Optional[float]:
    if not isinstance(wallet_position, dict):
        return None
    if not _safe_bool(wallet_position.get("redeemable"), False):
        return None
    mark = safe_float(wallet_position.get("curPrice"))
    if mark is None:
        mark = safe_float(wallet_position.get("currentPrice"))
    if mark is None:
        return None
    if mark <= 0.001:
        return 0.0
    if mark >= 0.999:
        return 1.0
    return None


def _extract_wallet_position_size(wallet_position: Optional[dict[str, Any]]) -> float:
    if not isinstance(wallet_position, dict):
        return 0.0
    size = safe_float(wallet_position.get("size"))
    if size is None:
        size = safe_float(wallet_position.get("positionSize"))
    if size is None:
        size = safe_float(wallet_position.get("shares"))
    return max(0.0, float(size or 0.0))


def _extract_wallet_mark_price(wallet_position: Optional[dict[str, Any]]) -> Optional[float]:
    if not isinstance(wallet_position, dict):
        return None
    mark = safe_float(wallet_position.get("curPrice"))
    if mark is None:
        mark = safe_float(wallet_position.get("currentPrice"))
    if mark is None:
        mark = safe_float(wallet_position.get("markPrice"))
    if mark is None:
        mark = safe_float(wallet_position.get("price"))
    if mark is None or mark < 0:
        return None
    return float(mark)


def _extract_wallet_entry_price(wallet_position: Optional[dict[str, Any]]) -> Optional[float]:
    if not isinstance(wallet_position, dict):
        return None
    price = safe_float(wallet_position.get("avgPrice"))
    if price is None:
        price = safe_float(wallet_position.get("average_price"))
    if price is None:
        initial_value = safe_float(wallet_position.get("initialValue"))
        size = _extract_wallet_position_size(wallet_position)
        if initial_value is not None and initial_value > 0.0 and size > _WALLET_SIZE_EPSILON:
            price = initial_value / size
    if price is None or price <= 0.0:
        return None
    return float(price)


def _payload_execution_plan(payload: dict[str, Any]) -> dict[str, Any]:
    execution_plan = payload.get("execution_plan")
    if isinstance(execution_plan, dict):
        return execution_plan
    strategy_context = payload.get("strategy_context")
    if isinstance(strategy_context, dict):
        nested_plan = strategy_context.get("execution_plan")
        if isinstance(nested_plan, dict):
            return nested_plan
    return {}


def _required_bundle_token_ids(payload: dict[str, Any], signal_payload: dict[str, Any]) -> list[str]:
    token_ids: list[str] = []
    for source_payload in (payload, signal_payload):
        execution_plan = _payload_execution_plan(source_payload)
        raw_legs = execution_plan.get("legs")
        raw_legs = raw_legs if isinstance(raw_legs, list) else []
        for leg in raw_legs:
            if not isinstance(leg, dict):
                continue
            token_id = str(leg.get("token_id") or "").strip()
            if token_id and token_id not in token_ids:
                token_ids.append(token_id)
    for source_payload in (signal_payload, payload):
        raw_positions = source_payload.get("positions_to_take")
        raw_positions = raw_positions if isinstance(raw_positions, list) else []
        for position in raw_positions:
            if not isinstance(position, dict):
                continue
            token_id = str(position.get("token_id") or "").strip()
            if token_id and token_id not in token_ids:
                token_ids.append(token_id)
    return token_ids


def _full_bundle_execution_required(payload: dict[str, Any], signal_payload: dict[str, Any]) -> bool:
    for source_payload in (payload, signal_payload):
        execution_plan = _payload_execution_plan(source_payload)
        metadata = execution_plan.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        if _safe_bool(metadata.get("full_bundle_execution_required"), False):
            return True
    if not _safe_bool(signal_payload.get("is_guaranteed"), False):
        return False
    required_token_ids = _required_bundle_token_ids(payload, signal_payload)
    return len(required_token_ids) >= 2


def _bundle_position_shortfall(
    *,
    token_id: str | None,
    payload: dict[str, Any],
    signal_payload: dict[str, Any],
    wallet_positions_by_token: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    required_token_ids = _required_bundle_token_ids(payload, signal_payload)
    if not _full_bundle_execution_required(payload, signal_payload):
        return None
    if len(required_token_ids) < 2:
        return None
    normalized_token_id = str(token_id or "").strip()
    if normalized_token_id and normalized_token_id not in required_token_ids:
        return None
    observed_token_ids = [
        required_token_id
        for required_token_id in required_token_ids
        if _extract_wallet_position_size(wallet_positions_by_token.get(required_token_id)) > _WALLET_SIZE_EPSILON
    ]
    missing_token_ids = [required_token_id for required_token_id in required_token_ids if required_token_id not in observed_token_ids]
    if not observed_token_ids or not missing_token_ids:
        return None
    execution_plan = _payload_execution_plan(payload)
    if not execution_plan:
        execution_plan = _payload_execution_plan(signal_payload)
    metadata = execution_plan.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    return {
        "required_token_ids": required_token_ids,
        "observed_token_ids": observed_token_ids,
        "missing_token_ids": missing_token_ids,
        "full_bundle_execution_mode": str(metadata.get("full_bundle_execution_mode") or "").strip() or None,
        "plan_id": str(execution_plan.get("plan_id") or "").strip() or None,
    }


def _pending_exit_fill_threshold(pending_exit: dict[str, Any]) -> float:
    parsed = safe_float(
        pending_exit.get("fill_ratio_threshold")
        if pending_exit.get("fill_ratio_threshold") is not None
        else pending_exit.get("fill_threshold_ratio")
    )
    if parsed is not None:
        return max(0.5, min(1.0, float(parsed)))

    required_exit_size = max(0.0, safe_float(pending_exit.get("exit_size"), 0.0) or 0.0)
    retry_count = int(safe_float(pending_exit.get("retry_count"), 0) or 0)
    close_trigger = str(pending_exit.get("close_trigger") or "").strip().lower()

    if required_exit_size <= 10.0:
        threshold = 0.88
    elif required_exit_size <= 25.0:
        threshold = 0.90
    elif required_exit_size <= 75.0:
        threshold = 0.93
    elif required_exit_size <= 200.0:
        threshold = 0.95
    else:
        threshold = 0.97

    if _is_rapid_close_trigger(close_trigger):
        threshold = min(threshold, 0.92)
    if retry_count >= 2:
        threshold = max(0.85, threshold - 0.03)
    return max(0.5, min(1.0, float(threshold)))


def _effective_exit_min_order_size_usd(min_order_size_usd: Any, close_trigger: Any) -> float:
    return 0.01


def _remaining_exit_size(
    *,
    required_exit_size: float,
    pending_exit: dict[str, Any],
    wallet_position_size: float,
) -> float:
    required = max(0.0, float(required_exit_size))
    already_filled = max(0.0, safe_float(pending_exit.get("filled_size"), 0.0) or 0.0)
    remaining = max(0.0, required - already_filled)
    if required > 0.0 and remaining <= 0.0 and already_filled <= 0.0:
        remaining = required
    wallet_cap = wallet_position_size if wallet_position_size > _WALLET_SIZE_EPSILON else 0.0
    if wallet_cap > 0.0:
        if remaining <= 0.0:
            remaining = wallet_cap
        else:
            remaining = min(remaining, wallet_cap)
    return max(0.0, remaining)


def _pending_exit_provider_clob_id(pending_exit: dict[str, Any]) -> str:
    direct = str(pending_exit.get("provider_clob_order_id") or pending_exit.get("exit_order_clob_id") or "").strip()
    if direct:
        return direct
    fallback = str(pending_exit.get("exit_order_id") or "").strip()
    if not fallback:
        return ""
    if fallback.startswith("0x") or fallback.isdigit():
        return fallback
    try:
        cached = live_execution_service.get_order(fallback)
    except Exception:
        cached = None
    if cached is None:
        return ""
    return str(getattr(cached, "clob_order_id", "") or "").strip()


def _pending_exit_fill_evidence(pending_exit: dict[str, Any]) -> tuple[str, float, float, Optional[float]]:
    snapshot = pending_exit.get("snapshot")
    if not isinstance(snapshot, dict):
        snapshot = {}
    candidates: list[Any] = [snapshot, pending_exit]
    provider_status = str(
        snapshot.get("normalized_status")
        or snapshot.get("status")
        or pending_exit.get("provider_status")
        or ""
    ).strip().lower()
    filled_size = max(
        0.0,
        _first_float_from_candidates(
            candidates,
            ("filled_size", "size_matched", "matched_size", "filled_shares", "executed_size"),
        )
        or 0.0,
    )
    filled_notional = max(
        0.0,
        _first_float_from_candidates(
            candidates,
            ("filled_notional_usd", "filled_notional", "executed_notional_usd", "matched_notional_usd"),
        )
        or 0.0,
    )
    average_fill_price = _first_float_from_candidates(
        candidates,
        ("average_fill_price", "avg_fill_price", "executed_price", "matched_price", "price"),
    )
    if filled_notional <= 0.0 and filled_size > 0.0 and average_fill_price is not None and average_fill_price > 0.0:
        filled_notional = filled_size * average_fill_price
    return provider_status, filled_size, filled_notional, average_fill_price


def _pending_exit_has_verified_terminal_fill(pending_exit: dict[str, Any]) -> bool:
    provider_status, filled_size, filled_notional, average_fill_price = _pending_exit_fill_evidence(pending_exit)
    if provider_status not in {"filled", "matched", "executed"}:
        return False
    if filled_size > _WALLET_SIZE_EPSILON:
        return True
    if filled_notional > 0.0:
        return True
    return average_fill_price is not None and average_fill_price > 0.0


def _position_close_wallet_trade_id(position_close: dict[str, Any]) -> str:
    return str(position_close.get("wallet_trade_id") or "").strip()


def _extract_wallet_trade_token_id(trade: dict[str, Any]) -> str:
    return str(
        trade.get("asset_id") or trade.get("asset") or trade.get("token_id") or trade.get("tokenId") or ""
    ).strip()


def _extract_wallet_trade_side(trade: dict[str, Any]) -> str:
    return str(trade.get("side") or trade.get("trade_side") or trade.get("type") or "").strip().lower()


def _extract_wallet_trade_size(trade: dict[str, Any]) -> float:
    size = safe_float(trade.get("size"))
    if size is None:
        size = safe_float(trade.get("amount"))
    if size is None:
        size = safe_float(trade.get("shares"))
    if size is None:
        size = safe_float(trade.get("matched_size"))
    return max(0.0, float(size or 0.0))


def _extract_wallet_trade_price(trade: dict[str, Any]) -> Optional[float]:
    price = safe_float(trade.get("price"))
    if price is None:
        price = safe_float(trade.get("avg_price"))
    if price is None:
        price = safe_float(trade.get("limit_price"))
    if price is None or price < 0:
        return None
    return float(price)


def _parse_iso_utc_naive(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1]
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _parse_wallet_trade_time(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            ts = float(value)
            if ts > 1e12:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    return _parse_iso_utc_naive(text)


def _extract_wallet_activity_token_id(activity: dict[str, Any]) -> str:
    return str(
        activity.get("asset")
        or activity.get("asset_id")
        or activity.get("token_id")
        or activity.get("tokenId")
        or ""
    ).strip()


def _extract_wallet_activity_type(activity: dict[str, Any]) -> str:
    return str(activity.get("type") or activity.get("activityType") or "").strip().lower()


def _extract_wallet_activity_side(activity: dict[str, Any]) -> str:
    return str(activity.get("side") or activity.get("trade_side") or "").strip().lower()


def _extract_wallet_activity_size(activity: dict[str, Any]) -> float:
    size = safe_float(activity.get("size"))
    if size is None:
        size = safe_float(activity.get("amount"))
    if size is None:
        size = safe_float(activity.get("shares"))
    return max(0.0, float(size or 0.0))


def _extract_wallet_activity_price(activity: dict[str, Any]) -> Optional[float]:
    price = safe_float(activity.get("price"))
    if price is None:
        price = safe_float(activity.get("avgPrice"))
    if price is None:
        price = safe_float(activity.get("avg_price"))
    if price is None or price < 0.0:
        return None
    return float(price)


def _extract_wallet_activity_tx_hash(activity: dict[str, Any]) -> str:
    return str(activity.get("transactionHash") or activity.get("txHash") or activity.get("tx_hash") or "").strip()


def _parse_wallet_activity_time(activity: dict[str, Any]) -> Optional[datetime]:
    return _parse_wallet_trade_time(
        activity.get("timestamp")
        if activity.get("timestamp") is not None
        else activity.get("createdAt") if activity.get("createdAt") is not None
        else activity.get("created_at")
    )


def _wallet_activity_condition_outcome_key(activity: dict[str, Any]) -> str:
    condition_id = str(activity.get("conditionId") or activity.get("condition_id") or "").strip()
    if not condition_id:
        return ""
    outcome_idx = activity.get("outcomeIndex")
    if outcome_idx is None:
        outcome_idx = activity.get("outcome_index")
    if outcome_idx is None:
        return ""
    return f"{condition_id}:{outcome_idx}"


def _wallet_activity_is_close_authority(activity: dict[str, Any]) -> bool:
    activity_type = _extract_wallet_activity_type(activity)
    if activity_type == "trade":
        return _extract_wallet_activity_side(activity) == "sell" and _extract_wallet_activity_size(activity) > _WALLET_SIZE_EPSILON
    return activity_type in {"merge", "redeem", "claim", "convert", "conversion"}


def _apply_position_close_verification(
    session: AsyncSession,
    *,
    row: TraderOrder,
    now: datetime,
    event_type: str,
    payload_json: dict[str, Any],
) -> None:
    previous_status = str(row.verification_status or "")
    previous_source = str(row.verification_source or "")
    previous_reason = row.verification_reason
    previous_order_id = str(row.provider_order_id or "")
    previous_clob_order_id = str(row.provider_clob_order_id or "")
    previous_wallet = str(row.execution_wallet_address or "")
    previous_tx_hash = str(row.verification_tx_hash or "")

    derived = derive_trader_order_verification(
        mode=row.mode,
        status=row.status,
        reason=row.reason,
        payload=payload_json,
    )
    changed = apply_trader_order_verification(
        row,
        verification_status=str(derived.get("verification_status") or TRADER_ORDER_VERIFICATION_LOCAL),
        verification_source=str(derived.get("verification_source") or "").strip() or None,
        verification_reason=str(derived.get("verification_reason") or "").strip() or None,
        provider_order_id=str(derived.get("provider_order_id") or "").strip() or None,
        provider_clob_order_id=str(derived.get("provider_clob_order_id") or "").strip() or None,
        execution_wallet_address=str(derived.get("execution_wallet_address") or "").strip() or None,
        verification_tx_hash=str(derived.get("verification_tx_hash") or "").strip() or None,
        verified_at=now,
        force=True,
    )
    if not changed:
        changed = (
            previous_status != str(row.verification_status or "")
            or previous_source != str(row.verification_source or "")
            or previous_reason != row.verification_reason
            or previous_order_id != str(row.provider_order_id or "")
            or previous_clob_order_id != str(row.provider_clob_order_id or "")
            or previous_wallet != str(row.execution_wallet_address or "")
            or previous_tx_hash != str(row.verification_tx_hash or "")
        )
    if changed:
        append_trader_order_verification_event(
            session,
            trader_order_id=str(row.id),
            verification_status=str(row.verification_status or TRADER_ORDER_VERIFICATION_LOCAL),
            source=row.verification_source,
            event_type=event_type,
            reason=row.verification_reason,
            provider_order_id=row.provider_order_id,
            provider_clob_order_id=row.provider_clob_order_id,
            execution_wallet_address=row.execution_wallet_address,
            tx_hash=row.verification_tx_hash,
            payload_json={},
            created_at=now,
        )


def _active_row_has_unfilled_terminal_provider_failure(row: TraderOrder, payload: dict[str, Any]) -> bool:
    filled_notional, filled_size, _average_fill_price = _extract_live_fill_metrics(payload)
    if filled_notional > 0.0 or filled_size > _WALLET_SIZE_EPSILON:
        return False

    provider_status = _provider_snapshot_status(payload)
    if provider_status not in {"failed", "cancelled", "expired", "rejected", "error"}:
        return False

    verification_status = str(getattr(row, "verification_status", None) or "").strip().lower()
    if verification_status in {"venue_fill", "wallet_position"}:
        return False

    live_wallet_authority = payload.get("live_wallet_authority")
    if isinstance(live_wallet_authority, dict):
        authority_source = str(live_wallet_authority.get("source") or "").strip().lower()
        if authority_source == "wallet_positions_api":
            return False

    return True


def _provider_failure_terminal_status(row: TraderOrder, payload: dict[str, Any]) -> str:
    provider_status = _provider_snapshot_status(payload)
    error_text = str(getattr(row, "error_message", None) or payload.get("error_message") or "").strip().lower()
    if "no orders found to match with fak order" in error_text:
        return "rejected"
    if provider_status in {"cancelled", "expired"}:
        return "cancelled"
    return "failed"


def _failed_terminal_row_can_reopen_from_wallet_position(row: TraderOrder, payload: dict[str, Any]) -> bool:
    if _active_row_has_unfilled_terminal_provider_failure(row, payload):
        return False

    filled_notional, filled_size, _average_fill_price = _extract_live_fill_metrics(payload)
    if filled_notional > 0.0 or filled_size > _WALLET_SIZE_EPSILON:
        return True

    verification_status = str(getattr(row, "verification_status", None) or "").strip().lower()
    if verification_status in {"venue_fill", "wallet_position"}:
        return True

    live_wallet_authority = payload.get("live_wallet_authority")
    if not isinstance(live_wallet_authority, dict):
        return False

    authority_source = str(live_wallet_authority.get("source") or "").strip().lower()
    if authority_source == "wallet_positions_api":
        return True

    provider_status = _provider_snapshot_status(payload)
    return provider_status in {"filled", "partially_filled", "open", "submitted", "pending"}


def _parse_market_end_time_naive(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if isinstance(value, (int, float)):
        try:
            ts = float(value)
            if ts > 1e12:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None
    return _parse_iso_utc_naive(value)


def _extract_market_end_time_naive(market_info: Any) -> Optional[datetime]:
    if not isinstance(market_info, dict):
        return None
    for key in (
        "end_time",
        "endTime",
        "end_date",
        "endDate",
        "market_end_time",
        "expiration_time",
    ):
        parsed = _parse_market_end_time_naive(market_info.get(key))
        if parsed is not None:
            return parsed
    return None


def _market_seconds_left(market_info: Any, now: datetime) -> Optional[float]:
    if not isinstance(market_info, dict):
        return None
    for key in ("seconds_left", "secondsLeft", "seconds_until_close", "secondsUntilClose"):
        parsed = safe_float(market_info.get(key), None)
        if parsed is not None and parsed >= 0.0:
            return float(parsed)
    end_time = _extract_market_end_time_naive(market_info)
    if end_time is None:
        return None
    now_naive = now.astimezone(timezone.utc).replace(tzinfo=None) if now.tzinfo is not None else now
    return max(0.0, (end_time - now_naive).total_seconds())


def _market_end_time_iso(market_info: Any) -> Optional[str]:
    end_time = _extract_market_end_time_naive(market_info)
    if end_time is None:
        return None
    return _iso_utc(end_time)


async def _resolve_execution_wallet_address() -> str:
    wallet = ""
    try:
        await live_execution_service.ensure_initialized()
    except Exception:
        pass
    try:
        wallet = str(live_execution_service.get_execution_wallet_address() or "").strip()
    except Exception:
        wallet = ""
    if wallet:
        return wallet
    try:
        runtime_signature_type = getattr(live_execution_service, "_balance_signature_type", None)
        signature_type = (
            int(runtime_signature_type)
            if isinstance(runtime_signature_type, int)
            else int(getattr(settings, "POLYMARKET_SIGNATURE_TYPE", 1))
        )
    except Exception:
        signature_type = 1
    try:
        wallet = str(live_execution_service._funder_for_signature_type(signature_type) or "").strip()
    except Exception:
        wallet = ""
    if not wallet:
        wallet = str(live_execution_service._get_wallet_address() or "").strip()
    # DB fallback: look up wallet from live_trading_positions if service not initialized
    if not wallet:
        try:
            from models.database import AsyncSessionLocal, LiveTradingPosition
            from sqlalchemy import select

            async with AsyncSessionLocal() as _session:
                row = (
                    await _session.execute(
                        select(LiveTradingPosition.wallet_address)
                        .where(LiveTradingPosition.wallet_address.isnot(None))
                        .limit(1)
                    )
                ).scalar()
                if row:
                    wallet = str(row).strip()
                    logger.info("wallet address resolved from DB fallback: %s", wallet[:12] + "...")
        except Exception:
            pass
    return wallet


async def _load_execution_wallet_positions_by_token() -> dict[str, dict[str, Any]]:
    global _wallet_positions_cache, _wallet_positions_last_refresh_succeeded
    cached_at, cached_data = _wallet_positions_cache
    if _wallet_positions_last_refresh_succeeded and (_time_mod.monotonic() - cached_at) < _WALLET_CACHE_TTL_SECONDS:
        return cached_data

    wallet = await _resolve_execution_wallet_address()
    if not wallet:
        _wallet_positions_last_refresh_succeeded = False
        return {}

    try:
        positions = await polymarket_client.get_wallet_positions(wallet)
    except Exception:
        _wallet_positions_last_refresh_succeeded = False
        return {}

    by_token: dict[str, dict[str, Any]] = {}
    for position in positions:
        if not isinstance(position, dict):
            continue
        token_id = str(position.get("asset") or position.get("asset_id") or position.get("token_id") or "").strip()
        if not token_id:
            continue
        by_token[token_id] = position
        # Also index by conditionId:outcomeIndex so lookups survive token
        # versioning — Polymarket can change a market's token_ids while the
        # order payload still references the old one.
        cid = str(position.get("conditionId") or position.get("condition_id") or "").strip()
        outcome_idx = position.get("outcomeIndex")
        if cid and outcome_idx is not None:
            by_token[f"{cid}:{outcome_idx}"] = position
    _wallet_positions_cache = (_time_mod.monotonic(), by_token)
    _wallet_positions_last_refresh_succeeded = True
    return by_token


async def _load_execution_wallet_closed_positions_by_token() -> dict[str, dict[str, Any]]:
    global _wallet_closed_positions_cache, _wallet_closed_positions_last_refresh_succeeded
    cached_at, cached_data = _wallet_closed_positions_cache
    if _wallet_closed_positions_last_refresh_succeeded and (_time_mod.monotonic() - cached_at) < _WALLET_HISTORY_CACHE_TTL_SECONDS:
        return cached_data

    wallet = await _resolve_execution_wallet_address()
    if not wallet:
        _wallet_closed_positions_last_refresh_succeeded = False
        return {}

    try:
        positions = await polymarket_client.get_closed_positions_paginated(wallet, max_positions=1000)
    except Exception:
        _wallet_closed_positions_last_refresh_succeeded = False
        return {}

    by_token: dict[str, dict[str, Any]] = {}
    for position in positions:
        if not isinstance(position, dict):
            continue
        token_id = str(position.get("asset") or position.get("asset_id") or position.get("token_id") or "").strip()
        if token_id:
            by_token[token_id] = position
        cid = str(position.get("conditionId") or position.get("condition_id") or "").strip()
        outcome_idx = position.get("outcomeIndex")
        if cid and outcome_idx is not None:
            by_token[f"{cid}:{outcome_idx}"] = position
    _wallet_closed_positions_cache = (_time_mod.monotonic(), by_token)
    _wallet_closed_positions_last_refresh_succeeded = True
    return by_token


async def _load_execution_wallet_recent_sell_trades_by_token() -> dict[str, dict[str, Any]]:
    global _wallet_sell_trades_cache
    cached_at, cached_data = _wallet_sell_trades_cache
    if cached_data and (_time_mod.monotonic() - cached_at) < _WALLET_HISTORY_CACHE_TTL_SECONDS:
        return cached_data

    wallet = await _resolve_execution_wallet_address()
    if not wallet:
        return {}
    try:
        trades = await polymarket_client.get_wallet_trades_paginated(wallet, max_trades=1500, page_size=500)
    except Exception:
        return {}
    if not isinstance(trades, list):
        return {}

    latest_by_token: dict[str, dict[str, Any]] = {}
    market_cache_by_condition: dict[str, Optional[dict[str, Any]]] = {}

    # Pre-fetch all unique condition_ids in parallel to avoid sequential API calls.
    condition_ids_to_fetch: set[str] = set()
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        if not _extract_wallet_trade_token_id(trade):
            cid = str(trade.get("conditionId") or trade.get("condition_id") or trade.get("market") or "").strip()
            if cid:
                condition_ids_to_fetch.add(cid)

    if condition_ids_to_fetch:
        _BATCH = 10

        async def _fetch_condition(cid: str) -> tuple[str, Optional[dict[str, Any]]]:
            try:
                return cid, await polymarket_client.get_market_by_condition_id(cid)
            except Exception:
                return cid, None

        cid_list = sorted(condition_ids_to_fetch)
        for i in range(0, len(cid_list), _BATCH):
            batch = cid_list[i : i + _BATCH]
            results = await asyncio.gather(*[_fetch_condition(c) for c in batch])
            for cid, info in results:
                market_cache_by_condition[cid] = info

    def _infer_trade_token_id(trade: dict[str, Any]) -> str:
        condition_id = str(trade.get("conditionId") or trade.get("condition_id") or trade.get("market") or "").strip()
        if not condition_id:
            return ""

        market_info = market_cache_by_condition.get(condition_id)
        if not isinstance(market_info, dict):
            return ""

        token_ids_raw = market_info.get("token_ids")
        if not isinstance(token_ids_raw, list):
            token_ids_raw = market_info.get("tokenIds")
        if not isinstance(token_ids_raw, list):
            return ""
        token_ids = [str(token_id or "").strip() for token_id in token_ids_raw]
        if not token_ids:
            return ""

        outcomes_raw = market_info.get("outcomes")
        outcomes: list[str] = []
        if isinstance(outcomes_raw, list):
            outcomes = [str(outcome or "").strip().lower() for outcome in outcomes_raw]

        outcome_idx = safe_float(
            trade.get("outcomeIndex") if trade.get("outcomeIndex") is not None else trade.get("outcome_index")
        )
        if outcome_idx is not None:
            idx = int(outcome_idx)
            if 0 <= idx < len(token_ids):
                return token_ids[idx]

        outcome_text = str(trade.get("outcome") or trade.get("token_outcome") or "").strip().lower()
        if outcome_text and outcomes:
            for idx, outcome_label in enumerate(outcomes):
                if outcome_label == outcome_text and idx < len(token_ids):
                    return token_ids[idx]

        return ""

    for trade in trades:
        if not isinstance(trade, dict):
            continue
        token_id = _extract_wallet_trade_token_id(trade)
        if not token_id:
            token_id = _infer_trade_token_id(trade)
        if not token_id:
            continue
        if _extract_wallet_trade_side(trade) != "sell":
            continue
        size = _extract_wallet_trade_size(trade)
        if size <= 0.0:
            continue
        price = _extract_wallet_trade_price(trade)
        timestamp = _parse_wallet_trade_time(
            trade.get("timestamp") or trade.get("created_at") or trade.get("createdAt") or trade.get("time")
        )
        record = {
            "trade_id": str(trade.get("id") or trade.get("order_id") or trade.get("orderId") or "").strip(),
            "token_id": token_id,
            "size": size,
            "price": price,
            "timestamp": timestamp,
        }
        existing = latest_by_token.get(token_id)
        if existing is None:
            latest_by_token[token_id] = record
            continue
        existing_ts = existing.get("timestamp")
        if timestamp is not None and (
            existing_ts is None or (isinstance(existing_ts, datetime) and timestamp > existing_ts)
        ):
            latest_by_token[token_id] = record
    _wallet_sell_trades_cache = (_time_mod.monotonic(), latest_by_token)
    return latest_by_token


async def _load_execution_wallet_recent_close_activity_by_token() -> dict[str, dict[str, Any]]:
    global _wallet_activity_cache, _wallet_activity_last_refresh_succeeded
    cached_at, cached_data = _wallet_activity_cache
    if _wallet_activity_last_refresh_succeeded and (_time_mod.monotonic() - cached_at) < _WALLET_HISTORY_CACHE_TTL_SECONDS:
        return cached_data

    wallet = await _resolve_execution_wallet_address()
    if not wallet:
        _wallet_activity_last_refresh_succeeded = False
        return {}

    try:
        activities = await polymarket_client.get_wallet_activity_paginated(
            wallet,
            max_items=1500,
            page_size=250,
            activity_types=("TRADE", "MERGE", "REDEEM", "CLAIM", "CONVERSION", "CONVERT"),
        )
    except Exception:
        _wallet_activity_last_refresh_succeeded = False
        return {}
    if not isinstance(activities, list):
        _wallet_activity_last_refresh_succeeded = False
        return {}

    latest_by_token: dict[str, dict[str, Any]] = {}

    def _remember(key: str, activity: dict[str, Any]) -> None:
        if not key:
            return
        timestamp = _parse_wallet_activity_time(activity)
        current = latest_by_token.get(key)
        current_timestamp = _parse_wallet_activity_time(current) if isinstance(current, dict) else None
        if current_timestamp is None or (timestamp is not None and timestamp >= current_timestamp):
            latest_by_token[key] = activity

    for activity in activities:
        if not isinstance(activity, dict) or not _wallet_activity_is_close_authority(activity):
            continue
        token_id = _extract_wallet_activity_token_id(activity)
        if token_id:
            _remember(token_id, activity)
        cond_key = _wallet_activity_condition_outcome_key(activity)
        if cond_key:
            _remember(cond_key, activity)

    _wallet_activity_cache = (_time_mod.monotonic(), latest_by_token)
    _wallet_activity_last_refresh_succeeded = True
    return latest_by_token


async def _load_mapping_with_timeout(
    loader,
    *,
    timeout: float,
    fallback: dict[str, Any],
) -> dict[str, Any]:
    try:
        return await asyncio.wait_for(loader(), timeout=timeout)
    except asyncio.CancelledError:
        raise
    except Exception:
        return dict(fallback or {})


def _market_lookup_candidates_for_order(order: Any) -> tuple[str, list[str], dict[str, Any]]:
    def _append_lookup_candidate(target: list[str], value: Any) -> None:
        candidate = str(value or "").strip()
        if candidate and candidate not in target:
            target.append(candidate)

    market_id = str(getattr(order, "market_id", "") or "").strip()
    if not market_id:
        return "", [], {}
    payload = dict(getattr(order, "payload_json", {}) or {})
    live_market = payload.get("live_market") if isinstance(payload.get("live_market"), dict) else {}
    candidates: list[str] = []
    _append_lookup_candidate(candidates, market_id)
    _append_lookup_candidate(candidates, payload.get("condition_id"))
    _append_lookup_candidate(candidates, payload.get("token_id"))
    _append_lookup_candidate(candidates, payload.get("selected_token_id"))
    _append_lookup_candidate(candidates, payload.get("yes_token_id"))
    _append_lookup_candidate(candidates, payload.get("no_token_id"))
    if live_market:
        _append_lookup_candidate(candidates, live_market.get("market_id"))
        _append_lookup_candidate(candidates, live_market.get("condition_id"))
        _append_lookup_candidate(candidates, live_market.get("selected_token_id"))
        _append_lookup_candidate(candidates, live_market.get("yes_token_id"))
        _append_lookup_candidate(candidates, live_market.get("no_token_id"))
        token_ids = live_market.get("token_ids")
        if isinstance(token_ids, list):
            for token_id in token_ids:
                _append_lookup_candidate(candidates, token_id)
    return market_id, candidates, dict(live_market or {})


def _fallback_market_info_for_orders(orders: list[TraderOrder]) -> dict[str, dict[str, Any]]:
    fallback: dict[str, dict[str, Any]] = {}
    for order in orders:
        market_id, candidates, live_market = _market_lookup_candidates_for_order(order)
        if not market_id:
            continue
        if live_market:
            fallback[market_id] = live_market
            continue
        for candidate in candidates:
            cached_entry = _market_info_cache.get(candidate)
            if cached_entry is None:
                continue
            cached_info = cached_entry[1]
            if isinstance(cached_info, dict):
                fallback[market_id] = dict(cached_info)
                break
    return fallback


def _cached_order_snapshots_by_clob_ids(clob_order_ids: list[str]) -> dict[str, dict[str, Any]]:
    requested = {str(order_id or "").strip() for order_id in clob_order_ids if str(order_id or "").strip()}
    if not requested:
        return {}
    snapshots: dict[str, dict[str, Any]] = {}
    for order in live_execution_service._orders.values():
        snapshot = live_execution_service._snapshot_from_cached_order(order)
        if snapshot is None:
            continue
        clob_order_id = str(snapshot.get("clob_order_id") or "").strip()
        if clob_order_id in requested:
            snapshots[clob_order_id] = snapshot
    return snapshots


# Module-level TTL cache for market metadata to avoid redundant REST API calls.
# Each entry maps lookup_id -> (fetched_at_monotonic, market_info_dict).
_market_info_cache: dict[str, tuple[float, Optional[dict[str, Any]]]] = {}
_MARKET_INFO_CACHE_TTL_SECONDS = 60.0
_MARKET_INFO_NEAR_RESOLUTION_SECONDS = 300.0  # 5 minutes
_MARKET_INFO_CACHE_MAX_SIZE = 2000
_market_info_cache_last_eviction: float = 0.0


def _evict_stale_market_info_cache(now_mono: float) -> None:
    """Remove expired entries and enforce max size to prevent unbounded growth."""
    global _market_info_cache_last_eviction
    if (now_mono - _market_info_cache_last_eviction) < 30.0:
        return
    _market_info_cache_last_eviction = now_mono
    expired_keys = [
        k for k, (fetched_at, _) in _market_info_cache.items()
        if (now_mono - fetched_at) > _MARKET_INFO_CACHE_TTL_SECONDS * 5
    ]
    for k in expired_keys:
        _market_info_cache.pop(k, None)
    if len(_market_info_cache) > _MARKET_INFO_CACHE_MAX_SIZE:
        sorted_entries = sorted(_market_info_cache.items(), key=lambda x: x[1][0])
        for k, _ in sorted_entries[: len(_market_info_cache) - _MARKET_INFO_CACHE_MAX_SIZE]:
            _market_info_cache.pop(k, None)


def _market_info_needs_refresh(lookup_id: str, now_mono: float) -> bool:
    _evict_stale_market_info_cache(now_mono)
    entry = _market_info_cache.get(lookup_id)
    if entry is None:
        return True
    fetched_at, info = entry
    age = now_mono - fetched_at
    if age >= _MARKET_INFO_CACHE_TTL_SECONDS:
        return True
    if isinstance(info, dict):
        if info.get("winning_outcome") is not None:
            return True
        end_time = _extract_market_end_time_naive(info)
        if end_time is not None:
            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
            seconds_left = (end_time - now_utc).total_seconds()
            if seconds_left <= _MARKET_INFO_NEAR_RESOLUTION_SECONDS:
                return True
    return False


async def load_market_info_for_orders(orders: list[TraderOrder]) -> dict[str, Optional[dict[str, Any]]]:
    lookup_candidates_by_market_id: dict[str, list[str]] = {}
    fallback_info_by_market_id = _fallback_market_info_for_orders(orders)

    for order in orders:
        market_id, candidates, _ = _market_lookup_candidates_for_order(order)
        if not market_id:
            continue
        lookup_candidates_by_market_id[market_id] = candidates

    lookup_ids = sorted({candidate for candidates in lookup_candidates_by_market_id.values() for candidate in candidates})
    if not lookup_ids:
        return fallback_info_by_market_id

    now_mono = _time_mod.monotonic()

    async def _fetch(lookup_id: str) -> tuple[str, Optional[dict[str, Any]]]:
        needs_refresh = _market_info_needs_refresh(lookup_id, now_mono)
        if not needs_refresh:
            _, cached_info = _market_info_cache[lookup_id]
            return lookup_id, cached_info

        info: Optional[dict[str, Any]] = None
        if lookup_id.startswith("0x"):
            info = await polymarket_client.get_market_by_condition_id(lookup_id, force_refresh=True)
            if info is None:
                info = await polymarket_client.get_market_by_condition_id(lookup_id)
        if info is None:
            info = await polymarket_client.get_market_by_token_id(lookup_id)

        _market_info_cache[lookup_id] = (now_mono, info)
        return lookup_id, info

    # Limit concurrency to avoid overwhelming the Gamma API rate limiter.
    _fetch_sem = asyncio.Semaphore(8)

    async def _fetch_limited(lookup_id: str) -> tuple[str, Optional[dict[str, Any]]]:
        async with _fetch_sem:
            return await _fetch(lookup_id)

    pairs = await asyncio.gather(*[_fetch_limited(lookup_id) for lookup_id in lookup_ids], return_exceptions=True)
    info_by_lookup_id: dict[str, Optional[dict[str, Any]]] = {}
    for item in pairs:
        if isinstance(item, Exception):
            continue
        lookup_id, info = item
        info_by_lookup_id[lookup_id] = info

    out: dict[str, Optional[dict[str, Any]]] = {}
    for market_id, candidates in lookup_candidates_by_market_id.items():
        info = fallback_info_by_market_id.get(market_id)
        for candidate in candidates:
            candidate_info = info_by_lookup_id.get(candidate)
            if candidate_info is not None:
                info = candidate_info
                break
        out[market_id] = info
    return out


async def reconcile_paper_positions(
    session: AsyncSession,
    *,
    trader_id: str,
    trader_params: Optional[dict[str, Any]] = None,
    dry_run: bool = False,
    force_mark_to_market: bool = False,
    max_age_hours: Optional[int] = None,
    order_ids: Optional[list[str]] = None,
    reason: str = "paper_position_lifecycle",
    position_mode: str = "paper",
    param_prefix: str = "paper",
    enable_simulation_ledger: bool = True,
) -> dict[str, Any]:
    params = dict(trader_params or {})
    mode_key = str(position_mode or "paper").strip().lower()
    if mode_key not in {"paper", "shadow"}:
        raise ValueError("position_mode must be 'paper' or 'shadow'")
    prefix_key = str(param_prefix or mode_key).strip().lower()

    def _trader_param(name: str, default: Any = None) -> Any:
        prefixed_key = f"{prefix_key}_{name}"
        if prefixed_key in params:
            return params.get(prefixed_key)
        if name in params:
            return params.get(name)
        if prefix_key != "paper":
            paper_key = f"paper_{name}"
            if paper_key in params:
                return params.get(paper_key)
        return default

    resolution_infer_from_prices = _safe_bool(_trader_param("resolution_infer_from_prices"), True)
    resolution_settle_floor = min(
        1.0,
        max(0.5, safe_float(_trader_param("resolution_settle_floor")) or 0.98),
    )

    candidates = list(
        (
            await session.execute(
                select(TraderOrder).where(
                    TraderOrder.trader_id == trader_id,
                    func.lower(func.coalesce(TraderOrder.mode, "")) == mode_key,
                    func.lower(func.coalesce(TraderOrder.status, "")).in_(tuple(PAPER_ACTIVE_STATUSES)),
                )
            )
        )
        .scalars()
        .all()
    )

    if order_ids:
        requested_order_ids = {str(value or "").strip() for value in order_ids if str(value or "").strip()}
        if requested_order_ids:
            candidates = [row for row in candidates if str(row.id or "") in requested_order_ids]
        else:
            candidates = []

    if max_age_hours is not None:
        cutoff = utcnow() - timedelta(hours=max(1, int(max_age_hours)))
        candidates = [
            row
            for row in candidates
            if (row.executed_at or row.updated_at or row.created_at) is not None
            and (row.executed_at or row.updated_at or row.created_at) <= cutoff
        ]
    candidates = _dedupe_live_authority_rows(candidates)

    signal_ids = [str(row.signal_id) for row in candidates if row.signal_id]
    signal_payloads: dict[str, dict[str, Any]] = {}
    if signal_ids:
        signal_rows = (
            await session.execute(
                select(TradeSignal.id, TradeSignal.payload_json).where(TradeSignal.id.in_(signal_ids))
            )
        ).all()
        signal_payloads = {str(row.id): dict(row.payload_json or {}) for row in signal_rows}

    from models.database import release_conn

    async with release_conn(session):
        market_info_by_id = await load_market_info_for_orders(candidates)

    now = utcnow()
    would_close = 0
    closed = 0
    held = 0
    skipped = 0
    total_realized_pnl = 0.0
    by_status = {"resolved_win": 0, "resolved_loss": 0, "closed_win": 0, "closed_loss": 0}
    skipped_reasons: dict[str, int] = {}
    details: list[dict[str, Any]] = []
    state_updates = 0
    reverse_signal_ids_by_source: dict[str, list[str]] = {}

    for row in candidates:
        entry_price = safe_float(row.effective_price)
        if entry_price is None or entry_price <= 0:
            entry_price = safe_float(row.entry_price)
        notional = safe_float(row.notional_usd) or 0.0
        outcome_idx = _direction_outcome_index(row.direction)
        if outcome_idx is None or entry_price is None or entry_price <= 0 or notional <= 0:
            skipped += 1
            skipped_reasons["invalid_entry"] = int(skipped_reasons.get("invalid_entry", 0)) + 1
            continue

        signal_payload = signal_payloads.get(str(row.signal_id), {})
        market_info = market_info_by_id.get(str(row.market_id or ""))
        market_tradable = polymarket_client.is_market_tradable(market_info, now=now)
        market_seconds_left = _market_seconds_left(market_info, now)
        market_end_time = _market_end_time_iso(market_info)
        winning_idx = _extract_winning_outcome_index(market_info)
        winning_idx_inferred = False
        if winning_idx is None and resolution_infer_from_prices:
            inferred_idx = _extract_winning_outcome_index_from_prices(
                market_info,
                market_tradable=market_tradable,
                settle_floor=resolution_settle_floor,
            )
            if inferred_idx is not None:
                winning_idx = inferred_idx
                winning_idx_inferred = True
        market_side_price = _extract_market_side_price(market_info, outcome_idx)
        snapshot_side_price = _extract_signal_side_price(signal_payload, outcome_idx)

        close_price: Optional[float] = None
        close_trigger: Optional[str] = None
        price_source: Optional[str] = None
        trailing_trigger_price: Optional[float] = None

        current_price = market_side_price if market_side_price is not None else snapshot_side_price
        current_price = _state_price_floor(current_price)
        current_price_source = (
            "market_mark"
            if market_side_price is not None
            else ("signal_snapshot_mark" if snapshot_side_price is not None else None)
        )

        age_anchor = row.executed_at or row.updated_at or row.created_at
        age_minutes = None
        if age_anchor is not None:
            age_minutes = max(0.0, (now - age_anchor).total_seconds() / 60.0)

        payload = dict(row.payload_json or {})
        take_profit_pct = safe_float(_payload_exit_param(payload, prefix_key=prefix_key, name="take_profit_pct"))
        stop_loss_pct = safe_float(_payload_exit_param(payload, prefix_key=prefix_key, name="stop_loss_pct"))
        max_hold_minutes = safe_float(_payload_exit_param(payload, prefix_key=prefix_key, name="max_hold_minutes"))
        min_hold_minutes = max(
            0.0,
            safe_float(_payload_exit_param(payload, prefix_key=prefix_key, name="min_hold_minutes")) or 0.0,
        )
        trailing_stop_pct = safe_float(_payload_exit_param(payload, prefix_key=prefix_key, name="trailing_stop_pct"))
        resolve_only = _safe_bool(_payload_exit_param(payload, prefix_key=prefix_key, name="resolve_only"), False)
        close_on_inactive_market = _safe_bool(
            _payload_exit_param(payload, prefix_key=prefix_key, name="close_on_inactive_market"),
            False,
        )
        min_hold_passed = age_minutes is None or age_minutes >= min_hold_minutes
        _exit_instance = None
        position_state = _extract_position_state(payload)
        prev_high = safe_float(position_state.get("highest_price"))
        prev_low = safe_float(position_state.get("lowest_price"))
        prev_last_mark = safe_float(position_state.get("last_mark_price"))
        prev_mark_source = str(position_state.get("last_mark_source") or "")
        prev_marked_at = _parse_iso_utc_naive(position_state.get("last_marked_at"))
        highest_price = prev_high
        lowest_price = prev_low
        if current_price is not None:
            if highest_price is None:
                highest_price = current_price
            else:
                highest_price = max(highest_price, current_price)
            if lowest_price is None:
                lowest_price = current_price
            else:
                lowest_price = min(lowest_price, current_price)

        mark_updated_at_value = (
            _iso_utc(now)
            if current_price is not None
            else (
                _iso_utc(prev_marked_at.replace(tzinfo=timezone.utc))
                if prev_marked_at is not None
                else ""
            )
        )
        next_state = {
            "highest_price": _state_price_floor(highest_price),
            "lowest_price": _state_price_floor(lowest_price),
            "last_mark_price": current_price if current_price is not None else _state_price_floor(prev_last_mark),
            "last_mark_source": str(current_price_source or prev_mark_source),
            "last_marked_at": mark_updated_at_value,
            "last_exit_evaluated_at": _iso_utc(now),
        }

        if winning_idx is not None:
            close_price = 1.0 if winning_idx == outcome_idx else 0.0
            close_trigger = "resolution_inferred" if winning_idx_inferred else "resolution"
            price_source = "resolved_settlement"
        else:
            pnl_pct = None
            if current_price is not None and entry_price > 0:
                pnl_pct = ((current_price - entry_price) / entry_price) * 100.0

            if force_mark_to_market and current_price is not None:
                close_price = current_price
                close_trigger = "manual_mark_to_market"
                price_source = current_price_source
            else:
                # ── Strategy-based exit check ──────────────────────────
                # If the strategy that opened this position has a
                # should_exit() method, call it first and respect its
                # decision before falling through to default TP/SL/etc.
                strategy_slug = (payload.get("strategy_type") or "").strip().lower()
                strategy_exit = None
                _exit_instance = await _strategy_exit_instance(session, strategy_slug) if strategy_slug else None
                if _exit_instance is not None:
                    try:

                        class _PaperPositionView:
                            pass

                        pos_view = _PaperPositionView()
                        pos_view.entry_price = entry_price
                        pos_view.current_price = current_price
                        pos_view.highest_price = highest_price
                        pos_view.lowest_price = lowest_price
                        pos_view.age_minutes = age_minutes
                        pos_view.pnl_percent = pnl_pct
                        pos_view.filled_size = (notional / entry_price) if entry_price > 0 else 0.0
                        pos_view.notional_usd = notional
                        if "strategy_context" not in payload:
                            payload["strategy_context"] = {}
                        strategy_context_payload = (
                            payload["strategy_context"] if isinstance(payload.get("strategy_context"), dict) else {}
                        )
                        pos_view.strategy_context = payload["strategy_context"]
                        pos_view.config = payload.get("strategy_exit_config", {})
                        pos_view.outcome_idx = outcome_idx

                        token_id = _extract_live_token_id(payload)
                        min_order_size_usd = _resolve_position_min_order_size_usd(
                            trader_params=params,
                            payload=payload,
                            mode=mode_key,
                        )
                        market_state_dict = {
                            "current_price": current_price,
                            "market_tradable": market_tradable,
                            "is_resolved": False,
                            "winning_outcome": None,
                            "seconds_left": market_seconds_left,
                            "end_time": market_end_time,
                            "token_id": token_id,
                            "mark_source": current_price_source,
                            "min_order_size_usd": min_order_size_usd,
                            "notional_usd": notional,
                            "oracle_price": strategy_context_payload.get("oracle_price"),
                            "price_to_beat": strategy_context_payload.get("price_to_beat"),
                            "oracle_age_seconds": strategy_context_payload.get("oracle_age_seconds"),
                            "confirmed_stage_triggered": strategy_context_payload.get("confirmed_stage_triggered"),
                            "strategy_stage": strategy_context_payload.get("stage"),
                        }

                        exit_decision = _exit_instance.should_exit(pos_view, market_state_dict)
                        exit_action = getattr(exit_decision, "action", None) if exit_decision is not None else None
                        if exit_action == "close":
                            strategy_exit = exit_decision
                        elif exit_action == "reduce":
                            strategy_exit = exit_decision
                        elif _strategy_hold_blocks_default_exit(exit_decision):
                            strategy_exit = exit_decision
                    except Exception as exc:
                        logger.warning(
                            "Strategy should_exit() error for %s: %s",
                            strategy_slug,
                            exc,
                        )

                if strategy_exit is not None and getattr(strategy_exit, "action", None) == "reduce":
                    if not dry_run:
                        payload["position_state"] = next_state
                        row.payload_json = payload
                        row.updated_at = now
                        state_updates += 1
                    held += 1
                    continue

                if _strategy_hold_blocks_default_exit(strategy_exit):
                    if not dry_run:
                        payload["position_state"] = next_state
                        row.payload_json = payload
                        row.updated_at = now
                        state_updates += 1
                    held += 1
                    continue

                if strategy_exit is not None:
                    _arm_reverse_entry_from_exit(
                        row=row,
                        payload=payload,
                        strategy_exit=strategy_exit,
                        market_info=market_info,
                        market_seconds_left=market_seconds_left,
                        current_price=current_price,
                        now=now,
                    )
                    close_price = strategy_exit.close_price if strategy_exit.close_price is not None else current_price
                    close_trigger = f"strategy:{strategy_exit.reason}"
                    price_source = current_price_source
                elif (
                    not resolve_only
                    and take_profit_pct is not None
                    and pnl_pct is not None
                    and min_hold_passed
                    and pnl_pct >= take_profit_pct
                ):
                    close_price = current_price
                    close_trigger = "take_profit"
                    price_source = current_price_source
                elif (
                    not resolve_only
                    and stop_loss_pct is not None
                    and pnl_pct is not None
                    and min_hold_passed
                    and pnl_pct <= -abs(stop_loss_pct)
                ):
                    close_price = current_price
                    close_trigger = "stop_loss"
                    price_source = current_price_source
                elif (
                    not resolve_only
                    and trailing_stop_pct is not None
                    and trailing_stop_pct > 0
                    and current_price is not None
                    and highest_price is not None
                    and min_hold_passed
                ):
                    trailing_trigger_price = highest_price * (1.0 - (trailing_stop_pct / 100.0))
                    if highest_price > entry_price and current_price <= trailing_trigger_price:
                        close_price = current_price
                        close_trigger = "trailing_stop"
                        price_source = current_price_source
                elif (
                    not resolve_only
                    and max_hold_minutes is not None
                    and age_minutes is not None
                    and age_minutes >= max_hold_minutes
                ):
                    if current_price is not None:
                        close_price = current_price
                        close_trigger = "max_hold"
                        price_source = current_price_source
                elif (
                    not resolve_only
                    and close_on_inactive_market
                    and not market_tradable
                    and current_price is not None
                    and min_hold_passed
                ):
                    close_price = current_price
                    close_trigger = "market_inactive"
                    price_source = current_price_source

        if close_price is None:
            state_changed = False
            if current_price is not None:
                state_changed = (
                    prev_last_mark is None
                    or abs(prev_last_mark - current_price) > 1e-9
                    or prev_high is None
                    or prev_low is None
                    or abs((prev_high or 0.0) - (highest_price or 0.0)) > 1e-9
                    or abs((prev_low or 0.0) - (lowest_price or 0.0)) > 1e-9
                    or prev_mark_source != str(current_price_source or "")
                )
            if not dry_run and state_changed:
                payload["position_state"] = next_state
                row.payload_json = payload
                row.updated_at = now
                state_updates += 1
            held += 1
            continue

        quantity = notional / entry_price
        proceeds = quantity * close_price
        pnl = proceeds - notional
        next_status = _status_for_close(pnl=pnl, close_trigger=close_trigger)

        simulation_close: dict[str, Any] | None = None
        simulation_ledger = payload.get("simulation_ledger")
        if not dry_run and enable_simulation_ledger and isinstance(simulation_ledger, dict):
            sim_account_id = str(simulation_ledger.get("account_id") or "").strip()
            sim_trade_id = str(simulation_ledger.get("trade_id") or "").strip()
            sim_position_id = str(simulation_ledger.get("position_id") or "").strip()
            if sim_account_id and sim_trade_id and sim_position_id:
                try:
                    simulation_close = await simulation_service.close_orchestrator_paper_fill(
                        account_id=sim_account_id,
                        trade_id=sim_trade_id,
                        position_id=sim_position_id,
                        close_price=float(close_price),
                        close_trigger=close_trigger,
                        price_source=price_source,
                        reason=reason,
                        session=session,
                        commit=False,
                    )
                    if simulation_close.get("closed"):
                        proceeds = float(simulation_close.get("actual_payout", proceeds))
                        pnl = float(simulation_close.get("actual_pnl", pnl))
                        next_status = str(simulation_close.get("trade_status") or next_status)
                    elif simulation_close.get("already_closed"):
                        existing_status = str(simulation_close.get("trade_status") or "")
                        if existing_status:
                            next_status = existing_status
                        pnl = float(simulation_close.get("actual_pnl", pnl))
                        proceeds = float(simulation_close.get("actual_payout", proceeds))
                except Exception as exc:
                    skipped += 1
                    skipped_reasons["simulation_close_error"] = (
                        int(skipped_reasons.get("simulation_close_error", 0)) + 1
                    )
                    details.append(
                        {
                            "order_id": row.id,
                            "market_id": row.market_id,
                            "direction": row.direction,
                            "close_trigger": close_trigger,
                            "reason": "simulation_close_error",
                            "error": str(exc),
                        }
                    )
                    continue

        total_realized_pnl += pnl
        by_status[next_status] = int(by_status.get(next_status, 0)) + 1

        detail = {
            "order_id": row.id,
            "market_id": row.market_id,
            "direction": row.direction,
            "entry_price": entry_price,
            "close_price": close_price,
            "price_source": price_source,
            "close_trigger": close_trigger,
            "market_tradable": market_tradable,
            "notional_usd": notional,
            "quantity": quantity,
            "realized_pnl": pnl,
            "next_status": next_status,
            "age_minutes": age_minutes,
            "min_hold_minutes": min_hold_minutes,
            "trailing_stop_trigger_price": trailing_trigger_price,
            "highest_price_seen": _state_price_floor(highest_price),
            "lowest_price_seen": _state_price_floor(lowest_price),
            "simulation_close": simulation_close,
        }
        details.append(detail)
        would_close += 1

        if dry_run:
            continue

        row.status = next_status
        row.actual_profit = pnl
        row.updated_at = now
        payload["position_state"] = next_state
        payload["position_close"] = {
            "close_price": close_price,
            "price_source": price_source,
            "close_trigger": close_trigger,
            "realized_pnl": pnl,
            "market_tradable": market_tradable,
            "age_minutes": age_minutes,
            "closed_at": _iso_utc(now),
            "reason": reason,
        }
        if simulation_close is not None:
            payload["position_close"]["simulation_close"] = simulation_close

        if _exit_instance is not None and hasattr(_exit_instance, "record_trade_outcome"):
            try:
                _exit_instance.record_trade_outcome(won=next_status in {"closed_win", "resolved_win"})
            except Exception:
                pass

        reverse_signal_id, reverse_source = await _emit_armed_reverse_signal(
            session,
            row=row,
            payload=payload,
            signal_payload=signal_payload,
            market_info=market_info,
            close_trigger=close_trigger,
            realized_pnl=pnl,
            now=now,
        )
        if reverse_signal_id and reverse_source:
            reverse_signal_ids_by_source.setdefault(reverse_source, []).append(reverse_signal_id)

        row.payload_json = payload
        if reason:
            if row.reason:
                row.reason = f"{row.reason} | {reason}:{close_trigger}"
            else:
                row.reason = f"{reason}:{close_trigger}"
        hot_state.record_order_resolved(
            trader_id=trader_id,
            mode=str(row.mode or ""),
            order_id=str(row.id or ""),
            market_id=str(row.market_id or ""),
            direction=str(row.direction or ""),
            source=str(row.source or ""),
            status=next_status,
            actual_profit=pnl,
            payload=payload,
            copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
        )
        closed += 1

    if not dry_run and (closed > 0 or state_updates > 0):
        touched_rows = [row for row in candidates if row.updated_at == now]
        if touched_rows:
            publish_rows = list(touched_rows)
            stmt = (
                update(TraderOrder)
                .where(TraderOrder.id == bindparam("_id"))
                .values(
                    status=bindparam("_status"),
                    actual_profit=bindparam("_actual_profit"),
                    updated_at=bindparam("_updated_at"),
                    payload_json=bindparam("_payload_json"),
                    reason=bindparam("_reason"),
                )
                .execution_options(synchronize_session=None)
            )
            params = [
                {
                    "id": row.id,
                    "_id": row.id,
                    "_status": row.status,
                    "_actual_profit": row.actual_profit,
                    "_updated_at": row.updated_at,
                    "_payload_json": row.payload_json,
                    "_reason": row.reason,
                }
                for row in publish_rows
            ]
            for row in touched_rows:
                if inspect(row).session is session.sync_session:
                    session.sync_session.expunge(row)
            with session.no_autoflush:
                await session.execute(stmt, params)
        else:
            publish_rows = []
        await session.commit()
        await _publish_trader_order_updates(publish_rows)
        if reverse_signal_ids_by_source:
            await _publish_reverse_signal_batches(reverse_signal_ids_by_source, emitted_at=now)

    return {
        "trader_id": trader_id,
        "mode": mode_key,
        "dry_run": bool(dry_run),
        "matched": len(candidates),
        "would_close": would_close,
        "closed": closed,
        "held": held,
        "skipped": skipped,
        "state_updates": state_updates,
        "total_realized_pnl": total_realized_pnl,
        "by_status": by_status,
        "skipped_reasons": skipped_reasons,
        "reverse_signals_emitted": sum(len(ids) for ids in reverse_signal_ids_by_source.values()),
        "details": details,
    }


async def reconcile_shadow_positions(
    session: AsyncSession,
    *,
    trader_id: str,
    trader_params: Optional[dict[str, Any]] = None,
    dry_run: bool = False,
    force_mark_to_market: bool = False,
    max_age_hours: Optional[int] = None,
    order_ids: Optional[list[str]] = None,
    reason: str = "shadow_position_lifecycle",
) -> dict[str, Any]:
    return await reconcile_paper_positions(
        session,
        trader_id=trader_id,
        trader_params=trader_params,
        dry_run=dry_run,
        force_mark_to_market=force_mark_to_market,
        max_age_hours=max_age_hours,
        order_ids=order_ids,
        reason=reason,
        position_mode="shadow",
        param_prefix="shadow",
        enable_simulation_ledger=False,
    )


async def reconcile_live_positions(
    session: AsyncSession,
    *,
    trader_id: str,
    trader_params: Optional[dict[str, Any]] = None,
    dry_run: bool = False,
    force_mark_to_market: bool = False,
    max_age_hours: Optional[int] = None,
    order_ids: Optional[list[str]] = None,
    reason: str = "live_position_lifecycle",
) -> dict[str, Any]:
    """Lifecycle management for live positions.

    Mirrors reconcile_paper_positions but operates on mode='live' orders.
    Handles: stop-loss, take-profit, trailing stop, max hold, market
    inactivity, and resolution detection.  Does NOT interact with the
    simulation ledger (that is paper-only).
    """
    params = dict(trader_params or {})
    resolution_infer_from_prices = _safe_bool(params.get("live_resolution_infer_from_prices"), True)
    resolution_settle_floor = min(
        1.0,
        max(0.5, safe_float(params.get("live_resolution_settle_floor")) or 0.98),
    )
    mark_touch_interval_seconds = _mark_touch_interval_seconds(params, mode="live")

    import time as _time
    _lc_t0 = _time.monotonic()

    candidates = list(
        (
            await session.execute(
                select(TraderOrder).where(
                    TraderOrder.trader_id == trader_id,
                    TraderOrder.mode == "live",
                    TraderOrder.status.in_(tuple(LIVE_ACTIVE_STATUSES)),
                )
            )
        )
        .scalars()
        .all()
    )

    if order_ids:
        requested_order_ids = {str(value or "").strip() for value in order_ids if str(value or "").strip()}
        if requested_order_ids:
            candidates = [row for row in candidates if str(row.id or "") in requested_order_ids]
        else:
            candidates = []

    if max_age_hours is not None:
        cutoff = utcnow() - timedelta(hours=max(1, int(max_age_hours)))
        candidates = [
            row
            for row in candidates
            if (row.executed_at or row.updated_at or row.created_at) is not None
            and (row.executed_at or row.updated_at or row.created_at) <= cutoff
        ]

    global _wallet_positions_last_refresh_succeeded, _wallet_closed_positions_last_refresh_succeeded, _wallet_activity_last_refresh_succeeded

    now = utcnow()
    now_naive = now.astimezone(timezone.utc).replace(tzinfo=None) if now.tzinfo is not None else now
    would_close = 0
    closed = 0
    held = 0
    skipped = 0
    total_realized_pnl = 0.0
    by_status: dict[str, int] = {"resolved_win": 0, "resolved_loss": 0, "closed_win": 0, "closed_loss": 0}
    skipped_reasons: dict[str, int] = {}
    details: list[dict[str, Any]] = []
    state_updates = 0
    reverse_signal_ids_by_source: dict[str, list[str]] = {}

    candidate_ids = {str(row.id) for row in candidates}
    terminal_age_expr = func.coalesce(
        TraderOrder.updated_at,
        TraderOrder.executed_at,
        TraderOrder.created_at,
    )
    terminal_stmt = select(TraderOrder).where(
        TraderOrder.trader_id == trader_id,
        TraderOrder.mode == "live",
        TraderOrder.status.in_(("closed_win", "closed_loss", "cancelled", "failed", "rejected", "error")),
    )
    if max_age_hours is not None:
        cutoff = now - timedelta(hours=max(1, int(max_age_hours)))
        terminal_stmt = terminal_stmt.where(terminal_age_expr <= cutoff)
    else:
        audit_cutoff = now_naive - timedelta(
            hours=max(_TERMINAL_REOPEN_LOOKBACK_HOURS, _NONACTIVE_WALLET_REOPEN_LOOKBACK_HOURS)
        )
        terminal_stmt = terminal_stmt.where(terminal_age_expr >= audit_cutoff)
    terminal_rows = list(((await session.execute(terminal_stmt)).scalars().all()))
    if max_age_hours is None:
        terminal_rows = [row for row in terminal_rows if _terminal_row_requires_reopen_audit(row, now_naive)]
    terminal_rows = _dedupe_live_authority_rows(terminal_rows)

    await session.commit()
    async with release_conn(session):
        wallet_positions_by_token = await _load_mapping_with_timeout(
            _load_execution_wallet_positions_by_token,
            timeout=_WALLET_POSITIONS_LOAD_TIMEOUT_SECONDS,
            fallback=dict(_wallet_positions_cache[1] or {}),
        )
    wallet_positions_loaded = bool(_wallet_positions_last_refresh_succeeded or wallet_positions_by_token)
    wallet_positions_index_present = bool(wallet_positions_by_token)
    wallet_closed_positions_by_token: dict[str, dict[str, Any]] = {}
    wallet_sell_trades_by_token: dict[str, dict[str, Any]] = {}
    wallet_close_activity_by_token: dict[str, dict[str, Any]] = {}
    wallet_closed_positions_loaded = False

    for row in terminal_rows:
        row_id = str(row.id)
        if row_id in candidate_ids:
            continue
        payload = dict(row.payload_json or {})
        token_id = _extract_live_token_id(payload)
        wallet_position = wallet_positions_by_token.get(token_id) if token_id else None
        if not isinstance(wallet_position, dict) and token_id:
            cond_key = _extract_condition_outcome_key(payload, str(row.direction or ""))
            if cond_key:
                wallet_position = wallet_positions_by_token.get(cond_key)
        wallet_position_size = _extract_wallet_position_size(wallet_position)
        status_key = str(row.status or "").strip().lower()
        if (
            status_key in {"cancelled", "failed", "rejected", "error"}
            and wallet_positions_loaded
            and wallet_position_size > _WALLET_SIZE_EPSILON
        ):
            if not _failed_terminal_row_can_reopen_from_wallet_position(row, payload):
                details.append(
                    {
                        "order_id": row.id,
                        "market_id": row.market_id,
                        "next_status": row.status,
                        "wallet_position_size": wallet_position_size,
                        "note": "kept_terminal_row_specific_authority_missing",
                    }
                )
                continue
            details.append(
                {
                    "order_id": row.id,
                    "market_id": row.market_id,
                    "next_status": "open",
                    "wallet_position_size": wallet_position_size,
                    "note": "reopened_wallet_position_authority",
                }
            )
            if not dry_run:
                row.status = "open"
                row.actual_profit = None
                row.error_message = None
                row.updated_at = now
                row.payload_json = payload
                state_updates += 1
            candidates.append(row)
            candidate_ids.add(row_id)
            continue
        _exit_instance = None
        position_close = payload.get("position_close")
        if isinstance(position_close, dict):
            close_trigger = str(position_close.get("close_trigger") or "").strip().lower()
            if close_trigger in {"wallet_absent_close", "wallet_flat_override"}:
                pending_exit = payload.get("pending_live_exit")
                pending_exit = pending_exit if isinstance(pending_exit, dict) else {}
                provider_status = _provider_snapshot_status(payload)
                provider_remaining_size = _provider_snapshot_remaining_size(payload)
                fallback_close_verified = (
                    _pending_exit_has_verified_terminal_fill(pending_exit)
                    or bool(_position_close_wallet_trade_id(position_close))
                )
                reopen_reason = ""
                if not fallback_close_verified:
                    if close_trigger == "wallet_absent_close" and _provider_entry_order_still_working(payload):
                        reopen_reason = "provider_entry_order_still_open"
                    else:
                        reopen_reason = "fallback_close_not_verified"
                if (
                    reopen_reason == "fallback_close_not_verified"
                    and wallet_positions_loaded
                    and token_id
                    and wallet_position_size <= _WALLET_SIZE_EPSILON
                ):
                    details.append(
                        {
                            "order_id": row.id,
                            "market_id": row.market_id,
                            "close_trigger": close_trigger,
                            "provider_status": provider_status,
                            "provider_remaining_size": provider_remaining_size,
                            "next_status": row.status,
                            "note": "kept_terminal_no_wallet_position",
                        }
                    )
                    continue
                if reopen_reason:
                    reopen_key = "wallet_absent_reopen" if close_trigger == "wallet_absent_close" else "wallet_flat_reopen"
                    if not dry_run:
                        row.status = "open"
                        row.actual_profit = None
                        row.updated_at = now
                        payload.pop("position_close", None)
                        payload[reopen_key] = {
                            "reopened_at": _iso_utc(now),
                            "reopen_reason": reopen_reason,
                            "provider_status": provider_status,
                            "provider_remaining_size": provider_remaining_size,
                        }
                        row.payload_json = payload
                        state_updates += 1
                    details.append(
                        {
                            "order_id": row.id,
                            "market_id": row.market_id,
                            "close_trigger": close_trigger,
                            "provider_status": provider_status,
                            "provider_remaining_size": provider_remaining_size,
                            "next_status": "open",
                            "note": f"reopened_{close_trigger}_{reopen_reason}",
                        }
                    )
                    candidates.append(row)
                    candidate_ids.add(row_id)
                    continue
        pending_exit = payload.get("pending_live_exit")
        if not isinstance(pending_exit, dict):
            continue
        pending_status = str(pending_exit.get("status") or "").strip().lower()
        if pending_status != "filled":
            continue
        if _safe_bool(pending_exit.get("allow_partial_fill_terminal"), False):
            continue
        close_trigger = str(pending_exit.get("close_trigger") or "").strip().lower()
        if "resolution" in close_trigger:
            continue
        required_exit_size = max(0.0, safe_float(pending_exit.get("exit_size"), 0.0) or 0.0)
        _entry_notional, _entry_size, _entry_price = _extract_live_fill_metrics(payload)
        if _entry_size <= 0.0 and _entry_notional > 0.0:
            _fallback_entry_price = (
                _entry_price if _entry_price and _entry_price > 0 else safe_float(row.effective_price)
            )
            if _fallback_entry_price is None or _fallback_entry_price <= 0:
                _fallback_entry_price = safe_float(row.entry_price)
            if _fallback_entry_price and _fallback_entry_price > 0:
                _entry_size = _entry_notional / _fallback_entry_price
        if _entry_size > 0.0:
            required_exit_size = max(required_exit_size, _entry_size)
        if required_exit_size <= 0.0:
            continue
        filled_exit_size = max(0.0, safe_float(pending_exit.get("filled_size"), 0.0) or 0.0)
        fill_ratio = safe_float(pending_exit.get("fill_ratio"))
        if fill_ratio is None:
            fill_ratio = (filled_exit_size / required_exit_size) if required_exit_size > 0.0 else 0.0
        threshold_ratio = _pending_exit_fill_threshold(pending_exit)
        if fill_ratio + 1e-9 >= threshold_ratio:
            continue
        if not dry_run:
            row.status = "open"
            row.actual_profit = None
            row.updated_at = now
            pending_exit["status"] = (
                "submitted"
                if str(pending_exit.get("exit_order_id") or "").strip()
                or str(pending_exit.get("provider_clob_order_id") or "").strip()
                else "pending"
            )
            pending_exit["reopened_at"] = _iso_utc(now)
            pending_exit["reopen_reason"] = "partial_exit_fill_below_threshold"
            pending_exit["fill_ratio"] = float(fill_ratio)
            payload["pending_live_exit"] = pending_exit
            payload.pop("position_close", None)
            row.payload_json = payload
            state_updates += 1
        details.append(
            {
                "order_id": row.id,
                "market_id": row.market_id,
                "close_trigger": str(pending_exit.get("close_trigger") or "live_exit_fill"),
                "filled_size": filled_exit_size,
                "required_exit_size": required_exit_size,
                "fill_ratio": float(fill_ratio),
                "fill_ratio_threshold": threshold_ratio,
                "next_status": "open",
                "note": "reopened_partial_exit_fill_below_threshold",
            }
        )
        candidates.append(row)
        candidate_ids.add(row_id)

    def _candidate_needs_wallet_history(row: TraderOrder) -> bool:
        payload = dict(row.payload_json or {})
        pending_exit = payload.get("pending_live_exit")
        if isinstance(pending_exit, dict):
            pending_status = str(pending_exit.get("status") or "").strip().lower()
            if pending_status in {"pending", "submitted", "working", "filled"}:
                return True
        token_id = _extract_live_token_id(payload)
        if not token_id:
            return False
        wallet_position = wallet_positions_by_token.get(token_id)
        if not isinstance(wallet_position, dict):
            cond_key = _extract_condition_outcome_key(payload, str(row.direction or ""))
            if cond_key:
                wallet_position = wallet_positions_by_token.get(cond_key)
        if isinstance(wallet_position, dict) and _extract_wallet_position_size(wallet_position) <= _WALLET_SIZE_EPSILON:
            return True
        age_anchor = getattr(row, "updated_at", None) or getattr(row, "executed_at", None) or getattr(row, "created_at", None)
        if not isinstance(age_anchor, datetime):
            return False
        if age_anchor.tzinfo is not None:
            age_anchor = age_anchor.astimezone(timezone.utc).replace(tzinfo=None)
        order_age_seconds = max(0.0, (now_naive - age_anchor).total_seconds())
        if order_age_seconds < _WALLET_HISTORY_GRACE_SECONDS:
            return False
        if not isinstance(wallet_position, dict):
            return True
        return _extract_wallet_position_size(wallet_position) <= _WALLET_SIZE_EPSILON

    signal_ids = [str(row.signal_id) for row in candidates if row.signal_id]
    signal_payloads: dict[str, dict[str, Any]] = {}
    if signal_ids:
        signal_rows = (
            await session.execute(
                select(TradeSignal.id, TradeSignal.payload_json).where(TradeSignal.id.in_(signal_ids))
            )
        ).all()
        signal_payloads = {str(row.id): dict(row.payload_json or {}) for row in signal_rows}

    _lc_t1 = _time.monotonic()
    if any(_candidate_needs_wallet_history(row) for row in candidates):
        async with release_conn(session):
            wallet_closed_positions_by_token, wallet_sell_trades_by_token, wallet_close_activity_by_token = await asyncio.gather(
                _load_mapping_with_timeout(
                    _load_execution_wallet_closed_positions_by_token,
                    timeout=_WALLET_HISTORY_LOAD_TIMEOUT_SECONDS,
                    fallback=dict(_wallet_closed_positions_cache[1] or {}),
                ),
                _load_mapping_with_timeout(
                    _load_execution_wallet_recent_sell_trades_by_token,
                    timeout=_WALLET_HISTORY_LOAD_TIMEOUT_SECONDS,
                    fallback=dict(_wallet_sell_trades_cache[1] or {}),
                ),
                _load_mapping_with_timeout(
                    _load_execution_wallet_recent_close_activity_by_token,
                    timeout=_WALLET_HISTORY_LOAD_TIMEOUT_SECONDS,
                    fallback=dict(_wallet_activity_cache[1] or {}),
                ),
            )
        wallet_closed_positions_loaded = bool(
            _wallet_closed_positions_last_refresh_succeeded or wallet_closed_positions_by_token
        )

    # Release the DB connection back to the pool while we do external I/O
    # (Polymarket API, WS cache, CLOB midpoints).  The session
    # lazily reconnects on the next DB operation.
    fallback_market_info_by_id = _fallback_market_info_for_orders(candidates)
    async with release_conn(session):
        market_info_by_id = await _load_mapping_with_timeout(
            lambda: load_market_info_for_orders(candidates),
            timeout=_MARKET_INFO_LOAD_TIMEOUT_SECONDS,
            fallback=fallback_market_info_by_id,
        )
        ws_mid_prices: dict[str, float] = {}
        clob_mid_prices: dict[str, float] = {}
        token_ids_for_prices = sorted(
            {
                token_id
                for token_id in (_extract_live_token_id(dict(row.payload_json or {})) for row in candidates)
                if token_id
            }
        )
        if token_ids_for_prices:
            strict_stale_seconds = max(0.05, float(settings.WS_EXECUTION_PRICE_STALE_SECONDS))
            relaxed_stale_seconds = max(strict_stale_seconds, float(settings.WS_PRICE_STALE_SECONDS))
            try:
                from services.ws_feeds import get_feed_manager

                feed_manager = get_feed_manager()
            except Exception:
                feed_manager = None

            def _collect_ws_mid_prices(token_ids: list[str], *, stale_seconds: float) -> dict[str, float]:
                if feed_manager is None or not getattr(feed_manager, "_started", False):
                    return {}
                cache = getattr(feed_manager, "cache", None)
                if cache is None:
                    return {}
                out: dict[str, float] = {}
                for token_id in token_ids:
                    try:
                        mid = safe_float(cache.get_mid_price(token_id))
                        age_s = safe_float(cache.staleness(token_id))
                    except Exception:
                        continue
                    if mid is None or mid < 0:
                        continue
                    if age_s is not None and age_s > stale_seconds:
                        continue
                    out[str(token_id).strip()] = float(mid)
                return out

            ws_mid_prices.update(_collect_ws_mid_prices(token_ids_for_prices, stale_seconds=strict_stale_seconds))
            unresolved_token_ids = [token_id for token_id in token_ids_for_prices if token_id not in ws_mid_prices]
            if unresolved_token_ids and relaxed_stale_seconds > strict_stale_seconds + 1e-9:
                ws_mid_prices.update(
                    _collect_ws_mid_prices(
                        unresolved_token_ids,
                        stale_seconds=relaxed_stale_seconds,
                    )
                )
            unresolved_token_ids = [token_id for token_id in token_ids_for_prices if token_id not in ws_mid_prices]
            if unresolved_token_ids:

                async def _fetch_clob_midpoint(token_id: str) -> tuple[str, Optional[float]]:
                    try:
                        midpoint = await asyncio.wait_for(
                            polymarket_client.get_midpoint(token_id),
                            timeout=1.5,
                        )
                    except Exception:
                        return token_id, None
                    parsed_midpoint = safe_float(midpoint)
                    if parsed_midpoint is None or parsed_midpoint < 0:
                        return token_id, None
                    return token_id, float(parsed_midpoint)

                midpoint_pairs = await asyncio.gather(
                    *[_fetch_clob_midpoint(token_id) for token_id in unresolved_token_ids],
                    return_exceptions=True,
                )
                for item in midpoint_pairs:
                    if isinstance(item, Exception):
                        continue
                    token_id, midpoint = item
                    if midpoint is not None:
                        clob_mid_prices[str(token_id).strip()] = midpoint

        pending_exit_provider_ids = sorted(
            {
                _pending_exit_provider_clob_id(pending_exit)
                for pending_exit in ((dict((row.payload_json or {})).get("pending_live_exit")) for row in candidates)
                if isinstance(pending_exit, dict)
                and str(pending_exit.get("status") or "").strip().lower() in {"submitted", "pending"}
                and _pending_exit_provider_clob_id(pending_exit)
            }
        )
        pending_exit_order_ids = sorted(
            {
                str(pending_exit.get("exit_order_id") or "").strip()
                for pending_exit in ((dict((row.payload_json or {})).get("pending_live_exit")) for row in candidates)
                if isinstance(pending_exit, dict)
                and str(pending_exit.get("status") or "").strip().lower() in {"submitted", "pending"}
                and str(pending_exit.get("exit_order_id") or "").strip()
            }
        )
        pending_exit_snapshots: dict[str, dict[str, Any]] = {}
        if pending_exit_provider_ids:
            pending_exit_snapshots = await _load_mapping_with_timeout(
                lambda: live_execution_service.get_order_snapshots_by_clob_ids(pending_exit_provider_ids),
                timeout=_ORDER_SNAPSHOT_LOAD_TIMEOUT_SECONDS,
                fallback=_cached_order_snapshots_by_clob_ids(pending_exit_provider_ids),
            )

        # Backfill: fetch entry fill snapshots for orders missing entry price.
        # IOC/FAK orders may have average_fill_price=0 in LiveTradingOrder because
        # the venue response didn't include fill price and the order completed before
        # the sync loop caught it.
        entry_backfill_snapshots: dict[str, dict[str, Any]] = {}
        entry_backfill_clob_ids: list[str] = []
        for row in candidates:
            _bf_payload = dict(row.payload_json or {})
            _bf_price = safe_float(row.effective_price) or safe_float(row.entry_price) or 0.0
            if _bf_price > 0:
                continue
            _bf_recon = _bf_payload.get("provider_reconciliation")
            if isinstance(_bf_recon, dict):
                _bf_price = safe_float(_bf_recon.get("average_fill_price")) or 0.0
            if _bf_price > 0:
                continue
            _bf_clob = str(_bf_payload.get("provider_clob_order_id") or "").strip()
            if _bf_clob:
                entry_backfill_clob_ids.append(_bf_clob)
        if entry_backfill_clob_ids:
            entry_backfill_snapshots = await _load_mapping_with_timeout(
                lambda: live_execution_service.get_order_snapshots_by_clob_ids(entry_backfill_clob_ids),
                timeout=_ORDER_SNAPSHOT_LOAD_TIMEOUT_SECONDS,
                fallback=_cached_order_snapshots_by_clob_ids(entry_backfill_clob_ids),
            )

    _lc_t2 = _time.monotonic()

    pending_exit_snapshot_fallbacks: dict[str, dict[str, Any]] = {}
    if pending_exit_provider_ids or pending_exit_order_ids:
        pending_exit_live_rows = (
            await session.execute(
                select(LiveTradingOrder).where(
                    or_(
                        LiveTradingOrder.clob_order_id.in_(pending_exit_provider_ids) if pending_exit_provider_ids else False,
                        LiveTradingOrder.id.in_(pending_exit_order_ids) if pending_exit_order_ids else False,
                    )
                )
            )
        ).scalars().all()
        for live_row in pending_exit_live_rows:
            snapshot = _live_trading_order_snapshot(live_row)
            clob_order_id = str(snapshot.get("clob_order_id") or "").strip()
            if clob_order_id and clob_order_id not in pending_exit_snapshots:
                pending_exit_snapshot_fallbacks[clob_order_id] = snapshot

    for row in candidates:
        payload = dict(row.payload_json or {})
        _exit_instance = None
        token_id = _extract_live_token_id(payload)
        take_profit_pct = safe_float(_payload_exit_param(payload, prefix_key="live", name="take_profit_pct"))
        stop_loss_pct = safe_float(_payload_exit_param(payload, prefix_key="live", name="stop_loss_pct"))
        max_hold_minutes = safe_float(_payload_exit_param(payload, prefix_key="live", name="max_hold_minutes"))
        min_hold_minutes = max(
            0.0,
            safe_float(_payload_exit_param(payload, prefix_key="live", name="min_hold_minutes")) or 0.0,
        )
        trailing_stop_pct = safe_float(_payload_exit_param(payload, prefix_key="live", name="trailing_stop_pct"))
        resolve_only = _safe_bool(_payload_exit_param(payload, prefix_key="live", name="resolve_only"), False)
        close_on_inactive_market = _safe_bool(
            _payload_exit_param(payload, prefix_key="live", name="close_on_inactive_market"),
            False,
        )
        wallet_position = wallet_positions_by_token.get(token_id) if token_id else None
        closed_position = wallet_closed_positions_by_token.get(token_id) if token_id else None
        wallet_close_activity = wallet_close_activity_by_token.get(token_id) if token_id else None
        # Fallback: if exact token_id lookup fails (token versioning), try
        # conditionId:outcomeIndex which is stable across token versions.
        if not isinstance(wallet_position, dict) and token_id:
            cond_key = _extract_condition_outcome_key(payload, str(row.direction or ""))
            if cond_key:
                wallet_position = wallet_positions_by_token.get(cond_key)
                closed_position = wallet_closed_positions_by_token.get(cond_key)
                wallet_close_activity = wallet_close_activity_by_token.get(cond_key)
        wallet_position_observed = isinstance(wallet_position, dict)
        wallet_position_size = _extract_wallet_position_size(wallet_position)
        wallet_mark_price = _extract_wallet_mark_price(wallet_position)
        wallet_settlement_price = _extract_wallet_settlement_price(wallet_position)
        latest_wallet_sell_trade = wallet_sell_trades_by_token.get(token_id) if token_id else None
        provider_reconciliation = payload.get("provider_reconciliation")
        provider_reconciliation = provider_reconciliation if isinstance(provider_reconciliation, dict) else {}
        provider_snapshot = provider_reconciliation.get("snapshot")
        provider_snapshot = provider_snapshot if isinstance(provider_snapshot, dict) else {}
        provider_raw = provider_snapshot.get("raw")
        provider_raw = provider_raw if isinstance(provider_raw, dict) else {}
        provider_side = str(provider_raw.get("side") or provider_snapshot.get("side") or "").strip().upper()
        is_recovered_sell_authority = (
            str(row.reason or "").strip().lower() == "recovered from live venue authority"
            and provider_side == "SELL"
        )
        if _active_row_has_unfilled_terminal_provider_failure(row, payload):
            terminal_status = _provider_failure_terminal_status(row, payload)
            details.append(
                {
                    "order_id": row.id,
                    "market_id": row.market_id,
                    "next_status": terminal_status,
                    "note": "finalized_unfilled_provider_failure",
                }
            )
            if not dry_run:
                row.status = terminal_status
                row.notional_usd = 0.0
                row.actual_profit = None
                row.executed_at = None
                row.updated_at = now
                payload["provider_failure_finalized"] = {
                    "finalized_at": _iso_utc(now),
                    "provider_status": _provider_snapshot_status(payload),
                    "reason": "unfilled_provider_failure",
                }
                row.payload_json = payload
                hot_state.record_order_cancelled(
                    trader_id=trader_id,
                    mode=str(row.mode or ""),
                    order_id=str(row.id or ""),
                    market_id=str(row.market_id or ""),
                    source=str(row.source or ""),
                    copy_source_wallet=None,
                )
                state_updates += 1
            skipped += 1
            skipped_reasons["unfilled_provider_failure"] = int(skipped_reasons.get("unfilled_provider_failure", 0)) + 1
            continue
        entry_fill_notional, entry_fill_size, entry_fill_price = _extract_live_fill_metrics(payload)
        if entry_fill_price is None or entry_fill_price <= 0.0:
            entry_fill_price = safe_float(row.effective_price)
        if entry_fill_price is None or entry_fill_price <= 0.0:
            entry_fill_price = safe_float(row.entry_price)
        # Backfill entry price from venue snapshot for orders that were
        # persisted without fill price data (e.g. IOC/FAK orders whose fill
        # price was not in the placement response).
        if entry_fill_price is None or entry_fill_price <= 0.0:
            _bf_clob = str(payload.get("provider_clob_order_id") or "").strip()
            _bf_snap = entry_backfill_snapshots.get(_bf_clob) if _bf_clob else None
            if isinstance(_bf_snap, dict):
                _bf_avg = safe_float(_bf_snap.get("average_fill_price"))
                _bf_filled = max(0.0, safe_float(_bf_snap.get("filled_size"), 0.0) or 0.0)
                _bf_notional = max(0.0, safe_float(_bf_snap.get("filled_notional_usd"), 0.0) or 0.0)
                if _bf_avg is not None and _bf_avg > 0:
                    entry_fill_price = _bf_avg
                elif _bf_notional > 0 and _bf_filled > 0:
                    entry_fill_price = _bf_notional / _bf_filled
                else:
                    _bf_limit = safe_float(_bf_snap.get("limit_price"))
                    if _bf_limit is not None and _bf_limit > 0:
                        entry_fill_price = _bf_limit
                if entry_fill_price is not None and entry_fill_price > 0:
                    if _bf_filled > 0:
                        entry_fill_size = max(entry_fill_size, _bf_filled)
                    if _bf_notional > 0:
                        entry_fill_notional = max(entry_fill_notional, _bf_notional)
                    # Persist backfilled price so future cycles don't need to
                    # re-fetch from the venue.
                    if not dry_run:
                        row.effective_price = float(entry_fill_price)
                        if row.entry_price is None or float(row.entry_price or 0) <= 0:
                            row.entry_price = float(entry_fill_price)
                        recon = payload.get("provider_reconciliation")
                        if isinstance(recon, dict) and not safe_float(recon.get("average_fill_price")):
                            recon["average_fill_price"] = float(entry_fill_price)
                            if _bf_filled > 0:
                                recon["filled_size"] = _bf_filled
                            if _bf_notional > 0:
                                recon["filled_notional_usd"] = _bf_notional
                            payload["provider_reconciliation"] = recon
                        row.payload_json = payload
                        row.updated_at = now
                        state_updates += 1
                        logger.info(
                            "Backfilled entry price for order=%s price=%.4f source=venue_snapshot",
                            row.id, entry_fill_price,
                        )
        if wallet_position_size > _WALLET_SIZE_EPSILON:
            wallet_entry_price = _extract_wallet_entry_price(wallet_position)
            if entry_fill_size <= 0.0:
                entry_fill_size = wallet_position_size
            if (entry_fill_price is None or entry_fill_price <= 0.0) and wallet_entry_price is not None:
                entry_fill_price = wallet_entry_price
            if entry_fill_notional <= 0.0 and entry_fill_price is not None and entry_fill_price > 0.0 and entry_fill_size > 0.0:
                entry_fill_notional = entry_fill_size * entry_fill_price
            if not dry_run and wallet_entry_price is not None and wallet_entry_price > 0.0:
                backfilled_from_wallet = False
                if row.entry_price is None or float(row.entry_price or 0.0) <= 0.0:
                    row.entry_price = float(wallet_entry_price)
                    backfilled_from_wallet = True
                if row.effective_price is None or float(row.effective_price or 0.0) <= 0.0:
                    row.effective_price = float(wallet_entry_price)
                    backfilled_from_wallet = True
                if (safe_float(row.notional_usd) or 0.0) <= 0.0 and entry_fill_notional > 0.0:
                    row.notional_usd = float(entry_fill_notional)
                    backfilled_from_wallet = True
                if backfilled_from_wallet:
                    row.updated_at = now
                    state_updates += 1
        if entry_fill_notional <= 0.0:
            entry_fill_notional = max(0.0, safe_float(row.notional_usd) or 0.0)
        if entry_fill_size <= 0.0 and entry_fill_notional > 0.0 and entry_fill_price and entry_fill_price > 0.0:
            entry_fill_size = entry_fill_notional / entry_fill_price
        if token_id and wallet_position_size <= _WALLET_SIZE_EPSILON and isinstance(wallet_close_activity, dict):
            activity_timestamp = _parse_wallet_activity_time(wallet_close_activity)
            row_created = row.created_at
            activity_after_entry = True
            if isinstance(activity_timestamp, datetime) and isinstance(row_created, datetime):
                entry_anchor = row_created
                if entry_anchor.tzinfo is not None:
                    entry_anchor = entry_anchor.astimezone(timezone.utc).replace(tzinfo=None)
                activity_after_entry = activity_timestamp >= entry_anchor
            if activity_after_entry:
                activity_type = _extract_wallet_activity_type(wallet_close_activity)
                realized_pnl = safe_float(closed_position.get("realizedPnl"), None) if isinstance(closed_position, dict) else None
                close_price = _extract_wallet_activity_price(wallet_close_activity)
                if close_price is None and entry_fill_size > 0.0 and entry_fill_notional > 0.0 and realized_pnl is not None:
                    close_price = (entry_fill_notional + realized_pnl) / entry_fill_size
                if close_price is None and wallet_settlement_price is not None:
                    close_price = wallet_settlement_price
                if close_price is None and entry_fill_price is not None and entry_fill_price > 0.0:
                    close_price = entry_fill_price
                if realized_pnl is None and close_price is not None and entry_fill_size > 0.0:
                    realized_pnl = (entry_fill_size * close_price) - entry_fill_notional
                if is_recovered_sell_authority:
                    details.append(
                        {
                            "order_id": row.id,
                            "market_id": row.market_id,
                            "close_trigger": "wallet_activity_auxiliary_exit",
                            "next_status": "resolved",
                        }
                    )
                    if not dry_run:
                        row.status = "resolved"
                        row.updated_at = now
                        payload["authority_recovery_state"] = {
                            "kind": "auxiliary_exit",
                            "source": "wallet_activity_api",
                            "resolved_at": _iso_utc(now),
                            "activity_type": activity_type,
                            "transaction_hash": _extract_wallet_activity_tx_hash(wallet_close_activity) or None,
                        }
                        row.payload_json = payload
                        _apply_position_close_verification(
                            session,
                            row=row,
                            now=now,
                            event_type="wallet_activity_auxiliary_exit",
                            payload_json=payload,
                        )
                        state_updates += 1
                    held += 1
                    continue
                if realized_pnl is not None:
                    close_trigger = "wallet_activity"
                    next_status = _status_for_close(pnl=realized_pnl, close_trigger=close_trigger)
                    details.append(
                        {
                            "order_id": row.id,
                            "market_id": row.market_id,
                            "close_trigger": close_trigger,
                            "close_price": close_price,
                            "realized_pnl": realized_pnl,
                            "next_status": next_status,
                            "wallet_activity_type": activity_type,
                        }
                    )
                    total_realized_pnl += realized_pnl
                    by_status[next_status] = int(by_status.get(next_status, 0)) + 1
                    would_close += 1
                    if not dry_run:
                        row.status = next_status
                        row.actual_profit = realized_pnl
                        row.updated_at = now
                        pending_exit = payload.get("pending_live_exit")
                        pending_exit = pending_exit if isinstance(pending_exit, dict) else {}
                        if pending_exit:
                            pending_exit["status"] = "superseded_wallet_activity"
                            pending_exit["resolved_at"] = _iso_utc(now)
                            payload["pending_live_exit"] = pending_exit
                        payload["position_close"] = {
                            "close_price": close_price,
                            "price_source": "wallet_activity",
                            "close_trigger": close_trigger,
                            "realized_pnl": realized_pnl,
                            "filled_size": entry_fill_size if entry_fill_size > 0.0 else _extract_wallet_activity_size(wallet_close_activity),
                            "closed_at": _iso_utc(now),
                            "reason": reason,
                            "wallet_activity_type": activity_type,
                            "wallet_activity_timestamp": (
                                _iso_utc(activity_timestamp)
                                if isinstance(activity_timestamp, datetime)
                                else None
                            ),
                            "wallet_activity_transaction_hash": _extract_wallet_activity_tx_hash(wallet_close_activity) or None,
                            "wallet_activity_id": str(wallet_close_activity.get("id") or "").strip() or None,
                            "wallet_trade_id": (
                                str(wallet_close_activity.get("tradeID") or wallet_close_activity.get("trade_id") or "").strip()
                                or None
                            ),
                        }
                        row.payload_json = payload
                        _apply_position_close_verification(
                            session,
                            row=row,
                            now=now,
                            event_type="wallet_activity_close",
                            payload_json=payload,
                        )
                        hot_state.record_order_resolved(
                            trader_id=trader_id,
                            mode=str(row.mode or ""),
                            order_id=str(row.id or ""),
                            market_id=str(row.market_id or ""),
                            direction=str(row.direction or ""),
                            source=str(row.source or ""),
                            status=next_status,
                            actual_profit=realized_pnl,
                            payload=payload,
                            copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                        )
                        closed += 1
                    continue
        if (
            token_id
            and wallet_position_size <= _WALLET_SIZE_EPSILON
            and wallet_closed_positions_loaded
            and isinstance(closed_position, dict)
        ):
            realized_pnl = safe_float(closed_position.get("realizedPnl"), None)
            close_price = None
            if entry_fill_size > 0.0 and entry_fill_notional > 0.0 and realized_pnl is not None:
                close_price = (entry_fill_notional + realized_pnl) / entry_fill_size
            close_trigger = "wallet_summary_recovery"
            next_status = "resolved" if is_recovered_sell_authority else _status_for_close(pnl=realized_pnl or 0.0, close_trigger=close_trigger)
            details.append(
                {
                    "order_id": row.id,
                    "market_id": row.market_id,
                    "close_trigger": close_trigger,
                    "close_price": close_price,
                    "realized_pnl": realized_pnl,
                    "next_status": next_status,
                    "summary_only": True,
                }
            )
            if realized_pnl is not None:
                total_realized_pnl += realized_pnl
            by_status[next_status] = int(by_status.get(next_status, 0)) + 1
            would_close += 1
            if not dry_run:
                row.status = next_status
                if realized_pnl is not None:
                    row.actual_profit = realized_pnl
                row.updated_at = now
                pending_exit = payload.get("pending_live_exit")
                pending_exit = pending_exit if isinstance(pending_exit, dict) else {}
                if pending_exit:
                    pending_exit["status"] = "superseded_closed_position"
                    pending_exit["resolved_at"] = _iso_utc(now)
                    payload["pending_live_exit"] = pending_exit
                payload["position_close"] = {
                    "close_price": close_price,
                    "price_source": "closed_positions_api",
                    "close_trigger": close_trigger,
                    "realized_pnl": realized_pnl,
                    "filled_size": entry_fill_size if entry_fill_size > 0.0 else safe_float(closed_position.get("totalBought"), None),
                    "closed_at": _iso_utc(now),
                    "reason": reason,
                    "wallet_closed_position_timestamp": safe_float(closed_position.get("timestamp"), None),
                }
                row.payload_json = payload
                _apply_position_close_verification(
                    session,
                    row=row,
                    now=now,
                    event_type="summary_close_recovery",
                    payload_json=payload,
                )
                hot_state.record_order_resolved(
                    trader_id=trader_id,
                    mode=str(row.mode or ""),
                    order_id=str(row.id or ""),
                    market_id=str(row.market_id or ""),
                    direction=str(row.direction or ""),
                    source=str(row.source or ""),
                    status=next_status,
                    actual_profit=realized_pnl,
                    payload=payload,
                    copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                )
                closed += 1
            continue
        pending_outcome_idx = _direction_outcome_index(row.direction)
        pending_market_info = market_info_by_id.get(str(row.market_id or ""))
        pending_market_tradable = polymarket_client.is_market_tradable(pending_market_info, now=now)
        pending_winning_idx = _extract_winning_outcome_index(pending_market_info)
        pending_winning_idx_inferred = False
        if pending_winning_idx is None and resolution_infer_from_prices:
            inferred_pending_idx = _extract_winning_outcome_index_from_prices(
                pending_market_info,
                market_tradable=pending_market_tradable,
                settle_floor=resolution_settle_floor,
            )
            if inferred_pending_idx is not None:
                pending_winning_idx = inferred_pending_idx
                pending_winning_idx_inferred = True

        pending_signal_payload = signal_payloads.get(str(row.signal_id), {})
        pending_market_side_price = (
            _extract_market_side_price(pending_market_info, pending_outcome_idx)
            if pending_outcome_idx is not None
            else None
        )
        pending_ws_side_price = ws_mid_prices.get(token_id) if token_id else None
        pending_clob_side_price = clob_mid_prices.get(token_id) if token_id else None
        pending_current_price = (
            pending_ws_side_price
            if pending_ws_side_price is not None
            else (
                pending_clob_side_price
                if pending_clob_side_price is not None
                else (pending_market_side_price if pending_market_side_price is not None else wallet_mark_price)
            )
        )
        pending_current_price = _state_price_floor(pending_current_price)
        pending_current_price_source = (
            "ws_mid"
            if pending_ws_side_price is not None
            else (
                "clob_midpoint"
                if pending_clob_side_price is not None
                else (
                    "market_mark"
                    if pending_market_side_price is not None
                    else ("wallet_mark" if wallet_mark_price is not None else None)
                )
            )
        )
        pending_position_state = _extract_position_state(payload)
        pending_prev_high = safe_float(pending_position_state.get("highest_price"))
        pending_prev_low = safe_float(pending_position_state.get("lowest_price"))
        pending_prev_last_mark = safe_float(pending_position_state.get("last_mark_price"))
        pending_prev_mark_source = str(pending_position_state.get("last_mark_source") or "")
        pending_prev_marked_at = _parse_iso_utc_naive(pending_position_state.get("last_marked_at"))
        pending_highest_price = pending_prev_high
        pending_lowest_price = pending_prev_low
        if pending_current_price is not None:
            if pending_highest_price is None:
                pending_highest_price = pending_current_price
            else:
                pending_highest_price = max(pending_highest_price, pending_current_price)
            if pending_lowest_price is None:
                pending_lowest_price = pending_current_price
            else:
                pending_lowest_price = min(pending_lowest_price, pending_current_price)
        pending_mark_updated_at_value = (
            _iso_utc(now)
            if pending_current_price is not None
            else (
                _iso_utc(pending_prev_marked_at.replace(tzinfo=timezone.utc))
                if pending_prev_marked_at is not None
                else ""
            )
        )
        pending_next_state = {
            "highest_price": _state_price_floor(pending_highest_price),
            "lowest_price": _state_price_floor(pending_lowest_price),
            "last_mark_price": (
                pending_current_price
                if pending_current_price is not None
                else _state_price_floor(pending_prev_last_mark)
            ),
            "last_mark_source": str(pending_current_price_source or pending_prev_mark_source),
            "last_marked_at": pending_mark_updated_at_value,
            "last_exit_evaluated_at": _iso_utc(now),
        }
        pending_state_changed = False
        if pending_current_price is not None:
            pending_mark_stale = (
                pending_prev_marked_at is None
                or (now_naive - pending_prev_marked_at).total_seconds() >= mark_touch_interval_seconds
            )
            pending_state_changed = (
                pending_prev_last_mark is None
                or abs(pending_prev_last_mark - pending_current_price) > 1e-9
                or pending_prev_high is None
                or pending_prev_low is None
                or abs((pending_prev_high or 0.0) - (pending_highest_price or 0.0)) > 1e-9
                or abs((pending_prev_low or 0.0) - (pending_lowest_price or 0.0)) > 1e-9
                or pending_prev_mark_source != str(pending_current_price_source or "")
                or pending_mark_stale
            )

        # External/manual flatten convergence:
        # if wallet inventory is flat and we have concrete wallet SELL evidence
        # for this token after entry, treat the position as externally closed.
        if (
            token_id
            and entry_fill_size > 0.0
            and wallet_position_size <= _WALLET_SIZE_EPSILON
            and pending_winning_idx is None
            and wallet_settlement_price is None
            and isinstance(latest_wallet_sell_trade, dict)
        ):
            trade_ts = latest_wallet_sell_trade.get("timestamp")
            row_created = row.created_at
            trade_after_entry = True
            if isinstance(trade_ts, datetime) and isinstance(row_created, datetime):
                entry_anchor = row_created
                if entry_anchor.tzinfo is not None:
                    entry_anchor = entry_anchor.astimezone(timezone.utc).replace(tzinfo=None)
                trade_after_entry = trade_ts >= entry_anchor
            if trade_after_entry:
                close_price = safe_float(latest_wallet_sell_trade.get("price"))
                if close_price is None or close_price <= 0.0:
                    close_price = (
                        wallet_mark_price
                        if wallet_mark_price is not None and wallet_mark_price > 0.0
                        else entry_fill_price
                    )
                if close_price is not None and close_price > 0.0:
                    close_qty = entry_fill_size
                    close_notional = close_qty * close_price
                    close_trigger = "external_wallet_flatten"
                    pnl = close_notional - entry_fill_notional
                    next_status = _status_for_close(pnl=pnl, close_trigger=close_trigger)
                    details.append(
                        {
                            "order_id": row.id,
                            "market_id": row.market_id,
                            "close_trigger": close_trigger,
                            "close_price": close_price,
                            "realized_pnl": pnl,
                            "next_status": next_status,
                            "wallet_trade_id": latest_wallet_sell_trade.get("trade_id"),
                        }
                    )
                    would_close += 1
                    total_realized_pnl += pnl
                    by_status[next_status] = int(by_status.get(next_status, 0)) + 1
                    if not dry_run:
                        pending_exit_local = payload.get("pending_live_exit")
                        if isinstance(pending_exit_local, dict):
                            pending_exit_local["status"] = "superseded_external"
                            pending_exit_local["resolved_at"] = _iso_utc(now)
                            payload["pending_live_exit"] = pending_exit_local
                        row.status = next_status
                        row.actual_profit = pnl
                        row.updated_at = now
                        payload["position_close"] = {
                            "close_price": close_price,
                            "price_source": "wallet_trade",
                            "close_trigger": close_trigger,
                            "realized_pnl": pnl,
                            "filled_size": close_qty,
                            "closed_at": _iso_utc(now),
                            "reason": reason,
                            "wallet_trade_id": str(latest_wallet_sell_trade.get("trade_id") or ""),
                            "wallet_trade_timestamp": (
                                _iso_utc(latest_wallet_sell_trade.get("timestamp"))
                                if isinstance(latest_wallet_sell_trade.get("timestamp"), datetime)
                                else None
                            ),
                            "wallet_activity_transaction_hash": (
                                str(
                                    latest_wallet_sell_trade.get("transactionHash")
                                    or latest_wallet_sell_trade.get("txHash")
                                    or latest_wallet_sell_trade.get("tx_hash")
                                    or ""
                                ).strip()
                                or None
                            ),
                        }
                        reverse_signal_id, reverse_source = await _emit_armed_reverse_signal(
                            session,
                            row=row,
                            payload=payload,
                            signal_payload=pending_signal_payload,
                            market_info=pending_market_info,
                            close_trigger=close_trigger,
                            realized_pnl=pnl,
                            now=now,
                        )
                        if reverse_signal_id and reverse_source:
                            reverse_signal_ids_by_source.setdefault(reverse_source, []).append(reverse_signal_id)
                        row.payload_json = payload
                        _apply_position_close_verification(
                            session,
                            row=row,
                            now=now,
                            event_type="wallet_trade_close",
                            payload_json=payload,
                        )
                        hot_state.record_order_resolved(
                            trader_id=trader_id,
                            mode=str(row.mode or ""),
                            order_id=str(row.id or ""),
                            market_id=str(row.market_id or ""),
                            direction=str(row.direction or ""),
                            source=str(row.source or ""),
                            status=next_status,
                            actual_profit=pnl,
                            payload=payload,
                            copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                        )
                        closed += 1
                    continue

        # ── Wallet-absent close: position vanished from on-chain wallet ──
        # If wallet data loaded successfully but this token is completely
        # absent (size=0 / redeemed), and there is no sell trade (i.e. auto-
        # redemption or external close), close the order.  Use the best
        # available price: market mark, wallet mark, or entry price.
        # Skip when there is already an active exit submitted to the provider
        # (identified by a CLOB order ID) — the pending-exit reconciliation
        # path handles that case and knows the real fill price.
        _pending_exit_tmp = payload.get("pending_live_exit")
        _has_active_provider_exit = isinstance(_pending_exit_tmp, dict) and bool(
            _pending_exit_provider_clob_id(_pending_exit_tmp)
        )
        _has_working_provider_entry = _provider_entry_order_still_working(payload)
        # Guard: do not wallet-absent-close orders placed in the last 120
        # seconds.  Polymarket's data API can lag behind the CLOB — a just-
        # placed buy may not appear in get_wallet_positions() yet, causing a
        # false wallet_absent_close that records a phantom loss and triggers
        # an immediate re-buy of the same market.
        _wab_order_age_anchor = row.executed_at or row.updated_at or row.created_at
        _wab_min_age_ok = (
            _wab_order_age_anchor is None
            or (now - _wab_order_age_anchor).total_seconds() >= 120.0
        )
        if (
            token_id
            and entry_fill_size > 0.0
            and wallet_positions_index_present
            and not wallet_position_observed
            and pending_winning_idx is None
            and wallet_settlement_price is None
            and not isinstance(latest_wallet_sell_trade, dict)
            and not _has_active_provider_exit
            and not _has_working_provider_entry
            and _wab_min_age_ok
        ):
            details.append(
                {
                    "order_id": row.id,
                    "market_id": row.market_id,
                    "close_trigger": "wallet_absent_close",
                    "next_status": row.status,
                    "note": "wallet_absent_without_verified_close_evidence",
                }
            )

        pending_exit = payload.get("pending_live_exit")
        pending_exit_status = (
            str(pending_exit.get("status") or "").strip().lower() if isinstance(pending_exit, dict) else ""
        )
        pending_exit_kind = (
            str(pending_exit.get("kind") or "").strip().lower() if isinstance(pending_exit, dict) else ""
        )

        def _attach_pending_state(target_payload: dict[str, Any]) -> None:
            if pending_state_changed:
                target_payload["position_state"] = pending_next_state

        # ── Force mark-to-market: supersede any existing pending exit ──
        # When a manual sell is requested, override whatever exit state
        # exists so the force_mark_to_market path is always reached below.
        if force_mark_to_market and isinstance(pending_exit, dict) and pending_exit_status:
            provider_clob_id = _pending_exit_provider_clob_id(pending_exit)
            if provider_clob_id and pending_exit_status in {"submitted", "pending"}:
                try:
                    async with release_conn(session):
                        await live_execution_service.cancel_order(provider_clob_id)
                except Exception:
                    pass
            pending_exit["status"] = "superseded_manual_sell"
            pending_exit["superseded_at"] = _iso_utc(now)
            payload["superseded_pending_exit"] = pending_exit
            payload.pop("pending_live_exit", None)
            if not dry_run:
                row.payload_json = payload
                row.updated_at = now
            pending_exit = None
            pending_exit_status = ""
            pending_exit_kind = ""

        # ── Submitted/pending exit reconciliation ──────────────────────
        # For live exits we trust provider fill truth; a terminal local close
        # only happens once provider fill size reaches the required threshold.
        if isinstance(pending_exit, dict) and pending_exit_status in {"submitted", "pending"}:
            if pending_winning_idx is not None and pending_outcome_idx is not None:
                _fill_not, _fill_sz, _fill_px = _extract_live_fill_metrics(payload)
                _ep = _fill_px if _fill_px and _fill_px > 0 else safe_float(row.effective_price)
                if _ep is None or _ep <= 0:
                    _ep = safe_float(row.entry_price)
                _not = _fill_not if _fill_not > 0 else (safe_float(row.notional_usd) or 0.0)
                _qty = _fill_sz if _fill_sz > 0 else (_not / _ep if _ep and _ep > 0 else 0.0)
                cp = 1.0 if pending_winning_idx == pending_outcome_idx else 0.0
                close_trigger = "resolution_inferred" if pending_winning_idx_inferred else "resolution"
                _pnl = (_qty * cp) - _not if _qty > 0 and _not > 0 else 0.0
                ns = _status_for_close(pnl=_pnl, close_trigger=close_trigger)
                if not dry_run:
                    row.status = ns
                    row.actual_profit = _pnl
                    row.updated_at = now
                    pending_exit["status"] = "superseded_resolution"
                    pending_exit["resolved_at"] = _iso_utc(now)
                    payload["pending_live_exit"] = pending_exit
                    payload["position_close"] = {
                        "close_price": cp,
                        "price_source": "resolved_settlement",
                        "close_trigger": close_trigger,
                        "realized_pnl": _pnl,
                        "closed_at": _iso_utc(now),
                        "reason": reason,
                    }
                    row.payload_json = payload
                    hot_state.record_order_resolved(
                        trader_id=trader_id,
                        mode=str(row.mode or ""),
                        order_id=str(row.id or ""),
                        market_id=str(row.market_id or ""),
                        direction=str(row.direction or ""),
                        source=str(row.source or ""),
                        status=ns,
                        actual_profit=_pnl,
                        payload=payload,
                        copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                    )
                    closed += 1
                total_realized_pnl += _pnl
                by_status[ns] = int(by_status.get(ns, 0)) + 1
                would_close += 1
                details.append(
                    {
                        "order_id": row.id,
                        "market_id": row.market_id,
                        "close_trigger": close_trigger,
                        "close_price": cp,
                        "realized_pnl": _pnl,
                        "next_status": ns,
                    }
                )
                continue

            if wallet_settlement_price is not None:
                _fill_not, _fill_sz, _fill_px = _extract_live_fill_metrics(payload)
                _ep = _fill_px if _fill_px and _fill_px > 0 else safe_float(row.effective_price)
                if _ep is None or _ep <= 0:
                    _ep = safe_float(row.entry_price)
                _not = _fill_not if _fill_not > 0 else (safe_float(row.notional_usd) or 0.0)
                _qty = _fill_sz if _fill_sz > 0 else (_not / _ep if _ep and _ep > 0 else 0.0)
                cp = wallet_settlement_price
                _pnl = (_qty * cp) - _not if _qty > 0 and _not > 0 else 0.0
                ns = _status_for_close(pnl=_pnl, close_trigger="resolution")
                if not dry_run:
                    row.status = ns
                    row.actual_profit = _pnl
                    row.updated_at = now
                    pending_exit["status"] = "superseded_resolution"
                    pending_exit["resolved_at"] = _iso_utc(now)
                    payload["pending_live_exit"] = pending_exit
                    payload["position_close"] = {
                        "close_price": cp,
                        "price_source": "wallet_redeemable_mark",
                        "close_trigger": "resolution",
                        "realized_pnl": _pnl,
                        "closed_at": _iso_utc(now),
                        "reason": reason,
                    }
                    row.payload_json = payload
                    hot_state.record_order_resolved(
                        trader_id=trader_id,
                        mode=str(row.mode or ""),
                        order_id=str(row.id or ""),
                        market_id=str(row.market_id or ""),
                        direction=str(row.direction or ""),
                        source=str(row.source or ""),
                        status=ns,
                        actual_profit=_pnl,
                        payload=payload,
                        copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                    )
                    closed += 1
                total_realized_pnl += _pnl
                by_status[ns] = int(by_status.get(ns, 0)) + 1
                would_close += 1
                details.append(
                    {
                        "order_id": row.id,
                        "market_id": row.market_id,
                        "close_trigger": "resolution",
                        "close_price": cp,
                        "realized_pnl": _pnl,
                        "next_status": ns,
                    }
                )
                continue

            provider_clob_order_id = _pending_exit_provider_clob_id(pending_exit)
            if provider_clob_order_id:
                pending_exit["provider_clob_order_id"] = provider_clob_order_id
            snapshot = pending_exit_snapshots.get(provider_clob_order_id) if provider_clob_order_id else None
            if snapshot is None and provider_clob_order_id:
                snapshot = pending_exit_snapshot_fallbacks.get(provider_clob_order_id)
            snapshot_status = str((snapshot or {}).get("normalized_status") or "").strip().lower()
            snapshot_filled_size = max(0.0, safe_float((snapshot or {}).get("filled_size"), 0.0) or 0.0)
            snapshot_fill_price = safe_float((snapshot or {}).get("average_fill_price"))
            if snapshot_fill_price is None or snapshot_fill_price <= 0:
                snapshot_fill_price = safe_float((snapshot or {}).get("limit_price"))
            if not snapshot_status:
                snapshot_status = str(pending_exit.get("provider_status") or "").strip().lower()
            if snapshot_filled_size <= 0.0:
                snapshot_filled_size = max(0.0, safe_float(pending_exit.get("filled_size"), 0.0) or 0.0)
            if snapshot_fill_price is None or snapshot_fill_price <= 0.0:
                snapshot_fill_price = safe_float(pending_exit.get("average_fill_price"))
            required_exit_size = max(0.0, safe_float(pending_exit.get("exit_size"), 0.0) or 0.0)
            reopened_partial_exit = (
                str(pending_exit.get("reopen_reason") or "").strip().lower() == "partial_exit_fill_below_threshold"
            )
            _fill_not, _fill_sz, _fill_px = _extract_live_fill_metrics(payload)
            _ep = _fill_px if _fill_px and _fill_px > 0 else safe_float(row.effective_price)
            if _ep is None or _ep <= 0:
                _ep = safe_float(row.entry_price)
            _not = _fill_not if _fill_not > 0 else (safe_float(row.notional_usd) or 0.0)
            entry_position_size = _fill_sz if _fill_sz > 0 else (_not / _ep if _ep and _ep > 0 else 0.0)
            if entry_position_size > 0.0:
                required_exit_size = max(required_exit_size, entry_position_size)
            if required_exit_size > 0.0:
                pending_exit["exit_size"] = float(required_exit_size)

            threshold_ratio = _pending_exit_fill_threshold(pending_exit)
            fill_ratio = (
                (snapshot_filled_size / required_exit_size)
                if required_exit_size > 0.0
                else (1.0 if snapshot_status == "filled" and snapshot_filled_size > 0.0 else 0.0)
            )
            pending_exit["provider_status"] = snapshot_status or pending_exit.get("provider_status")
            if snapshot_filled_size > 0.0:
                pending_exit["filled_size"] = float(snapshot_filled_size)
            if snapshot_fill_price is not None and snapshot_fill_price > 0.0:
                pending_exit["average_fill_price"] = float(snapshot_fill_price)
            if fill_ratio > 0.0:
                pending_exit["fill_ratio"] = float(fill_ratio)
            if snapshot is not None:
                pending_exit["last_snapshot_at"] = _iso_utc(now)

            terminal_provider_status = snapshot_status in {"filled", "matched", "executed"}
            provider_status_unknown = snapshot_status in {"", "missing", "invalid", "unknown"}
            wallet_flat_confirmed = wallet_position_size <= _WALLET_SIZE_EPSILON and (
                wallet_position_observed
                or wallet_positions_index_present
                or isinstance(latest_wallet_sell_trade, dict)
            )
            close_fill_threshold_met = (
                required_exit_size > 0.0
                and terminal_provider_status
                and snapshot_filled_size >= (required_exit_size * threshold_ratio)
            )
            close_fill_threshold_with_wallet_confirmation = (
                required_exit_size > 0.0
                and provider_status_unknown
                and snapshot_filled_size >= (required_exit_size * threshold_ratio)
                and wallet_flat_confirmed
            )
            close_fill_terminal_with_wallet_confirmation = (
                required_exit_size > 0.0
                and terminal_provider_status
                and snapshot_filled_size > 0.0
                and wallet_flat_confirmed
                and not reopened_partial_exit
            )
            close_fill_unknown_but_wallet_flat = (
                required_exit_size <= 0.0 and terminal_provider_status and wallet_flat_confirmed
            )
            if (
                close_fill_threshold_met
                or close_fill_threshold_with_wallet_confirmation
                or close_fill_terminal_with_wallet_confirmation
                or close_fill_unknown_but_wallet_flat
            ):
                base_qty = _fill_sz if _fill_sz > 0 else (_not / _ep if _ep and _ep > 0 else 0.0)
                close_qty = snapshot_filled_size if snapshot_filled_size > 0 else required_exit_size
                if close_qty <= 0.0:
                    close_qty = base_qty
                if base_qty <= 0.0 or _not <= 0.0:
                    held += 1
                    if not dry_run:
                        payload["pending_live_exit"] = pending_exit
                        _attach_pending_state(payload)
                        row.payload_json = payload
                        row.updated_at = now
                        state_updates += 1
                    continue
                close_qty = min(base_qty, close_qty if close_qty > 0.0 else base_qty)
                cost_basis = _not * (close_qty / base_qty) if base_qty > 0 else _not
                cp = snapshot_fill_price
                if cp is None or cp <= 0.0:
                    cp = safe_float(pending_exit.get("close_price")) or _ep
                close_trigger = str(pending_exit.get("close_trigger") or "live_exit_fill")
                _pnl = (close_qty * cp) - cost_basis
                ns = _status_for_close(pnl=_pnl, close_trigger=close_trigger)
                if not dry_run:
                    row.status = ns
                    row.actual_profit = _pnl
                    row.updated_at = now
                    pending_exit["status"] = "filled"
                    if close_fill_terminal_with_wallet_confirmation and not close_fill_threshold_met:
                        pending_exit["allow_partial_fill_terminal"] = True
                    pending_exit["filled_at"] = _iso_utc(now)
                    payload["pending_live_exit"] = pending_exit
                    payload["position_close"] = {
                        "close_price": cp,
                        "price_source": "provider_exit_fill",
                        "close_trigger": close_trigger,
                        "realized_pnl": _pnl,
                        "cost_basis_usd": cost_basis,
                        "settlement_proceeds_usd": close_qty * cp,
                        "filled_size": close_qty,
                        "closed_at": _iso_utc(now),
                        "reason": reason,
                    }
                    reverse_signal_id, reverse_source = await _emit_armed_reverse_signal(
                        session,
                        row=row,
                        payload=payload,
                        signal_payload=pending_signal_payload,
                        market_info=pending_market_info,
                        close_trigger=close_trigger,
                        realized_pnl=_pnl,
                        now=now,
                    )
                    if reverse_signal_id and reverse_source:
                        reverse_signal_ids_by_source.setdefault(reverse_source, []).append(reverse_signal_id)
                    row.payload_json = payload
                    _apply_position_close_verification(
                        session,
                        row=row,
                        now=now,
                        event_type="provider_exit_fill",
                        payload_json=payload,
                    )
                    hot_state.record_order_resolved(
                        trader_id=trader_id,
                        mode=str(row.mode or ""),
                        order_id=str(row.id or ""),
                        market_id=str(row.market_id or ""),
                        direction=str(row.direction or ""),
                        source=str(row.source or ""),
                        status=ns,
                        actual_profit=_pnl,
                        payload=payload,
                        copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                    )
                    closed += 1
                total_realized_pnl += _pnl
                by_status[ns] = int(by_status.get(ns, 0)) + 1
                would_close += 1
                details.append(
                    {
                        "order_id": row.id,
                        "market_id": row.market_id,
                        "close_trigger": close_trigger,
                        "close_price": cp,
                        "realized_pnl": _pnl,
                        "next_status": ns,
                    }
                )
                continue

            pending_close_trigger = str(pending_exit.get("close_trigger") or "").strip().lower()
            working_provider_status = snapshot_status in {
                "open",
                "working",
                "active",
                "live",
                "unmatched",
                "partially_filled",
            }
            rapid_exit_requote_enabled = _is_rapid_close_trigger(pending_close_trigger)
            take_profit_requote_enabled = pending_exit_kind == "take_profit_limit"
            partial_fill_terminal_requires_retry = (
                required_exit_size > 0.0
                and terminal_provider_status
                and snapshot_filled_size > 0.0
                and fill_ratio + 1e-9 < threshold_ratio
                and wallet_position_size > _WALLET_SIZE_EPSILON
            )
            if partial_fill_terminal_requires_retry:
                retry_delay_seconds = (
                    0.5
                    if rapid_exit_requote_enabled or take_profit_requote_enabled
                    else max(1.0, _failed_exit_retry_delay_seconds("provider_partial_fill_terminal"))
                )
                if not dry_run:
                    pending_exit["status"] = "failed"
                    pending_exit["retry_count"] = int(pending_exit.get("retry_count", 0) or 0) + 1
                    pending_exit["last_attempt_at"] = _iso_utc(now)
                    pending_exit["last_error"] = (
                        "provider_partial_fill_terminal:"
                        f"{snapshot_status or 'unknown'}:{snapshot_filled_size:.6f}/{required_exit_size:.6f}"
                    )
                    pending_exit["next_retry_at"] = _iso_utc(now + timedelta(seconds=retry_delay_seconds))
                    payload["pending_live_exit"] = pending_exit
                    _attach_pending_state(payload)
                    row.payload_json = payload
                    row.updated_at = now
                    state_updates += 1
                held += 1
                continue

            reprice_attempts = int(safe_float(pending_exit.get("reprice_attempts"), 0) or 0)
            max_reprice_attempts_default = (
                8
                if rapid_exit_requote_enabled
                else (4 if take_profit_requote_enabled else 6)
            )
            max_reprice_attempts = max(
                1,
                int(safe_float(pending_exit.get("max_reprice_attempts"), max_reprice_attempts_default) or max_reprice_attempts_default),
            )
            last_attempt_dt = _parse_iso_utc_naive(
                pending_exit.get("last_attempt_at") or pending_exit.get("triggered_at")
            )
            reprice_cooldown_default = 0.5 if rapid_exit_requote_enabled else (1.0 if take_profit_requote_enabled else 2.0)
            reprice_cooldown_seconds = max(
                0.5,
                safe_float(pending_exit.get("reprice_cooldown_seconds"), reprice_cooldown_default) or reprice_cooldown_default,
            )
            cooldown_elapsed = (
                last_attempt_dt is None or (now_naive - last_attempt_dt).total_seconds() >= reprice_cooldown_seconds
            )
            stale_requote_after_seconds_default = (
                1.0
                if rapid_exit_requote_enabled
                else (2.0 if take_profit_requote_enabled else 6.0)
            )
            stale_requote_after_seconds = max(
                0.5,
                safe_float(
                    pending_exit.get("reprice_stale_after_seconds"),
                    stale_requote_after_seconds_default,
                )
                or stale_requote_after_seconds_default,
            )
            stale_requote_due = last_attempt_dt is not None and (
                (now_naive - last_attempt_dt).total_seconds() >= stale_requote_after_seconds
            )
            reprice_enabled = rapid_exit_requote_enabled or take_profit_requote_enabled or stale_requote_due
            if (
                reprice_enabled
                and working_provider_status
                and required_exit_size > 0.0
                and fill_ratio + 1e-9 < threshold_ratio
                and reprice_attempts < max_reprice_attempts
                and cooldown_elapsed
            ):
                remaining_exit_size = _remaining_exit_size(
                    required_exit_size=required_exit_size,
                    pending_exit=pending_exit,
                    wallet_position_size=wallet_position_size,
                )
                if token_id and remaining_exit_size > 0.0:
                    cancel_target = provider_clob_order_id
                    cancel_ok = True
                    if cancel_target:
                        async with release_conn(session):
                            try:
                                cancel_ok = bool(await live_execution_service.cancel_order(cancel_target))
                            except Exception:
                                cancel_ok = False

                    pending_exit["reprice_attempts"] = reprice_attempts + 1
                    pending_exit["last_reprice_at"] = _iso_utc(now)
                    pending_exit["last_attempt_at"] = _iso_utc(now)

                    if cancel_ok:
                        try:
                            from services.live_execution_adapter import execute_live_order

                            base_min_order_size_usd = _resolve_position_min_order_size_usd(
                                trader_params=params,
                                payload=payload,
                                mode="live",
                            )
                            min_order_size_usd = _effective_exit_min_order_size_usd(
                                base_min_order_size_usd,
                                pending_close_trigger,
                            )
                            pending_exit["current_mid_price"] = pending_current_price
                            take_profit_floor_price = _take_profit_price_floor(pending_exit)
                            fallback_exit_price = (
                                pending_current_price
                                if pending_current_price is not None and pending_current_price > 0.0
                                else (
                                    safe_float(pending_exit.get("close_price"))
                                    if safe_float(pending_exit.get("close_price")) is not None
                                    else 0.01
                                )
                            )
                            if take_profit_floor_price is None and pending_exit_kind == "take_profit_limit":
                                pending_exit["close_trigger"] = "tp_underwater_market_sell"
                                pending_exit["post_only"] = False
                                async with release_conn(session):
                                    try:
                                        await live_execution_service.prepare_sell_balance_allowance(token_id)
                                    except Exception:
                                        pass
                                    exec_result = await asyncio.wait_for(
                                        execute_live_order(
                                            token_id=token_id,
                                            side="SELL",
                                            size=remaining_exit_size,
                                            fallback_price=fallback_exit_price,
                                            min_order_size_usd=min_order_size_usd,
                                            time_in_force="IOC",
                                            post_only=False,
                                            resolve_live_price=True,
                                            enforce_fallback_bound=True,
                                        ),
                                        timeout=_LIVE_EXIT_ORDER_TIMEOUT_SECONDS,
                                    )
                            else:
                                if take_profit_floor_price is not None:
                                    fallback_exit_price = max(fallback_exit_price, take_profit_floor_price)
                                    pending_exit["close_price"] = float(fallback_exit_price)
                                    pending_exit["post_only"] = True
                                requote_as_rapid_exit = rapid_exit_requote_enabled
                                async with release_conn(session):
                                    try:
                                        await live_execution_service.prepare_sell_balance_allowance(token_id)
                                    except Exception:
                                        pass
                                    exec_result = await asyncio.wait_for(
                                        execute_live_order(
                                            token_id=token_id,
                                            side="SELL",
                                            size=remaining_exit_size,
                                            fallback_price=fallback_exit_price,
                                            min_order_size_usd=min_order_size_usd,
                                            time_in_force="IOC" if requote_as_rapid_exit else "GTC",
                                            post_only=bool(take_profit_requote_enabled and not requote_as_rapid_exit),
                                            resolve_live_price=requote_as_rapid_exit,
                                            enforce_fallback_bound=True,
                                        ),
                                        timeout=_LIVE_EXIT_ORDER_TIMEOUT_SECONDS,
                                    )
                            if exec_result.status in {"executed", "open", "submitted"}:
                                pending_exit["status"] = "submitted"
                                pending_exit["allowance_error_count"] = 0
                                pending_exit["exit_order_id"] = str(exec_result.order_id or "")
                                pending_exit["provider_clob_order_id"] = str(
                                    (exec_result.payload or {}).get("clob_order_id") or ""
                                )
                                pending_exit["provider_status"] = str(
                                    (exec_result.payload or {}).get("trading_status") or ""
                                ).strip().lower()
                                incremental_fill = max(
                                    0.0,
                                    safe_float((exec_result.payload or {}).get("filled_size"), 0.0) or 0.0,
                                )
                                if incremental_fill > 0.0:
                                    prior_filled = max(
                                        0.0,
                                        safe_float(pending_exit.get("filled_size"), 0.0) or 0.0,
                                    )
                                    pending_exit["filled_size"] = float(prior_filled + incremental_fill)
                                    pending_exit["fill_ratio"] = float(
                                        (prior_filled + incremental_fill) / required_exit_size
                                    )
                                reprice_fill_price = safe_float((exec_result.payload or {}).get("average_fill_price"))
                                if reprice_fill_price is not None and reprice_fill_price > 0.0:
                                    pending_exit["average_fill_price"] = float(reprice_fill_price)
                                pending_exit["next_retry_at"] = None
                            else:
                                pending_exit["status"] = "failed"
                                pending_exit["retry_count"] = int(pending_exit.get("retry_count", 0) or 0) + 1
                                pending_exit["last_error"] = str(exec_result.error_message or "exit_requote_failed")
                                _bump_allowance_error_counter(pending_exit, pending_exit["last_error"])
                                pending_exit["next_retry_at"] = _iso_utc(
                                    now + timedelta(seconds=_failed_exit_retry_delay_seconds(pending_exit["last_error"]))
                                )
                        except Exception as exc:
                            pending_exit["status"] = "failed"
                            pending_exit["retry_count"] = int(pending_exit.get("retry_count", 0) or 0) + 1
                            pending_exit["last_error"] = str(exc)
                            _bump_allowance_error_counter(pending_exit, pending_exit["last_error"])
                            pending_exit["next_retry_at"] = _iso_utc(
                                now + timedelta(seconds=_failed_exit_retry_delay_seconds(pending_exit["last_error"]))
                            )
                    else:
                        pending_exit["status"] = "failed"
                        pending_exit["retry_count"] = int(pending_exit.get("retry_count", 0) or 0) + 1
                        pending_exit["last_error"] = "exit_requote_cancel_failed"
                        _bump_allowance_error_counter(pending_exit, pending_exit["last_error"])
                        pending_exit["next_retry_at"] = _iso_utc(
                            now + timedelta(seconds=_failed_exit_retry_delay_seconds(pending_exit["last_error"]))
                        )

                    if not dry_run:
                        payload["pending_live_exit"] = pending_exit
                        _attach_pending_state(payload)
                        row.payload_json = payload
                        row.updated_at = now
                        state_updates += 1
                    held += 1
                    continue

            if snapshot_status in {"cancelled", "expired", "failed"}:
                if not dry_run:
                    pending_exit["status"] = "failed"
                    pending_exit["retry_count"] = int(pending_exit.get("retry_count", 0) or 0) + 1
                    pending_exit["last_attempt_at"] = _iso_utc(now)
                    pending_exit["last_error"] = f"provider_exit_status:{snapshot_status}"
                    pending_exit["next_retry_at"] = _iso_utc(
                        now + timedelta(seconds=_failed_exit_retry_delay_seconds(pending_exit["last_error"]))
                    )
                    payload["pending_live_exit"] = pending_exit
                    _attach_pending_state(payload)
                    row.payload_json = payload
                    row.updated_at = now
                    state_updates += 1
                held += 1
                continue

            if not dry_run and snapshot is not None:
                payload["pending_live_exit"] = pending_exit
                _attach_pending_state(payload)
                row.payload_json = payload
                row.updated_at = now
                state_updates += 1

            if pending_exit_kind != "take_profit_limit":
                if not dry_run and snapshot is None and pending_state_changed:
                    payload["position_state"] = pending_next_state
                    row.payload_json = payload
                    row.updated_at = now
                    state_updates += 1
                held += 1
                continue

        # ── Failed exit retry logic ────────────────────────────────────
        # If a previous cycle recorded a pending_live_exit that failed,
        # retry submitting the sell order (with cooldown & max retries).
        if isinstance(pending_exit, dict) and pending_exit.get("status") == "failed":
            if pending_winning_idx is not None and pending_outcome_idx is not None:
                _fill_not, _fill_sz, _fill_px = _extract_live_fill_metrics(payload)
                _ep = _fill_px if _fill_px and _fill_px > 0 else safe_float(row.effective_price)
                if _ep is None or _ep <= 0:
                    _ep = safe_float(row.entry_price)
                _not = _fill_not if _fill_not > 0 else (safe_float(row.notional_usd) or 0.0)
                _qty = _fill_sz if _fill_sz > 0 else (_not / _ep if _ep and _ep > 0 else 0.0)
                cp = 1.0 if pending_winning_idx == pending_outcome_idx else 0.0
                close_trigger = "resolution_inferred" if pending_winning_idx_inferred else "resolution"
                _pnl = (_qty * cp) - _not if _qty > 0 and _not > 0 else 0.0
                ns = _status_for_close(pnl=_pnl, close_trigger=close_trigger)
                if not dry_run:
                    row.status = ns
                    row.actual_profit = _pnl
                    row.updated_at = now
                    pending_exit["status"] = "superseded_resolution"
                    pending_exit["resolved_at"] = _iso_utc(now)
                    payload["pending_live_exit"] = pending_exit
                    payload["position_close"] = {
                        "close_price": cp,
                        "price_source": "resolved_settlement",
                        "close_trigger": close_trigger,
                        "realized_pnl": _pnl,
                        "closed_at": _iso_utc(now),
                        "reason": reason,
                    }
                    row.payload_json = payload
                    hot_state.record_order_resolved(
                        trader_id=trader_id,
                        mode=str(row.mode or ""),
                        order_id=str(row.id or ""),
                        market_id=str(row.market_id or ""),
                        direction=str(row.direction or ""),
                        source=str(row.source or ""),
                        status=ns,
                        actual_profit=_pnl,
                        payload=payload,
                        copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                    )
                    closed += 1
                total_realized_pnl += _pnl
                by_status[ns] = int(by_status.get(ns, 0)) + 1
                would_close += 1
                details.append(
                    {
                        "order_id": row.id,
                        "market_id": row.market_id,
                        "close_trigger": close_trigger,
                        "close_price": cp,
                        "realized_pnl": _pnl,
                        "next_status": ns,
                    }
                )
                continue

            if wallet_settlement_price is not None:
                _fill_not, _fill_sz, _fill_px = _extract_live_fill_metrics(payload)
                _ep = _fill_px if _fill_px and _fill_px > 0 else safe_float(row.effective_price)
                if _ep is None or _ep <= 0:
                    _ep = safe_float(row.entry_price)
                _not = _fill_not if _fill_not > 0 else (safe_float(row.notional_usd) or 0.0)
                _qty = _fill_sz if _fill_sz > 0 else (_not / _ep if _ep and _ep > 0 else 0.0)
                cp = wallet_settlement_price
                _pnl = (_qty * cp) - _not if _qty > 0 and _not > 0 else 0.0
                ns = _status_for_close(pnl=_pnl, close_trigger="resolution")
                if not dry_run:
                    row.status = ns
                    row.actual_profit = _pnl
                    row.updated_at = now
                    pending_exit["status"] = "superseded_resolution"
                    pending_exit["resolved_at"] = _iso_utc(now)
                    payload["pending_live_exit"] = pending_exit
                    payload["position_close"] = {
                        "close_price": cp,
                        "price_source": "wallet_redeemable_mark",
                        "close_trigger": "resolution",
                        "realized_pnl": _pnl,
                        "closed_at": _iso_utc(now),
                        "reason": reason,
                    }
                    row.payload_json = payload
                    hot_state.record_order_resolved(
                        trader_id=trader_id,
                        mode=str(row.mode or ""),
                        order_id=str(row.id or ""),
                        market_id=str(row.market_id or ""),
                        direction=str(row.direction or ""),
                        source=str(row.source or ""),
                        status=ns,
                        actual_profit=_pnl,
                        payload=payload,
                        copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                    )
                    closed += 1
                total_realized_pnl += _pnl
                by_status[ns] = int(by_status.get(ns, 0)) + 1
                would_close += 1
                details.append(
                    {
                        "order_id": row.id,
                        "market_id": row.market_id,
                        "close_trigger": "resolution",
                        "close_price": cp,
                        "realized_pnl": _pnl,
                        "next_status": ns,
                    }
                )
                continue

            retry_count = int(pending_exit.get("retry_count", 0) or 0)
            last_attempt_iso = pending_exit.get("last_attempt_at") or pending_exit.get("triggered_at")
            last_attempt_dt = _parse_iso_utc_naive(last_attempt_iso)
            next_retry_iso = pending_exit.get("next_retry_at")
            next_retry_dt = _parse_iso_utc_naive(next_retry_iso)
            pending_close_trigger = str(pending_exit.get("close_trigger") or "").strip().lower()
            pending_exit_kind = str(pending_exit.get("kind") or "").strip().lower()
            last_error_text = str(pending_exit.get("last_error") or "")
            allow_unbounded_retry = (
                bool(token_id)
                and wallet_position_size > _WALLET_SIZE_EPSILON
                and pending_winning_idx is None
                and wallet_settlement_price is None
                and not _is_zero_balance_error(last_error_text)
                and not _is_allowance_error(last_error_text)
                and not _is_rate_limited_error(last_error_text)
                and not _is_invalid_signature_error(last_error_text)
            )
            if allow_unbounded_retry and retry_count >= _FAILED_EXIT_MAX_RETRIES:
                retry_count = _FAILED_EXIT_MAX_RETRIES - 1
            force_rapid_retry = _is_rapid_close_trigger(pending_close_trigger)
            if force_rapid_retry:
                min_retry_seconds = max(0.5, safe_float(pending_exit.get("rapid_retry_seconds"), 1.0) or 1.0)
            else:
                min_retry_seconds = max(
                    _FAILED_EXIT_MIN_RETRY_INTERVAL_SECONDS,
                    _failed_exit_retry_delay_seconds(pending_exit.get("last_error")),
                )
            now_naive = now.astimezone(timezone.utc).replace(tzinfo=None)
            seconds_since_attempt = (
                (now_naive - last_attempt_dt).total_seconds() if last_attempt_dt is not None else float("inf")
            )

            if retry_count >= _FAILED_EXIT_MAX_RETRIES:
                if not dry_run:
                    pending_exit["status"] = "blocked_retry_exhausted"
                    pending_exit["exhausted_at"] = _iso_utc(now)
                    pending_exit["retry_count"] = retry_count
                    pending_exit["last_attempt_at"] = _iso_utc(now)
                    pending_exit["last_error"] = str(pending_exit.get("last_error") or "exit_retry_exhausted")
                    pending_exit["next_retry_at"] = None
                    payload["pending_live_exit"] = pending_exit
                    _attach_pending_state(payload)
                    row.payload_json = payload
                    row.updated_at = now
                    state_updates += 1
                held += 1
                continue

            if next_retry_dt is not None and now_naive < next_retry_dt:
                if not dry_run and pending_state_changed:
                    payload["position_state"] = pending_next_state
                    row.payload_json = payload
                    row.updated_at = now
                    state_updates += 1
                held += 1
                continue

            if seconds_since_attempt < min_retry_seconds:
                # Cooldown not elapsed — hold, don't spam.
                if not dry_run and pending_state_changed:
                    payload["position_state"] = pending_next_state
                    row.payload_json = payload
                    row.updated_at = now
                    state_updates += 1
                held += 1
                continue

            # Retry: refresh allowance and re-place exit order
            _fill_not_r, _fill_sz_r, _ = _extract_live_fill_metrics(payload)
            exit_size = max(
                max(0.0, safe_float(pending_exit.get("exit_size"), 0.0) or 0.0),
                _fill_sz_r if _fill_sz_r > 0 else 0.0,
            )
            wallet_exit_size_cap = wallet_position_size if wallet_position_size > _WALLET_SIZE_EPSILON else 0.0
            if exit_size <= 0.0 and wallet_exit_size_cap > 0.0:
                exit_size = wallet_exit_size_cap
            if exit_size > 0.0:
                pending_exit["exit_size"] = float(exit_size)
            exit_size = _remaining_exit_size(
                required_exit_size=exit_size,
                pending_exit=pending_exit,
                wallet_position_size=wallet_exit_size_cap,
            )
            if exit_size > 0.0:
                pending_exit["remaining_size"] = float(exit_size)
            exit_price = safe_float(pending_exit.get("close_price")) or 0.01
            take_profit_floor_price = _take_profit_price_floor(pending_exit)
            if take_profit_floor_price is not None:
                exit_price = max(exit_price, take_profit_floor_price)
                pending_exit["close_price"] = float(exit_price)
                pending_exit["post_only"] = True
            base_min_order_size_usd = _resolve_position_min_order_size_usd(
                trader_params=params,
                payload=payload,
                mode="live",
            )
            min_order_size_usd = _effective_exit_min_order_size_usd(
                base_min_order_size_usd,
                pending_exit.get("close_trigger"),
            )

            if token_id and exit_size > 0:
                exit_notional_estimate = float(exit_size) * float(max(exit_price, 0.0))
                if exit_notional_estimate + 1e-9 < min_order_size_usd:
                    if not dry_run:
                        pending_exit["status"] = "blocked_min_notional"
                        pending_exit["retry_count"] = retry_count + 1
                        pending_exit["exhausted_at"] = _iso_utc(now)
                        pending_exit["last_attempt_at"] = _iso_utc(now)
                        pending_exit["last_error"] = (
                            f"exit_notional_below_min:{exit_notional_estimate:.4f}<{min_order_size_usd:.4f}"
                        )
                        pending_exit["next_retry_at"] = None
                        payload["pending_live_exit"] = pending_exit
                        _attach_pending_state(payload)
                        row.payload_json = payload
                        row.updated_at = now
                        state_updates += 1
                    held += 1
                    continue
                try:
                    from services.live_execution_adapter import execute_live_order

                    rapid_retry_exit = force_rapid_retry
                    post_only_retry = bool(pending_exit_kind == "take_profit_limit" and not rapid_retry_exit)
                    async with release_conn(session):
                        try:
                            await live_execution_service.prepare_sell_balance_allowance(token_id)
                        except Exception:
                            pass
                        exec_result = await asyncio.wait_for(
                            execute_live_order(
                                token_id=token_id,
                                side="SELL",
                                size=exit_size,
                                fallback_price=exit_price,
                                min_order_size_usd=min_order_size_usd,
                                time_in_force="IOC" if rapid_retry_exit else "GTC",
                                post_only=post_only_retry,
                                resolve_live_price=rapid_retry_exit,
                                enforce_fallback_bound=True,
                            ),
                            timeout=_LIVE_EXIT_RETRY_TIMEOUT_SECONDS,
                        )
                    if exec_result.status in {"executed", "open", "submitted"}:
                        logger.info(
                            "Exit retry succeeded for order=%s attempt=%d status=%s",
                            row.id,
                            retry_count + 1,
                            exec_result.status,
                        )
                        if not dry_run:
                            pending_exit["status"] = "submitted"
                            pending_exit["retry_count"] = retry_count + 1
                            pending_exit["allowance_error_count"] = 0
                            pending_exit["last_attempt_at"] = _iso_utc(now)
                            pending_exit["next_retry_at"] = None
                            pending_exit["exit_order_id"] = exec_result.order_id
                            pending_exit["provider_clob_order_id"] = str(
                                (exec_result.payload or {}).get("clob_order_id") or ""
                            )
                            payload["pending_live_exit"] = pending_exit
                            _attach_pending_state(payload)
                            row.payload_json = payload
                            row.updated_at = now
                            state_updates += 1
                        held += 1
                        continue
                    logger.warning(
                        "Exit retry failed for order=%s attempt=%d error=%s",
                        row.id,
                        retry_count + 1,
                        exec_result.error_message,
                    )
                    if not dry_run:
                        _apply_failed_exit_state(
                            pending_exit,
                            error=exec_result.error_message,
                            now=now,
                            retry_count=retry_count + 1,
                        )
                        payload["pending_live_exit"] = pending_exit
                        _attach_pending_state(payload)
                        row.payload_json = payload
                        row.updated_at = now
                        state_updates += 1
                    held += 1
                    continue
                except Exception as exc:
                    logger.warning(
                        "Exit retry exception for order=%s attempt=%d: %s",
                        row.id,
                        retry_count + 1,
                        exc,
                    )
                    if not dry_run:
                        _apply_failed_exit_state(
                            pending_exit,
                            error=exc,
                            now=now,
                            retry_count=retry_count + 1,
                        )
                        payload["pending_live_exit"] = pending_exit
                        _attach_pending_state(payload)
                        row.payload_json = payload
                        row.updated_at = now
                        state_updates += 1
                    held += 1
                    continue
            else:
                # No token_id or size — can't retry, mark exhausted
                if not dry_run:
                    pending_exit["status"] = "failed"
                    pending_exit["exhausted_at"] = _iso_utc(now)
                    pending_exit["retry_count"] = retry_count + 1
                    pending_exit["last_attempt_at"] = _iso_utc(now)
                    pending_exit["last_error"] = "missing token_id or fill_size"
                    pending_exit["next_retry_at"] = _iso_utc(
                        now + timedelta(seconds=_failed_exit_retry_delay_seconds(pending_exit["last_error"]))
                    )
                    payload["pending_live_exit"] = pending_exit
                    _attach_pending_state(payload)
                    row.payload_json = payload
                    row.updated_at = now
                    state_updates += 1
                held += 1
                continue

        if isinstance(pending_exit, dict) and pending_exit.get("status") in {
            "blocked_no_inventory",
            "blocked_min_notional",
            "blocked_retry_exhausted",
        }:
            if pending_winning_idx is not None and pending_outcome_idx is not None:
                _fill_not, _fill_sz, _fill_px = _extract_live_fill_metrics(payload)
                _ep = _fill_px if _fill_px and _fill_px > 0 else safe_float(row.effective_price)
                if _ep is None or _ep <= 0:
                    _ep = safe_float(row.entry_price)
                _not = _fill_not if _fill_not > 0 else (safe_float(row.notional_usd) or 0.0)
                _qty = _fill_sz if _fill_sz > 0 else (_not / _ep if _ep and _ep > 0 else 0.0)
                cp = 1.0 if pending_winning_idx == pending_outcome_idx else 0.0
                close_trigger = "resolution_inferred" if pending_winning_idx_inferred else "resolution"
                _pnl = (_qty * cp) - _not if _qty > 0 and _not > 0 else 0.0
                ns = _status_for_close(pnl=_pnl, close_trigger=close_trigger)
                if not dry_run:
                    row.status = ns
                    row.actual_profit = _pnl
                    row.updated_at = now
                    pending_exit["status"] = "superseded_resolution"
                    pending_exit["resolved_at"] = _iso_utc(now)
                    payload["pending_live_exit"] = pending_exit
                    payload["position_close"] = {
                        "close_price": cp,
                        "price_source": "resolved_settlement",
                        "close_trigger": close_trigger,
                        "realized_pnl": _pnl,
                        "closed_at": _iso_utc(now),
                        "reason": reason,
                    }
                    row.payload_json = payload
                    hot_state.record_order_resolved(
                        trader_id=trader_id,
                        mode=str(row.mode or ""),
                        order_id=str(row.id or ""),
                        market_id=str(row.market_id or ""),
                        direction=str(row.direction or ""),
                        source=str(row.source or ""),
                        status=ns,
                        actual_profit=_pnl,
                        payload=payload,
                        copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                    )
                    closed += 1
                total_realized_pnl += _pnl
                by_status[ns] = int(by_status.get(ns, 0)) + 1
                would_close += 1
                details.append(
                    {
                        "order_id": row.id,
                        "market_id": row.market_id,
                        "close_trigger": close_trigger,
                        "close_price": cp,
                        "realized_pnl": _pnl,
                        "next_status": ns,
                    }
                )
                continue
            if wallet_settlement_price is not None:
                _fill_not, _fill_sz, _fill_px = _extract_live_fill_metrics(payload)
                _ep = _fill_px if _fill_px and _fill_px > 0 else safe_float(row.effective_price)
                if _ep is None or _ep <= 0:
                    _ep = safe_float(row.entry_price)
                _not = _fill_not if _fill_not > 0 else (safe_float(row.notional_usd) or 0.0)
                _qty = _fill_sz if _fill_sz > 0 else (_not / _ep if _ep and _ep > 0 else 0.0)
                cp = wallet_settlement_price
                _pnl = (_qty * cp) - _not if _qty > 0 and _not > 0 else 0.0
                ns = _status_for_close(pnl=_pnl, close_trigger="resolution")
                if not dry_run:
                    row.status = ns
                    row.actual_profit = _pnl
                    row.updated_at = now
                    pending_exit["status"] = "superseded_resolution"
                    pending_exit["resolved_at"] = _iso_utc(now)
                    payload["pending_live_exit"] = pending_exit
                    payload["position_close"] = {
                        "close_price": cp,
                        "price_source": "wallet_redeemable_mark",
                        "close_trigger": "resolution",
                        "realized_pnl": _pnl,
                        "closed_at": _iso_utc(now),
                        "reason": reason,
                    }
                    row.payload_json = payload
                    hot_state.record_order_resolved(
                        trader_id=trader_id,
                        mode=str(row.mode or ""),
                        order_id=str(row.id or ""),
                        market_id=str(row.market_id or ""),
                        direction=str(row.direction or ""),
                        source=str(row.source or ""),
                        status=ns,
                        actual_profit=_pnl,
                        payload=payload,
                        copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                    )
                    closed += 1
                total_realized_pnl += _pnl
                by_status[ns] = int(by_status.get(ns, 0)) + 1
                would_close += 1
                details.append(
                    {
                        "order_id": row.id,
                        "market_id": row.market_id,
                        "close_trigger": "resolution",
                        "close_price": cp,
                        "realized_pnl": _pnl,
                        "next_status": ns,
                    }
                )
                continue
            pending_provider_status = str(pending_exit.get("provider_status") or "").strip().lower()
            provider_close_verified = _pending_exit_has_verified_terminal_fill(pending_exit)
            wallet_flat_by_snapshot = wallet_position_observed and wallet_position_size <= _WALLET_SIZE_EPSILON
            wallet_flat_by_absence = bool(token_id) and wallet_positions_index_present and wallet_position is None
            wallet_trade_confirms_exit = (
                isinstance(latest_wallet_sell_trade, dict)
                and _extract_wallet_trade_size(latest_wallet_sell_trade) > _WALLET_SIZE_EPSILON
            )
            wallet_flat_override = (
                (wallet_flat_by_snapshot or wallet_flat_by_absence)
                and (provider_close_verified or wallet_trade_confirms_exit)
            )
            if wallet_flat_override:
                close_trigger = "wallet_flat_override"
                close_price = (
                    safe_float(latest_wallet_sell_trade.get("price"))
                    if isinstance(latest_wallet_sell_trade, dict)
                    else None
                )
                if close_price is None or close_price < 0.0:
                    close_price = safe_float(pending_exit.get("average_fill_price"))
                if close_price is None or close_price < 0.0:
                    close_price = pending_current_price if pending_current_price is not None else pending_market_side_price
                if close_price is None or close_price < 0.0:
                    close_price = 0.0

                filled_notional, filled_size, fill_price = _extract_live_fill_metrics(payload)
                entry_price = fill_price if fill_price is not None and fill_price > 0 else safe_float(row.effective_price)
                if entry_price is None or entry_price <= 0:
                    entry_price = safe_float(row.entry_price)
                notional = filled_notional if filled_notional > 0.0 else (safe_float(row.notional_usd) or 0.0)
                quantity = filled_size if filled_size > 0.0 else (notional / entry_price if entry_price and entry_price > 0 else 0.0)

                if quantity > 0.0 and notional > 0.0:
                    proceeds = float(quantity) * float(close_price)
                    pnl = proceeds - float(notional)
                    next_status = _status_for_close(pnl=pnl, close_trigger=close_trigger)
                    if not dry_run:
                        row.status = next_status
                        row.actual_profit = pnl
                        row.updated_at = now
                        pending_exit["status"] = "superseded_wallet_flat"
                        pending_exit["resolved_at"] = _iso_utc(now)
                        pending_exit["provider_status"] = pending_provider_status or pending_exit.get("provider_status")
                        payload["pending_live_exit"] = pending_exit
                        payload["position_state"] = pending_next_state
                        payload["position_close"] = {
                            "close_price": close_price,
                            "price_source": "wallet_flat_override",
                            "close_trigger": close_trigger,
                            "realized_pnl": pnl,
                            "cost_basis_usd": notional,
                            "settlement_proceeds_usd": proceeds,
                            "filled_size": quantity,
                            "filled_notional_usd": notional,
                            "market_tradable": pending_market_tradable,
                            "age_minutes": pending_exit.get("age_minutes"),
                            "closed_at": _iso_utc(now),
                            "reason": reason,
                            "wallet_trade_id": (
                                str(latest_wallet_sell_trade.get("trade_id") or "")
                                if isinstance(latest_wallet_sell_trade, dict)
                                else ""
                            ),
                            "wallet_trade_timestamp": (
                                _iso_utc(latest_wallet_sell_trade.get("timestamp"))
                                if isinstance(latest_wallet_sell_trade, dict)
                                and isinstance(latest_wallet_sell_trade.get("timestamp"), datetime)
                                else None
                            ),
                            "wallet_activity_transaction_hash": (
                                str(
                                    latest_wallet_sell_trade.get("transactionHash")
                                    or latest_wallet_sell_trade.get("txHash")
                                    or latest_wallet_sell_trade.get("tx_hash")
                                    or ""
                                ).strip()
                                if isinstance(latest_wallet_sell_trade, dict)
                                else ""
                            ) or None,
                        }
                        row.payload_json = payload
                        _apply_position_close_verification(
                            session,
                            row=row,
                            now=now,
                            event_type="wallet_flat_close",
                            payload_json=payload,
                        )
                        hot_state.record_order_resolved(
                            trader_id=trader_id,
                            mode=str(row.mode or ""),
                            order_id=str(row.id or ""),
                            market_id=str(row.market_id or ""),
                            direction=str(row.direction or ""),
                            source=str(row.source or ""),
                            status=next_status,
                            actual_profit=pnl,
                            payload=payload,
                            copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                        )
                        closed += 1
                    total_realized_pnl += pnl
                    by_status[next_status] = int(by_status.get(next_status, 0)) + 1
                    would_close += 1
                    details.append(
                        {
                            "order_id": row.id,
                            "market_id": row.market_id,
                            "close_trigger": close_trigger,
                            "close_price": close_price,
                            "realized_pnl": pnl,
                            "next_status": next_status,
                        }
                    )
                    continue
            if not dry_run and pending_state_changed:
                payload["pending_live_exit"] = pending_exit
                payload["position_state"] = pending_next_state
                row.payload_json = payload
                row.updated_at = now
                state_updates += 1
            held += 1
            continue

        # If pending_live_exit is in-flight (submitted), skip normal processing
        if (
            isinstance(pending_exit, dict)
            and pending_exit.get("status") in {"submitted", "pending"}
            and str(pending_exit.get("kind") or "").strip().lower() != "take_profit_limit"
        ):
            if not dry_run and pending_state_changed:
                payload["position_state"] = pending_next_state
                row.payload_json = payload
                row.updated_at = now
                state_updates += 1
            held += 1
            continue

        filled_notional = max(0.0, float(entry_fill_notional or 0.0))
        filled_size = max(0.0, float(entry_fill_size or 0.0))
        fill_price = entry_fill_price if entry_fill_price is not None and entry_fill_price > 0.0 else None
        status_key = str(row.status or "").strip().lower()
        raw_filled_notional, raw_filled_size, _raw_fill_price = _extract_live_fill_metrics(payload)
        provider_snapshot_status = _provider_snapshot_status(payload)
        if (
            status_key in {"open", "submitted"}
            and raw_filled_notional <= 0.0
            and raw_filled_size <= 0.0
            and wallet_position_size <= _WALLET_SIZE_EPSILON
            and provider_snapshot_status not in {"filled", "partially_filled", "matched", "executed"}
        ):
            if pending_market_info is not None and not pending_market_tradable:
                next_status = "cancelled"
                if not dry_run:
                    row.status = next_status
                    row.actual_profit = 0.0
                    row.updated_at = now
                    payload["position_close"] = {
                        "close_price": 0.0,
                        "price_source": "terminal_unfilled_cancel",
                        "close_trigger": "terminal_unfilled_cancel",
                        "realized_pnl": 0.0,
                        "closed_at": _iso_utc(now),
                        "reason": reason,
                    }
                    row.payload_json = payload
                    hot_state.record_order_resolved(
                        trader_id=trader_id,
                        mode=str(row.mode or ""),
                        order_id=str(row.id or ""),
                        market_id=str(row.market_id or ""),
                        direction=str(row.direction or ""),
                        source=str(row.source or ""),
                        status=next_status,
                        actual_profit=0.0,
                        payload=payload,
                        copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
                    )
                    closed += 1
                would_close += 1
                details.append(
                    {
                        "order_id": row.id,
                        "market_id": row.market_id,
                        "close_trigger": "terminal_unfilled_cancel",
                        "close_price": 0.0,
                        "realized_pnl": 0.0,
                        "next_status": next_status,
                    }
                )
                continue
            skipped += 1
            skipped_reasons["awaiting_fill"] = int(skipped_reasons.get("awaiting_fill", 0)) + 1
            continue
        entry_price = fill_price if fill_price is not None and fill_price > 0 else safe_float(row.effective_price)
        if entry_price is None or entry_price <= 0:
            entry_price = safe_float(row.entry_price)
        notional = filled_notional if filled_notional > 0.0 else (safe_float(row.notional_usd) or 0.0)
        outcome_idx = _direction_outcome_index(row.direction)
        if outcome_idx is None or entry_price is None or entry_price <= 0 or notional <= 0:
            skipped += 1
            skipped_reasons["invalid_entry"] = int(skipped_reasons.get("invalid_entry", 0)) + 1
            continue

        signal_payload = signal_payloads.get(str(row.signal_id), {})
        market_info = market_info_by_id.get(str(row.market_id or ""))
        market_tradable = polymarket_client.is_market_tradable(market_info, now=now)
        market_seconds_left = _market_seconds_left(market_info, now)
        market_end_time = _market_end_time_iso(market_info)
        winning_idx = _extract_winning_outcome_index(market_info)
        winning_idx_inferred = False
        if winning_idx is None and resolution_infer_from_prices:
            inferred_idx = _extract_winning_outcome_index_from_prices(
                market_info,
                market_tradable=market_tradable,
                settle_floor=resolution_settle_floor,
            )
            if inferred_idx is not None:
                winning_idx = inferred_idx
                winning_idx_inferred = True
        market_side_price = _extract_market_side_price(market_info, outcome_idx)
        ws_side_price = ws_mid_prices.get(token_id) if token_id else None
        clob_side_price = clob_mid_prices.get(token_id) if token_id else None

        close_price: Optional[float] = None
        close_trigger: Optional[str] = None
        price_source: Optional[str] = None
        trailing_trigger_price: Optional[float] = None

        current_price = (
            ws_side_price
            if ws_side_price is not None
            else (
                clob_side_price
                if clob_side_price is not None
                else (market_side_price if market_side_price is not None else wallet_mark_price)
            )
        )
        current_price = _state_price_floor(current_price)
        current_price_source = (
            "ws_mid"
            if ws_side_price is not None
            else (
                "clob_midpoint"
                if clob_side_price is not None
                else (
                    "market_mark"
                    if market_side_price is not None
                    else ("wallet_mark" if wallet_mark_price is not None else None)
                )
            )
        )

        age_anchor = row.executed_at or row.updated_at or row.created_at
        age_minutes = None
        if age_anchor is not None:
            age_minutes = max(0.0, (now - age_anchor).total_seconds() / 60.0)
        min_hold_passed = age_minutes is None or age_minutes >= min_hold_minutes

        position_state = _extract_position_state(payload)
        prev_high = safe_float(position_state.get("highest_price"))
        prev_low = safe_float(position_state.get("lowest_price"))
        prev_last_mark = safe_float(position_state.get("last_mark_price"))
        prev_mark_source = str(position_state.get("last_mark_source") or "")
        prev_marked_at = _parse_iso_utc_naive(position_state.get("last_marked_at"))
        highest_price = prev_high
        lowest_price = prev_low
        if current_price is not None:
            if highest_price is None:
                highest_price = current_price
            else:
                highest_price = max(highest_price, current_price)
            if lowest_price is None:
                lowest_price = current_price
            else:
                lowest_price = min(lowest_price, current_price)

        mark_updated_at_value = (
            _iso_utc(now)
            if current_price is not None
            else (
                _iso_utc(prev_marked_at.replace(tzinfo=timezone.utc))
                if prev_marked_at is not None
                else ""
            )
        )
        next_state = {
            "highest_price": _state_price_floor(highest_price),
            "lowest_price": _state_price_floor(lowest_price),
            "last_mark_price": current_price if current_price is not None else _state_price_floor(prev_last_mark),
            "last_mark_source": str(current_price_source or prev_mark_source),
            "last_marked_at": mark_updated_at_value,
            "last_exit_evaluated_at": _iso_utc(now),
        }
        prev_mark_age_seconds = (
            max(0.0, (now_naive - prev_marked_at).total_seconds()) if prev_marked_at is not None else None
        )
        fallback_mark_fresh = bool(
            prev_last_mark is not None
            and prev_mark_age_seconds is not None
            and prev_mark_age_seconds <= _MAX_LIVE_EXIT_FALLBACK_MARK_AGE_SECONDS
        )
        if current_price is not None:
            exit_eval_price = current_price
            exit_eval_price_source = current_price_source
        elif fallback_mark_fresh:
            exit_eval_price = _state_price_floor(prev_last_mark)
            exit_eval_price_source = "position_state_mark"
        else:
            exit_eval_price = None
            exit_eval_price_source = None

        if winning_idx is not None:
            close_price = 1.0 if winning_idx == outcome_idx else 0.0
            close_trigger = "resolution_inferred" if winning_idx_inferred else "resolution"
            price_source = "resolved_settlement"
        elif wallet_settlement_price is not None:
            close_price = wallet_settlement_price
            close_trigger = "resolution"
            price_source = "wallet_redeemable_mark"
        else:
            bundle_position_shortfall = _bundle_position_shortfall(
                token_id=token_id,
                payload=payload,
                signal_payload=signal_payload,
                wallet_positions_by_token=wallet_positions_by_token,
            )
            pnl_pct = None
            if exit_eval_price is not None and entry_price > 0:
                pnl_pct = ((exit_eval_price - entry_price) / entry_price) * 100.0

            if bundle_position_shortfall is not None:
                close_price = exit_eval_price
                if close_price is None or close_price <= 0.0:
                    close_price = _state_price_floor(entry_price)
                close_trigger = "force_flatten_bundle_residual"
                price_source = exit_eval_price_source or ("entry_price" if close_price is not None else None)
                if not dry_run:
                    payload["bundle_execution_state"] = {
                        "status": "incomplete_live_wallet_bundle",
                        "required_token_ids": list(bundle_position_shortfall["required_token_ids"]),
                        "observed_token_ids": list(bundle_position_shortfall["observed_token_ids"]),
                        "missing_token_ids": list(bundle_position_shortfall["missing_token_ids"]),
                        "full_bundle_execution_mode": bundle_position_shortfall.get("full_bundle_execution_mode"),
                        "plan_id": bundle_position_shortfall.get("plan_id"),
                        "detected_at": _iso_utc(now),
                    }
            elif force_mark_to_market and exit_eval_price is not None:
                close_price = exit_eval_price
                close_trigger = "manual_mark_to_market"
                price_source = exit_eval_price_source
            else:
                # ── Strategy-based exit check ──────────────────────────
                # If the strategy that opened this position has a
                # should_exit() method, call it first and respect its
                # decision before falling through to default TP/SL/etc.
                strategy_slug = (payload.get("strategy_type") or "").strip().lower()
                strategy_exit = None
                _exit_instance = await _strategy_exit_instance(session, strategy_slug) if strategy_slug else None
                if _exit_instance is not None:
                    try:

                        class _LivePositionView:
                            pass

                        pos_view = _LivePositionView()
                        pos_view.entry_price = entry_price
                        pos_view.current_price = exit_eval_price
                        pos_view.highest_price = highest_price
                        pos_view.lowest_price = lowest_price
                        pos_view.age_minutes = age_minutes
                        pos_view.pnl_percent = pnl_pct
                        pos_view.filled_size = (
                            filled_size
                            if filled_size > 0.0
                            else (notional / entry_price if entry_price > 0 else 0.0)
                        )
                        pos_view.notional_usd = notional
                        if "strategy_context" not in payload:
                            payload["strategy_context"] = {}
                        strategy_context_payload = (
                            payload["strategy_context"] if isinstance(payload.get("strategy_context"), dict) else {}
                        )
                        pos_view.strategy_context = payload["strategy_context"]
                        pos_view.config = payload.get("strategy_exit_config", {})
                        pos_view.outcome_idx = outcome_idx

                        min_order_size_usd = _resolve_position_min_order_size_usd(
                            trader_params=params,
                            payload=payload,
                            mode="live",
                        )
                        market_state_dict = {
                            "current_price": exit_eval_price,
                            "market_tradable": market_tradable,
                            "is_resolved": False,
                            "winning_outcome": None,
                            "seconds_left": market_seconds_left,
                            "end_time": market_end_time,
                            "token_id": token_id,
                            "mark_source": exit_eval_price_source,
                            "min_order_size_usd": min_order_size_usd,
                            "notional_usd": notional,
                            "oracle_price": strategy_context_payload.get("oracle_price"),
                            "price_to_beat": strategy_context_payload.get("price_to_beat"),
                            "oracle_age_seconds": strategy_context_payload.get("oracle_age_seconds"),
                            "confirmed_stage_triggered": strategy_context_payload.get("confirmed_stage_triggered"),
                            "strategy_stage": strategy_context_payload.get("stage"),
                        }

                        exit_decision = _exit_instance.should_exit(pos_view, market_state_dict)
                        exit_action = getattr(exit_decision, "action", None) if exit_decision is not None else None
                        if exit_action == "close":
                            strategy_exit = exit_decision
                        elif exit_action == "reduce":
                            strategy_exit = exit_decision
                        elif _strategy_hold_blocks_default_exit(exit_decision):
                            strategy_exit = exit_decision
                    except Exception as exc:
                        logger.warning(
                            "Strategy should_exit() error for %s: %s",
                            strategy_slug,
                            exc,
                        )

                if strategy_exit is not None and getattr(strategy_exit, "action", None) == "reduce":
                    if not dry_run:
                        payload["position_state"] = next_state
                        row.payload_json = payload
                        row.updated_at = now
                        state_updates += 1
                    held += 1
                    continue

                if _strategy_hold_blocks_default_exit(strategy_exit):
                    if not dry_run:
                        payload["position_state"] = next_state
                        row.payload_json = payload
                        row.updated_at = now
                        state_updates += 1
                    held += 1
                    continue

                active_take_profit_limit = (
                    isinstance(pending_exit, dict)
                    and str(pending_exit.get("kind") or "").strip().lower() == "take_profit_limit"
                    and str(pending_exit.get("status") or "").strip().lower() in {"submitted", "pending"}
                )
                if strategy_exit is not None:
                    _arm_reverse_entry_from_exit(
                        row=row,
                        payload=payload,
                        strategy_exit=strategy_exit,
                        market_info=market_info,
                        market_seconds_left=market_seconds_left,
                        current_price=exit_eval_price,
                        now=now,
                    )
                    close_price = (
                        strategy_exit.close_price if strategy_exit.close_price is not None else exit_eval_price
                    )
                    close_trigger = f"strategy:{strategy_exit.reason}"
                    price_source = exit_eval_price_source
                elif (
                    not resolve_only
                    and take_profit_pct is not None
                    and pnl_pct is not None
                    and min_hold_passed
                    and not active_take_profit_limit
                    and pnl_pct >= take_profit_pct
                ):
                    close_price = exit_eval_price
                    close_trigger = "take_profit"
                    price_source = exit_eval_price_source
                elif (
                    not resolve_only
                    and stop_loss_pct is not None
                    and pnl_pct is not None
                    and min_hold_passed
                    and pnl_pct <= -abs(stop_loss_pct)
                ):
                    close_price = exit_eval_price
                    close_trigger = "stop_loss"
                    price_source = exit_eval_price_source
                elif (
                    not resolve_only
                    and trailing_stop_pct is not None
                    and trailing_stop_pct > 0
                    and exit_eval_price is not None
                    and highest_price is not None
                    and min_hold_passed
                ):
                    trailing_trigger_price = highest_price * (1.0 - (trailing_stop_pct / 100.0))
                    if highest_price > entry_price and exit_eval_price <= trailing_trigger_price:
                        close_price = exit_eval_price
                        close_trigger = "trailing_stop"
                        price_source = exit_eval_price_source
                elif (
                    not resolve_only
                    and max_hold_minutes is not None
                    and age_minutes is not None
                    and age_minutes >= max_hold_minutes
                ):
                    if exit_eval_price is not None:
                        close_price = exit_eval_price
                        close_trigger = "max_hold"
                        price_source = exit_eval_price_source
                elif (
                    not resolve_only
                    and close_on_inactive_market
                    and not market_tradable
                    and exit_eval_price is not None
                    and min_hold_passed
                ):
                    close_price = exit_eval_price
                    close_trigger = "market_inactive"
                    price_source = exit_eval_price_source

        close_is_resolution = close_trigger in {"resolution", "resolution_inferred"}

        if close_price is None:
            state_changed = False
            if current_price is not None:
                mark_stale = (
                    prev_marked_at is None
                    or (now_naive - prev_marked_at).total_seconds() >= mark_touch_interval_seconds
                )
                state_changed = (
                    prev_last_mark is None
                    or abs(prev_last_mark - current_price) > 1e-9
                    or prev_high is None
                    or prev_low is None
                    or abs((prev_high or 0.0) - (highest_price or 0.0)) > 1e-9
                    or abs((prev_low or 0.0) - (lowest_price or 0.0)) > 1e-9
                    or prev_mark_source != str(current_price_source or "")
                    or mark_stale
                )
            if not dry_run and state_changed:
                payload["position_state"] = next_state
                row.payload_json = payload
                row.updated_at = now
                state_updates += 1
            held += 1
            continue

        quantity = filled_size if filled_size > 0.0 else (notional / entry_price if entry_price > 0 else 0.0)
        cost_basis = filled_notional if filled_notional > 0.0 else notional
        if quantity <= 0.0 or cost_basis <= 0.0:
            skipped += 1
            skipped_reasons["invalid_fill_state"] = int(skipped_reasons.get("invalid_fill_state", 0)) + 1
            continue
        proceeds = quantity * close_price
        pnl = proceeds - cost_basis
        next_status = _status_for_close(pnl=pnl, close_trigger=close_trigger)

        detail = {
            "order_id": row.id,
            "market_id": row.market_id,
            "direction": row.direction,
            "entry_price": entry_price,
            "close_price": close_price,
            "price_source": price_source,
            "close_trigger": close_trigger,
            "market_tradable": market_tradable,
            "notional_usd": notional,
            "cost_basis_usd": cost_basis,
            "filled_size": filled_size,
            "filled_notional_usd": filled_notional,
            "quantity": quantity,
            "next_status": next_status,
            "age_minutes": age_minutes,
            "min_hold_minutes": min_hold_minutes,
            "trailing_stop_trigger_price": trailing_trigger_price,
            "highest_price_seen": _state_price_floor(highest_price),
            "lowest_price_seen": _state_price_floor(lowest_price),
        }

        if not close_is_resolution:
            detail["next_status"] = str(row.status or "").strip().lower()
            detail["realized_pnl"] = None
            detail["hypothetical_pnl"] = pnl
            details.append(detail)
            would_close += 1

            if not dry_run:
                payload["position_state"] = next_state
                existing_tp_limit = (
                    pending_exit
                    if isinstance(pending_exit, dict)
                    and str(pending_exit.get("kind") or "").strip().lower() == "take_profit_limit"
                    and str(pending_exit.get("status") or "").strip().lower() in {"submitted", "pending"}
                    else None
                )
                if existing_tp_limit is not None:
                    cancel_target = _pending_exit_provider_clob_id(existing_tp_limit)
                    if not cancel_target:
                        cancel_target = str(existing_tp_limit.get("exit_order_id") or "").strip()
                    cancel_success = False
                    if cancel_target:
                        try:
                            async with release_conn(session):
                                cancel_success = bool(await live_execution_service.cancel_order(cancel_target))
                        except Exception:
                            cancel_success = False
                    existing_tp_limit["cancelled_for_override_at"] = _iso_utc(now)
                    existing_tp_limit["override_cancel_target"] = cancel_target
                    existing_tp_limit["override_cancel_success"] = cancel_success
                    if cancel_success:
                        existing_tp_limit["status"] = "cancelled"
                    payload["superseded_take_profit_exit"] = existing_tp_limit

                exit_record: dict[str, Any] = {
                    "triggered_at": _iso_utc(now),
                    "close_trigger": close_trigger,
                    "close_price": close_price,
                    "price_source": price_source,
                    "market_tradable": market_tradable,
                    "hypothetical_pnl": pnl,
                    "age_minutes": age_minutes,
                    "reason": reason,
                    "retry_count": 0,
                    "status": "pending",
                }

                # Immediately attempt to place the sell order
                token_id = _extract_live_token_id(payload)
                exit_size = filled_size if filled_size > 0.0 else quantity
                if exit_size > 0.0:
                    exit_record["exit_size"] = float(exit_size)
                wallet_exit_size_cap = wallet_position_size if wallet_position_size > _WALLET_SIZE_EPSILON else 0.0
                if wallet_exit_size_cap > 0.0:
                    if exit_size <= 0.0:
                        exit_size = wallet_exit_size_cap
                    else:
                        exit_size = min(exit_size, wallet_exit_size_cap)
                    exit_record["exit_size"] = float(exit_size)
                base_min_order_size_usd = _resolve_position_min_order_size_usd(
                    trader_params=params,
                    payload=payload,
                    mode="live",
                )
                min_order_size_usd = _effective_exit_min_order_size_usd(
                    base_min_order_size_usd,
                    close_trigger,
                )
                rapid_close_exit = _is_rapid_close_trigger(close_trigger)
                if token_id and exit_size > 0:
                    exit_size = _remaining_exit_size(
                        required_exit_size=exit_size,
                        pending_exit=exit_record,
                        wallet_position_size=wallet_exit_size_cap,
                    )
                    if exit_size > 0.0:
                        exit_record["remaining_size"] = float(exit_size)
                    exit_notional_estimate = float(exit_size) * float(max(close_price, 0.0))
                    if exit_notional_estimate + 1e-9 < min_order_size_usd:
                        exit_record["status"] = "blocked_min_notional"
                        exit_record["retry_count"] = 1
                        exit_record["exhausted_at"] = _iso_utc(now)
                        exit_record["last_error"] = (
                            f"exit_notional_below_min:{exit_notional_estimate:.4f}<{min_order_size_usd:.4f}"
                        )
                        exit_record["last_attempt_at"] = _iso_utc(now)
                        exit_record["next_retry_at"] = None
                        payload["pending_live_exit"] = exit_record
                        row.payload_json = payload
                        row.updated_at = now
                        state_updates += 1
                        held += 1
                        continue
                    try:
                        from services.live_execution_adapter import execute_live_order

                        async with release_conn(session):
                            try:
                                await live_execution_service.prepare_sell_balance_allowance(token_id)
                            except Exception:
                                pass
                            exec_result = await asyncio.wait_for(
                                execute_live_order(
                                    token_id=token_id,
                                    side="SELL",
                                    size=exit_size,
                                    fallback_price=close_price,
                                    min_order_size_usd=min_order_size_usd,
                                    time_in_force="IOC" if rapid_close_exit else "GTC",
                                    resolve_live_price=rapid_close_exit,
                                    enforce_fallback_bound=True,
                                ),
                                timeout=_LIVE_EXIT_ORDER_TIMEOUT_SECONDS,
                            )
                        if exec_result.status in {"executed", "open", "submitted"}:
                            exit_record["status"] = "submitted"
                            exit_record["exit_order_id"] = exec_result.order_id
                            exit_record["provider_clob_order_id"] = str(
                                (exec_result.payload or {}).get("clob_order_id") or ""
                            )
                            exit_record["last_attempt_at"] = _iso_utc(now)
                            logger.info(
                                "Exit order placed for order=%s trigger=%s status=%s",
                                row.id,
                                close_trigger,
                                exec_result.status,
                            )
                        else:
                            _apply_failed_exit_state(
                                exit_record,
                                error=exec_result.error_message,
                                now=now,
                                retry_count=1,
                            )
                            logger.warning(
                                "Exit order failed for order=%s trigger=%s error=%s",
                                row.id,
                                close_trigger,
                                exec_result.error_message,
                            )
                    except Exception as exc:
                        _apply_failed_exit_state(exit_record, error=exc, now=now, retry_count=1)
                        logger.warning(
                            "Exit order exception for order=%s trigger=%s: %s",
                            row.id,
                            close_trigger,
                            exc,
                        )
                else:
                    exit_record["status"] = "failed"
                    exit_record["last_error"] = "missing token_id or fill_size"
                    exit_record["retry_count"] = 1
                    exit_record["next_retry_at"] = _iso_utc(
                        now + timedelta(seconds=_failed_exit_retry_delay_seconds(exit_record["last_error"]))
                    )

                payload["pending_live_exit"] = exit_record
                row.payload_json = payload
                row.updated_at = now
                state_updates += 1
            held += 1
            continue

        # NOTE: No simulation ledger interaction for live positions.
        # Live positions settle against real exchange state.
        total_realized_pnl += pnl
        by_status[next_status] = int(by_status.get(next_status, 0)) + 1
        detail["realized_pnl"] = pnl
        details.append(detail)
        would_close += 1

        if dry_run:
            continue

        row.status = next_status
        row.actual_profit = pnl
        row.updated_at = now
        payload["position_state"] = next_state
        payload["position_close"] = {
            "close_price": close_price,
            "price_source": price_source,
            "close_trigger": close_trigger,
            "realized_pnl": pnl,
            "cost_basis_usd": cost_basis,
            "settlement_proceeds_usd": proceeds,
            "filled_size": filled_size,
            "filled_notional_usd": filled_notional,
            "market_tradable": market_tradable,
            "age_minutes": age_minutes,
            "closed_at": _iso_utc(now),
            "reason": reason,
        }
        if _exit_instance is not None and hasattr(_exit_instance, "record_trade_outcome"):
            try:
                _exit_instance.record_trade_outcome(won=next_status in {"closed_win", "resolved_win"})
            except Exception:
                pass

        reverse_signal_id, reverse_source = await _emit_armed_reverse_signal(
            session,
            row=row,
            payload=payload,
            signal_payload=signal_payload,
            market_info=market_info,
            close_trigger=close_trigger,
            realized_pnl=pnl,
            now=now,
        )
        if reverse_signal_id and reverse_source:
            reverse_signal_ids_by_source.setdefault(reverse_source, []).append(reverse_signal_id)
        row.payload_json = payload
        if reason:
            if row.reason:
                row.reason = f"{row.reason} | {reason}:{close_trigger}"
            else:
                row.reason = f"{reason}:{close_trigger}"
        hot_state.record_order_resolved(
            trader_id=trader_id,
            mode=str(row.mode or ""),
            order_id=str(row.id or ""),
            market_id=str(row.market_id or ""),
            direction=str(row.direction or ""),
            source=str(row.source or ""),
            status=next_status,
            actual_profit=pnl,
            payload=payload,
            copy_source_wallet=_extract_copy_source_wallet_from_payload(payload),
        )
        closed += 1

    # Grouped exit: when any order in a (market, direction) group gets an exit
    # triggered, propagate the exit to all siblings in the same group so stacked
    # orders exit together.
    if not dry_run:
        order_groups: dict[tuple[str, str], list] = {}
        for row in candidates:
            gk = (str(row.market_id or "").strip(), str(row.direction or "").strip().lower())
            order_groups.setdefault(gk, []).append(row)
        for group_key, group_rows in order_groups.items():
            if len(group_rows) < 2:
                continue
            trigger_source: dict[str, Any] | None = None
            for row in group_rows:
                payload = dict(row.payload_json or {})
                pe = payload.get("pending_live_exit")
                if isinstance(pe, dict) and pe.get("status") in ("pending", "submitted"):
                    trigger_source = pe
                    break
                if row.status in ("closed_win", "closed_loss", "resolved_win", "resolved_loss"):
                    pc = payload.get("position_close", {})
                    trigger_source = {
                        "close_trigger": str(pc.get("close_trigger") or "grouped_exit"),
                        "close_price": pc.get("close_price"),
                        "price_source": pc.get("price_source"),
                    }
                    break
            if trigger_source is None:
                continue
            for row in group_rows:
                if row.status not in LIVE_ACTIVE_STATUSES:
                    continue
                payload = dict(row.payload_json or {})
                existing_pe = payload.get("pending_live_exit")
                if isinstance(existing_pe, dict) and existing_pe.get("status") in ("pending", "submitted", "filled"):
                    continue
                token_id = _extract_live_token_id(payload)
                _fill_not, _fill_sz, _fill_px = _extract_live_fill_metrics(payload)
                exit_size = _fill_sz if _fill_sz > 0 else ((_fill_not / _fill_px) if _fill_px and _fill_px > 0 else 0.0)
                source_trigger = str(trigger_source.get("close_trigger") or "grouped_exit")
                payload["pending_live_exit"] = {
                    "status": "pending",
                    "close_trigger": f"grouped:{source_trigger}" if not source_trigger.startswith("grouped:") else source_trigger,
                    "close_price": trigger_source.get("close_price"),
                    "price_source": trigger_source.get("price_source"),
                    "token_id": token_id,
                    "exit_size": float(exit_size),
                    "triggered_at": _iso_utc(now),
                    "reason": "Grouped exit: sibling order in same market exited",
                    "retry_count": 0,
                }
                row.payload_json = payload
                row.updated_at = now
                state_updates += 1

    _lc_t3 = _time.monotonic()

    if not dry_run and (closed > 0 or state_updates > 0):
        touched_rows = [row for row in candidates if row.updated_at == now]
        publish_rows = list(touched_rows)
        if touched_rows:
            _lc_t3a = _time.monotonic()
            stmt = (
                update(TraderOrder)
                .where(TraderOrder.id == bindparam("_id"))
                .values(
                    status=bindparam("_status"),
                    actual_profit=bindparam("_actual_profit"),
                    updated_at=bindparam("_updated_at"),
                    payload_json=bindparam("_payload_json"),
                    reason=bindparam("_reason"),
                )
                .execution_options(synchronize_session=None)
            )
            params = [
                {
                    "id": row.id,
                    "_id": row.id,
                    "_status": row.status,
                    "_actual_profit": row.actual_profit,
                    "_updated_at": row.updated_at,
                    "_payload_json": row.payload_json,
                    "_reason": row.reason,
                }
                for row in touched_rows
            ]
            for row in touched_rows:
                if inspect(row).session is session.sync_session:
                    session.sync_session.expunge(row)
            with session.no_autoflush:
                await session.execute(stmt, params)
            _lc_t3b = _time.monotonic()
            await session.commit()
            _lc_t3c = _time.monotonic()
            _total_elapsed = _lc_t3c - _lc_t0
            _timing_msg = (
                f"  lifecycle_detail: pre_io={_lc_t1 - _lc_t0:.1f}s "
                f"external_io={_lc_t2 - _lc_t1:.1f}s processing={_lc_t3 - _lc_t2:.1f}s "
                f"executemany={_lc_t3b - _lc_t3a:.1f}s(n={len(touched_rows)}) "
                f"commit={_lc_t3c - _lc_t3b:.1f}s total={_total_elapsed:.1f}s "
                f"(candidates={len(candidates)} terminal_audit={len(terminal_rows)})"
            )
            if _total_elapsed >= _RECONCILE_TIMING_WARN_SECONDS:
                logger.warning("reconcile_live_positions timing: %s", _timing_msg)
            else:
                logger.debug("reconcile_live_positions timing: %s", _timing_msg)
        else:
            await session.commit()
        await _publish_trader_order_updates(publish_rows)
        if reverse_signal_ids_by_source:
            await _publish_reverse_signal_batches(reverse_signal_ids_by_source, emitted_at=now)

    return {
        "trader_id": trader_id,
        "mode": "live",
        "dry_run": bool(dry_run),
        "matched": len(candidates),
        "would_close": would_close,
        "closed": closed,
        "held": held,
        "skipped": skipped,
        "state_updates": state_updates,
        "total_realized_pnl": total_realized_pnl,
        "by_status": by_status,
        "skipped_reasons": skipped_reasons,
        "reverse_signals_emitted": sum(len(ids) for ids in reverse_signal_ids_by_source.values()),
        "details": details,
    }


# ---------------------------------------------------------------------------
# Event-driven exit evaluation
# ---------------------------------------------------------------------------
# In-memory registry of open orders by token_id for event-driven exit checks.
# Populated by the reconciliation worker safety net to keep the registry fresh.
_open_orders_by_token: dict[str, list[dict[str, Any]]] = {}


def register_open_orders(token_id: str, orders: list[dict[str, Any]]) -> None:
    _open_orders_by_token[token_id] = orders


def unregister_token(token_id: str) -> None:
    _open_orders_by_token.pop(token_id, None)


def get_registered_token_ids() -> list[str]:
    return list(_open_orders_by_token.keys())


async def evaluate_exit_for_token(
    token_id: str,
    mid_price: float,
    bid: float,
    ask: float,
) -> dict[str, Any]:
    """Event-driven exit evaluation triggered by significant price changes.

    Called by the FeedManager on_change callback (>0.5% price move) for tokens
    that have open positions. This replaces the polling-based exit checks for
    latency-sensitive triggers (stop-loss, take-profit, trailing stop).

    Returns a summary dict with orders that triggered exit conditions.
    No heavy DB work is performed here; the actual exit submission is delegated
    to the reconciliation safety-net or a direct live execution call.
    """
    orders = _open_orders_by_token.get(token_id)
    if not orders:
        return {"token_id": token_id, "evaluated": 0, "triggered": []}

    now = utcnow()
    triggered: list[dict[str, Any]] = []

    for order in orders:
        order_id = str(order.get("order_id") or "").strip()
        if not order_id:
            continue

        entry_price = safe_float(order.get("entry_price"))
        if entry_price is None or entry_price <= 0:
            continue

        # Skip orders that already have a pending exit in flight
        if order.get("has_pending_exit"):
            continue

        take_profit_pct = safe_float(order.get("take_profit_pct"))
        stop_loss_pct = safe_float(order.get("stop_loss_pct"))
        trailing_stop_pct = safe_float(order.get("trailing_stop_pct"))
        min_hold_minutes = max(0.0, safe_float(order.get("min_hold_minutes")) or 0.0)
        highest_price = safe_float(order.get("highest_price"))

        age_anchor_iso = order.get("age_anchor")
        age_minutes: Optional[float] = None
        if age_anchor_iso:
            age_anchor = _parse_iso_utc_naive(age_anchor_iso)
            if age_anchor is not None:
                now_naive = now.astimezone(timezone.utc).replace(tzinfo=None) if now.tzinfo else now
                age_minutes = max(0.0, (now_naive - age_anchor).total_seconds() / 60.0)

        min_hold_passed = age_minutes is None or age_minutes >= min_hold_minutes
        if not min_hold_passed:
            continue

        pnl_pct = ((mid_price - entry_price) / entry_price) * 100.0

        close_trigger: Optional[str] = None

        if take_profit_pct is not None and pnl_pct >= take_profit_pct:
            close_trigger = "take_profit"
        elif stop_loss_pct is not None and pnl_pct <= -abs(stop_loss_pct):
            close_trigger = "stop_loss"
        elif (
            trailing_stop_pct is not None
            and trailing_stop_pct > 0
            and highest_price is not None
            and highest_price > entry_price
        ):
            # Update highest price in the registry opportunistically
            if mid_price > highest_price:
                order["highest_price"] = mid_price
                highest_price = mid_price
            trailing_trigger = highest_price * (1.0 - (trailing_stop_pct / 100.0))
            if mid_price <= trailing_trigger:
                close_trigger = "trailing_stop"

        if close_trigger is not None:
            triggered.append({
                "order_id": order_id,
                "trader_id": order.get("trader_id", ""),
                "token_id": token_id,
                "close_trigger": close_trigger,
                "mid_price": mid_price,
                "bid": bid,
                "ask": ask,
                "entry_price": entry_price,
                "pnl_pct": round(pnl_pct, 4),
                "highest_price": highest_price,
                "age_minutes": round(age_minutes, 2) if age_minutes is not None else None,
            })

    return {
        "token_id": token_id,
        "evaluated": len(orders),
        "triggered": triggered,
    }
