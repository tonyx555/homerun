from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.database import TraderOrder, TraderOrderVerificationEvent
from utils.utcnow import utcnow


TRADER_ORDER_VERIFICATION_LOCAL = "local"
TRADER_ORDER_VERIFICATION_VENUE_ORDER = "venue_order"
TRADER_ORDER_VERIFICATION_VENUE_FILL = "venue_fill"
TRADER_ORDER_VERIFICATION_WALLET_POSITION = "wallet_position"
TRADER_ORDER_VERIFICATION_WALLET_ACTIVITY = "wallet_activity"
# Operator-asserted close.  The system cannot self-assign this status —
# only the manual-writeoff API path (backed by an immutable
# TraderOrderVerificationEvent + operator identity) does.  Used when a
# position is genuinely unrecoverable (illiquid market that won't
# resolve, contract bug, etc.) and the operator has externally
# accounted for the loss.  Distinct from wallet_activity so an auditor
# can grep operator-touched rows.
TRADER_ORDER_VERIFICATION_MANUAL_WRITEOFF = "manual_writeoff"
TRADER_ORDER_VERIFICATION_SUMMARY_ONLY = "summary_only"
TRADER_ORDER_VERIFICATION_DISPUTED = "disputed"

TRADER_ORDER_HIDDEN_VERIFICATION_STATUSES = {
    TRADER_ORDER_VERIFICATION_SUMMARY_ONLY,
    TRADER_ORDER_VERIFICATION_DISPUTED,
}


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def normalize_trader_order_verification_status(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {
        TRADER_ORDER_VERIFICATION_LOCAL,
        TRADER_ORDER_VERIFICATION_VENUE_ORDER,
        TRADER_ORDER_VERIFICATION_VENUE_FILL,
        TRADER_ORDER_VERIFICATION_WALLET_POSITION,
        TRADER_ORDER_VERIFICATION_WALLET_ACTIVITY,
        TRADER_ORDER_VERIFICATION_MANUAL_WRITEOFF,
        TRADER_ORDER_VERIFICATION_SUMMARY_ONLY,
        TRADER_ORDER_VERIFICATION_DISPUTED,
    }:
        return normalized
    return TRADER_ORDER_VERIFICATION_LOCAL


def trader_order_verification_rank(value: Any) -> int:
    normalized = normalize_trader_order_verification_status(value)
    if normalized == TRADER_ORDER_VERIFICATION_DISPUTED:
        return -10
    if normalized == TRADER_ORDER_VERIFICATION_SUMMARY_ONLY:
        return -5
    if normalized == TRADER_ORDER_VERIFICATION_LOCAL:
        return 0
    if normalized == TRADER_ORDER_VERIFICATION_VENUE_ORDER:
        return 10
    if normalized == TRADER_ORDER_VERIFICATION_VENUE_FILL:
        return 20
    if normalized == TRADER_ORDER_VERIFICATION_WALLET_POSITION:
        return 30
    if normalized == TRADER_ORDER_VERIFICATION_WALLET_ACTIVITY:
        return 40
    if normalized == TRADER_ORDER_VERIFICATION_MANUAL_WRITEOFF:
        # Higher than wallet_activity — operator override is final.
        return 50
    return 0


def trader_order_verification_hidden(value: Any) -> bool:
    return normalize_trader_order_verification_status(value) in TRADER_ORDER_HIDDEN_VERIFICATION_STATUSES


def extract_trader_order_provider_order_id(payload: dict[str, Any] | None) -> str | None:
    source = payload if isinstance(payload, dict) else {}
    execution_payload = source.get("execution_session")
    execution_payload = execution_payload if isinstance(execution_payload, dict) else {}
    candidates = (
        source.get("provider_order_id"),
        source.get("order_id"),
        execution_payload.get("provider_order_id"),
        execution_payload.get("order_id"),
    )
    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text
    return None


def extract_trader_order_provider_clob_order_id(payload: dict[str, Any] | None) -> str | None:
    source = payload if isinstance(payload, dict) else {}
    execution_payload = source.get("execution_session")
    execution_payload = execution_payload if isinstance(execution_payload, dict) else {}
    provider_reconciliation = source.get("provider_reconciliation")
    provider_reconciliation = provider_reconciliation if isinstance(provider_reconciliation, dict) else {}
    candidates = (
        source.get("provider_clob_order_id"),
        source.get("clob_order_id"),
        execution_payload.get("provider_clob_order_id"),
        execution_payload.get("clob_order_id"),
        provider_reconciliation.get("provider_clob_order_id"),
    )
    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text
    return None


def extract_trader_order_execution_wallet_address(payload: dict[str, Any] | None) -> str | None:
    source = payload if isinstance(payload, dict) else {}
    live_wallet_authority = source.get("live_wallet_authority")
    live_wallet_authority = live_wallet_authority if isinstance(live_wallet_authority, dict) else {}
    candidates = (
        source.get("execution_wallet_address"),
        live_wallet_authority.get("wallet_address"),
    )
    for value in candidates:
        text = str(value or "").strip().lower()
        if text:
            return text
    return None


def extract_trader_order_verification_tx_hash(payload: dict[str, Any] | None) -> str | None:
    source = payload if isinstance(payload, dict) else {}
    position_close = source.get("position_close")
    position_close = position_close if isinstance(position_close, dict) else {}
    timeout_taker_rescue = source.get("timeout_taker_rescue")
    timeout_taker_rescue = timeout_taker_rescue if isinstance(timeout_taker_rescue, dict) else {}
    provider_reconciliation = source.get("provider_reconciliation")
    provider_reconciliation = provider_reconciliation if isinstance(provider_reconciliation, dict) else {}
    provider_snapshot = provider_reconciliation.get("snapshot")
    provider_snapshot = provider_snapshot if isinstance(provider_snapshot, dict) else {}
    provider_raw = provider_snapshot.get("raw")
    provider_raw = provider_raw if isinstance(provider_raw, dict) else {}
    candidates = (
        position_close.get("wallet_activity_transaction_hash"),
        position_close.get("transaction_hash"),
        position_close.get("tx_hash"),
        timeout_taker_rescue.get("transaction_hash"),
        timeout_taker_rescue.get("tx_hash"),
        provider_raw.get("transactionHash"),
        provider_raw.get("transaction_hash"),
        source.get("transaction_hash"),
        source.get("tx_hash"),
    )
    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text
    return None


def extract_trader_order_trade_fields(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    position_close = source.get("position_close")
    position_close = position_close if isinstance(position_close, dict) else {}

    token_id = str(source.get("token_id") or source.get("asset") or "").strip() or None

    side = str(source.get("side") or "").strip().upper() or None
    if position_close:
        side = "SELL"

    close_price = _safe_float(position_close.get("close_price"))
    entry_price = _safe_float(source.get("effective_price")) or _safe_float(source.get("entry_price"))
    price = close_price if close_price is not None else entry_price

    size = _safe_float(position_close.get("wallet_activity_size"))
    if size is None:
        size = _safe_float(position_close.get("wallet_trade_size"))
    if size is None:
        size = _safe_float(position_close.get("filled_size"))
    if size is None:
        requested_size = _safe_float(source.get("requested_size"))
        if requested_size is not None:
            size = requested_size
    if size is None:
        notional = _safe_float(source.get("notional_usd"))
        if notional is not None and price and price > 0:
            size = notional / price

    trade_timestamp = (
        position_close.get("wallet_activity_timestamp")
        or position_close.get("wallet_trade_timestamp")
        or position_close.get("closed_at")
    )
    trade_id = (
        str(position_close.get("wallet_trade_id") or position_close.get("wallet_activity_id") or "").strip() or None
    )

    return {
        "token_id": token_id,
        "side": side,
        "price": price,
        "size": size,
        "trade_timestamp": trade_timestamp,
        "trade_id": trade_id,
    }


def _derive_live_trader_order_lineage(
    *,
    status_key: str,
    reason_key: str,
    payload: dict[str, Any],
    provider_order_id: str | None,
    provider_clob_order_id: str | None,
    execution_wallet_address: str | None,
    verification_tx_hash: str | None,
    wallet_trade_tx_hash: str,
    wallet_trade_id: str,
) -> dict[str, Any]:
    position_close = payload.get("position_close")
    position_close = position_close if isinstance(position_close, dict) else {}
    provider_reconciliation = payload.get("provider_reconciliation")
    provider_reconciliation = provider_reconciliation if isinstance(provider_reconciliation, dict) else {}
    provider_snapshot = provider_reconciliation.get("snapshot")
    provider_snapshot = provider_snapshot if isinstance(provider_snapshot, dict) else {}
    position_state = payload.get("position_state")
    position_state = position_state if isinstance(position_state, dict) else {}
    close_price_source = str(position_close.get("price_source") or "").strip().lower()
    close_trigger = str(position_close.get("close_trigger") or "").strip().lower()
    snapshot_status = str(
        provider_reconciliation.get("snapshot_status")
        or provider_snapshot.get("normalized_status")
        or provider_snapshot.get("status")
        or ""
    ).strip().lower()
    filled_size = float(provider_reconciliation.get("filled_size") or provider_snapshot.get("filled_size") or 0.0)

    if wallet_trade_id or wallet_trade_tx_hash or close_price_source in {"wallet_trade", "wallet_flat_override", "wallet_activity"}:
        return {
            "verification_status": TRADER_ORDER_VERIFICATION_WALLET_ACTIVITY,
            "verification_source": "wallet_activity_api" if close_price_source == "wallet_activity" else "wallet_trade_api",
            "verification_reason": close_trigger or close_price_source or None,
            "provider_order_id": provider_order_id,
            "provider_clob_order_id": provider_clob_order_id,
            "execution_wallet_address": execution_wallet_address,
            "verification_tx_hash": verification_tx_hash or wallet_trade_tx_hash or None,
        }

    if position_state and status_key in {"open", "executed", "submitted"} and execution_wallet_address:
        return {
            "verification_status": TRADER_ORDER_VERIFICATION_WALLET_POSITION,
            "verification_source": "wallet_positions_api",
            "verification_reason": None,
            "provider_order_id": provider_order_id,
            "provider_clob_order_id": provider_clob_order_id,
            "execution_wallet_address": execution_wallet_address,
            "verification_tx_hash": verification_tx_hash or None,
        }

    if snapshot_status in {"filled", "partially_filled"} or filled_size > 0.0:
        return {
            "verification_status": TRADER_ORDER_VERIFICATION_VENUE_FILL,
            "verification_source": "live_order_snapshot",
            "verification_reason": snapshot_status or None,
            "provider_order_id": provider_order_id,
            "provider_clob_order_id": provider_clob_order_id,
            "execution_wallet_address": execution_wallet_address,
            "verification_tx_hash": verification_tx_hash or None,
        }

    if provider_clob_order_id or provider_order_id:
        return {
            "verification_status": TRADER_ORDER_VERIFICATION_VENUE_ORDER,
            "verification_source": "live_order_ack",
            "verification_reason": None,
            "provider_order_id": provider_order_id,
            "provider_clob_order_id": provider_clob_order_id,
            "execution_wallet_address": execution_wallet_address,
            "verification_tx_hash": verification_tx_hash or None,
        }

    if reason_key == "recovered from live venue authority":
        return {
            "verification_status": TRADER_ORDER_VERIFICATION_DISPUTED,
            "verification_source": "live_wallet_authority",
            "verification_reason": "recovered_row_without_order_identifiers",
            "provider_order_id": provider_order_id,
            "provider_clob_order_id": provider_clob_order_id,
            "execution_wallet_address": execution_wallet_address,
            "verification_tx_hash": verification_tx_hash or None,
        }

    if execution_wallet_address:
        return {
            "verification_status": TRADER_ORDER_VERIFICATION_WALLET_POSITION,
            "verification_source": "wallet_positions_api",
            "verification_reason": "wallet_adopted_position",
            "provider_order_id": provider_order_id,
            "provider_clob_order_id": provider_clob_order_id,
            "execution_wallet_address": execution_wallet_address,
            "verification_tx_hash": verification_tx_hash or None,
        }

    return {
        "verification_status": TRADER_ORDER_VERIFICATION_LOCAL,
        "verification_source": "local_runtime",
        "verification_reason": None,
        "provider_order_id": provider_order_id,
        "provider_clob_order_id": provider_clob_order_id,
        "execution_wallet_address": execution_wallet_address,
        "verification_tx_hash": verification_tx_hash or None,
    }


def derive_trader_order_verification(
    *,
    mode: Any,
    status: Any,
    reason: Any,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    mode_key = str(mode or "").strip().lower()
    status_key = str(status or "").strip().lower()
    reason_key = str(reason or "").strip().lower()
    position_close = payload.get("position_close")
    position_close = position_close if isinstance(position_close, dict) else {}
    provider_order_id = extract_trader_order_provider_order_id(payload)
    provider_clob_order_id = extract_trader_order_provider_clob_order_id(payload)
    execution_wallet_address = extract_trader_order_execution_wallet_address(payload)
    verification_tx_hash = extract_trader_order_verification_tx_hash(payload)
    close_price_source = str(position_close.get("price_source") or "").strip().lower()
    close_trigger = str(position_close.get("close_trigger") or "").strip().lower()
    wallet_trade_id = str(position_close.get("wallet_trade_id") or "").strip()
    wallet_trade_tx_hash = str(
        position_close.get("wallet_trade_transaction_hash")
        or position_close.get("transaction_hash")
        or position_close.get("tx_hash")
        or ""
    ).strip()

    if mode_key != "live":
        return {
            "verification_status": TRADER_ORDER_VERIFICATION_LOCAL,
            "verification_source": "local_runtime",
            "verification_reason": None,
            "provider_order_id": provider_order_id,
            "provider_clob_order_id": provider_clob_order_id,
            "execution_wallet_address": execution_wallet_address,
            "verification_tx_hash": verification_tx_hash or wallet_trade_tx_hash or None,
        }

    lineage = _derive_live_trader_order_lineage(
        status_key=status_key,
        reason_key=reason_key,
        payload=payload,
        provider_order_id=provider_order_id,
        provider_clob_order_id=provider_clob_order_id,
        execution_wallet_address=execution_wallet_address,
        verification_tx_hash=verification_tx_hash,
        wallet_trade_tx_hash=wallet_trade_tx_hash,
        wallet_trade_id=wallet_trade_id,
    )

    if close_price_source == "closed_positions_api":
        lineage_status = normalize_trader_order_verification_status(lineage.get("verification_status"))
        if lineage_status in {
            TRADER_ORDER_VERIFICATION_VENUE_ORDER,
            TRADER_ORDER_VERIFICATION_VENUE_FILL,
            TRADER_ORDER_VERIFICATION_WALLET_ACTIVITY,
            TRADER_ORDER_VERIFICATION_WALLET_POSITION,
        }:
            lineage["verification_reason"] = "summary_only_close_recovery"
            return lineage
        if reason_key == "recovered from live venue authority":
            return {
                "verification_status": TRADER_ORDER_VERIFICATION_DISPUTED,
                "verification_source": "closed_positions_api",
                "verification_reason": "recovered_row_without_order_lineage",
                "provider_order_id": provider_order_id,
                "provider_clob_order_id": provider_clob_order_id,
                "execution_wallet_address": execution_wallet_address,
                "verification_tx_hash": verification_tx_hash or wallet_trade_tx_hash or None,
            }
        return {
            "verification_status": TRADER_ORDER_VERIFICATION_SUMMARY_ONLY,
            "verification_source": "closed_positions_api",
            "verification_reason": "summary_only_close_recovery",
            "provider_order_id": provider_order_id,
            "provider_clob_order_id": provider_clob_order_id,
            "execution_wallet_address": execution_wallet_address,
            "verification_tx_hash": verification_tx_hash or wallet_trade_tx_hash or None,
        }

    if close_price_source in {"provider_exit_fill", "resolved_settlement", "wallet_redeemable_mark"}:
        return {
            "verification_status": TRADER_ORDER_VERIFICATION_VENUE_FILL,
            "verification_source": close_price_source,
            "verification_reason": close_trigger or close_price_source or None,
            "provider_order_id": provider_order_id,
            "provider_clob_order_id": provider_clob_order_id,
            "execution_wallet_address": execution_wallet_address,
            "verification_tx_hash": verification_tx_hash or wallet_trade_tx_hash or None,
        }

    return lineage


def apply_trader_order_verification(
    row: TraderOrder,
    *,
    verification_status: str,
    verification_source: str | None,
    verification_reason: str | None,
    provider_order_id: str | None = None,
    provider_clob_order_id: str | None = None,
    execution_wallet_address: str | None = None,
    verification_tx_hash: str | None = None,
    verified_at: datetime | None = None,
    force: bool = False,
) -> bool:
    changed = False
    normalized_status = normalize_trader_order_verification_status(verification_status)
    current_status = normalize_trader_order_verification_status(getattr(row, "verification_status", None))
    if force or trader_order_verification_rank(normalized_status) >= trader_order_verification_rank(current_status):
        verification_state_changed = False
        if current_status != normalized_status:
            row.verification_status = normalized_status
            changed = True
            verification_state_changed = True
        if verification_source and str(getattr(row, "verification_source", "") or "") != verification_source:
            row.verification_source = verification_source
            changed = True
            verification_state_changed = True
        if verification_reason != getattr(row, "verification_reason", None):
            row.verification_reason = verification_reason
            changed = True
            verification_state_changed = True
        resolved_verified_at = verified_at or utcnow()
        if (verification_state_changed or getattr(row, "verified_at", None) is None) and getattr(row, "verified_at", None) != resolved_verified_at:
            row.verified_at = resolved_verified_at
            changed = True

    if provider_order_id and str(getattr(row, "provider_order_id", "") or "") != provider_order_id:
        row.provider_order_id = provider_order_id
        changed = True
    if provider_clob_order_id and str(getattr(row, "provider_clob_order_id", "") or "") != provider_clob_order_id:
        row.provider_clob_order_id = provider_clob_order_id
        changed = True
    if execution_wallet_address:
        wallet = execution_wallet_address.strip().lower()
        if wallet and str(getattr(row, "execution_wallet_address", "") or "") != wallet:
            row.execution_wallet_address = wallet
            changed = True
    if verification_tx_hash and str(getattr(row, "verification_tx_hash", "") or "") != verification_tx_hash:
        row.verification_tx_hash = verification_tx_hash
        changed = True
    return changed


def append_trader_order_verification_event(
    session: AsyncSession,
    *,
    trader_order_id: str,
    verification_status: str,
    source: str | None,
    event_type: str,
    reason: str | None = None,
    provider_order_id: str | None = None,
    provider_clob_order_id: str | None = None,
    execution_wallet_address: str | None = None,
    tx_hash: str | None = None,
    token_id: str | None = None,
    side: str | None = None,
    price: float | None = None,
    size: float | None = None,
    trade_timestamp: datetime | None = None,
    trade_id: str | None = None,
    payload_json: dict[str, Any] | None = None,
    created_at: datetime | None = None,
) -> TraderOrderVerificationEvent:
    derived_trade = extract_trader_order_trade_fields(payload_json)
    resolved_token_id = token_id or derived_trade.get("token_id")
    resolved_side = side or derived_trade.get("side")
    resolved_price = price if price is not None else derived_trade.get("price")
    resolved_size = size if size is not None else derived_trade.get("size")
    resolved_trade_timestamp = trade_timestamp or derived_trade.get("trade_timestamp")
    resolved_trade_id = trade_id or derived_trade.get("trade_id")
    resolved_created_at = created_at or utcnow()
    event_key = (
        str(trader_order_id or "").strip(),
        normalize_trader_order_verification_status(verification_status),
        str(source or "").strip(),
        str(event_type or "").strip() or "verification_update",
        str(reason or "").strip(),
        str(provider_order_id or "").strip(),
        str(provider_clob_order_id or "").strip(),
        str(execution_wallet_address or "").strip().lower(),
        str(tx_hash or "").strip(),
        str(resolved_token_id or "").strip(),
        str(resolved_side or "").strip(),
        _safe_float(resolved_price),
        _safe_float(resolved_size),
        _parse_timestamp(resolved_trade_timestamp),
        str(resolved_trade_id or "").strip(),
    )
    event_rows_by_key = session.info.setdefault("trader_order_verification_event_rows_by_key", {})
    if event_key in event_rows_by_key:
        return event_rows_by_key[event_key]

    row = TraderOrderVerificationEvent(
        id=uuid4().hex,
        trader_order_id=trader_order_id,
        verification_status=normalize_trader_order_verification_status(verification_status),
        source=str(source or "").strip() or None,
        event_type=str(event_type or "").strip() or "verification_update",
        reason=str(reason or "").strip() or None,
        provider_order_id=str(provider_order_id or "").strip() or None,
        provider_clob_order_id=str(provider_clob_order_id or "").strip() or None,
        execution_wallet_address=str(execution_wallet_address or "").strip().lower() or None,
        tx_hash=str(tx_hash or "").strip() or None,
        token_id=str(resolved_token_id or "").strip() or None,
        side=str(resolved_side or "").strip() or None,
        price=_safe_float(resolved_price),
        size=_safe_float(resolved_size),
        trade_timestamp=_parse_timestamp(resolved_trade_timestamp),
        trade_id=str(resolved_trade_id or "").strip() or None,
        payload_json=dict(payload_json or {}),
        created_at=resolved_created_at,
    )
    session.add(row)
    event_rows_by_key[event_key] = row
    return row


async def backfill_trader_order_verification(
    session: AsyncSession,
    *,
    batch_size: int = 1000,
    emit_events: bool = True,
) -> dict[str, int]:
    capped_batch_size = max(100, min(int(batch_size or 1000), 5000))
    rows = list(
        (
            await session.execute(
                select(TraderOrder).order_by(TraderOrder.created_at.asc(), TraderOrder.id.asc())
            )
        )
        .scalars()
        .all()
    )
    updated = 0
    hidden = 0
    summary_only = 0
    disputed = 0
    emitted = 0

    for index, row in enumerate(rows, start=1):
        payload = dict(row.payload_json or {})
        previous_status = normalize_trader_order_verification_status(getattr(row, "verification_status", None))
        previous_source = str(getattr(row, "verification_source", "") or "")
        previous_reason = getattr(row, "verification_reason", None)
        previous_order_id = str(getattr(row, "provider_order_id", "") or "")
        previous_clob_id = str(getattr(row, "provider_clob_order_id", "") or "")
        previous_wallet = str(getattr(row, "execution_wallet_address", "") or "")
        previous_tx_hash = str(getattr(row, "verification_tx_hash", "") or "")

        derived = derive_trader_order_verification(
            mode=row.mode,
            status=row.status,
            reason=row.reason,
            payload=payload,
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
            verified_at=row.verified_at or row.updated_at or row.executed_at or row.created_at or utcnow(),
            force=True,
        )
        current_status = normalize_trader_order_verification_status(getattr(row, "verification_status", None))
        current_source = str(getattr(row, "verification_source", "") or "")
        current_reason = getattr(row, "verification_reason", None)
        current_order_id = str(getattr(row, "provider_order_id", "") or "")
        current_clob_id = str(getattr(row, "provider_clob_order_id", "") or "")
        current_wallet = str(getattr(row, "execution_wallet_address", "") or "")
        current_tx_hash = str(getattr(row, "verification_tx_hash", "") or "")
        state_changed = (
            previous_status != current_status
            or previous_source != current_source
            or previous_reason != current_reason
            or previous_order_id != current_order_id
            or previous_clob_id != current_clob_id
            or previous_wallet != current_wallet
            or previous_tx_hash != current_tx_hash
        )
        if state_changed or changed:
            updated += 1
            if emit_events:
                append_trader_order_verification_event(
                    session,
                    trader_order_id=str(row.id),
                    verification_status=current_status,
                    source=current_source or None,
                    event_type="verification_backfill",
                    reason=current_reason,
                    provider_order_id=current_order_id or None,
                    provider_clob_order_id=current_clob_id or None,
                    execution_wallet_address=current_wallet or None,
                    tx_hash=current_tx_hash or None,
                    payload_json={},
                    created_at=row.verified_at or row.updated_at or row.executed_at or row.created_at or utcnow(),
                )
                emitted += 1
        if trader_order_verification_hidden(current_status):
            hidden += 1
        if current_status == TRADER_ORDER_VERIFICATION_SUMMARY_ONLY:
            summary_only += 1
        elif current_status == TRADER_ORDER_VERIFICATION_DISPUTED:
            disputed += 1
        if index % capped_batch_size == 0:
            await session.flush()

    return {
        "total_orders": len(rows),
        "updated_orders": updated,
        "hidden_orders": hidden,
        "summary_only_orders": summary_only,
        "disputed_orders": disputed,
        "emitted_events": emitted,
    }
