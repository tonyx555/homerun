from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from services.data_events import BlockReason
from utils.converters import safe_float


def _parse_hhmm_utc(value: Any) -> tuple[int, int] | None:
    text = str(value or "").strip()
    if not text:
        return None
    parts = text.split(":")
    if len(parts) != 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except Exception:
        return None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour, minute


def is_within_trading_window_utc(metadata: dict[str, Any], now_utc: datetime) -> bool:
    window = metadata.get("trading_window_utc")
    if not isinstance(window, dict):
        return True

    start = _parse_hhmm_utc(window.get("start"))
    end = _parse_hhmm_utc(window.get("end"))
    if start is None or end is None:
        return True

    start_minutes = (start[0] * 60) + start[1]
    end_minutes = (end[0] * 60) + end[1]
    now_minutes = (now_utc.hour * 60) + now_utc.minute

    if start_minutes == end_minutes:
        return True
    if start_minutes < end_minutes:
        return start_minutes <= now_minutes < end_minutes
    return now_minutes >= start_minutes or now_minutes < end_minutes


_RISK_CHECK_KEY_TO_BLOCK_REASON: dict[str, str] = {
    "global_daily_loss": BlockReason.RISK_DAILY_LOSS,
    "trader_daily_loss": BlockReason.RISK_DAILY_LOSS,
    "global_daily_total_loss": BlockReason.RISK_DAILY_LOSS,
    "trader_daily_total_loss": BlockReason.RISK_DAILY_LOSS,
    "global_gross_exposure": BlockReason.RISK_GROSS_EXPOSURE,
    "trader_loss_streak": BlockReason.RISK_CONSECUTIVE_LOSS,
    "trader_cooldown": BlockReason.RISK_CONSECUTIVE_LOSS,
    "trader_trade_notional": BlockReason.RISK_TRADE_NOTIONAL,
    "trader_orders_per_cycle": BlockReason.RISK_OPEN_POSITIONS,
    "trader_open_positions": BlockReason.RISK_OPEN_POSITIONS,
    "trader_market_exposure": BlockReason.RISK_MARKET_EXPOSURE,
}


def _risk_block_reason(risk_result: Any) -> str:
    for check in getattr(risk_result, "checks", []) or []:
        if not getattr(check, "passed", True):
            mapped = _RISK_CHECK_KEY_TO_BLOCK_REASON.get(str(getattr(check, "key", "") or ""))
            if mapped:
                return mapped
    return BlockReason.RISK_DAILY_LOSS


def _risk_checks_payload(risk_result: Any) -> list[dict[str, Any]]:
    return [
        {
            "check_key": check.key,
            "check_label": check.key,
            "passed": check.passed,
            "score": check.score,
            "detail": check.detail,
        }
        for check in getattr(risk_result, "checks", []) or []
    ]


def apply_platform_decision_gates(
    *,
    decision_obj: Any,
    runtime_signal: Any,
    strategy: Any | None,
    checks_payload: list[dict[str, Any]],
    trading_window_ok: bool,
    trading_window_config: dict[str, Any] | None,
    global_limits: dict[str, Any],
    effective_risk_limits: dict[str, Any],
    allow_averaging: bool,
    open_market_ids: set[str],
    risk_evaluator: Callable[[float], tuple[Any, dict[str, Any]]] | None,
    invoke_hooks: bool,
) -> dict[str, Any]:
    final_decision = str(getattr(decision_obj, "decision", "failed") or "failed")
    final_reason = str(getattr(decision_obj, "reason", "") or "")
    score = getattr(decision_obj, "score", None)
    size_usd = float(max(1.0, safe_float(getattr(decision_obj, "size_usd", None), 10.0)))
    risk_snapshot: dict[str, Any] = {}
    platform_gates: list[dict[str, Any]] = []

    if final_decision == "selected":
        if trading_window_ok:
            platform_gates.append(
                {
                    "gate": "trading_window",
                    "status": "passed",
                    "detail": "Inside configured UTC trading window",
                }
            )
        else:
            final_decision = "blocked"
            final_reason = "Outside configured trading window (UTC)"
            platform_gates.append(
                {
                    "gate": "trading_window",
                    "status": "blocked",
                    "detail": final_reason,
                }
            )
            if invoke_hooks and strategy is not None:
                if hasattr(strategy, "on_blocked"):
                    strategy.on_blocked(
                        runtime_signal,
                        BlockReason.TRADING_WINDOW,
                        {"trading_window": trading_window_config},
                    )
    else:
        platform_gates.append(
            {
                "gate": "trading_window",
                "status": "skipped",
                "detail": f"Skipped because strategy decision is '{final_decision}'",
            }
        )

    if final_decision == "selected":
        gross_cap = safe_float(global_limits.get("max_gross_exposure_usd"), 5000.0)
        notional_default = max(50.0, gross_cap * 0.10)
        max_trade_notional = max(
            1.0,
            safe_float(
                effective_risk_limits.get("max_trade_notional_usd"),
                notional_default,
            ),
        )
        if size_usd > max_trade_notional:
            original_size = size_usd
            size_usd = max_trade_notional
            platform_gates.append(
                {
                    "gate": "size_cap",
                    "status": "capped",
                    "detail": f"Capped to max_trade_notional_usd={max_trade_notional:.2f}",
                    "payload": {
                        "original_size_usd": original_size,
                        "capped_size_usd": size_usd,
                    },
                }
            )
            if invoke_hooks and strategy is not None:
                if hasattr(strategy, "on_size_capped"):
                    strategy.on_size_capped(original_size, size_usd, "Max position size exceeded")
        else:
            platform_gates.append(
                {
                    "gate": "size_cap",
                    "status": "passed",
                    "detail": f"Size {size_usd:.2f} within max_trade_notional_usd={max_trade_notional:.2f}",
                }
            )
    else:
        platform_gates.append(
            {
                "gate": "size_cap",
                "status": "skipped",
                "detail": f"Skipped because decision is '{final_decision}'",
            }
        )

    if final_decision == "selected" and risk_evaluator is not None:
        risk_result, risk_snapshot_base = risk_evaluator(size_usd)
        if isinstance(risk_snapshot_base, dict):
            risk_snapshot.update(risk_snapshot_base)
        risk_checks = _risk_checks_payload(risk_result)
        checks_payload.extend(risk_checks)
        risk_snapshot.update(
            {
                "allowed": bool(getattr(risk_result, "allowed", False)),
                "reason": str(getattr(risk_result, "reason", "") or ""),
                "checks": risk_checks,
            }
        )

        if bool(getattr(risk_result, "allowed", False)):
            platform_gates.append(
                {
                    "gate": "risk",
                    "status": "passed",
                    "detail": str(getattr(risk_result, "reason", "") or "Risk checks passed"),
                }
            )
        else:
            final_decision = "blocked"
            final_reason = str(getattr(risk_result, "reason", "") or "Risk blocked")
            platform_gates.append(
                {
                    "gate": "risk",
                    "status": "blocked",
                    "detail": final_reason,
                }
            )
            if invoke_hooks and strategy is not None:
                if hasattr(strategy, "on_blocked"):
                    strategy.on_blocked(
                        runtime_signal,
                        _risk_block_reason(risk_result),
                        {"risk_snapshot": risk_snapshot},
                    )
    else:
        platform_gates.append(
            {
                "gate": "risk",
                "status": "skipped",
                "detail": (
                    "Skipped because no risk evaluator was provided"
                    if risk_evaluator is None
                    else f"Skipped because decision is '{final_decision}'"
                ),
            }
        )

    if final_decision == "selected" and not allow_averaging:
        signal_market_id = str(getattr(runtime_signal, "market_id", "") or "").strip()
        stacking_blocked = bool(signal_market_id) and signal_market_id in open_market_ids
        checks_payload.append(
            {
                "check_key": "stacking_guard",
                "check_label": "One active entry per market",
                "passed": not stacking_blocked,
                "score": None,
                "detail": (
                    "allow_averaging=false and market already has an open position"
                    if stacking_blocked
                    else "allow_averaging=false and no open position exists for this market"
                ),
                "payload": {
                    "allow_averaging": False,
                    "market_id": signal_market_id or None,
                },
            }
        )
        if stacking_blocked:
            final_decision = "blocked"
            final_reason = "Stacking guard: market already open while allow_averaging=false"
            platform_gates.append(
                {
                    "gate": "stacking_guard",
                    "status": "blocked",
                    "detail": final_reason,
                }
            )
            if invoke_hooks and strategy is not None:
                if hasattr(strategy, "on_blocked"):
                    strategy.on_blocked(runtime_signal, BlockReason.STACKING_GUARD, {"market_id": signal_market_id})
        else:
            platform_gates.append(
                {
                    "gate": "stacking_guard",
                    "status": "passed",
                    "detail": "No existing open position for market",
                }
            )
    else:
        platform_gates.append(
            {
                "gate": "stacking_guard",
                "status": "skipped",
                "detail": (
                    "Skipped because allow_averaging=true"
                    if allow_averaging
                    else f"Skipped because decision is '{final_decision}'"
                ),
            }
        )

    return {
        "strategy_decision": str(getattr(decision_obj, "decision", "failed") or "failed"),
        "strategy_reason": str(getattr(decision_obj, "reason", "") or ""),
        "final_decision": final_decision,
        "final_reason": final_reason,
        "score": score,
        "size_usd": size_usd,
        "checks_payload": checks_payload,
        "risk_snapshot": risk_snapshot,
        "platform_gates": platform_gates,
    }
