"""Single-leg direct submission path for fast-tier traders.

``execute_fast_signal`` is the narrow fast-path alternative to the full
``ExecutionSessionEngine.execute_signal`` orchestration.  It is designed
for ``latency_class='fast'`` traders that trade one leg on one market at
a time (sub-second crypto binaries are the canonical use case).

Key differences vs. SessionEngine:

* No ``execution_sessions`` / ``execution_session_orders`` /
  ``execution_session_legs`` / ``execution_session_events`` rows.
* No pre-submit placeholder with a 45s ack timeout.
* No leg-wave / reprice orchestration — one attempt, one result.
* A single DB transaction writes one ``TraderOrder`` row; position
  inventory is rolled into the same commit via ``sync_trader_position_inventory``.

The low-level submission to the provider still flows through the
well-tested ``submit_execution_leg`` primitive in ``order_manager``, so
every token-resolution, buy pre-submit gate, shadow-mode paper-fill and
live-provider submission path remains identical to the orchestrated one.

A fast trader *must* be single-leg single-market.  If the signal has no
``positions_to_take`` or multiple positions, we refuse and return a
``blocked`` result — the trader config is the bug, not the runtime.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

import services.trader_hot_state as hot_state
from services.trader_orchestrator.order_manager import LegSubmitResult, submit_execution_leg
from services.signal_bus import set_trade_signal_status
from services.trader_orchestrator_state import (
    _now,
    create_trader_decision,
    create_trader_order,
    record_signal_consumption,
    upsert_trader_signal_cursor,
)
from utils.converters import safe_float
from utils.logger import get_logger
from utils.signal_helpers import normalize_position_side
from utils.utcnow import utcnow

logger = get_logger(__name__)


@dataclass
class FastSubmitResult:
    """Shape-compatible with ``SessionExecutionResult`` for downstream consumers."""

    session_id: str  # always "" for the fast path (no session was created)
    status: str
    effective_price: float | None
    error_message: str | None
    orders_written: int
    payload: dict[str, Any]
    created_orders: list[dict[str, Any]] = field(default_factory=list)


def _extract_single_position(signal: Any) -> tuple[dict[str, Any] | None, str | None]:
    """Pull the single leg's position dict off a trade signal payload.

    Returns ``(position, error)`` — exactly one of the two is non-None.
    """
    payload = getattr(signal, "payload_json", None)
    if not isinstance(payload, dict):
        return None, "Signal has no payload_json — fast path requires positions_to_take[0]."
    positions = payload.get("positions_to_take") or payload.get("positions")
    if not isinstance(positions, list) or not positions:
        return None, "Signal has no positions_to_take — fast path requires exactly one leg."
    if len(positions) > 1:
        # Fast path is single-leg by design.  A multi-leg signal belongs on the
        # session_engine path (latency_class='normal' or 'slow').  Refusing
        # here is safer than silently dropping legs.
        return None, (
            f"Fast path refuses multi-leg signal (got {len(positions)} legs). "
            "Set trader latency_class to 'normal' or split the strategy."
        )
    pos = positions[0]
    if not isinstance(pos, dict):
        return None, "Signal positions_to_take[0] is not a dict."
    return pos, None


def _leg_from_position(position: dict[str, Any], signal: Any) -> dict[str, Any]:
    """Build the ``leg`` dict that ``submit_execution_leg`` expects."""
    action = str(position.get("action") or position.get("side") or "").strip()
    side = normalize_position_side(action)
    outcome = str(position.get("outcome") or "").strip().upper()
    market_id = str(position.get("market_id") or "").strip() or str(getattr(signal, "market_id", "") or "").strip()
    price = safe_float(position.get("price"), None)
    token_id = str(position.get("token_id") or "").strip() or None
    return {
        "leg_id": f"fast-{getattr(signal, 'id', 'unknown')}-0",
        "leg_index": 0,
        "market_id": market_id,
        "market_question": position.get("market_question") or getattr(signal, "market_question", None),
        "outcome": outcome or None,
        "side": side,
        "token_id": token_id,
        "price": price,
        # Fast-tier defaults: aggressive taker-limit crossing the book for an
        # immediate fill or nothing.  No post-only, no chase, no reprice.
        "price_policy": "taker_limit",
        "time_in_force": "FAK",
        "post_only": False,
        "metadata": {"fast_tier": True},
    }


def _result_payload_for_trader_order(
    *,
    leg_result: LegSubmitResult,
    now_iso: str,
) -> dict[str, Any]:
    """Build the TraderOrder.payload_json for a fast-tier fill.

    Must populate the fields that downstream consumers read:
    * ``provider_reconciliation.snapshot`` — used by
      ``_extract_live_fill_metrics`` for realized P&L and fill tracking.
    * ``position_state`` — used by ``reconcile_live_positions`` for mark
      price bookkeeping when the position is later closed.
    """
    base_payload = dict(leg_result.payload or {})
    filled_size = safe_float(leg_result.shares, 0.0) or 0.0
    filled_notional = safe_float(leg_result.notional_usd, 0.0) or 0.0
    avg_price = leg_result.effective_price if leg_result.effective_price is not None else None

    # Mirror into provider_reconciliation so reconciler + position_lifecycle
    # pick up the fill immediately without waiting for an async reconcile pass.
    snapshot: dict[str, Any] = {
        "filled_size": filled_size,
        "average_fill_price": avg_price,
        "filled_notional_usd": filled_notional,
        "normalized_status": leg_result.status,
    }
    if leg_result.provider_clob_order_id:
        snapshot["clob_order_id"] = leg_result.provider_clob_order_id
    if leg_result.provider_order_id:
        snapshot["provider_order_id"] = leg_result.provider_order_id

    base_payload.setdefault("provider_reconciliation", {})
    base_payload["provider_reconciliation"].update({"snapshot": snapshot})

    base_payload.setdefault(
        "position_state",
        {
            "last_mark_price": avg_price,
            "last_mark_source": "fast_submit_fill",
            "last_marked_at": now_iso,
        },
    )

    if leg_result.provider_order_id:
        base_payload.setdefault("provider_order_id", leg_result.provider_order_id)
    if leg_result.provider_clob_order_id:
        base_payload.setdefault("provider_clob_order_id", leg_result.provider_clob_order_id)

    base_payload["fast_tier"] = True
    return base_payload


async def execute_fast_signal(
    session: AsyncSession,
    *,
    trader_id: str,
    signal: Any,
    decision_id: str | None,
    decision_audit: dict[str, Any] | None = None,
    strategy_key: str | None,
    strategy_version: int | None,
    strategy_params: dict[str, Any] | None,
    mode: str,
    size_usd: float,
    reason: str | None,
) -> FastSubmitResult:
    """Fast-tier single-leg direct submit.

    Writes exactly one ``TraderOrder`` row (plus the verification event
    that ``create_trader_order`` already appends and the
    ``sync_trader_position_inventory`` upsert that fires inside the same
    commit).  Returns a ``FastSubmitResult`` for the caller to record.
    """
    now = _now()
    now_iso = now.isoformat()

    mode_key = str(mode or "").strip().lower() or "shadow"
    notional = float(max(0.0, safe_float(size_usd, 0.0) or 0.0))

    position, parse_error = _extract_single_position(signal)
    if parse_error is not None or position is None:
        return FastSubmitResult(
            session_id="",
            status="failed",
            effective_price=None,
            error_message=parse_error,
            orders_written=0,
            payload={"fast_tier": True, "reason": "bad_signal_shape"},
        )

    leg = _leg_from_position(position, signal)

    try:
        leg_result = await submit_execution_leg(
            mode=mode_key,
            signal=signal,
            leg=leg,
            notional_usd=notional,
            strategy_params=strategy_params,
        )
    except Exception as exc:
        logger.warning("Fast-tier leg submit raised", trader_id=trader_id, exc_info=exc)
        return FastSubmitResult(
            session_id="",
            status="failed",
            effective_price=None,
            error_message=f"submit_execution_leg raised: {type(exc).__name__}: {exc}",
            orders_written=0,
            payload={"fast_tier": True, "reason": "submit_exception"},
        )

    # Map leg result status onto trader_order status.  submit_execution_leg
    # returns: executed | failed | skipped | cancelled.  "skipped" is a
    # pre-submit gate rejection (e.g. buy gate) and produces no order.
    leg_status = str(leg_result.status or "").strip().lower()
    if leg_status == "skipped":
        return FastSubmitResult(
            session_id="",
            status="skipped",
            effective_price=leg_result.effective_price,
            error_message=leg_result.error_message,
            orders_written=0,
            payload={"fast_tier": True, "reason": "pre_submit_gate", "leg": leg_result.payload},
        )

    order_status = {
        "executed": "executed",
        "failed": "failed",
        "cancelled": "cancelled",
    }.get(leg_status, "failed")

    order_payload = _result_payload_for_trader_order(leg_result=leg_result, now_iso=now_iso)

    try:
        if decision_audit is not None and decision_id:
            await create_trader_decision(
                session,
                decision_id=decision_id,
                trader_id=trader_id,
                signal=signal,
                strategy_key=strategy_key or str(getattr(signal, "strategy_type", "") or ""),
                strategy_version=strategy_version,
                decision=str(decision_audit.get("decision") or "selected"),
                reason=decision_audit.get("reason"),
                score=decision_audit.get("score"),
                checks_summary=decision_audit.get("checks_summary"),
                risk_snapshot=decision_audit.get("risk_snapshot"),
                payload=decision_audit.get("payload"),
                trace_id=decision_audit.get("trace_id"),
                commit=False,
            )
        order = await create_trader_order(
            session,
            trader_id=trader_id,
            signal=signal,
            decision_id=decision_id,
            strategy_key=strategy_key,
            strategy_version=strategy_version,
            mode=mode_key,
            status=order_status,
            notional_usd=leg_result.notional_usd if leg_result.notional_usd is not None else notional,
            effective_price=leg_result.effective_price,
            reason=reason,
            payload=order_payload,
            error_message=leg_result.error_message,
            commit=False,
        )
    except Exception as exc:
        try:
            await session.rollback()
        except Exception as rollback_exc:
            logger.debug("Fast-tier trader_order write rollback failed", trader_id=trader_id, exc_info=rollback_exc)
        logger.error("Fast-tier trader_order write failed", trader_id=trader_id, exc_info=exc)
        return FastSubmitResult(
            session_id="",
            status="failed",
            effective_price=leg_result.effective_price,
            error_message=f"create_trader_order raised: {type(exc).__name__}: {exc}",
            orders_written=0,
            payload={"fast_tier": True, "reason": "persist_failed"},
        )

    return FastSubmitResult(
        session_id="",
        status=order_status,
        effective_price=leg_result.effective_price,
        error_message=leg_result.error_message,
        orders_written=1,
        payload={
            "fast_tier": True,
            "trader_order_id": str(getattr(order, "id", "")),
            "leg": leg_result.payload,
            "mode": mode_key,
            "submitted_at": now_iso,
        },
        created_orders=[{"id": str(getattr(order, "id", ""))}],
    )


async def advance_fast_trader_cursor(
    session: AsyncSession,
    *,
    trader_id: str,
    signal: Any,
    decision_id: str | None,
    outcome: str,
    reason: str | None,
) -> None:
    """Advance the trader signal cursor + mark consumption after fast submit.

    Called by the fast-tier runtime once it has committed a TraderOrder so
    the signal is not re-evaluated on the next cycle.  Any DB error here
    is logged but swallowed — leaving the cursor stale is a recoverable
    annoyance, not a money-losing bug (duplicate submission is already
    gated by the occupancy check).
    """
    signal_id = str(getattr(signal, "id", "") or "").strip()
    if not signal_id:
        return
    try:
        await set_trade_signal_status(session, signal_id, outcome, commit=False)
    except Exception as exc:
        logger.debug("Fast-tier set_trade_signal_status failed", exc_info=exc)
    try:
        await record_signal_consumption(
            session,
            trader_id=trader_id,
            signal_id=signal_id,
            outcome=outcome,
            reason=reason,
            decision_id=decision_id,
            commit=False,
        )
    except Exception as exc:
        logger.debug("Fast-tier record_signal_consumption failed", exc_info=exc)
    try:
        created_at = getattr(signal, "created_at", None) or utcnow()
        runtime_sequence = getattr(signal, "runtime_sequence", None)
        normalized_runtime_sequence = int(runtime_sequence) if runtime_sequence is not None else None
        hot_state.update_signal_cursor(
            trader_id,
            "live",
            created_at,
            signal_id,
            normalized_runtime_sequence,
        )
        await upsert_trader_signal_cursor(
            session,
            trader_id=trader_id,
            last_signal_created_at=created_at,
            last_signal_id=signal_id,
            last_runtime_sequence=normalized_runtime_sequence,
            commit=False,
        )
    except Exception as exc:
        logger.debug("Fast-tier upsert_trader_signal_cursor failed", exc_info=exc)
