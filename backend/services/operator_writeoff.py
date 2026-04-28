"""Operator-asserted manual write-off for stuck trader orders.

This is the institutional escape hatch for positions that are
genuinely unrecoverable through automatic means (illiquid market that
will not resolve, contract misconfiguration, etc.).  It is the ONLY
code path that can write a non-NULL ``actual_profit`` outside of the
``polymarket_trade_verifier``'s wallet-activity match.

Hard rules — enforced both here AND at the DB layer:

  1. The order MUST currently be in a stuck state — open lifecycle
     status AND ``payload['pending_live_exit']['status']`` in
     ``_BLOCKED_TERMINAL_STATES``.  Random orders cannot be
     manually written off; that's what wallet-activity verification
     is for.

  2. Caller MUST supply:
       * ``realized_pnl`` (explicit float; the operator is asserting
         a number, not asking the system to infer one),
       * ``reason`` (non-empty text; mandatory paper trail),
       * ``operator_id`` (free-form identifier — auth context fills
         this from the API gate).

  3. An immutable ``TraderOrderVerificationEvent`` is appended with
     ``event_type='manual_writeoff'`` and a payload containing the
     operator id, reason, the on-chain status snapshot at decision
     time (if available), and prior ``actual_profit`` (almost always
     ``None`` due to the DB guard, but recorded for completeness).

  4. The function NEVER writes if any rule fails.  It returns a
     structured rejection rather than mutating partially.

The write is performed in the caller's session — the API endpoint is
responsible for committing.  This module is pure orchestration.
"""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.database import TraderOrder
from services.trader_order_verification import (
    TRADER_ORDER_VERIFICATION_MANUAL_WRITEOFF,
    apply_trader_order_verification,
    append_trader_order_verification_event,
)
from utils.utcnow import utcnow


_BLOCKED_TERMINAL_STATES = frozenset(
    {
        "blocked_persistent_timeout",
        "blocked_no_inventory",
        "blocked_retry_exhausted",
        "blocked_retry_exhausted_hard",
    }
)

_OPEN_LIFECYCLE_STATUSES = frozenset(
    {"submitted", "executed", "completed", "open", "pending", "placing", "queued"}
)


class ManualWriteoffRejected(ValueError):
    """Raised when the operator's request fails a precondition.

    The error message identifies the failing rule so the API can
    surface it directly to the caller.
    """


def _close_status_for_pnl(realized_pnl: float) -> str:
    """Map a realized P&L number to the canonical ``trader_orders.status``
    closing value."""
    if realized_pnl > 0:
        return "closed_win"
    if realized_pnl < 0:
        return "closed_loss"
    return "closed_loss"


async def manual_writeoff_order(
    session: AsyncSession,
    *,
    order_id: str,
    realized_pnl: float,
    reason: str,
    operator_id: str,
    chain_evidence: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Apply an operator-asserted close to a stuck order.

    Returns a structured summary on success.  Raises
    ``ManualWriteoffRejected`` (subclass of ``ValueError``) when any
    rule fails — caller should map that to a 400-class HTTP response.

    The session is NOT committed here; the caller commits.
    """
    normalized_order_id = str(order_id or "").strip()
    if not normalized_order_id:
        raise ManualWriteoffRejected("order_id is required")

    normalized_reason = str(reason or "").strip()
    if not normalized_reason:
        raise ManualWriteoffRejected("reason is required (non-empty string)")
    if len(normalized_reason) > 2000:
        raise ManualWriteoffRejected("reason must be ≤ 2000 characters")

    normalized_operator = str(operator_id or "").strip()
    if not normalized_operator:
        raise ManualWriteoffRejected("operator_id is required")

    try:
        realized_pnl_float = float(realized_pnl)
    except (TypeError, ValueError) as exc:
        raise ManualWriteoffRejected(f"realized_pnl must be a number: {exc}")
    if realized_pnl_float != realized_pnl_float:  # NaN check
        raise ManualWriteoffRejected("realized_pnl must be a finite number (got NaN)")
    if abs(realized_pnl_float) == float("inf"):
        raise ManualWriteoffRejected("realized_pnl must be a finite number (got inf)")

    row = (
        await session.execute(
            select(TraderOrder).where(TraderOrder.id == normalized_order_id)
        )
    ).scalar_one_or_none()
    if row is None:
        raise ManualWriteoffRejected(f"order_id={normalized_order_id} not found")

    current_status = str(row.status or "").strip().lower()
    if current_status not in _OPEN_LIFECYCLE_STATUSES:
        raise ManualWriteoffRejected(
            f"order is already terminal (status={current_status}); manual writeoff requires "
            f"a stuck OPEN order"
        )

    payload = dict(row.payload_json or {})
    pending_exit = payload.get("pending_live_exit")
    if not isinstance(pending_exit, dict):
        raise ManualWriteoffRejected(
            "order has no pending_live_exit payload — manual writeoff is only allowed for "
            "orders the lifecycle has already attempted to close (and failed)"
        )
    pe_status = str(pending_exit.get("status") or "").strip().lower()
    if pe_status not in _BLOCKED_TERMINAL_STATES:
        raise ManualWriteoffRejected(
            f"pending_live_exit.status={pe_status!r} is not a blocked-terminal state; "
            f"manual writeoff is only allowed when the lifecycle has circuit-broken the retry"
        )

    now = utcnow()
    prior_actual_profit = (
        float(row.actual_profit) if row.actual_profit is not None else None
    )

    next_status = _close_status_for_pnl(realized_pnl_float)

    # Mutations — atomic w.r.t. the caller's transaction; failure here
    # raises and the session rolls back.
    row.actual_profit = float(realized_pnl_float)
    row.status = next_status
    row.updated_at = now

    pending_exit["status"] = "superseded_manual_writeoff"
    pending_exit["resolved_at"] = now.isoformat()
    payload["pending_live_exit"] = pending_exit
    payload["position_close"] = {
        "close_price": None,
        "price_source": "manual_writeoff",
        "close_trigger": "manual_writeoff",
        "realized_pnl": float(realized_pnl_float),
        "closed_at": now.isoformat(),
        "reason": normalized_reason,
        "operator_id": normalized_operator,
    }
    row.payload_json = payload

    apply_trader_order_verification(
        row,
        verification_status=TRADER_ORDER_VERIFICATION_MANUAL_WRITEOFF,
        verification_source="operator",
        verification_reason=normalized_reason,
        verified_at=now,
        force=True,
    )

    event_payload: dict[str, Any] = {
        "operator_id": normalized_operator,
        "reason": normalized_reason,
        "realized_pnl": float(realized_pnl_float),
        "prior_actual_profit": prior_actual_profit,
        "prior_pending_exit_status": pe_status,
        "prior_lifecycle_status": current_status,
    }
    if isinstance(chain_evidence, dict):
        event_payload["chain_evidence"] = chain_evidence

    append_trader_order_verification_event(
        session,
        trader_order_id=normalized_order_id,
        verification_status=TRADER_ORDER_VERIFICATION_MANUAL_WRITEOFF,
        source="operator",
        event_type="manual_writeoff",
        reason=normalized_reason,
        payload_json=event_payload,
        created_at=now,
    )

    return {
        "order_id": normalized_order_id,
        "next_status": next_status,
        "realized_pnl": float(realized_pnl_float),
        "operator_id": normalized_operator,
        "reason": normalized_reason,
        "applied_at": now.isoformat(),
    }
