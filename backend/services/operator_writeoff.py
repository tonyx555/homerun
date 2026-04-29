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

# Substrings that indicate a *genuine venue-side rejection* of a SELL
# order — i.e. evidence that Polymarket itself refused to accept the
# trade, not that our own retry path timed out.  A row is only eligible
# for an automated manual writeoff when its
# ``pending_live_exit.last_error`` matches one of these markers, OR the
# operator explicitly passes ``override_venue_rejection_check=True``
# with a rationale.
#
# Background: prior to this guard, the bulk-writeoff script
# (``scripts/operator_writeoff_stuck_batch.py``) treated any
# ``blocked_persistent_timeout`` row as eligible.  But that status fires
# whenever ``consecutive_timeout_count >= 3`` regardless of cause —
# including our own DB pressure causing SELL retries to time out
# client-side.  On 2026-04-28 that misclassification cost ~$200 of
# unrealised gain when 12 positions whose wallets STILL held shares
# were written off as full-loss.  See commit
# 4a319cd for the underlying DB-pressure fix and this commit for the
# scope-tightening that prevents the fix from masking the symptom.
_VENUE_REJECTION_MARKERS: tuple[str, ...] = (
    # Polymarket / CLOB SDK explicit rejections
    "orderbook does not exist",
    "market not tradable",
    "market_tradable=false",
    "market is closed",
    "market closed",
    "market is resolved",
    "trading is closed",
    "not accepting orders",
    "min_order_size",
    "min order size",
    "tick_size",
    "post-only",
    "post only",
    "rejected by venue",
    "rejected by clob",
    "rejected by exchange",
    "invalid order",
    # Allowance / balance errors are venue-side: the wallet doesn't
    # have what's needed to back the order, which is genuinely
    # unrecoverable without operator action.
    "not enough balance",
    "insufficient balance",
    "insufficient allowance",
)


def _is_venue_rejection_error(error_text: object) -> bool:
    """True iff ``error_text`` looks like a Polymarket/CLOB rejection
    rather than one of our own client-side failure modes (TimeoutError,
    ConnectionError, asyncpg protocol errors, etc.).
    """
    if not error_text:
        return False
    text = str(error_text).lower()
    return any(marker in text for marker in _VENUE_REJECTION_MARKERS)


class ManualWriteoffRejected(ValueError):
    """Raised when the operator's request fails a precondition.

    The error message identifies the failing rule so the API can
    surface it directly to the caller.
    """


class ManualWriteoffReversalRejected(ValueError):
    """Raised when a reverse-writeoff request fails a precondition.

    Mirrors ``ManualWriteoffRejected`` so the API can surface the
    failing rule directly to the caller as a 400.
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
    override_venue_rejection_check: bool = False,
    override_rationale: Optional[str] = None,
) -> dict[str, Any]:
    """Apply an operator-asserted close to a stuck order.

    Returns a structured summary on success.  Raises
    ``ManualWriteoffRejected`` (subclass of ``ValueError``) when any
    rule fails — caller should map that to a 400-class HTTP response.

    The session is NOT committed here; the caller commits.

    Default behaviour requires ``pending_live_exit.last_error`` to
    match a venue-rejection marker (see ``_VENUE_REJECTION_MARKERS``)
    so that rows whose only failure was our own client-side timeout
    cannot be silently written off — the lifecycle should retry those
    once the underlying DB / transport pressure clears, not realise
    them as a full loss.

    Operators can override that gate with
    ``override_venue_rejection_check=True``, but ``override_rationale``
    is then mandatory and the override is captured immutably in the
    audit event payload alongside the original error text.
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

    # Venue-rejection gate: writeoff is only safe when there is evidence
    # the venue itself is refusing the order.  Our own retry timeouts
    # (``TimeoutError``, asyncpg protocol errors, network issues) look
    # identical at the lifecycle layer — a row with
    # ``consecutive_timeout_count=3`` could be either a stuck market or
    # a transient client-side problem.  Realising the latter as a full
    # loss is what produced the 2026-04-28 incident; this guard makes
    # that outcome explicit and auditable.
    last_error_raw = pending_exit.get("last_error")
    last_error_text = str(last_error_raw or "")
    venue_rejected = _is_venue_rejection_error(last_error_raw)
    override_rationale_norm = str(override_rationale or "").strip()
    if not venue_rejected:
        if not override_venue_rejection_check:
            raise ManualWriteoffRejected(
                "pending_live_exit.last_error does not match a known venue-rejection "
                f"pattern (last_error={last_error_text[:160]!r}); the failure looks "
                "client-side (timeout / transport / DB pressure).  Resolve the "
                "underlying issue and let the lifecycle retry the SELL, OR pass "
                "override_venue_rejection_check=True with override_rationale "
                "explaining why a full-loss realisation is the correct call."
            )
        if not override_rationale_norm:
            raise ManualWriteoffRejected(
                "override_venue_rejection_check=True requires a non-empty "
                "override_rationale (why are we writing off despite no venue "
                "rejection evidence?)"
            )
        if len(override_rationale_norm) > 2000:
            raise ManualWriteoffRejected("override_rationale must be ≤ 2000 characters")

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
        "venue_rejection_evidence": {
            "matched": bool(venue_rejected),
            "last_error_excerpt": last_error_text[:512] if last_error_text else None,
        },
    }
    if not venue_rejected:
        event_payload["override_venue_rejection_check"] = True
        event_payload["override_rationale"] = override_rationale_norm
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


# ---------------------------------------------------------------------------
# Manual writeoff reversal
# ---------------------------------------------------------------------------
#
# When a manual writeoff turns out to have been the wrong call (e.g. the
# SELL failures were our own DB pressure rather than venue-side, and the
# wallet still holds the shares with the market still tradable), the
# institutional response is to reverse it via an audited path — not to
# UPDATE the row by hand.  ``reverse_manual_writeoff_order`` is that
# audited path.
#
# Behaviour:
#
#   * Only acts on rows whose ``verification_status`` is
#     ``manual_writeoff`` AND whose lifecycle ``status`` is one of the
#     terminal closures the writeoff produced (``closed_loss`` /
#     ``closed_win``).  Non-writeoff terminal rows (real wallet exits,
#     resolution settlements) are NEVER reversed.
#
#   * Restores the row to ``status='executed'``,
#     ``verification_status='wallet_position'`` (the recovery
#     verification source — we trust the wallet over our own
#     bookkeeping when the two disagree).  Clears
#     ``actual_profit``, ``payload['position_close']``, and resets
#     ``payload['pending_live_exit']`` to a state the lifecycle will
#     re-evaluate (``status='reopened_after_writeoff_reversal'``,
#     cleared retry counters) so reconcile picks it up again.
#
#   * Emits an immutable
#     ``TraderOrderVerificationEvent(event_type='manual_writeoff_reversed')``
#     with the operator id, reason, and the prior writeoff's recorded
#     P&L / event-id reference.
#
# The session is NOT committed here; the caller commits.


async def reverse_manual_writeoff_order(
    session: AsyncSession,
    *,
    order_id: str,
    reason: str,
    operator_id: str,
    expected_chain_balance_shares: Optional[float] = None,
) -> dict[str, Any]:
    """Reverse a prior manual writeoff and restore the row to active.

    Args:
        order_id: The trader order to reverse.
        reason: Mandatory free-form rationale (audit log).
        operator_id: Identifier for the reversing operator.
        expected_chain_balance_shares: Optional sanity check.  When
            provided, the reversal aborts unless the writeoff event's
            recorded ``chain_balance_shares`` is at least this value —
            so a reversal driven by "the wallet still holds shares"
            cannot fire against a row whose wallet was actually empty
            at writeoff time.

    Raises ``ManualWriteoffReversalRejected`` on precondition failure.
    """
    normalized_order_id = str(order_id or "").strip()
    if not normalized_order_id:
        raise ManualWriteoffReversalRejected("order_id is required")

    normalized_reason = str(reason or "").strip()
    if not normalized_reason:
        raise ManualWriteoffReversalRejected("reason is required (non-empty string)")
    if len(normalized_reason) > 2000:
        raise ManualWriteoffReversalRejected("reason must be ≤ 2000 characters")

    normalized_operator = str(operator_id or "").strip()
    if not normalized_operator:
        raise ManualWriteoffReversalRejected("operator_id is required")

    row = (
        await session.execute(
            select(TraderOrder).where(TraderOrder.id == normalized_order_id)
        )
    ).scalar_one_or_none()
    if row is None:
        raise ManualWriteoffReversalRejected(f"order_id={normalized_order_id} not found")

    current_status = str(row.status or "").strip().lower()
    if current_status not in {"closed_loss", "closed_win"}:
        raise ManualWriteoffReversalRejected(
            f"order status={current_status!r} is not a writeoff terminal status "
            f"(expected closed_loss or closed_win)"
        )

    # ``position_close.close_trigger`` is the canonical signal for "this
    # row was closed via the manual-writeoff path".  ``verification_status``
    # may have drifted away from ``manual_writeoff`` after a reconcile
    # cycle noticed the wallet still held the shares and updated
    # verification to ``wallet_position`` — but that doesn't change the
    # fact that the close itself was an operator writeoff.  Use the
    # close_trigger as the gate so the reversal works on rows that have
    # been touched by the lifecycle since the original writeoff.
    payload = dict(row.payload_json or {})
    position_close = payload.get("position_close")
    if not isinstance(position_close, dict) or str(
        position_close.get("close_trigger") or ""
    ).strip().lower() != "manual_writeoff":
        raise ManualWriteoffReversalRejected(
            "position_close.close_trigger != 'manual_writeoff'; refusing to reverse "
            "a row whose terminal closure didn't come from this code path"
        )
    current_verification = str(row.verification_status or "").strip().lower()

    # Optional chain-balance sanity check from the original writeoff event
    # payload.  We pull the most-recent manual_writeoff event to find what
    # chain balance the writeoff was applied with.
    from models.database import TraderOrderVerificationEvent  # local to avoid cycle

    ev_row = (
        await session.execute(
            select(TraderOrderVerificationEvent)
            .where(TraderOrderVerificationEvent.trader_order_id == normalized_order_id)
            .where(TraderOrderVerificationEvent.event_type == "manual_writeoff")
            .order_by(TraderOrderVerificationEvent.created_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    prior_writeoff_event_id: Optional[str] = None
    prior_writeoff_event_payload: dict[str, Any] = {}
    prior_writeoff_event_reason: Optional[str] = None
    if ev_row is not None:
        prior_writeoff_event_id = str(getattr(ev_row, "id", "") or "") or None
        prior_writeoff_event_reason = getattr(ev_row, "reason", None)
        raw_payload = getattr(ev_row, "payload_json", None)
        if isinstance(raw_payload, dict):
            prior_writeoff_event_payload = dict(raw_payload)

    if expected_chain_balance_shares is not None:
        # The bulk script encodes ``chain_balance_shares=X`` in the
        # reason text; parse it out for the sanity check.
        recorded_balance: Optional[float] = None
        ev_reason = prior_writeoff_event_reason or ""
        for fragment in ev_reason.split("|"):
            cleaned = fragment.strip()
            if cleaned.startswith("chain_balance_shares="):
                try:
                    recorded_balance = float(cleaned.split("=", 1)[1])
                except (TypeError, ValueError):
                    recorded_balance = None
                break
        if recorded_balance is None or recorded_balance < float(expected_chain_balance_shares):
            raise ManualWriteoffReversalRejected(
                "expected_chain_balance_shares sanity check failed: original writeoff "
                f"event recorded chain_balance_shares={recorded_balance}, caller asserted "
                f">= {expected_chain_balance_shares}.  Refusing to reverse — investigate "
                "before retrying."
            )

    now = utcnow()
    prior_actual_profit = (
        float(row.actual_profit) if row.actual_profit is not None else None
    )
    prior_pending_exit = payload.get("pending_live_exit") or {}
    prior_position_close = dict(position_close)

    # Reset the row to a state the lifecycle will pick back up:
    #   * status = ``executed`` (an active live position awaiting exit)
    #   * verification_status = wallet_position (we trust the wallet —
    #     the writeoff event recorded chain shares were held)
    #   * actual_profit cleared (the writeoff's number is no longer valid)
    #   * payload['position_close'] removed
    #   * payload['pending_live_exit'] reset so retry counters are zero
    #     and the next reconcile cycle re-evaluates exit triggers
    row.actual_profit = None
    row.status = "executed"
    row.updated_at = now

    if isinstance(prior_pending_exit, dict):
        reset_pending_exit = dict(prior_pending_exit)
    else:
        reset_pending_exit = {}
    reset_pending_exit["status"] = "reopened_after_writeoff_reversal"
    reset_pending_exit["reopened_at"] = now.isoformat()
    reset_pending_exit["consecutive_timeout_count"] = 0
    reset_pending_exit["consecutive_blocked_failure_count"] = 0
    reset_pending_exit["retry_count"] = 0
    reset_pending_exit["last_error"] = None
    reset_pending_exit["next_retry_at"] = None
    reset_pending_exit["exhausted_at"] = None

    payload["pending_live_exit"] = reset_pending_exit
    payload.pop("position_close", None)
    row.payload_json = payload

    apply_trader_order_verification(
        row,
        verification_status="wallet_position",
        verification_source="operator_reversal",
        verification_reason=normalized_reason,
        verified_at=now,
        force=True,
    )

    event_payload: dict[str, Any] = {
        "operator_id": normalized_operator,
        "reason": normalized_reason,
        "prior_actual_profit": prior_actual_profit,
        "prior_lifecycle_status": current_status,
        "prior_verification_status": current_verification,
        "prior_position_close": prior_position_close,
        "prior_writeoff_event_id": prior_writeoff_event_id,
        "prior_writeoff_event_reason": prior_writeoff_event_reason,
        "prior_writeoff_event_payload": prior_writeoff_event_payload,
    }
    if expected_chain_balance_shares is not None:
        event_payload["expected_chain_balance_shares"] = float(expected_chain_balance_shares)

    append_trader_order_verification_event(
        session,
        trader_order_id=normalized_order_id,
        verification_status="wallet_position",
        source="operator_reversal",
        event_type="manual_writeoff_reversed",
        reason=normalized_reason,
        payload_json=event_payload,
        created_at=now,
    )

    return {
        "order_id": normalized_order_id,
        "next_status": "executed",
        "verification_status": "wallet_position",
        "operator_id": normalized_operator,
        "reason": normalized_reason,
        "prior_actual_profit": prior_actual_profit,
        "prior_lifecycle_status": current_status,
        "prior_writeoff_event_id": prior_writeoff_event_id,
        "applied_at": now.isoformat(),
    }
