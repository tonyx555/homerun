"""Operator-only endpoints for manual interventions on stuck orders.

The endpoints here are the institutional escape hatch for cases the
automated lifecycle cannot resolve (e.g. positions in markets that
will not resolve and will not accept SELL orders).  Every action
through these routes is recorded in the immutable
``TraderOrderVerificationEvent`` audit log with operator identity,
reason, and on-chain evidence snapshot.

Routes
------

  GET  /api/operator/stuck-orders
       List orders currently in a blocked-terminal exit state, with
       on-chain holdings + market resolution status for each.

  POST /api/operator/orders/{order_id}/manual-writeoff
       Apply an operator-asserted close.  Body:
         { "realized_pnl": float, "reason": str, "operator_id": str }

       This is the only code path that can write a non-NULL
       ``actual_profit`` outside of the verifier's wallet-activity
       match.  See backend/services/operator_writeoff.py for the
       enforcement rules.

Auth
----

These endpoints are intentionally NOT protected by the lightweight
auth used for the GUI today — the assumption is that the deploy
itself controls who can reach the API process.  Future hardening
should require a per-call bearer token + audit who fired what.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from models.database import AsyncSessionLocal
from services.operator_writeoff import (
    ManualWriteoffRejected,
    ManualWriteoffReversalRejected,
    WalletAbsentResolutionReversalRejected,
    manual_writeoff_order,
    reverse_manual_writeoff_order,
    reverse_wallet_absent_resolution,
)
from services.stuck_position_monitor import (
    classify_stuck_position,
    scan_stuck_positions,
)
from utils.logger import get_logger

logger = get_logger("routes_operator")

router = APIRouter(prefix="/operator", tags=["Operator"])


class ManualWriteoffRequest(BaseModel):
    realized_pnl: float = Field(
        ...,
        description=(
            "The P&L value the operator is asserting.  This is what "
            "appears in the order's ``actual_profit`` column.  Must be "
            "a finite number; no synthetic auto-computation."
        ),
    )
    reason: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        description="Mandatory free-form explanation for the write-off.",
    )
    operator_id: str = Field(
        ...,
        min_length=1,
        max_length=120,
        description=(
            "Free-form operator identifier (e.g. email, ops handle).  "
            "Recorded immutably in the verification event log."
        ),
    )
    chain_evidence: Optional[dict] = Field(
        default=None,
        description=(
            "Optional snapshot of on-chain state at decision time "
            "(e.g. the dict returned by "
            "CTFExecutionService.fetch_position_chain_status).  "
            "Recorded in the audit event for traceability."
        ),
    )
    override_venue_rejection_check: bool = Field(
        default=False,
        description=(
            "Default False — the writeoff is rejected unless the row's "
            "``pending_live_exit.last_error`` matches a known "
            "venue-rejection marker (orderbook gone, market not "
            "tradable, allowance error, etc.).  Setting True bypasses "
            "that gate; ``override_rationale`` then becomes mandatory "
            "and is captured immutably in the audit event."
        ),
    )
    override_rationale: Optional[str] = Field(
        default=None,
        max_length=2000,
        description=(
            "Required when override_venue_rejection_check=True: a "
            "free-form explanation for why a full-loss realisation is "
            "the correct call despite no venue-rejection evidence."
        ),
    )


class ManualWriteoffResponse(BaseModel):
    order_id: str
    next_status: str
    realized_pnl: float
    operator_id: str
    reason: str
    applied_at: str


class ReverseManualWriteoffRequest(BaseModel):
    reason: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        description=(
            "Mandatory free-form explanation for the reversal.  Recorded "
            "in the manual_writeoff_reversed verification event."
        ),
    )
    operator_id: str = Field(
        ...,
        min_length=1,
        max_length=120,
        description="Identifier for the reversing operator.",
    )
    expected_chain_balance_shares: Optional[float] = Field(
        default=None,
        ge=0.0,
        description=(
            "Optional sanity check: the reversal aborts unless the "
            "original writeoff event recorded a chain balance at least "
            "this large.  Set this to the wallet share count you've "
            "verified against Polymarket so a reversal driven by 'the "
            "wallet still holds shares' cannot fire against a row whose "
            "wallet was actually empty at writeoff time."
        ),
    )


class ReverseManualWriteoffResponse(BaseModel):
    order_id: str
    next_status: str
    verification_status: str
    operator_id: str
    reason: str
    prior_actual_profit: Optional[float] = None
    prior_lifecycle_status: str
    prior_writeoff_event_id: Optional[str] = None
    applied_at: str


class StuckOrdersResponse(BaseModel):
    count: int
    rows: list[dict]


@router.get("/stuck-orders", response_model=StuckOrdersResponse)
async def list_stuck_orders(age_hours: float = 6.0) -> StuckOrdersResponse:
    """Return every order in a blocked-terminal exit state older than
    *age_hours*, with on-chain holdings + market resolution status.

    On-chain calls are bounded internally to avoid hammering the RPC.
    """
    observations = await scan_stuck_positions(age_hours=age_hours)
    if not observations:
        return StuckOrdersResponse(count=0, rows=[])

    # Classify with bounded concurrency.
    import asyncio as _asyncio

    sem = _asyncio.Semaphore(4)

    async def _classify(o):
        async with sem:
            return await classify_stuck_position(o)

    classified = await _asyncio.gather(*(_classify(o) for o in observations))
    return StuckOrdersResponse(count=len(classified), rows=list(classified))


@router.post("/orders/{order_id}/manual-writeoff", response_model=ManualWriteoffResponse)
async def operator_manual_writeoff(
    order_id: str,
    body: ManualWriteoffRequest,
) -> ManualWriteoffResponse:
    """Apply an operator-asserted close to a stuck order.

    Returns 400 with a structured ``detail`` if the request fails any
    of the precondition rules in ``manual_writeoff_order``.
    """
    async with AsyncSessionLocal() as session:
        try:
            result = await manual_writeoff_order(
                session,
                order_id=order_id,
                realized_pnl=body.realized_pnl,
                reason=body.reason,
                operator_id=body.operator_id,
                chain_evidence=body.chain_evidence,
                override_venue_rejection_check=body.override_venue_rejection_check,
                override_rationale=body.override_rationale,
            )
        except ManualWriteoffRejected as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception(
                "manual_writeoff failed unexpectedly",
                extra={"order_id": order_id, "operator_id": body.operator_id},
            )
            raise HTTPException(status_code=500, detail=f"manual_writeoff failed: {exc}") from exc
        await session.commit()

    logger.warning(
        "Operator manual write-off applied",
        extra={
            "order_id": result["order_id"],
            "operator_id": result["operator_id"],
            "realized_pnl": result["realized_pnl"],
            "reason": result["reason"],
        },
    )
    return ManualWriteoffResponse(**result)


@router.post(
    "/orders/{order_id}/reverse-manual-writeoff",
    response_model=ReverseManualWriteoffResponse,
)
async def operator_reverse_manual_writeoff(
    order_id: str,
    body: ReverseManualWriteoffRequest,
) -> ReverseManualWriteoffResponse:
    """Reverse a prior manual writeoff and restore the row to active.

    Use when a writeoff turns out to have been the wrong call (the
    wallet still holds shares and the market is still tradable — i.e.
    the SELL failures were our own client-side timeouts rather than a
    venue rejection).  The row is restored to ``status='executed'`` /
    ``verification_status='wallet_position'``, ``actual_profit`` is
    cleared, and the lifecycle's exit retry counters are reset so the
    reconcile sweep re-evaluates the position from scratch.

    Returns 400 with a structured ``detail`` if the request fails any
    precondition (row not found, not a writeoff terminal row, optional
    chain-balance sanity check fails).
    """
    async with AsyncSessionLocal() as session:
        try:
            result = await reverse_manual_writeoff_order(
                session,
                order_id=order_id,
                reason=body.reason,
                operator_id=body.operator_id,
                expected_chain_balance_shares=body.expected_chain_balance_shares,
            )
        except ManualWriteoffReversalRejected as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception(
                "reverse_manual_writeoff failed unexpectedly",
                extra={"order_id": order_id, "operator_id": body.operator_id},
            )
            raise HTTPException(
                status_code=500,
                detail=f"reverse_manual_writeoff failed: {exc}",
            ) from exc
        await session.commit()

    logger.warning(
        "Operator manual write-off REVERSED",
        extra={
            "order_id": result["order_id"],
            "operator_id": result["operator_id"],
            "reason": result["reason"],
            "prior_actual_profit": result.get("prior_actual_profit"),
            "prior_lifecycle_status": result.get("prior_lifecycle_status"),
        },
    )
    return ReverseManualWriteoffResponse(**result)


class ReverseWalletAbsentResolutionRequest(BaseModel):
    reason: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        description=(
            "Mandatory free-form explanation for the reversal.  Recorded "
            "in the wallet_absent_resolution_reversed verification event."
        ),
    )
    operator_id: str = Field(
        ...,
        min_length=1,
        max_length=120,
    )


class ReverseWalletAbsentResolutionResponse(BaseModel):
    order_id: str
    next_status: str
    verification_status: str
    operator_id: str
    reason: str
    prior_actual_profit: Optional[float] = None
    prior_lifecycle_status: str
    prior_pending_exit_status: str
    applied_at: str


@router.post(
    "/orders/{order_id}/reverse-wallet-absent-resolution",
    response_model=ReverseWalletAbsentResolutionResponse,
)
async def operator_reverse_wallet_absent_resolution(
    order_id: str,
    body: ReverseWalletAbsentResolutionRequest,
) -> ReverseWalletAbsentResolutionResponse:
    """Reverse a ``superseded_wallet_absent_*`` resolution.

    Sibling of ``reverse-manual-writeoff`` for the OTHER false-close
    class — rows the lifecycle marked ``status='resolved'`` after a
    transient wallet-snapshot blip.  See
    ``services.operator_writeoff.reverse_wallet_absent_resolution``
    for the precondition rules.
    """
    async with AsyncSessionLocal() as session:
        try:
            result = await reverse_wallet_absent_resolution(
                session,
                order_id=order_id,
                reason=body.reason,
                operator_id=body.operator_id,
            )
        except WalletAbsentResolutionReversalRejected as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception(
                "reverse_wallet_absent_resolution failed unexpectedly",
                extra={"order_id": order_id, "operator_id": body.operator_id},
            )
            raise HTTPException(
                status_code=500,
                detail=f"reverse_wallet_absent_resolution failed: {exc}",
            ) from exc
        await session.commit()

    logger.warning(
        "Operator wallet-absent resolution REVERSED",
        extra={
            "order_id": result["order_id"],
            "operator_id": result["operator_id"],
            "reason": result["reason"],
            "prior_pending_exit_status": result.get("prior_pending_exit_status"),
        },
    )
    return ReverseWalletAbsentResolutionResponse(**result)
