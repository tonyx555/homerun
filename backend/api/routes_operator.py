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
    manual_writeoff_order,
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


class ManualWriteoffResponse(BaseModel):
    order_id: str
    next_status: str
    realized_pnl: float
    operator_id: str
    reason: str
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
