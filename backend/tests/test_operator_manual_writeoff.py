"""Tests for the operator manual-writeoff path.

This is the institutional escape hatch — the only code path that can
write a non-NULL ``actual_profit`` outside of the verifier's
wallet-activity match.  Every rule in ``manual_writeoff_order`` is a
financial-correctness invariant; we test each in isolation.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import (
    Base,
    Trader,
    TraderOrder,
    TraderOrderVerificationEvent,
)
from services.operator_writeoff import (
    ManualWriteoffRejected,
    manual_writeoff_order,
)
from tests.postgres_test_db import build_postgres_session_factory
from utils.utcnow import utcnow


async def _seed(
    session,
    trader_id: str,
    order_id: str,
    *,
    pending_exit_status: str = "blocked_persistent_timeout",
    lifecycle_status: str = "executed",
):
    session.add(
        Trader(
            id=trader_id,
            name=f"trader-{trader_id}",
            mode="live",
            source_configs_json=[],
            risk_limits_json={},
        )
    )
    now = utcnow()
    session.add(
        TraderOrder(
            id=order_id,
            trader_id=trader_id,
            source="scanner",
            market_id="market-1",
            direction="buy_yes",
            mode="live",
            status=lifecycle_status,
            notional_usd=7.56,
            entry_price=0.87,
            effective_price=0.87,
            payload_json={
                "selected_token_id": "token-1",
                "pending_live_exit": {
                    "status": pending_exit_status,
                    "consecutive_blocked_failure_count": 3,
                },
            },
            created_at=now - timedelta(hours=24),
            updated_at=now - timedelta(minutes=1),
            executed_at=now - timedelta(hours=24),
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_manual_writeoff_happy_path_writes_pnl_and_audit_event(tmp_path):
    """A valid request should: set actual_profit, transition status,
    emit a TraderOrderVerificationEvent with operator + reason."""
    engine, session_factory = await build_postgres_session_factory(
        Base, "manual_writeoff_happy"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-1", "order-happy")

        async with session_factory() as session:
            result = await manual_writeoff_order(
                session,
                order_id="order-happy",
                realized_pnl=-7.56,  # full loss
                reason="market unresolved 30 days post-end-date; assuming total loss",
                operator_id="ops@homerun",
            )
            await session.commit()

        async with session_factory() as session:
            row = await session.get(TraderOrder, "order-happy")
            assert row is not None
            assert row.actual_profit == pytest.approx(-7.56)
            assert row.status == "closed_loss"
            assert row.verification_status == "manual_writeoff"
            assert row.verification_source == "operator"
            payload = row.payload_json or {}
            close = payload.get("position_close") or {}
            assert close.get("close_trigger") == "manual_writeoff"
            assert close.get("operator_id") == "ops@homerun"
            assert close.get("realized_pnl") == pytest.approx(-7.56)

            events = (
                await session.execute(
                    select(TraderOrderVerificationEvent).where(
                        TraderOrderVerificationEvent.trader_order_id == "order-happy"
                    )
                )
            ).scalars().all()
            assert any(e.event_type == "manual_writeoff" for e in events)
            evt = next(e for e in events if e.event_type == "manual_writeoff")
            assert evt.source == "operator"
            assert "market unresolved" in (evt.reason or "")
            assert evt.payload_json.get("operator_id") == "ops@homerun"
            assert evt.payload_json.get("realized_pnl") == pytest.approx(-7.56)

        assert result["next_status"] == "closed_loss"
        assert result["operator_id"] == "ops@homerun"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_manual_writeoff_rejects_already_closed_order(tmp_path):
    """An already-closed order cannot be manually written off — that
    would let an operator overwrite verified P&L."""
    engine, session_factory = await build_postgres_session_factory(
        Base, "manual_writeoff_rejects_closed"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-c", "order-closed", lifecycle_status="closed_win")

        async with session_factory() as session:
            with pytest.raises(ManualWriteoffRejected, match="already terminal"):
                await manual_writeoff_order(
                    session,
                    order_id="order-closed",
                    realized_pnl=0.0,
                    reason="trying to overwrite a winner",
                    operator_id="ops",
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_manual_writeoff_rejects_order_without_pending_exit(tmp_path):
    """An order the lifecycle has NOT yet attempted to close cannot
    be written off — the operator path is for stuck orders only."""
    engine, session_factory = await build_postgres_session_factory(
        Base, "manual_writeoff_rejects_no_pending_exit"
    )
    try:
        async with session_factory() as session:
            session.add(
                Trader(
                    id="trader-np",
                    name="t-np",
                    mode="live",
                    source_configs_json=[],
                    risk_limits_json={},
                )
            )
            now = utcnow()
            session.add(
                TraderOrder(
                    id="order-no-pending",
                    trader_id="trader-np",
                    source="scanner",
                    market_id="market-x",
                    direction="buy_yes",
                    mode="live",
                    status="executed",
                    notional_usd=10.0,
                    payload_json={"selected_token_id": "tok"},  # no pending_live_exit
                    created_at=now,
                    updated_at=now,
                )
            )
            await session.commit()

        async with session_factory() as session:
            with pytest.raises(ManualWriteoffRejected, match="no pending_live_exit"):
                await manual_writeoff_order(
                    session,
                    order_id="order-no-pending",
                    realized_pnl=-5.0,
                    reason="should fail",
                    operator_id="ops",
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_manual_writeoff_rejects_non_blocked_pending_exit(tmp_path):
    """An order whose pending_exit is just 'failed' (still retrying)
    cannot be written off — only blocked-terminal states qualify."""
    engine, session_factory = await build_postgres_session_factory(
        Base, "manual_writeoff_rejects_non_blocked"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-f", "order-failed", pending_exit_status="failed")

        async with session_factory() as session:
            with pytest.raises(ManualWriteoffRejected, match="not a blocked-terminal state"):
                await manual_writeoff_order(
                    session,
                    order_id="order-failed",
                    realized_pnl=-5.0,
                    reason="should fail",
                    operator_id="ops",
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_manual_writeoff_requires_non_empty_reason(tmp_path):
    engine, session_factory = await build_postgres_session_factory(
        Base, "manual_writeoff_requires_reason"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-r", "order-reason")

        async with session_factory() as session:
            with pytest.raises(ManualWriteoffRejected, match="reason is required"):
                await manual_writeoff_order(
                    session,
                    order_id="order-reason",
                    realized_pnl=0.0,
                    reason="   ",  # whitespace only
                    operator_id="ops",
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_manual_writeoff_requires_operator_id(tmp_path):
    engine, session_factory = await build_postgres_session_factory(
        Base, "manual_writeoff_requires_operator"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-o", "order-op")

        async with session_factory() as session:
            with pytest.raises(ManualWriteoffRejected, match="operator_id is required"):
                await manual_writeoff_order(
                    session,
                    order_id="order-op",
                    realized_pnl=0.0,
                    reason="valid reason",
                    operator_id="",
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_manual_writeoff_rejects_nan_pnl(tmp_path):
    engine, session_factory = await build_postgres_session_factory(
        Base, "manual_writeoff_rejects_nan"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-n", "order-nan")

        async with session_factory() as session:
            with pytest.raises(ManualWriteoffRejected, match="finite number"):
                await manual_writeoff_order(
                    session,
                    order_id="order-nan",
                    realized_pnl=float("nan"),
                    reason="valid reason",
                    operator_id="ops",
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_db_guard_still_nulls_actual_profit_for_random_writes(tmp_path):
    """The DB-layer guard (``_enforce_pnl_verification_guard``) must
    still null actual_profit if someone tries to write it with a status
    OUTSIDE the new {wallet_activity, manual_writeoff} whitelist.

    This locks in the safety-net property: even if a code path bypasses
    the operator helper, the guard is a backstop.
    """
    engine, session_factory = await build_postgres_session_factory(
        Base, "manual_writeoff_db_guard_backstop"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-g", "order-guard", lifecycle_status="executed")

        async with session_factory() as session:
            row = await session.get(TraderOrder, "order-guard")
            assert row is not None
            # Try to write a synthetic loss WITHOUT going through the
            # operator helper — set status to anything but the
            # whitelisted values.
            row.actual_profit = -123.45
            row.verification_status = "venue_fill"  # NOT whitelisted
            await session.commit()

            persisted = await session.get(TraderOrder, "order-guard")
            assert persisted.actual_profit is None, (
                "DB guard must coerce actual_profit to None when "
                "verification_status is not in the whitelist"
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_db_guard_allows_actual_profit_when_status_is_manual_writeoff(tmp_path):
    """The new whitelisted status must let actual_profit persist."""
    engine, session_factory = await build_postgres_session_factory(
        Base, "manual_writeoff_db_guard_allows_new_status"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-w", "order-mw", lifecycle_status="executed")

        async with session_factory() as session:
            row = await session.get(TraderOrder, "order-mw")
            assert row is not None
            row.actual_profit = -7.56
            row.verification_status = "manual_writeoff"
            await session.commit()

            persisted = await session.get(TraderOrder, "order-mw")
            assert persisted.actual_profit == pytest.approx(-7.56)
            assert persisted.verification_status == "manual_writeoff"
    finally:
        await engine.dispose()
