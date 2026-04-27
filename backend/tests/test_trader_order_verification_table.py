"""Tests for the trader_order_verification side table.

This is the writer-isolation table introduced to eliminate row-level
lock contention between the Polymarket verifier and the orchestrator
on trader_orders.  Step 1 of the migration only adds the table — no
application code reads or writes it yet — but we still need to verify:

  1. The DB-layer guard fires (actual_profit non-null requires
     verification_status='wallet_activity').
  2. CASCADE delete works (deleting a TraderOrder removes its
     verification row).
  3. The 1:1 invariant holds (PK on trader_order_id).
"""

import sys
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from sqlalchemy import select

from models.database import (
    Base,
    Trader,
    TraderOrder,
    TraderOrderVerification,
)
from tests.postgres_test_db import build_postgres_session_factory
from utils.utcnow import utcnow


async def _seed(session, trader_id: str, order_id: str) -> None:
    session.add(
        Trader(
            id=trader_id,
            name=f"trader-{trader_id}",
            mode="shadow",
            source_configs_json=[],
            risk_limits_json={},
        )
    )
    session.add(
        TraderOrder(
            id=order_id,
            trader_id=trader_id,
            source="test",
            market_id="m1",
            direction="buy_yes",
            mode="live",
            status="closed_win",
            payload_json={},
            created_at=utcnow(),
            updated_at=utcnow(),
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_guard_coerces_actual_profit_when_status_unverified(tmp_path):
    """Guard must zero out actual_profit unless status is wallet_activity."""
    engine, session_factory = await build_postgres_session_factory(
        Base, "trader_order_verification_guard"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-guard", "order-guard")
            row = TraderOrderVerification(
                trader_order_id="order-guard",
                verification_status="local",  # NOT wallet_activity
                actual_profit=42.5,           # should be coerced to None
            )
            session.add(row)
            await session.commit()

            persisted = await session.get(TraderOrderVerification, "order-guard")
            assert persisted is not None
            assert persisted.verification_status == "local"
            assert persisted.actual_profit is None, (
                "Guard must coerce actual_profit to None when status != wallet_activity"
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_guard_allows_actual_profit_when_status_is_wallet_activity(tmp_path):
    """When status IS wallet_activity, actual_profit must be preserved."""
    engine, session_factory = await build_postgres_session_factory(
        Base, "trader_order_verification_pass"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-pass", "order-pass")
            row = TraderOrderVerification(
                trader_order_id="order-pass",
                verification_status="wallet_activity",
                actual_profit=42.5,
            )
            session.add(row)
            await session.commit()

            persisted = await session.get(TraderOrderVerification, "order-pass")
            assert persisted is not None
            assert persisted.verification_status == "wallet_activity"
            assert persisted.actual_profit == pytest.approx(42.5)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_cascade_delete_cleans_up_verification_row(tmp_path):
    """Deleting a TraderOrder must CASCADE-delete its verification row."""
    engine, session_factory = await build_postgres_session_factory(
        Base, "trader_order_verification_cascade"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-cascade", "order-cascade")
            session.add(
                TraderOrderVerification(
                    trader_order_id="order-cascade",
                    verification_status="wallet_activity",
                    actual_profit=10.0,
                )
            )
            await session.commit()

            # Confirm both rows exist
            order = await session.get(TraderOrder, "order-cascade")
            verif = await session.get(TraderOrderVerification, "order-cascade")
            assert order is not None and verif is not None

            # Delete the parent
            await session.delete(order)
            await session.commit()

            # Verification row should be gone via CASCADE
            after = (
                await session.execute(
                    select(TraderOrderVerification).where(
                        TraderOrderVerification.trader_order_id == "order-cascade"
                    )
                )
            ).scalar_one_or_none()
            assert after is None, (
                "trader_order_verification row must be CASCADE-deleted when its "
                "parent trader_orders row is deleted"
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_one_verification_row_per_order(tmp_path):
    """The PK on trader_order_id enforces 1:1."""
    from sqlalchemy.exc import IntegrityError

    engine, session_factory = await build_postgres_session_factory(
        Base, "trader_order_verification_unique"
    )
    try:
        async with session_factory() as session:
            await _seed(session, "trader-uniq", "order-uniq")
            session.add(
                TraderOrderVerification(
                    trader_order_id="order-uniq",
                    verification_status="local",
                )
            )
            await session.commit()

        async with session_factory() as session:
            session.add(
                TraderOrderVerification(
                    trader_order_id="order-uniq",  # same PK
                    verification_status="wallet_activity",
                    actual_profit=1.0,
                )
            )
            with pytest.raises(IntegrityError):
                await session.commit()
    finally:
        await engine.dispose()
