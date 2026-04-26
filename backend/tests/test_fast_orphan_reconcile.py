"""End-to-end test for the orphan-reconcile sweep.

Simulates the failure mode the v1 skeleton row alone can't fix:
``CLOB call succeeded → post-update DB write failed``. The skeleton row
sits with ``post_update_failed`` and no ``provider_clob_order_id``;
the sweep should look up the venue order by metadata key and patch
the row so the regular reconcile flow takes over.

Also exercises the negative case where the venue has nothing matching
the key (the venue never received the order) — that row should be
marked ``failed`` with reason ``orphan_no_venue_match``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from sqlalchemy import select

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import (  # noqa: E402
    Base,
    Trader,
    TraderOrder,
)
from services.trader_orchestrator.fast_idempotency import (  # noqa: E402
    derive_fast_idempotency_key,
)
from services.trader_orchestrator_state import (  # noqa: E402
    reconcile_orphaned_fast_submissions,
)
from tests.postgres_test_db import build_postgres_session_factory  # noqa: E402
from utils.utcnow import utcnow  # noqa: E402


def _seed_trader(session) -> None:
    now = utcnow().replace(tzinfo=None)
    session.add(
        Trader(
            id="orphan-trader",
            name="Orphan Trader",
            source_configs_json={},
            risk_limits_json={},
            metadata_json={},
            mode="live",
            latency_class="fast",
            is_enabled=True,
            is_paused=False,
            interval_seconds=1,
            created_at=now,
            updated_at=now,
        )
    )


def _seed_skeleton(
    session,
    *,
    signal_id: str,
    submission_state: str,
    age_seconds: float = 60.0,
) -> tuple[str, str]:
    from datetime import timedelta

    now = utcnow().replace(tzinfo=None)
    created_at = now - timedelta(seconds=age_seconds)
    key = derive_fast_idempotency_key(trader_id="orphan-trader", signal_id=signal_id)
    order_id = f"order-{signal_id}"
    session.add(
        TraderOrder(
            id=order_id,
            trader_id="orphan-trader",
            signal_id=signal_id,
            source="generic-source",
            market_id="market-orphan",
            mode="live",
            status="submitted",
            notional_usd=3.0,
            payload_json={
                "fast_tier": True,
                "fast_submission_state": submission_state,
                "fast_idempotency_key": key,
            },
            created_at=created_at,
            updated_at=created_at,
        )
    )
    return order_id, key


@pytest.mark.asyncio
async def test_orphan_match_patches_provider_clob_id_when_venue_has_order(monkeypatch):
    engine, session_factory = await build_postgres_session_factory(Base, "fast_orphan_match")

    matched_orphan_key: dict[str, str] = {}

    async def fake_metadata_map():
        # Venue returns one open order whose metadata equals our orphan key.
        key = matched_orphan_key["key"]
        return {
            key.lower(): [
                {
                    "clob_order_id": "venue-clob-12345",
                    "metadata": key,
                    "size": 10.0,
                    "filled_size": 0.0,
                    "limit_price": 0.42,
                }
            ]
        }

    from services import live_execution_service as les_module

    monkeypatch.setattr(
        les_module.live_execution_service,
        "get_open_order_snapshots_by_metadata",
        fake_metadata_map,
    )

    try:
        async with session_factory() as session:
            _seed_trader(session)
            order_id, key = _seed_skeleton(
                session,
                signal_id="sig-match",
                submission_state="post_update_failed",
            )
            matched_orphan_key["key"] = key
            await session.commit()

        async with session_factory() as session:
            result = await reconcile_orphaned_fast_submissions(
                session,
                trader_id="orphan-trader",
            )

        assert result["eligible"] == 1
        assert result["matched"] == 1
        assert result["marked_orphan"] == 0
        assert result["venue_unreachable"] is False

        async with session_factory() as session:
            row = (
                await session.execute(
                    select(TraderOrder).where(TraderOrder.id == order_id)
                )
            ).scalar_one()

        assert row.provider_clob_order_id == "venue-clob-12345"
        assert row.payload_json["fast_submission_state"] == "reconciled"
        assert row.payload_json["reconciled_via"] == "orphan_metadata_match"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_orphan_with_no_venue_match_is_marked_failed(monkeypatch):
    engine, session_factory = await build_postgres_session_factory(Base, "fast_orphan_no_match")

    async def fake_metadata_map():
        return {}  # Venue has nothing for our maker

    from services import live_execution_service as les_module

    monkeypatch.setattr(
        les_module.live_execution_service,
        "get_open_order_snapshots_by_metadata",
        fake_metadata_map,
    )

    try:
        async with session_factory() as session:
            _seed_trader(session)
            order_id, _ = _seed_skeleton(
                session,
                signal_id="sig-orphan",
                submission_state="clob_exception",
            )
            await session.commit()

        async with session_factory() as session:
            result = await reconcile_orphaned_fast_submissions(
                session,
                trader_id="orphan-trader",
            )

        assert result["eligible"] == 1
        assert result["matched"] == 0
        assert result["marked_orphan"] == 1

        async with session_factory() as session:
            row = (
                await session.execute(
                    select(TraderOrder).where(TraderOrder.id == order_id)
                )
            ).scalar_one()

        assert row.status == "failed"
        assert row.payload_json["fast_submission_state"] == "orphan_no_venue_match"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_orphan_reconcile_skips_recent_rows(monkeypatch):
    """Rows created within the cooldown window must not be touched —
    the post-update flush may still be running."""
    engine, session_factory = await build_postgres_session_factory(Base, "fast_orphan_recent")

    async def fake_metadata_map():
        return {}

    from services import live_execution_service as les_module

    monkeypatch.setattr(
        les_module.live_execution_service,
        "get_open_order_snapshots_by_metadata",
        fake_metadata_map,
    )

    try:
        async with session_factory() as session:
            _seed_trader(session)
            _seed_skeleton(
                session,
                signal_id="sig-recent",
                submission_state="in_flight",
                age_seconds=1.0,  # too young
            )
            await session.commit()

        async with session_factory() as session:
            result = await reconcile_orphaned_fast_submissions(
                session,
                trader_id="orphan-trader",
                min_age_seconds=30.0,
            )

        assert result["eligible"] == 0
        assert result["matched"] == 0
        assert result["marked_orphan"] == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_orphan_reconcile_does_not_mark_when_venue_unreachable(monkeypatch):
    """If the venue API is down we must NOT flip rows to failed —
    that would lose state that's actually still recoverable."""
    engine, session_factory = await build_postgres_session_factory(Base, "fast_orphan_venue_unreachable")

    async def fake_metadata_map():
        raise RuntimeError("venue down")

    from services import live_execution_service as les_module

    monkeypatch.setattr(
        les_module.live_execution_service,
        "get_open_order_snapshots_by_metadata",
        fake_metadata_map,
    )

    try:
        async with session_factory() as session:
            _seed_trader(session)
            order_id, _ = _seed_skeleton(
                session,
                signal_id="sig-venue-down",
                submission_state="post_update_failed",
            )
            await session.commit()

        async with session_factory() as session:
            result = await reconcile_orphaned_fast_submissions(
                session,
                trader_id="orphan-trader",
            )

        assert result["venue_unreachable"] is True
        assert result["matched"] == 0
        assert result["marked_orphan"] == 0
        assert "venue_fetch_failed" in str(result["errors"])

        async with session_factory() as session:
            row = (
                await session.execute(
                    select(TraderOrder).where(TraderOrder.id == order_id)
                )
            ).scalar_one()

        # Untouched: still skeleton state, no clob id assigned.
        assert row.status == "submitted"
        assert row.payload_json["fast_submission_state"] == "post_update_failed"
        assert row.provider_clob_order_id is None
    finally:
        await engine.dispose()
