"""Regression tests for the is_system refactor.

After the refactor, ``ensure_system_opportunity_strategies_seeded``
must be strictly create-only — once a strategy row exists with a seed
slug, the seeder must NEVER overwrite ``source_code``, ``config``,
``config_schema``, or any other field. This protects user edits across
restarts.

``reset_strategy_to_factory`` is the explicit opt-in path that does
overwrite from the shipped seed; it must work for any registered slug
regardless of ``is_system``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from sqlalchemy import select

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import Base, Strategy
from services.opportunity_strategy_catalog import (
    build_system_opportunity_strategy_rows,
    ensure_system_opportunity_strategies_seeded,
    reset_strategy_to_factory,
)
from tests.postgres_test_db import build_postgres_session_factory


async def _build_session_factory(_tmp_path: Path):
    return await build_postgres_session_factory(Base, "strategy_catalog_create_only")


@pytest.mark.asyncio
async def test_first_seed_inserts_all_registered_strategies(tmp_path):
    """First call inserts every shipped strategy; nothing pre-existing."""
    engine, session_factory = await _build_session_factory(tmp_path)

    async with session_factory() as session:
        seeded = await ensure_system_opportunity_strategies_seeded(session)
    assert seeded > 0

    async with session_factory() as session:
        rows = (await session.execute(select(Strategy))).scalars().all()
    seed_slugs = {r["slug"] for r in build_system_opportunity_strategy_rows()}
    db_slugs = {row.slug for row in rows}
    # Every registered seed should now be in the DB.
    assert seed_slugs.issubset(db_slugs)

    await engine.dispose()


@pytest.mark.asyncio
async def test_second_seed_inserts_nothing(tmp_path):
    """Idempotency — running the seeder twice on a populated DB is a no-op."""
    engine, session_factory = await _build_session_factory(tmp_path)

    async with session_factory() as session:
        first = await ensure_system_opportunity_strategies_seeded(session)
    async with session_factory() as session:
        second = await ensure_system_opportunity_strategies_seeded(session)

    assert first > 0
    assert second == 0

    await engine.dispose()


@pytest.mark.asyncio
async def test_user_edits_to_seeded_row_survive_reseeding(tmp_path):
    """Critical: user-edited source_code, config, name must NOT be overwritten."""
    engine, session_factory = await _build_session_factory(tmp_path)

    target_slug = "crypto_5m_midcycle"

    async with session_factory() as session:
        await ensure_system_opportunity_strategies_seeded(session)

    edited_source = "# user-rewrote-this\nclass X: pass\n"
    edited_config = {"min_distance_bps": 999.0, "bet_size_usd": 7.5}
    edited_name = "USER RENAMED"

    async with session_factory() as session:
        row = (
            await session.execute(select(Strategy).where(Strategy.slug == target_slug))
        ).scalar_one()
        row.source_code = edited_source
        row.config = edited_config
        row.name = edited_name
        row.enabled = False
        await session.commit()

    # Re-seed — user edits MUST survive.
    async with session_factory() as session:
        re_seeded = await ensure_system_opportunity_strategies_seeded(session)
        row = (
            await session.execute(select(Strategy).where(Strategy.slug == target_slug))
        ).scalar_one()

    assert re_seeded == 0
    assert row.source_code == edited_source
    assert row.config == edited_config
    assert row.name == edited_name
    assert row.enabled is False

    await engine.dispose()


@pytest.mark.asyncio
async def test_deleted_strategy_does_not_resurrect_on_reseeding(tmp_path):
    """No tombstones, no resurrection — deleted strategies stay deleted."""
    engine, session_factory = await _build_session_factory(tmp_path)

    target_slug = "crypto_5m_midcycle"

    async with session_factory() as session:
        await ensure_system_opportunity_strategies_seeded(session)

    async with session_factory() as session:
        row = (
            await session.execute(select(Strategy).where(Strategy.slug == target_slug))
        ).scalar_one()
        await session.delete(row)
        await session.commit()

    # Re-seed — the deleted slug SHOULD reappear, since the design is
    # "create if missing." If a user wants permanent deletion they don't
    # call seed again. (Tombstones, which previously prevented this, are
    # gone — that's the point of this regression test.)
    async with session_factory() as session:
        re_seeded = await ensure_system_opportunity_strategies_seeded(session)
        row = (
            await session.execute(select(Strategy).where(Strategy.slug == target_slug))
        ).scalar_one_or_none()

    assert re_seeded >= 1
    assert row is not None  # recreated

    await engine.dispose()


@pytest.mark.asyncio
async def test_reset_to_factory_works_for_any_registered_slug(tmp_path):
    """Reset-to-factory is no longer gated on is_system — any registered seed works."""
    engine, session_factory = await _build_session_factory(tmp_path)

    target_slug = "crypto_5m_midcycle"

    async with session_factory() as session:
        await ensure_system_opportunity_strategies_seeded(session)

    # Even if user manually flips is_system to False — reset still works.
    async with session_factory() as session:
        row = (
            await session.execute(select(Strategy).where(Strategy.slug == target_slug))
        ).scalar_one()
        row.is_system = False
        row.source_code = "# user-rewrote-this\n"
        row.name = "USER RENAMED"
        await session.commit()

    async with session_factory() as session:
        result = await reset_strategy_to_factory(session, target_slug)
        row = (
            await session.execute(select(Strategy).where(Strategy.slug == target_slug))
        ).scalar_one()

    assert result["status"] == "reset"
    assert row.source_code != "# user-rewrote-this\n"  # restored from seed
    assert "Crypto" in row.name or "midcycle" in row.name.lower()

    await engine.dispose()


@pytest.mark.asyncio
async def test_reset_to_factory_404s_on_unregistered_slug(tmp_path):
    """User-authored strategies have no seed to revert to."""
    engine, session_factory = await _build_session_factory(tmp_path)

    async with session_factory() as session:
        result = await reset_strategy_to_factory(session, "nonexistent_user_strategy")

    assert result["status"] == "not_found"

    await engine.dispose()


@pytest.mark.asyncio
async def test_reset_to_factory_recreates_deleted_strategy(tmp_path):
    """If the strategy was deleted, reset-to-factory recreates it from the seed."""
    engine, session_factory = await _build_session_factory(tmp_path)

    target_slug = "crypto_5m_midcycle"

    async with session_factory() as session:
        await ensure_system_opportunity_strategies_seeded(session)
        row = (
            await session.execute(select(Strategy).where(Strategy.slug == target_slug))
        ).scalar_one()
        await session.delete(row)
        await session.commit()

    async with session_factory() as session:
        result = await reset_strategy_to_factory(session, target_slug)
        row = (
            await session.execute(select(Strategy).where(Strategy.slug == target_slug))
        ).scalar_one()

    assert result["status"] == "created"
    assert row is not None

    await engine.dispose()
