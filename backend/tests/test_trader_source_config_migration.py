"""Regression tests for the legacy btc_eth_highfreq → 3-successor migration.

When ``btc_eth_highfreq`` was tombstoned and replaced by 3 standalone
strategies (``btc_eth_directional_edge``, ``btc_eth_maker_quote``,
``btc_eth_convergence``), any pre-existing trader still pointing at
the old slug would silently match zero published signals and surface as
"Fast cycle idle: no pending signals." in the trader UI — because the
intent_runtime filter rejects strategy_types that aren't in the
trader's source_configs.

``_migrate_legacy_trader_source_configs`` rewrites those configs at
startup so the trader picks up the 3 successors automatically.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import Base, Trader
from services.opportunity_strategy_catalog import (
    _migrate_legacy_trader_source_configs,
    ensure_all_strategies_seeded,
)
from tests.postgres_test_db import build_postgres_session_factory


async def _build_session_factory():
    return await build_postgres_session_factory(Base, "trader_source_config_migration")


def _make_trader(name: str, source_configs: list[dict]) -> Trader:
    return Trader(
        id=uuid.uuid4().hex,
        name=name,
        description=None,
        source_configs_json=source_configs,
        risk_limits_json={},
        metadata_json={},
        mode="shadow",
        latency_class="normal",
        is_enabled=True,
        is_paused=False,
        block_new_orders=False,
        interval_seconds=1,
    )


@pytest.mark.asyncio
async def test_legacy_btc_eth_highfreq_config_expands_to_three_successors():
    engine, session_factory = await _build_session_factory()

    user_params = {
        "min_edge_percent": 2.0,
        "min_confidence": 0.45,
        "include_assets": ["BTC", "ETH"],
        "exclude_timeframes": ["1h", "4h"],
    }

    async with session_factory() as session:
        session.add(
            _make_trader(
                "Crypto",
                [
                    {
                        "source_key": "crypto",
                        "strategy_key": "btc_eth_highfreq",
                        "strategy_params": dict(user_params),
                    }
                ],
            )
        )
        await session.commit()

    async with session_factory() as session:
        mutated = await _migrate_legacy_trader_source_configs(session)
    assert mutated == 1

    async with session_factory() as session:
        trader = (await session.execute(select(Trader))).scalar_one()
    keys = [cfg["strategy_key"] for cfg in trader.source_configs_json]
    assert keys == [
        "btc_eth_directional_edge",
        "btc_eth_maker_quote",
        "btc_eth_convergence",
    ]
    # Each successor preserves the original strategy_params.
    for cfg in trader.source_configs_json:
        assert cfg["source_key"] == "crypto"
        assert cfg["strategy_params"] == user_params

    await engine.dispose()


@pytest.mark.asyncio
async def test_migration_is_idempotent():
    engine, session_factory = await _build_session_factory()

    async with session_factory() as session:
        session.add(
            _make_trader(
                "Crypto",
                [
                    {
                        "source_key": "crypto",
                        "strategy_key": "btc_eth_highfreq",
                        "strategy_params": {},
                    }
                ],
            )
        )
        await session.commit()

    async with session_factory() as session:
        first = await _migrate_legacy_trader_source_configs(session)
    async with session_factory() as session:
        second = await _migrate_legacy_trader_source_configs(session)
    assert first == 1
    assert second == 0

    await engine.dispose()


@pytest.mark.asyncio
async def test_migration_skips_traders_already_on_new_slugs():
    engine, session_factory = await _build_session_factory()

    pristine_configs = [
        {
            "source_key": "crypto",
            "strategy_key": "btc_eth_directional_edge",
            "strategy_params": {"min_edge_percent": 1.8},
        },
        {
            "source_key": "crypto",
            "strategy_key": "btc_eth_maker_quote",
            "strategy_params": {"min_edge_percent": 1.8},
        },
    ]

    async with session_factory() as session:
        session.add(_make_trader("CryptoNew", list(pristine_configs)))
        await session.commit()

    async with session_factory() as session:
        mutated = await _migrate_legacy_trader_source_configs(session)
    assert mutated == 0

    async with session_factory() as session:
        trader = (await session.execute(select(Trader))).scalar_one()
    # Untouched.
    assert [cfg["strategy_key"] for cfg in trader.source_configs_json] == [
        "btc_eth_directional_edge",
        "btc_eth_maker_quote",
    ]

    await engine.dispose()


@pytest.mark.asyncio
async def test_migration_preserves_unrelated_source_configs():
    """Mixed trader: legacy crypto config + a non-crypto config — only the
    legacy one is rewritten; the rest pass through verbatim."""
    engine, session_factory = await _build_session_factory()

    scanner_cfg = {
        "source_key": "scanner",
        "strategy_key": "basic",
        "strategy_params": {"min_edge_percent": 4.0},
    }

    async with session_factory() as session:
        session.add(
            _make_trader(
                "Mixed",
                [
                    {
                        "source_key": "crypto",
                        "strategy_key": "btc_eth_highfreq",
                        "strategy_params": {"min_edge_percent": 2.0},
                    },
                    scanner_cfg,
                ],
            )
        )
        await session.commit()

    async with session_factory() as session:
        mutated = await _migrate_legacy_trader_source_configs(session)
    assert mutated == 1

    async with session_factory() as session:
        trader = (await session.execute(select(Trader))).scalar_one()
    keys = [cfg["strategy_key"] for cfg in trader.source_configs_json]
    assert keys == [
        "btc_eth_directional_edge",
        "btc_eth_maker_quote",
        "btc_eth_convergence",
        "basic",
    ]
    assert trader.source_configs_json[-1] == scanner_cfg

    await engine.dispose()


@pytest.mark.asyncio
async def test_ensure_all_strategies_seeded_runs_migration():
    """Smoke: the public seed entrypoint reports the migration count too."""
    engine, session_factory = await _build_session_factory()

    async with session_factory() as session:
        session.add(
            _make_trader(
                "Crypto",
                [
                    {
                        "source_key": "crypto",
                        "strategy_key": "btc_eth_highfreq",
                        "strategy_params": {},
                    }
                ],
            )
        )
        await session.commit()

    async with session_factory() as session:
        result = await ensure_all_strategies_seeded(session)

    assert result["trader_configs_migrated"] == 1

    await engine.dispose()
