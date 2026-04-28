"""Regression tests for the legacy btc_eth_highfreq → successor migration.

When ``btc_eth_highfreq`` was tombstoned and replaced by 3 standalone
strategies (``btc_eth_directional_edge``, ``btc_eth_maker_quote``,
``btc_eth_convergence``), pre-existing trader rows still pointing at
the old slug would silently match zero published signals and surface as
"Fast cycle idle: no pending signals." in the trader UI — because the
intent_runtime filter rejects strategy_types that aren't in the
trader's source_configs.

``_migrate_legacy_trader_source_configs`` resolves
``strategy_params.mode`` to one of the 3 successor slugs (1→1).
The "auto" / missing-mode path maps to ``btc_eth_directional_edge``
— the most general successor.

The 1→1 mapping is load-bearing: ``_serialize_trader`` enforces a
no-duplicate-source_key invariant, so emitting multiple configs all
under ``source_key="crypto"`` would make ``list_traders`` raise and
the trader list go empty in the UI.
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
async def test_legacy_directional_mode_maps_to_directional_edge():
    engine, session_factory = await _build_session_factory()

    user_params = {
        "mode": "directional",
        "min_edge_percent": 2.0,
        "min_confidence": 0.45,
        "include_assets": ["BTC", "ETH"],
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
    configs = trader.source_configs_json
    assert len(configs) == 1
    assert configs[0]["source_key"] == "crypto"
    assert configs[0]["strategy_key"] == "btc_eth_directional_edge"
    # ``mode`` is dropped (it's now expressed by the strategy slug);
    # every other tuned param is preserved.
    expected_params = {k: v for k, v in user_params.items() if k != "mode"}
    assert configs[0]["strategy_params"] == expected_params

    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode_value, expected_slug",
    [
        ("maker_quote", "btc_eth_maker_quote"),
        ("convergence", "btc_eth_convergence"),
        ("auto", "btc_eth_directional_edge"),
        ("", "btc_eth_directional_edge"),  # empty mode treated as "auto"
        ("nonsense_mode", "btc_eth_directional_edge"),  # unknown → safe default
    ],
)
async def test_mode_maps_to_correct_successor(mode_value: str, expected_slug: str):
    engine, session_factory = await _build_session_factory()

    async with session_factory() as session:
        session.add(
            _make_trader(
                f"Crypto-{expected_slug}",
                [
                    {
                        "source_key": "crypto",
                        "strategy_key": "btc_eth_highfreq",
                        "strategy_params": {"mode": mode_value},
                    }
                ],
            )
        )
        await session.commit()

    async with session_factory() as session:
        await _migrate_legacy_trader_source_configs(session)

    async with session_factory() as session:
        trader = (await session.execute(select(Trader))).scalar_one()
    assert trader.source_configs_json[0]["strategy_key"] == expected_slug
    # Mode key removed from params after migration.
    assert "mode" not in trader.source_configs_json[0]["strategy_params"]

    await engine.dispose()


@pytest.mark.asyncio
async def test_missing_strategy_params_defaults_to_directional_edge():
    engine, session_factory = await _build_session_factory()

    async with session_factory() as session:
        session.add(
            _make_trader(
                "Crypto",
                [
                    {
                        "source_key": "crypto",
                        "strategy_key": "btc_eth_highfreq",
                        # No strategy_params at all
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
    assert trader.source_configs_json[0]["strategy_key"] == "btc_eth_directional_edge"

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
                        "strategy_params": {"mode": "directional"},
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
    ]

    async with session_factory() as session:
        session.add(_make_trader("CryptoNew", list(pristine_configs)))
        await session.commit()

    async with session_factory() as session:
        mutated = await _migrate_legacy_trader_source_configs(session)
    assert mutated == 0

    async with session_factory() as session:
        trader = (await session.execute(select(Trader))).scalar_one()
    assert trader.source_configs_json == pristine_configs

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
                        "strategy_params": {"mode": "convergence", "min_edge_percent": 2.0},
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
    assert keys == ["btc_eth_convergence", "basic"]
    assert trader.source_configs_json[1] == scanner_cfg
    # ``mode`` is dropped, ``min_edge_percent`` survives.
    assert trader.source_configs_json[0]["strategy_params"] == {"min_edge_percent": 2.0}

    await engine.dispose()


@pytest.mark.asyncio
async def test_migrated_trader_serializes_without_raising():
    """After migration, ``_serialize_trader`` (which calls
    ``_normalize_source_configs``) must succeed.  This is the regression
    that broke the trader UI in the previous migration revision."""
    from services.trader_orchestrator_state import _serialize_trader

    engine, session_factory = await _build_session_factory()

    async with session_factory() as session:
        session.add(
            _make_trader(
                "Crypto",
                [
                    {
                        "source_key": "crypto",
                        "strategy_key": "btc_eth_highfreq",
                        "strategy_params": {"mode": "auto"},
                    }
                ],
            )
        )
        await session.commit()

    async with session_factory() as session:
        await _migrate_legacy_trader_source_configs(session)

    async with session_factory() as session:
        trader = (await session.execute(select(Trader))).scalar_one()
    # Must not raise.
    serialized = _serialize_trader(trader)
    assert serialized["source_configs"][0]["strategy_key"] == "btc_eth_directional_edge"

    await engine.dispose()


@pytest.mark.asyncio
async def test_heal_path_collapses_broken_three_successor_fanout():
    """An earlier revision of this migration emitted a 1→3 fan-out under
    a single source_key, which broke ``_serialize_trader``.  The heal
    path collapses such a trader into a single config (preserving the
    first successor by order), so the trader list re-appears in the UI
    after an upgrade."""
    from services.trader_orchestrator_state import _serialize_trader

    engine, session_factory = await _build_session_factory()

    broken_configs = [
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
        {
            "source_key": "crypto",
            "strategy_key": "btc_eth_convergence",
            "strategy_params": {"min_edge_percent": 1.8},
        },
    ]

    async with session_factory() as session:
        session.add(_make_trader("BrokenCrypto", list(broken_configs)))
        await session.commit()

    async with session_factory() as session:
        mutated = await _migrate_legacy_trader_source_configs(session)
    assert mutated == 1

    async with session_factory() as session:
        trader = (await session.execute(select(Trader))).scalar_one()
    # First successor wins — earliest user choice preserved.
    assert len(trader.source_configs_json) == 1
    assert trader.source_configs_json[0]["strategy_key"] == "btc_eth_directional_edge"
    # Serializer must succeed (regression guard for the bug that hid the trader list).
    serialized = _serialize_trader(trader)
    assert serialized["source_configs"][0]["strategy_key"] == "btc_eth_directional_edge"

    await engine.dispose()


@pytest.mark.asyncio
async def test_heal_path_keeps_single_successor_untouched():
    """Single crypto successor config — heal path is a no-op."""
    engine, session_factory = await _build_session_factory()

    async with session_factory() as session:
        session.add(
            _make_trader(
                "SingleCrypto",
                [
                    {
                        "source_key": "crypto",
                        "strategy_key": "btc_eth_maker_quote",
                        "strategy_params": {"min_edge_percent": 1.8},
                    }
                ],
            )
        )
        await session.commit()

    async with session_factory() as session:
        mutated = await _migrate_legacy_trader_source_configs(session)
    assert mutated == 0

    async with session_factory() as session:
        trader = (await session.execute(select(Trader))).scalar_one()
    assert len(trader.source_configs_json) == 1
    assert trader.source_configs_json[0]["strategy_key"] == "btc_eth_maker_quote"

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
                        "strategy_params": {"mode": "maker_quote"},
                    }
                ],
            )
        )
        await session.commit()

    async with session_factory() as session:
        result = await ensure_all_strategies_seeded(session)

    assert result["trader_configs_migrated"] == 1

    await engine.dispose()
