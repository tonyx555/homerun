import sys
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import func, select

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import services.insider_detector as insider_detector_module  # noqa: E402
from models.database import Base, DiscoveredWallet, MarketCatalog  # noqa: E402
from models.market import Event, Market  # noqa: E402
from services.insider_detector import DEFAULT_RESCORE_MAX_WALLETS, InsiderDetectorService  # noqa: E402
from services.shared_state import read_market_catalog, write_market_catalog  # noqa: E402
from tests.postgres_test_db import build_postgres_session_factory  # noqa: E402
from utils.utcnow import utcnow  # noqa: E402


@pytest.mark.asyncio
async def test_market_catalog_storage_strips_nested_event_markets_and_relinks_on_read():
    engine, session_factory = await build_postgres_session_factory(Base, "market_catalog_storage")

    market_yes = Market(
        id="market-yes",
        condition_id="condition-yes",
        question="Will Team A win?",
        slug="team-a-win",
        event_slug="event-1",
        clob_token_ids=["yes-token", "no-token"],
        outcome_prices=[0.55, 0.45],
    )
    market_no = Market(
        id="market-no",
        condition_id="condition-no",
        question="Will Team A win?",
        slug="team-a-win-2",
        event_slug="event-1",
        clob_token_ids=["yes-token-2", "no-token-2"],
        outcome_prices=[0.60, 0.40],
    )
    event = Event(
        id="event-1",
        slug="event-1",
        title="Event One",
        markets=[market_yes, market_no],
    )

    async with session_factory() as session:
        await write_market_catalog(session, [event], [market_yes, market_no], duration_seconds=12.5)

    async with session_factory() as session:
        row = (
            await session.execute(select(MarketCatalog).where(MarketCatalog.id == "latest"))
        ).scalar_one()
        assert row.event_count == 1
        assert row.market_count == 2
        assert isinstance(row.events_json, list)
        assert row.events_json == []
        assert row.markets_json == []

    async with session_factory() as session:
        events, markets, metadata = await read_market_catalog(
            session,
            include_events=True,
            include_markets=True,
        )

    assert metadata["event_count"] == 1
    assert metadata["market_count"] == 2
    assert len(markets) == 2
    assert len(events) == 1
    assert [market.id for market in events[0].markets] == ["market-yes", "market-no"]

    await engine.dispose()


@pytest.mark.asyncio
async def test_insider_rescore_limits_default_batch_and_persists_updates(monkeypatch):
    engine, session_factory = await build_postgres_session_factory(Base, "insider_rescore_batch")
    monkeypatch.setattr(insider_detector_module, "AsyncSessionLocal", session_factory)

    now = utcnow()
    async with session_factory() as session:
        session.add_all(
            [
                DiscoveredWallet(
                    address=f"wallet_{index:03d}",
                    total_trades=80,
                    wins=50,
                    losses=20,
                    win_rate=0.714,
                    avg_roi=12.0,
                    max_drawdown=0.08,
                    last_analyzed_at=now - timedelta(minutes=index),
                    insider_last_scored_at=now - timedelta(hours=2),
                )
                for index in range(DEFAULT_RESCORE_MAX_WALLETS + 6)
            ]
        )
        await session.commit()

    service = InsiderDetectorService()

    async def _score_wallet(*, session, wallet, cluster_sizes, now):
        return {
            "insider_score": 0.73,
            "insider_confidence": 0.64,
            "sample_size": int((wallet.wins or 0) + (wallet.losses or 0)),
            "classification": "flagged_insider",
            "metrics": {"address": wallet.address},
            "reasons": ["unit_test"],
        }

    monkeypatch.setattr(service, "_score_wallet", _score_wallet)

    result = await service.rescore_wallets(stale_minutes=15)

    assert result["scored_wallets"] == DEFAULT_RESCORE_MAX_WALLETS
    assert result["flagged_insiders"] == DEFAULT_RESCORE_MAX_WALLETS
    assert result["watch_insiders"] == 0

    async with session_factory() as session:
        scored_count = (
            await session.execute(
                select(func.count())
                .select_from(DiscoveredWallet)
                .where(DiscoveredWallet.insider_last_scored_at >= now)
            )
        ).scalar_one()
        assert int(scored_count or 0) == DEFAULT_RESCORE_MAX_WALLETS

    await engine.dispose()
