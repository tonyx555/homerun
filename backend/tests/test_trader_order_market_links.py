import sys
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import Base, TradeSignal, TraderOrder
from services.market_cache import CachedMarket
from services.trader_orchestrator_state import list_serialized_trader_orders
from tests.postgres_test_db import build_postgres_session_factory


async def _build_session_factory(_tmp_path: Path):
    return await build_postgres_session_factory(Base, "trader_order_market_links")


@pytest.mark.asyncio
async def test_list_serialized_trader_orders_enriches_payload_from_cached_market(tmp_path):
    condition_id = "0xf0eac4f4ef5b0f09952b9635beba71e2130fc1ded8da4ba1c5b29a2bc79b34e8"
    market_slug = "jazz-vs-rockets-over-under-228-5"
    event_slug = "nba-jazz-rockets-2026-02-23"
    expected_url = f"https://polymarket.com/event/{event_slug}/{market_slug}"

    engine, session_factory = await _build_session_factory(tmp_path)
    async with session_factory() as session:
        session.add(
            CachedMarket(
                condition_id=condition_id,
                question="Jazz vs Rockets",
                slug=market_slug,
                extra_data={
                    "id": "123456",
                    "condition_id": condition_id,
                    "event_slug": event_slug,
                },
            )
        )
        session.add(
            TraderOrder(
                id="order_1",
                trader_id="trader_1",
                source="traders",
                market_id=condition_id,
                market_question="Jazz vs Rockets",
                mode="paper",
                status="open",
                payload_json={},
            )
        )
        await session.commit()

        rows = await list_serialized_trader_orders(session, trader_id="trader_1", limit=20)

    await engine.dispose()
    assert len(rows) == 1
    payload = rows[0]["payload"]
    assert payload["market_slug"] == market_slug
    assert payload["event_slug"] == event_slug
    assert payload["condition_id"] == condition_id
    assert payload["market_url"] == expected_url
    assert payload["polymarket_url"] == expected_url


@pytest.mark.asyncio
async def test_list_serialized_trader_orders_keeps_existing_payload_metadata(tmp_path):
    condition_id = "0x6f4a82c35b64e93d8f6299654f65e82c53d53f42d8df95de57d802c62af70a26"
    engine, session_factory = await _build_session_factory(tmp_path)
    async with session_factory() as session:
        session.add(
            CachedMarket(
                condition_id=condition_id,
                question="Cache metadata should not override payload metadata",
                slug="cache-market-slug",
                extra_data={
                    "id": "777777",
                    "condition_id": condition_id,
                    "event_slug": "cache-event-slug",
                },
            )
        )
        session.add(
            TraderOrder(
                id="order_2",
                trader_id="trader_2",
                source="traders",
                market_id=condition_id,
                market_question="Manual payload metadata",
                mode="paper",
                status="open",
                payload_json={
                    "market_slug": "payload-market-slug",
                    "event_slug": "payload-event-slug",
                    "market_url": "https://polymarket.com/market/payload-market-slug",
                },
            )
        )
        await session.commit()

        rows = await list_serialized_trader_orders(session, trader_id="trader_2", limit=20)

    await engine.dispose()
    assert len(rows) == 1
    payload = rows[0]["payload"]
    assert payload["market_slug"] == "payload-market-slug"
    assert payload["event_slug"] == "payload-event-slug"
    assert payload["market_url"] == "https://polymarket.com/market/payload-market-slug"
    assert payload["polymarket_url"] == "https://polymarket.com/market/payload-market-slug"


@pytest.mark.asyncio
async def test_list_serialized_trader_orders_uses_cached_market_outcome_labels_for_direction(tmp_path):
    condition_id = "0x5e20ce52f9449f89d6a14341a26167e6eea76a3ec78357d5a17ad5df85c74bc2"
    yes_label = "BOS"
    no_label = "DEN"

    engine, session_factory = await _build_session_factory(tmp_path)
    async with session_factory() as session:
        session.add(
            CachedMarket(
                condition_id=condition_id,
                question="BOS vs DEN",
                slug="bos-vs-den",
                extra_data={
                    "condition_id": condition_id,
                    "outcome_labels": [yes_label, no_label],
                },
            )
        )
        session.add(
            TraderOrder(
                id="order_3",
                trader_id="trader_3",
                source="traders",
                market_id=condition_id,
                market_question="BOS vs DEN",
                direction="buy_yes",
                mode="paper",
                status="open",
                payload_json={},
            )
        )
        await session.commit()

        rows = await list_serialized_trader_orders(session, trader_id="trader_3", limit=20)

    await engine.dispose()
    assert len(rows) == 1
    row = rows[0]
    assert row["direction_side"] == "YES"
    assert row["direction_label"] == yes_label
    assert row["yes_label"] == yes_label
    assert row["no_label"] == no_label


@pytest.mark.asyncio
async def test_list_serialized_trader_orders_includes_trade_bundle_metadata_for_multileg_signal(tmp_path):
    engine, session_factory = await _build_session_factory(tmp_path)
    async with session_factory() as session:
        session.add(
            TradeSignal(
                id="signal_bundle_1",
                source="scanner",
                signal_type="opportunity",
                strategy_type="settlement_lag",
                market_id="market-atletico",
                market_question="Will Atlético Nacional win on 2026-04-01?",
                direction="buy_yes",
                dedupe_key="bundle-signal-1",
                payload_json={
                    "total_cost": 0.865,
                    "expected_payout": 1.0,
                    "gross_profit": 0.135,
                    "net_profit": 0.112405,
                    "roi_percent": 12.9948,
                    "is_guaranteed": True,
                    "roi_type": "guaranteed_spread",
                    "mispricing_type": "settlement_lag",
                    "positions_to_take": [
                        {
                            "action": "BUY",
                            "outcome": "YES",
                            "market": "Will Atlético Nacional win on 2026-04-01?",
                            "price": 0.79,
                            "token_id": "token-atletico",
                        },
                        {
                            "action": "BUY",
                            "outcome": "YES",
                            "market": "Will Cúcuta Deportivo FC win on 2026-04-01?",
                            "price": 0.075,
                            "token_id": "token-cucuta",
                        },
                    ],
                    "execution_plan": {
                        "plan_id": "plan-bundle-1",
                        "legs": [
                            {
                                "leg_id": "leg_1",
                                "market_id": "market-atletico",
                                "market_question": "Will Atlético Nacional win on 2026-04-01?",
                                "token_id": "token-atletico",
                                "side": "buy",
                                "outcome": "yes",
                                "limit_price": 0.79,
                                "notional_weight": 0.79,
                            },
                            {
                                "leg_id": "leg_2",
                                "market_id": "market-cucuta",
                                "market_question": "Will Cúcuta Deportivo FC win on 2026-04-01?",
                                "token_id": "token-cucuta",
                                "side": "buy",
                                "outcome": "yes",
                                "limit_price": 0.075,
                                "notional_weight": 0.075,
                            },
                        ],
                    },
                },
            )
        )
        session.add(
            TraderOrder(
                id="order_bundle_1",
                trader_id="trader_bundle_1",
                signal_id="signal_bundle_1",
                source="scanner",
                market_id="market-atletico",
                market_question="Will Atlético Nacional win on 2026-04-01?",
                direction="buy_yes",
                mode="paper",
                status="open",
                payload_json={
                    "token_id": "token-atletico",
                },
            )
        )
        await session.commit()

        rows = await list_serialized_trader_orders(session, trader_id="trader_bundle_1", limit=20)

    await engine.dispose()
    assert len(rows) == 1
    bundle = rows[0]["trade_bundle"]
    assert bundle is not None
    assert bundle["bundle_id"] == "signal_bundle_1"
    assert bundle["plan_id"] == "plan-bundle-1"
    assert bundle["kind"] == "multi_outcome_yes"
    assert bundle["leg_count"] == 2
    assert bundle["is_guaranteed"] is True
    assert bundle["total_cost"] == pytest.approx(0.865)
    assert bundle["expected_payout"] == pytest.approx(1.0)
    assert bundle["net_profit"] == pytest.approx(0.112405)
    assert bundle["current_leg_id"] == "leg_1"
    assert bundle["current_leg_index"] == 0
    assert [leg["token_id"] for leg in bundle["legs"]] == ["token-atletico", "token-cucuta"]
    assert [leg["notional_weight"] for leg in bundle["legs"]] == pytest.approx([0.79, 0.075])
