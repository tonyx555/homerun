import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import models.database as database_module
import services.live_execution_service as live_execution_service_module
import services.polymarket as polymarket_module
from sqlalchemy import select

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import Base, LiveTradingOrder, LiveTradingPosition
from services.live_execution_service import LiveExecutionService, Order, OrderSide, OrderStatus, Position
from tests.postgres_test_db import build_postgres_session_factory
from utils.utcnow import utcnow


async def _build_session_factory():
    return await build_postgres_session_factory(Base, "live_execution_runtime_state")


@pytest.mark.asyncio
async def test_restore_runtime_state_prefers_position_market_question(tmp_path, monkeypatch):
    engine, session_factory = await _build_session_factory()
    wallet = "0xruntimewallet"
    token_id = "token-atletico"
    wrong_question = "Will Cucuta Deportivo FC win on 2026-04-01?"
    correct_question = "Will Atletico Nacional win on 2026-04-01?"
    try:
        async with session_factory() as session:
            now = utcnow()
            session.add(
                LiveTradingOrder(
                    id="live-order-atletico",
                    wallet_address=wallet,
                    clob_order_id="clob-atletico",
                    token_id=token_id,
                    side="BUY",
                    price=0.79,
                    size=101.6,
                    order_type="GTC",
                    status="filled",
                    filled_size=101.6,
                    average_fill_price=0.79,
                    market_question=wrong_question,
                    opportunity_id="signal-atletico",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                LiveTradingPosition(
                    id=f"{wallet}:{token_id}",
                    wallet_address=wallet,
                    token_id=token_id,
                    market_id="market-atletico",
                    market_question=correct_question,
                    outcome="Yes",
                    size=101.6,
                    average_cost=0.79,
                    current_price=0.745,
                    unrealized_pnl=-4.57,
                    created_at=now,
                    updated_at=now,
                )
            )
            await session.commit()

        monkeypatch.setattr(database_module, "AsyncSessionLocal", session_factory)

        service = LiveExecutionService()
        service._wallet_address = wallet

        await service._restore_runtime_state()

        restored = service.get_order("live-order-atletico")
        assert restored is not None
        assert restored.market_question == correct_question

        restored.market_question = wrong_question
        await service._persist_orders([restored])

        async with session_factory() as session:
            row = (
                await session.execute(select(LiveTradingOrder).where(LiveTradingOrder.id == "live-order-atletico"))
            ).scalar_one()
            assert row.market_question == correct_question
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_persist_positions_upserts_current_rows_and_deletes_stale_rows(tmp_path, monkeypatch):
    engine, session_factory = await _build_session_factory()
    wallet = "0xruntimewallet-upsert"
    now = utcnow()
    try:
        async with session_factory() as session:
            session.add_all(
                [
                    LiveTradingPosition(
                        id=f"{wallet}:token-keep",
                        wallet_address=wallet,
                        token_id="token-keep",
                        market_id="market-old",
                        market_question="Old question",
                        outcome="Yes",
                        size=1.0,
                        average_cost=0.4,
                        current_price=0.41,
                        unrealized_pnl=0.01,
                        created_at=now,
                        updated_at=now,
                    ),
                    LiveTradingPosition(
                        id=f"{wallet}:token-stale",
                        wallet_address=wallet,
                        token_id="token-stale",
                        market_id="market-stale",
                        market_question="Stale question",
                        outcome="No",
                        size=2.0,
                        average_cost=0.6,
                        current_price=0.55,
                        unrealized_pnl=-0.1,
                        created_at=now,
                        updated_at=now,
                    ),
                ]
            )
            await session.commit()

        monkeypatch.setattr(database_module, "AsyncSessionLocal", session_factory)

        service = LiveExecutionService()
        service._wallet_address = wallet
        service._positions = {
            "token-keep": Position(
                token_id="token-keep",
                market_id="market-new",
                market_question="Updated question",
                outcome="Yes",
                size=3.0,
                average_cost=0.72,
                current_price=0.75,
                unrealized_pnl=0.09,
                created_at=now,
            ),
            "token-new": Position(
                token_id="token-new",
                market_id="market-new-2",
                market_question="Brand new question",
                outcome="No",
                size=4.0,
                average_cost=0.21,
                current_price=0.18,
                unrealized_pnl=-0.12,
                created_at=now,
            ),
        }

        await service._persist_positions()

        async with session_factory() as session:
            rows = (
                await session.execute(
                    select(LiveTradingPosition)
                    .where(LiveTradingPosition.wallet_address == wallet)
                    .order_by(LiveTradingPosition.token_id.asc())
                )
            ).scalars().all()
            assert [row.token_id for row in rows] == ["token-keep", "token-new"]
            kept_row = next(row for row in rows if row.token_id == "token-keep")
            assert kept_row.market_id == "market-new"
            assert kept_row.market_question == "Updated question"
            assert kept_row.size == 3.0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_get_order_snapshots_returns_cached_fallback_after_bulk_timeout(monkeypatch):
    service = LiveExecutionService()
    service._initialized = True
    service._client = SimpleNamespace(get_orders=object(), get_order=object())

    order = Order(
        id="order-1",
        token_id="token-1",
        side=OrderSide.BUY,
        price=0.73,
        size=15.0,
        status=OrderStatus.OPEN,
        clob_order_id="clob-1",
        market_question="Question 1",
    )
    service._orders[order.id] = order

    calls: list[str] = []

    async def _run_client_io(method, *args, timeout=None):
        del args
        del timeout
        if method is service._client.get_orders:
            calls.append("get_orders")
            raise asyncio.TimeoutError()
        if method is service._client.get_order:
            calls.append("get_order")
            raise AssertionError("single-order fallback should not run after bulk timeout")
        raise AssertionError("unexpected client method")

    monkeypatch.setattr(service, "_run_client_io", _run_client_io)
    monkeypatch.setattr(service, "ensure_initialized", AsyncMock(return_value=False))
    monkeypatch.setattr(service, "_persist_orders", AsyncMock())

    snapshots = await service.get_order_snapshots_by_clob_ids(["clob-1"])

    assert calls == ["get_orders"]
    assert snapshots["clob-1"]["clob_order_id"] == "clob-1"
    assert snapshots["clob-1"]["normalized_status"] == "open"


@pytest.mark.asyncio
async def test_sync_positions_marks_redeemable_claims_as_not_open(monkeypatch):
    service = LiveExecutionService()
    service._initialized = True
    service._client = object()
    service._wallet_address = "0xruntimewallet-sync"
    monkeypatch.setattr(service, "_persist_positions", AsyncMock())
    monkeypatch.setattr(service, "_persist_runtime_state", AsyncMock())
    monkeypatch.setattr(
        polymarket_module.polymarket_client,
        "get_wallet_positions_with_prices",
        AsyncMock(
            return_value=[
                {
                    "asset": "token-open",
                    "conditionId": "market-open",
                    "title": "Open market",
                    "outcome": "Yes",
                    "size": 4.0,
                    "avgPrice": 0.55,
                    "currentPrice": 0.6,
                    "cashPnl": 0.2,
                    "redeemable": False,
                    "endDate": "2026-04-11",
                },
                {
                    "asset": "token-claim",
                    "conditionId": "market-claim",
                    "title": "Redeemable market",
                    "outcome": "No",
                    "size": 3.0,
                    "avgPrice": 0.92,
                    "currentPrice": 1.0,
                    "cashPnl": 0.24,
                    "redeemable": True,
                    "endDate": "2026-04-10",
                },
            ]
        ),
    )

    positions = await service.sync_positions()

    assert len(positions) == 2
    positions_by_token = {position.token_id: position for position in positions}
    assert positions_by_token["token-open"].counts_as_open is True
    assert positions_by_token["token-open"].redeemable is False
    assert positions_by_token["token-claim"].counts_as_open is False
    assert positions_by_token["token-claim"].redeemable is True
    assert service._stats.open_positions == 1
