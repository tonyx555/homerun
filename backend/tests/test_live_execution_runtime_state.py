import sys
from pathlib import Path

import pytest
import models.database as database_module
from sqlalchemy import select

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import Base, LiveTradingOrder, LiveTradingPosition
from services.live_execution_service import LiveExecutionService, Position
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
