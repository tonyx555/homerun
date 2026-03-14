from __future__ import annotations

import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services import trader_hot_state


def _reset_hot_state() -> None:
    trader_hot_state._snapshots.clear()
    trader_hot_state._global_gross.clear()
    trader_hot_state._global_daily_pnl.clear()
    trader_hot_state._global_daily_pnl_date.clear()
    trader_hot_state._seeded = False


def test_executed_orders_do_not_increment_open_order_count():
    _reset_hot_state()
    try:
        trader_hot_state.record_order_created(
            trader_id="trader-1",
            mode="live",
            order_id="order-1",
            status="executed",
            market_id="market-1",
            direction="buy_yes",
            source="scanner",
            notional_usd=25.0,
            entry_price=0.5,
            token_id="token-1",
            filled_shares=50.0,
            payload={"token_id": "token-1"},
        )

        assert trader_hot_state.get_open_order_count("trader-1", "live") == 0
        assert trader_hot_state.get_open_position_count("trader-1", "live") == 1
    finally:
        _reset_hot_state()


def test_unfilled_orders_increment_and_cancelled_orders_clear_open_order_count():
    _reset_hot_state()
    try:
        trader_hot_state.record_order_created(
            trader_id="trader-1",
            mode="live",
            order_id="order-1",
            status="open",
            market_id="market-1",
            direction="buy_yes",
            source="scanner",
            notional_usd=25.0,
            entry_price=0.5,
            token_id="token-1",
            filled_shares=0.0,
            payload={"token_id": "token-1"},
        )

        assert trader_hot_state.get_open_order_count("trader-1", "live") == 1

        trader_hot_state.record_order_cancelled(
            trader_id="trader-1",
            mode="live",
            order_id="order-1",
            market_id="market-1",
            source="scanner",
        )

        assert trader_hot_state.get_open_order_count("trader-1", "live") == 0
    finally:
        _reset_hot_state()
