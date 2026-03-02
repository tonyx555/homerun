"""Tests for TailEndCarryStrategy exit behavior."""

from __future__ import annotations

import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.strategies.tail_end_carry import TailEndCarryStrategy


class _Position:
    pass


def _make_position(
    *,
    entry_price: float = 0.85,
    current_price: float = 0.85,
    config: dict | None = None,
) -> _Position:
    pos = _Position()
    pos.entry_price = entry_price
    pos.current_price = current_price
    pos.highest_price = max(entry_price, current_price)
    pos.lowest_price = min(entry_price, current_price)
    pos.age_minutes = 30.0
    pos.pnl_percent = ((current_price - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0
    pos.filled_size = 100.0
    pos.notional_usd = 50.0
    pos.strategy_context = {}
    pos.config = dict(config or {})
    pos.outcome_idx = 0
    return pos


def _market_state(current_price: float, **overrides) -> dict:
    state = {
        "current_price": current_price,
        "market_tradable": True,
        "is_resolved": False,
        "winning_outcome": None,
        "seconds_left": 3600.0,
        "end_time": None,
    }
    state.update(overrides)
    return state


def test_should_hold_on_small_price_noise():
    strategy = TailEndCarryStrategy()
    position = _make_position(entry_price=0.86, current_price=0.84)

    decision = strategy.should_exit(position, _market_state(0.84))

    assert decision.action == "hold"
    assert bool((decision.payload or {}).get("skip_default_exit")) is True


def test_should_close_when_market_inverts():
    strategy = TailEndCarryStrategy()
    position = _make_position(entry_price=0.86, current_price=0.49)

    decision = strategy.should_exit(position, _market_state(0.49))

    assert decision.action == "close"
    assert "inversion stop" in decision.reason.lower()


def test_should_respect_configured_inversion_threshold():
    strategy = TailEndCarryStrategy()
    position = _make_position(
        entry_price=0.86,
        current_price=0.45,
        config={"inversion_stop_enabled": True, "inversion_price_threshold": 0.40},
    )

    decision = strategy.should_exit(position, _market_state(0.45))

    assert decision.action == "hold"
    assert bool((decision.payload or {}).get("skip_default_exit")) is True


def test_should_take_smart_profit_near_ceiling():
    strategy = TailEndCarryStrategy()
    position = _make_position(entry_price=0.86, current_price=0.98)

    decision = strategy.should_exit(position, _market_state(0.98))

    assert decision.action == "close"
    assert "smart take profit" in decision.reason.lower()
