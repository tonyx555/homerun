"""Tests for TailEndCarryStrategy exit behavior."""

from __future__ import annotations

import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.strategies.tail_end_carry import (
    TailEndCarryStrategy,
    _classify_market,
    CATEGORY_SPORTS,
    CATEGORY_ESPORTS,
    CATEGORY_CRYPTO,
    CATEGORY_POLITICAL,
    CATEGORY_OTHER,
)


class _Position:
    pass


def _make_position(
    *,
    entry_price: float = 0.85,
    current_price: float = 0.85,
    highest_price: float | None = None,
    lowest_price: float | None = None,
    age_minutes: float = 30.0,
    config: dict | None = None,
) -> _Position:
    pos = _Position()
    pos.entry_price = entry_price
    pos.current_price = current_price
    pos.highest_price = highest_price if highest_price is not None else max(entry_price, current_price)
    pos.lowest_price = lowest_price if lowest_price is not None else min(entry_price, current_price)
    pos.age_minutes = age_minutes
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
        "seconds_left": 86400.0,  # 24h — well outside resolution hold window
        "end_time": None,
    }
    state.update(overrides)
    return state


# -- Classification tests --


def test_classify_sports_by_keyword():
    assert _classify_market("Stetson Hatters vs. Austin Peay Governors", None) == CATEGORY_SPORTS


def test_classify_sports_by_market_type():
    assert _classify_market("Some random question", "moneyline") == CATEGORY_SPORTS
    assert _classify_market("Some random question", "totals") == CATEGORY_SPORTS


def test_classify_esports():
    assert _classify_market("LoL: Galions Sharks vs French Flair - Game 1 Winner", None) == CATEGORY_ESPORTS
    assert _classify_market("Counter-Strike: G2 vs NIP", None) == CATEGORY_ESPORTS
    assert _classify_market("ESL Pro League Stage 1", None) == CATEGORY_ESPORTS


def test_classify_crypto():
    assert _classify_market("Will the price of Bitcoin be above $70,000?", None) == CATEGORY_CRYPTO
    assert _classify_market("XRP Up or Down - March 6", None) == CATEGORY_CRYPTO
    assert _classify_market("Solana Up or Down on March 6?", None) == CATEGORY_CRYPTO


def test_classify_political():
    assert _classify_market("Will Ken Paxton come in 2nd in the 2026 Texas Republican Primary?", None) == CATEGORY_POLITICAL
    assert _classify_market("US forces enter Iran by March 7?", None) == CATEGORY_POLITICAL


def test_classify_other():
    assert _classify_market("Will it rain in Tokyo tomorrow?", None) == CATEGORY_OTHER


# -- Exit behavior tests --


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
        config={
            "inversion_stop_enabled": True,
            "inversion_price_threshold": 0.40,
            "trailing_stop_enabled": False,
        },
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


# -- Fix #1: Sports markets disable inversion stop --


def test_sports_market_skips_inversion_stop():
    """Sports markets with sports_inversion_stop_enabled=False should NOT trigger inversion stop."""
    strategy = TailEndCarryStrategy()
    position = _make_position(
        entry_price=0.86,
        current_price=0.49,
        config={
            "sports_inversion_stop_enabled": False,
            "inversion_stop_enabled": True,
            "trailing_stop_enabled": False,
            "_market_question": "Stetson Hatters vs. Austin Peay Governors",
        },
    )

    decision = strategy.should_exit(position, _market_state(0.49))

    assert decision.action == "hold"
    assert "inversion stop" not in decision.reason.lower()


def test_non_sports_market_still_triggers_inversion_stop():
    """Non-sports markets should still respect the inversion stop."""
    strategy = TailEndCarryStrategy()
    position = _make_position(
        entry_price=0.86,
        current_price=0.49,
        config={
            "sports_inversion_stop_enabled": False,
            "inversion_stop_enabled": True,
            "trailing_stop_enabled": False,
            "_market_question": "Will it rain in Tokyo?",
        },
    )

    decision = strategy.should_exit(position, _market_state(0.49))

    assert decision.action == "close"
    assert "inversion stop" in decision.reason.lower()


# -- Fix #2: Trailing stop from high water mark --


def test_trailing_stop_triggers_on_large_drop_from_high():
    strategy = TailEndCarryStrategy()
    position = _make_position(
        entry_price=0.86,
        current_price=0.65,
        highest_price=0.95,
        config={
            "trailing_stop_enabled": True,
            "trailing_stop_pct": 25.0,
            "inversion_stop_enabled": False,
            "_market_question": "Will it rain?",
        },
    )

    decision = strategy.should_exit(position, _market_state(0.65))

    assert decision.action == "close"
    assert "trailing stop" in decision.reason.lower()


def test_trailing_stop_holds_on_small_drop():
    strategy = TailEndCarryStrategy()
    position = _make_position(
        entry_price=0.86,
        current_price=0.88,
        highest_price=0.95,
        config={
            "trailing_stop_enabled": True,
            "trailing_stop_pct": 25.0,
            "inversion_stop_enabled": False,
        },
    )

    decision = strategy.should_exit(position, _market_state(0.88))

    assert decision.action == "hold"


def test_sports_trailing_stop_uses_wider_threshold():
    """Sports markets use sports_trailing_stop_pct (wider) instead of trailing_stop_pct."""
    strategy = TailEndCarryStrategy()
    # 30% drop from high: would trigger 25% normal trailing, but NOT 45% sports trailing
    position = _make_position(
        entry_price=0.86,
        current_price=0.665,
        highest_price=0.95,
        config={
            "trailing_stop_enabled": True,
            "trailing_stop_pct": 25.0,
            "sports_trailing_stop_pct": 45.0,
            "inversion_stop_enabled": False,
            "sports_inversion_stop_enabled": False,
            "_market_question": "Stetson Hatters vs. Austin Peay Governors",
        },
    )

    decision = strategy.should_exit(position, _market_state(0.665))

    assert decision.action == "hold"


# -- Fix #7: Resolution proximity hold --


def test_resolution_hold_overrides_inversion_stop():
    """Within resolution hold window with moderate loss, hold to let it resolve."""
    strategy = TailEndCarryStrategy()
    position = _make_position(
        entry_price=0.86,
        current_price=0.72,
        config={
            "inversion_stop_enabled": True,
            "inversion_price_threshold": 0.50,
            "resolution_hold_enabled": True,
            "resolution_hold_minutes": 120.0,
            "resolution_hold_max_loss_pct": 25.0,
        },
    )

    # 60 minutes left = within 120 minute hold window, loss ~16% < 25% threshold
    decision = strategy.should_exit(position, _market_state(0.72, seconds_left=3600.0))

    assert decision.action == "hold"
    assert "resolution proximity hold" in decision.reason.lower()


def test_resolution_hold_max_loss_override():
    """Within resolution hold window but loss exceeds threshold — close anyway."""
    strategy = TailEndCarryStrategy()
    position = _make_position(
        entry_price=0.86,
        current_price=0.49,
        config={
            "inversion_stop_enabled": True,
            "inversion_price_threshold": 0.50,
            "resolution_hold_enabled": True,
            "resolution_hold_minutes": 120.0,
            "resolution_hold_max_loss_pct": 25.0,
        },
    )

    # 60 minutes left, but 43% loss exceeds 25% threshold — max-loss override fires
    decision = strategy.should_exit(position, _market_state(0.49, seconds_left=3600.0))

    assert decision.action == "close"
    assert "max-loss override" in decision.reason.lower()


def test_resolution_hold_does_not_apply_when_far_from_resolution():
    """Outside the resolution hold window, inversion stop fires normally."""
    strategy = TailEndCarryStrategy()
    position = _make_position(
        entry_price=0.86,
        current_price=0.49,
        config={
            "inversion_stop_enabled": True,
            "inversion_price_threshold": 0.50,
            "trailing_stop_enabled": False,
            "resolution_hold_enabled": True,
            "resolution_hold_minutes": 120.0,
            "_market_question": "Will something happen?",
        },
    )

    # 5 hours left = outside 120 minute hold window
    decision = strategy.should_exit(position, _market_state(0.49, seconds_left=18000.0))

    assert decision.action == "close"
    assert "inversion stop" in decision.reason.lower()


def test_max_hold_still_triggers():
    strategy = TailEndCarryStrategy()
    position = _make_position(
        entry_price=0.86,
        current_price=0.82,
        age_minutes=1500.0,
        config={"max_hold_minutes": 1440.0},
    )

    decision = strategy.should_exit(position, _market_state(0.82, seconds_left=86400.0))

    assert decision.action == "close"
    assert "max hold" in decision.reason.lower()
