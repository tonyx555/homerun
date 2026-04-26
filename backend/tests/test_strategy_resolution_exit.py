"""Tests for the resolution-boundary exit hook in BaseStrategy.

Strategies opting in via ``force_exit_seconds_before_resolution`` must close
any open position when the market is approaching resolution — the Chainlink
heartbeat window is too narrow to risk holding a leg into."""

from __future__ import annotations

from types import SimpleNamespace

from services.strategies.base import BaseStrategy, ExitDecision


class _Strategy(BaseStrategy):
    """Minimal concrete subclass so we can instantiate BaseStrategy."""

    strategy_type = "test_resolution_exit"

    def detect(self, *args, **kwargs):  # pragma: no cover - not exercised
        return []


def _position(*, entry_price: float = 0.50, current_price: float = 0.55):
    return SimpleNamespace(
        entry_price=entry_price,
        current_price=current_price,
        highest_price=current_price,
        lowest_price=current_price,
        age_minutes=1.0,
        pnl_percent=((current_price - entry_price) / entry_price) * 100.0,
        strategy_context={"end_time": None},
        config={},
    )


def _market_state(*, seconds_left: float, current_price: float = 0.55) -> dict:
    return {
        "current_price": current_price,
        "market_tradable": True,
        "is_resolved": False,
        "winning_outcome": None,
        "seconds_left": seconds_left,
    }


def test_no_resolution_exit_when_config_unset():
    strat = _Strategy()
    decision = strat.default_exit_check(
        _position(),
        _market_state(seconds_left=15.0),
    )
    # Without the config knob, default holds (or hits TP/SL above this path).
    assert isinstance(decision, ExitDecision)
    assert decision.action != "close" or "Approaching resolution" not in str(decision.reason)


def test_resolution_exit_fires_when_seconds_left_below_threshold():
    strat = _Strategy()
    pos = _position()
    pos.config = {"force_exit_seconds_before_resolution": 30.0}
    decision = strat.default_exit_check(
        pos,
        _market_state(seconds_left=20.0),
    )
    assert decision.action == "close"
    assert "Approaching resolution" in str(decision.reason)


def test_resolution_exit_holds_when_runway_remains():
    strat = _Strategy()
    pos = _position()
    pos.config = {"force_exit_seconds_before_resolution": 30.0}
    decision = strat.default_exit_check(
        pos,
        _market_state(seconds_left=180.0),
    )
    # Plenty of time left → not a close-for-resolution.
    assert decision.action != "close" or "Approaching resolution" not in str(decision.reason)


def test_resolution_exit_disabled_with_zero_threshold():
    strat = _Strategy()
    pos = _position()
    pos.config = {"force_exit_seconds_before_resolution": 0.0}
    decision = strat.default_exit_check(
        pos,
        _market_state(seconds_left=2.0),
    )
    # Threshold of 0 means "off" — we don't force-exit even with 2s left.
    assert decision.action != "close" or "Approaching resolution" not in str(decision.reason)
