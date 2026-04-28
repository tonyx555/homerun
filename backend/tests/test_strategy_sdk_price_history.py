"""Tests for the MarketPriceHistory rolling-window helper.

Lifted from btc_eth_highfreq's local dataclass into
``services.strategy_helpers.price_history`` and re-exported on the
StrategySDK class so any strategy can use it via either import path.
"""

from __future__ import annotations

import time

import pytest

from services.strategy_helpers.price_history import MarketPriceHistory, PriceSnapshot
from services.strategy_sdk import StrategySDK


def test_sdk_class_exposes_dataclasses():
    """Strategies should be able to use ``StrategySDK.MarketPriceHistory(...)``."""
    assert StrategySDK.MarketPriceHistory is MarketPriceHistory
    assert StrategySDK.PriceSnapshot is PriceSnapshot


def test_has_data_requires_two_snapshots():
    h = MarketPriceHistory(window_seconds=60.0)
    assert h.has_data is False
    h.record(yes_price=0.5, no_price=0.5)
    assert h.has_data is False
    h.record(yes_price=0.6, no_price=0.4)
    assert h.has_data is True


def test_max_drop_yes_returns_peak_minus_current():
    h = MarketPriceHistory(window_seconds=60.0)
    h.record(yes_price=0.50, no_price=0.50)
    h.record(yes_price=0.70, no_price=0.30)
    h.record(yes_price=0.55, no_price=0.45)
    assert h.max_drop_yes() == pytest.approx(0.15, rel=1e-9)


def test_max_drop_no_returns_peak_minus_current():
    h = MarketPriceHistory(window_seconds=60.0)
    h.record(yes_price=0.50, no_price=0.50)
    h.record(yes_price=0.20, no_price=0.80)
    h.record(yes_price=0.40, no_price=0.60)
    assert h.max_drop_no() == pytest.approx(0.20, rel=1e-9)


def test_max_drop_clamps_to_zero_when_current_above_peak():
    """A monotonically rising series should never report a drop."""
    h = MarketPriceHistory(window_seconds=60.0)
    h.record(yes_price=0.40, no_price=0.60)
    h.record(yes_price=0.50, no_price=0.50)
    h.record(yes_price=0.60, no_price=0.40)
    assert h.max_drop_yes() == 0.0


def test_recent_volatility_is_max_minus_min_yes():
    h = MarketPriceHistory(window_seconds=60.0)
    h.record(yes_price=0.40, no_price=0.60)
    h.record(yes_price=0.55, no_price=0.45)
    h.record(yes_price=0.45, no_price=0.55)
    assert h.recent_volatility() == pytest.approx(0.15, rel=1e-9)


def test_window_evicts_old_snapshots(monkeypatch):
    """Snapshots older than window_seconds should be dropped on each record()."""
    h = MarketPriceHistory(window_seconds=10.0)
    base = 1000.0
    monkeypatch.setattr(time, "monotonic", lambda: base)
    h.record(yes_price=0.30, no_price=0.70)
    monkeypatch.setattr(time, "monotonic", lambda: base + 5.0)
    h.record(yes_price=0.40, no_price=0.60)
    monkeypatch.setattr(time, "monotonic", lambda: base + 12.0)
    h.record(yes_price=0.50, no_price=0.50)
    assert len(h.snapshots) == 2
    assert h.snapshots[0].yes_price == 0.40


def test_empty_history_returns_zero_for_all_metrics():
    h = MarketPriceHistory(window_seconds=60.0)
    assert h.max_drop_yes() == 0.0
    assert h.max_drop_no() == 0.0
    assert h.recent_volatility() == 0.0


def test_default_window_is_300_seconds():
    """Default matches the constant lifted from btc_eth_highfreq."""
    from services.strategy_helpers.price_history import DEFAULT_WINDOW_SECONDS
    assert DEFAULT_WINDOW_SECONDS == 300.0
    assert MarketPriceHistory().window_seconds == 300.0
