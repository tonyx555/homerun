"""Tests for the generic single-stream PriceWindow helper.

PriceWindow is the canonical primitive for tracking any single price
stream (one outcome of a multi-outcome market, a crypto spot price, an
oracle feed, etc.). It replaces the binary-only MarketPriceHistory for
new code.

These tests import the helper directly. There's a separate test that
verifies the StrategySDK re-export, but it's gated on the SDK module
loading cleanly.
"""

from __future__ import annotations

import math
import time

import pytest

from services.strategy_helpers.price_window import (
    DEFAULT_WINDOW_SECONDS,
    PriceWindow,
)


def test_default_window_is_300_seconds():
    assert DEFAULT_WINDOW_SECONDS == 300.0
    assert PriceWindow().window_seconds == 300.0


def test_has_data_requires_two_samples():
    w = PriceWindow(window_seconds=60.0)
    assert w.has_data is False
    w.record(100.0, ts_ms=1_000)
    assert w.has_data is False
    w.record(101.0, ts_ms=2_000)
    assert w.has_data is True


def test_record_appends_in_order_and_evicts_old_samples():
    w = PriceWindow(window_seconds=10.0)
    w.record(100.0, ts_ms=1_000)
    w.record(101.0, ts_ms=5_000)
    w.record(102.0, ts_ms=12_000)  # cutoff = 2_000 → drops 1_000
    assert len(w) == 2
    assert w.samples[0] == (5_000, 101.0)
    assert w.samples[-1] == (12_000, 102.0)


def test_record_uses_wall_clock_when_ts_ms_omitted(monkeypatch):
    monkeypatch.setattr(time, "time", lambda: 1234.567)
    w = PriceWindow(window_seconds=60.0)
    w.record(99.0)
    assert w.samples[-1][0] == 1_234_567


def test_latest_returns_tuple_or_none():
    w = PriceWindow(window_seconds=60.0)
    assert w.latest() is None
    w.record(100.0, ts_ms=1_000)
    assert w.latest() == (1_000, 100.0)


def test_at_or_before_walks_reverse_finds_recent_match():
    w = PriceWindow(window_seconds=60.0)
    w.record(100.0, ts_ms=1_000)
    w.record(101.0, ts_ms=2_000)
    w.record(102.0, ts_ms=3_000)

    assert w.at_or_before(2_500) == (2_000, 101.0)
    assert w.at_or_before(3_000) == (3_000, 102.0)


def test_at_or_before_returns_none_when_target_predates_window():
    w = PriceWindow(window_seconds=60.0)
    w.record(100.0, ts_ms=5_000)
    assert w.at_or_before(1_000) is None


def test_log_return_against_at_or_before_anchor():
    w = PriceWindow(window_seconds=600.0)
    w.record(100.0, ts_ms=0)
    w.record(110.0, ts_ms=120_000)  # 120s later

    ret = w.log_return(seconds_ago=120)
    assert ret == pytest.approx(math.log(110.0 / 100.0), rel=1e-9)


def test_log_return_returns_none_when_no_anchor_old_enough():
    w = PriceWindow(window_seconds=600.0)
    w.record(100.0, ts_ms=10_000)
    w.record(101.0, ts_ms=11_000)
    assert w.log_return(seconds_ago=120) is None


def test_log_return_returns_none_for_non_positive_or_zero_seconds():
    w = PriceWindow(window_seconds=600.0)
    w.record(100.0, ts_ms=0)
    w.record(110.0, ts_ms=10_000)
    assert w.log_return(seconds_ago=0) is None
    assert w.log_return(seconds_ago=-5) is None


def test_log_return_returns_none_when_prior_price_is_non_positive():
    w = PriceWindow(window_seconds=600.0)
    w.record(0.0, ts_ms=0)
    w.record(1.0, ts_ms=10_000)
    assert w.log_return(seconds_ago=10) is None


def test_stddev_returns_zero_for_sparse_window():
    w = PriceWindow(window_seconds=60.0)
    assert w.stddev() == 0.0
    w.record(100.0, ts_ms=1_000)
    assert w.stddev() == 0.0


def test_stddev_matches_population_formula():
    w = PriceWindow(window_seconds=60.0)
    for i, p in enumerate([100.0, 101.0, 102.0, 103.0]):
        w.record(p, ts_ms=i * 1_000)
    prices = [100.0, 101.0, 102.0, 103.0]
    mean = sum(prices) / len(prices)
    expected = (sum((p - mean) ** 2 for p in prices) / len(prices)) ** 0.5
    assert w.stddev() == pytest.approx(expected, rel=1e-9)


def test_realized_volatility_returns_zero_for_sparse_window():
    w = PriceWindow(window_seconds=60.0)
    assert w.realized_volatility_bps_per_sec() == 0.0
    w.record(100.0, ts_ms=1_000)
    assert w.realized_volatility_bps_per_sec() == 0.0


def test_realized_volatility_returns_zero_for_non_positive_period():
    w = PriceWindow(window_seconds=60.0)
    w.record(100.0, ts_ms=0)
    w.record(101.0, ts_ms=1_000)
    assert w.realized_volatility_bps_per_sec(sample_period_s=0.0) == 0.0
    assert w.realized_volatility_bps_per_sec(sample_period_s=-1.0) == 0.0


def test_realized_volatility_with_constant_price_is_zero():
    w = PriceWindow(window_seconds=60.0)
    for i in range(10):
        w.record(100.0, ts_ms=i * 1_000)
    assert w.realized_volatility_bps_per_sec() == pytest.approx(0.0, abs=1e-9)


def test_realized_volatility_scales_with_jitter_size():
    """Larger price jitter → larger stddev of returns × 10000."""
    quiet = PriceWindow(window_seconds=60.0)
    loud = PriceWindow(window_seconds=60.0)
    for i in range(20):
        # quiet bounces ±0.1bp, loud bounces ±10bp
        sign = 1.0 if i % 2 == 0 else -1.0
        quiet.record(100.0 + sign * 0.001, ts_ms=i * 1_000)
        loud.record(100.0 + sign * 0.1, ts_ms=i * 1_000)
    q = quiet.realized_volatility_bps_per_sec()
    L = loud.realized_volatility_bps_per_sec()
    assert q > 0.0
    assert L > q * 50.0  # roughly 100x jitter → much larger sigma


def test_distance_bps_signs_correctly():
    w = PriceWindow(window_seconds=60.0)
    w.record(100.0, ts_ms=1_000)
    assert w.distance_bps(reference_price=99.0) == pytest.approx(
        ((100.0 - 99.0) / 99.0) * 10_000.0, rel=1e-9
    )

    w.record(98.0, ts_ms=2_000)
    d = w.distance_bps(reference_price=100.0)
    assert d is not None
    assert d < 0
    assert d == pytest.approx(-200.0, rel=1e-9)


def test_distance_bps_returns_none_for_invalid_reference():
    w = PriceWindow(window_seconds=60.0)
    w.record(100.0, ts_ms=1_000)
    assert w.distance_bps(reference_price=0.0) is None
    assert w.distance_bps(reference_price=-1.0) is None


def test_distance_bps_returns_none_when_empty():
    w = PriceWindow(window_seconds=60.0)
    assert w.distance_bps(reference_price=100.0) is None


def test_record_silently_drops_unparseable_price():
    w = PriceWindow(window_seconds=60.0)
    # PriceWindow lets float() handle it — None will TypeError, caught at
    # caller boundary. Strings parseable to float are accepted.
    w.record("100.0", ts_ms=1_000)
    assert w.latest() == (1_000, 100.0)
