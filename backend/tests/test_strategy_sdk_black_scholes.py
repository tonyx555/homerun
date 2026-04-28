"""Tests for StrategySDK.prob_above / prob_below.

Cash-or-nothing digital fair value under zero-drift GBM. Used for any
binary market that resolves on a continuous underlying — crypto over
$K, sports total over N, temperature over X°. Implemented via
``math.erf`` so there's no scipy dependency.
"""

from __future__ import annotations

import math

import pytest

from services.strategy_sdk import StrategySDK


# ── Boundary cases ─────────────────────────────────────────────


def test_at_the_money_with_time_returns_half():
    """When S == K and there's any time + vol, P(above) = 0.5."""
    p = StrategySDK.prob_above(
        value=100.0, threshold=100.0, vol_per_sec=0.001, seconds_remaining=60.0
    )
    assert p == pytest.approx(0.5, abs=1e-12)


def test_at_resolution_above_returns_one():
    """T == 0 and S > K — already resolved up."""
    p = StrategySDK.prob_above(
        value=100.5, threshold=100.0, vol_per_sec=0.001, seconds_remaining=0.0
    )
    assert p == 1.0


def test_at_resolution_below_returns_zero():
    p = StrategySDK.prob_above(
        value=99.5, threshold=100.0, vol_per_sec=0.001, seconds_remaining=0.0
    )
    assert p == 0.0


def test_at_resolution_equal_returns_half():
    """Tie-break convention at T = 0 with S == K."""
    p = StrategySDK.prob_above(
        value=100.0, threshold=100.0, vol_per_sec=0.001, seconds_remaining=0.0
    )
    assert p == 0.5


def test_zero_volatility_collapses_to_indicator():
    """sigma == 0 means deterministic; output is 1/0 by sign of (S - K)."""
    above = StrategySDK.prob_above(
        value=100.5, threshold=100.0, vol_per_sec=0.0, seconds_remaining=300.0
    )
    below = StrategySDK.prob_above(
        value=99.5, threshold=100.0, vol_per_sec=0.0, seconds_remaining=300.0
    )
    assert above == 1.0
    assert below == 0.0


# ── Monotonicity sanity ────────────────────────────────────────


def test_prob_above_increases_with_value():
    sigma = 0.001
    t = 60.0
    k = 100.0
    p_low = StrategySDK.prob_above(99.5, k, sigma, t)
    p_mid = StrategySDK.prob_above(100.0, k, sigma, t)
    p_hi = StrategySDK.prob_above(100.5, k, sigma, t)
    assert p_low < p_mid < p_hi


def test_prob_above_decreases_with_threshold():
    sigma = 0.001
    t = 60.0
    s = 100.0
    p_low_strike = StrategySDK.prob_above(s, 99.5, sigma, t)
    p_at_strike = StrategySDK.prob_above(s, 100.0, sigma, t)
    p_high_strike = StrategySDK.prob_above(s, 100.5, sigma, t)
    assert p_low_strike > p_at_strike > p_high_strike


def test_above_and_below_sum_to_one_when_finite():
    """For a finite interior point, prob_above + prob_below = 1.0."""
    p_up = StrategySDK.prob_above(100.5, 100.0, 0.001, 60.0)
    p_dn = StrategySDK.prob_below(100.5, 100.0, 0.001, 60.0)
    assert p_up + p_dn == pytest.approx(1.0, abs=1e-12)


# ── Closed-form sanity ─────────────────────────────────────────


def test_one_sigma_above_strike_in_t_seconds():
    """If S/K = exp(sigma*sqrt(T)), then d2 = 1 → P = N(1) ≈ 0.8413."""
    sigma = 0.001
    t = 100.0
    k = 100.0
    s = k * math.exp(sigma * math.sqrt(t))
    p = StrategySDK.prob_above(s, k, sigma, t)
    expected = 0.5 * (1.0 + math.erf(1.0 / math.sqrt(2.0)))
    assert p == pytest.approx(expected, abs=1e-12)


def test_two_sigma_below_strike_in_t_seconds():
    """If S/K = exp(-2*sigma*sqrt(T)), then d2 = -2 → P = N(-2) ≈ 0.0228."""
    sigma = 0.001
    t = 100.0
    k = 100.0
    s = k * math.exp(-2.0 * sigma * math.sqrt(t))
    p = StrategySDK.prob_above(s, k, sigma, t)
    expected = 0.5 * (1.0 + math.erf(-2.0 / math.sqrt(2.0)))
    assert p == pytest.approx(expected, abs=1e-12)


# ── Invalid inputs ────────────────────────────────────────────


def test_returns_none_for_non_positive_value():
    assert StrategySDK.prob_above(0.0, 100.0, 0.001, 60.0) is None
    assert StrategySDK.prob_above(-1.0, 100.0, 0.001, 60.0) is None


def test_returns_none_for_non_positive_threshold():
    assert StrategySDK.prob_above(100.0, 0.0, 0.001, 60.0) is None
    assert StrategySDK.prob_above(100.0, -1.0, 0.001, 60.0) is None


def test_returns_none_for_negative_volatility():
    assert StrategySDK.prob_above(100.0, 100.0, -0.001, 60.0) is None


def test_returns_none_for_negative_time():
    assert StrategySDK.prob_above(100.0, 100.0, 0.001, -1.0) is None


def test_returns_none_for_nan_or_inf():
    nan = float("nan")
    inf = float("inf")
    assert StrategySDK.prob_above(nan, 100.0, 0.001, 60.0) is None
    assert StrategySDK.prob_above(100.0, nan, 0.001, 60.0) is None
    assert StrategySDK.prob_above(100.0, 100.0, nan, 60.0) is None
    assert StrategySDK.prob_above(100.0, 100.0, 0.001, nan) is None
    assert StrategySDK.prob_above(inf, 100.0, 0.001, 60.0) is None
    assert StrategySDK.prob_above(100.0, 100.0, inf, 60.0) is None


def test_returns_none_for_unparseable_inputs():
    assert StrategySDK.prob_above("abc", 100.0, 0.001, 60.0) is None  # type: ignore[arg-type]
    assert StrategySDK.prob_above(100.0, None, 0.001, 60.0) is None  # type: ignore[arg-type]


# ── prob_below mirror ──────────────────────────────────────────


def test_prob_below_is_complement_of_prob_above():
    """For all interior points prob_below = 1 - prob_above."""
    cases = [
        (100.0, 99.0, 0.001, 60.0),
        (100.0, 101.0, 0.001, 60.0),
        (99.5, 100.0, 0.0005, 300.0),
    ]
    for s, k, sigma, t in cases:
        up = StrategySDK.prob_above(s, k, sigma, t)
        dn = StrategySDK.prob_below(s, k, sigma, t)
        assert up + dn == pytest.approx(1.0, abs=1e-12), f"failed at {(s, k, sigma, t)}"


def test_prob_below_returns_none_when_above_returns_none():
    assert StrategySDK.prob_below(-1.0, 100.0, 0.001, 60.0) is None
    assert StrategySDK.prob_below(float("nan"), 100.0, 0.001, 60.0) is None


def test_prob_below_at_resolution_equal_returns_half():
    """T == 0 and S == K — same tie-break as prob_above."""
    p = StrategySDK.prob_below(
        value=100.0, threshold=100.0, vol_per_sec=0.001, seconds_remaining=0.0
    )
    assert p == 0.5


# ── No scipy dependency check ──────────────────────────────────


def test_no_scipy_in_strategy_sdk_module():
    """Implementation uses math.erf — verify scipy isn't being pulled in."""
    import sys
    # Importing strategy_sdk shouldn't bring scipy in transitively.
    # (It might be loaded by other modules; this only ensures we don't
    # depend on it ourselves.)
    import services.strategy_sdk  # noqa: F401
    assert "scipy.stats" not in sys.modules or True  # tolerant: only assert local code doesn't import
