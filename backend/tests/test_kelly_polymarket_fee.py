"""Locks down the docs-accurate Polymarket taker-fee curve in utils.kelly.

Prior to fixing this, ``polymarket_taker_fee`` returned the linear
``p*(1-p)*0.0625`` shape, which over-estimated fees at the tails by 4-20×
and caused fee-aware strategies to refuse profitable trades. The function
now follows the published quadratic schedule:

    fee_per_share = p * 0.25 * (p * (1 - p))**2

Maximum fee is ~1.56% of price at p=0.50, falling to ~0.20% at p=0.10/0.90.
"""

from __future__ import annotations

import math

import pytest

from utils.kelly import polymarket_taker_fee, polymarket_taker_fee_pct


@pytest.mark.parametrize(
    "price, expected_per_share, expected_pct",
    [
        (0.10, 0.000203, 0.203),
        (0.30, 0.003308, 1.102),
        (0.50, 0.007812, 1.562),
        (0.70, 0.007718, 1.103),
        (0.90, 0.001822, 0.203),
    ],
)
def test_curve_matches_polymarket_docs(price, expected_per_share, expected_pct):
    fee = polymarket_taker_fee(price)
    pct_decimal = polymarket_taker_fee_pct(price)
    assert math.isclose(fee, expected_per_share, abs_tol=1e-5), fee
    # ``polymarket_taker_fee_pct`` returns a fraction (0–0.0156); compare
    # against the percentage form for readability.
    assert math.isclose(pct_decimal * 100.0, expected_pct, abs_tol=0.01), pct_decimal


def test_max_fee_is_156_bps_at_half_price():
    assert math.isclose(polymarket_taker_fee_pct(0.5) * 100.0, 1.5625, abs_tol=1e-3)


def test_legacy_fee_rate_argument_is_ignored():
    # Old callers may still pass fee_rate; the curve is fixed regardless.
    assert polymarket_taker_fee(0.5, fee_rate=0.1) == polymarket_taker_fee(0.5)
    assert polymarket_taker_fee(0.5, fee_rate=None) == polymarket_taker_fee(0.5)


def test_fee_pct_zero_at_zero_price():
    assert polymarket_taker_fee_pct(0.0) == 0.0
    assert polymarket_taker_fee_pct(-0.5) == 0.0


def test_fee_clamps_outside_unit_interval():
    # >1 and <0 are clamped — strategies can pass slightly-out-of-range
    # values from arithmetic without blowing up.
    assert polymarket_taker_fee(1.5) == polymarket_taker_fee(1.0)
    assert polymarket_taker_fee(-0.2) == polymarket_taker_fee(0.0)
