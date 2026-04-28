"""Tests for StrategySDK orderbook methods.

These methods read from the live WS-fed PriceCache via FeedManager. Tests
seed the cache directly with synthetic OrderBook data, then call the SDK
methods to verify they return correctly-shaped results and that
missing-data paths return None instead of raising.

Replaces the previously-broken implementation that imported a
non-existent ``vwap_calculator`` module-level instance and silently
returned None on every call.
"""

from __future__ import annotations

import pytest

from services.optimization.vwap import OrderBookLevel
from services.strategy_sdk import StrategySDK
from services.ws_feeds import FeedManager, get_feed_manager


class _Market:
    """Minimal Market-shaped object — only ``id`` and ``clob_token_ids`` are read."""

    def __init__(self, market_id: str, yes_token: str, no_token: str) -> None:
        self.id = market_id
        self.clob_token_ids = [yes_token, no_token]


@pytest.fixture
def fresh_cache():
    """Reset the FeedManager singleton so each test starts with a clean cache."""
    FeedManager.reset_instance()
    yield get_feed_manager().cache
    FeedManager.reset_instance()


def _seed(cache, token_id, bids, asks):
    bid_levels = [OrderBookLevel(price=p, size=s) for p, s in bids]
    ask_levels = [OrderBookLevel(price=p, size=s) for p, s in asks]
    cache.update(token_id, bid_levels, ask_levels)


def test_get_best_bid_ask_returns_top_of_book(fresh_cache):
    market = _Market("m1", "tok_yes", "tok_no")
    _seed(fresh_cache, "tok_yes", bids=[(0.55, 100), (0.54, 200)], asks=[(0.56, 80), (0.57, 150)])

    assert StrategySDK.get_best_bid_ask(market, "YES") == (0.55, 0.56)


def test_get_best_bid_ask_returns_none_when_token_not_subscribed(fresh_cache):
    market = _Market("m1", "tok_yes_missing", "tok_no_missing")
    assert StrategySDK.get_best_bid_ask(market, "YES") is None


def test_get_best_bid_ask_returns_none_when_market_has_no_token_for_side(fresh_cache):
    class HalfMarket:
        id = "half"
        clob_token_ids = ["tok_yes_only"]
    assert StrategySDK.get_best_bid_ask(HalfMarket(), "NO") is None


def test_get_book_levels_returns_asks_sorted_best_first(fresh_cache):
    market = _Market("m1", "tok_yes", "tok_no")
    _seed(
        fresh_cache,
        "tok_yes",
        bids=[(0.55, 100)],
        asks=[(0.58, 200), (0.56, 100), (0.57, 150)],  # intentionally out of order
    )

    levels = StrategySDK.get_book_levels(market, "YES", max_levels=5)
    assert levels is not None
    assert [lv["price"] for lv in levels] == [0.56, 0.57, 0.58]
    assert all("size" in lv for lv in levels)


def test_get_book_levels_respects_max_levels(fresh_cache):
    market = _Market("m1", "tok_yes", "tok_no")
    _seed(
        fresh_cache,
        "tok_yes",
        bids=[(0.50, 100)],
        asks=[(0.51, 10), (0.52, 20), (0.53, 30), (0.54, 40)],
    )

    levels = StrategySDK.get_book_levels(market, "YES", max_levels=2)
    assert levels is not None
    assert len(levels) == 2
    assert levels[0]["price"] == 0.51


def test_get_book_levels_returns_none_when_book_absent(fresh_cache):
    market = _Market("m1", "tok_yes", "tok_no")
    assert StrategySDK.get_book_levels(market, "YES") is None


def test_get_order_book_depth_partial_fill_within_top_level(fresh_cache):
    market = _Market("m1", "tok_yes", "tok_no")
    _seed(
        fresh_cache,
        "tok_yes",
        bids=[(0.40, 1000)],
        asks=[(0.50, 1000)],
    )

    result = StrategySDK.get_order_book_depth(market, "YES", size_usd=10.0)
    assert result is not None
    assert result["vwap_price"] == pytest.approx(0.50, rel=1e-9)
    assert result["available_liquidity"] > 0
    assert result["fill_probability"] > 0.0
    assert result["is_fresh"] is True
    assert result["staleness_ms"] >= 0.0


def test_get_order_book_depth_returns_none_when_book_absent(fresh_cache):
    market = _Market("m1", "tok_yes", "tok_no")
    assert StrategySDK.get_order_book_depth(market, "YES", 100.0) is None


def test_no_side_uses_second_token_id(fresh_cache):
    market = _Market("m1", "tok_yes", "tok_no")
    _seed(fresh_cache, "tok_no", bids=[(0.45, 100)], asks=[(0.46, 100)])

    assert StrategySDK.get_best_bid_ask(market, "YES") is None
    assert StrategySDK.get_best_bid_ask(market, "NO") == (0.45, 0.46)


def test_dict_market_works_for_token_resolution(fresh_cache):
    """Strategies that work from worker payloads pass dict markets."""
    market = {"id": "m_dict", "clob_token_ids": ["tok_yes", "tok_no"]}
    _seed(fresh_cache, "tok_yes", bids=[(0.55, 100)], asks=[(0.56, 80)])
    assert StrategySDK.get_best_bid_ask(market, "YES") == (0.55, 0.56)


def test_get_book_imbalance_balanced_book_returns_zero(fresh_cache):
    market = _Market("m1", "tok_yes", "tok_no")
    _seed(
        fresh_cache,
        "tok_yes",
        bids=[(0.55, 100), (0.54, 50)],
        asks=[(0.56, 100), (0.57, 50)],
    )
    imb = StrategySDK.get_book_imbalance(market, "YES", levels=5)
    assert imb is not None
    assert abs(imb) < 1e-9


def test_get_book_imbalance_bid_heavy_returns_positive(fresh_cache):
    market = _Market("m1", "tok_yes", "tok_no")
    _seed(
        fresh_cache,
        "tok_yes",
        bids=[(0.55, 300), (0.54, 200)],
        asks=[(0.56, 100), (0.57, 100)],
    )
    imb = StrategySDK.get_book_imbalance(market, "YES", levels=5)
    assert imb is not None
    assert imb == pytest.approx((500 - 200) / (500 + 200), rel=1e-9)


def test_get_book_imbalance_ask_heavy_returns_negative(fresh_cache):
    market = _Market("m1", "tok_yes", "tok_no")
    _seed(
        fresh_cache,
        "tok_yes",
        bids=[(0.55, 50)],
        asks=[(0.56, 500)],
    )
    imb = StrategySDK.get_book_imbalance(market, "YES", levels=5)
    assert imb is not None
    assert imb < 0


def test_get_book_imbalance_returns_none_when_book_absent(fresh_cache):
    market = _Market("m1", "tok_yes", "tok_no")
    assert StrategySDK.get_book_imbalance(market, "YES") is None


def test_get_book_imbalance_levels_param_caps_depth(fresh_cache):
    """Only the top N levels per side should be summed."""
    market = _Market("m1", "tok_yes", "tok_no")
    _seed(
        fresh_cache,
        "tok_yes",
        bids=[(0.55, 100), (0.54, 100), (0.53, 5000)],
        asks=[(0.56, 100), (0.57, 100)],
    )
    assert StrategySDK.get_book_imbalance(market, "YES", levels=2) == pytest.approx(0.0)
    imb_full = StrategySDK.get_book_imbalance(market, "YES", levels=10)
    assert imb_full > 0.9
