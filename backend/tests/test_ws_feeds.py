import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services import ws_feeds


def test_polymarket_book_update_latency_uses_epoch_time_and_clamps_negative():
    cache = ws_feeds.PriceCache()
    feed = ws_feeds.PolymarketWSFeed(cache)

    feed._apply_book_update(
        {
            "asset_id": "asset-1",
            "bids": [{"price": 0.44, "size": 10}],
            "asks": [{"price": 0.56, "size": 10}],
            "timestamp": 1_700_000_000.250,
        },
        1_700_000_000.500,
    )
    assert feed.stats.last_latency_ms == pytest.approx(250.0)

    feed._apply_book_update(
        {
            "asset_id": "asset-1",
            "bids": [{"price": 0.45, "size": 10}],
            "asks": [{"price": 0.55, "size": 10}],
            "timestamp": 1_700_000_001.500,
        },
        1_700_000_001.000,
    )
    assert feed.stats.last_latency_ms == 0.0


def test_safe_binary_mid_rejects_degenerate_books():
    # Healthy two-sided book in-range
    assert ws_feeds._safe_binary_mid(0.44, 0.56) == pytest.approx(0.50)
    # >50¢ spread on a 0-1 contract → no real market
    assert ws_feeds._safe_binary_mid(0.10, 0.80) is None
    # Both sides at extreme edge → degenerate print
    assert ws_feeds._safe_binary_mid(0.001, 0.998) is None
    # One-sided fallback when the surviving side is in the non-extreme
    # band (0.10–0.90).  Outside that band we refuse — see below.
    assert ws_feeds._safe_binary_mid(0.0, 0.42) == pytest.approx(0.42)
    assert ws_feeds._safe_binary_mid(0.42, 0.0) == pytest.approx(0.42)
    # Empty book
    assert ws_feeds._safe_binary_mid(0.0, 0.0) is None


def test_safe_binary_mid_refuses_extreme_one_sided_quote():
    """Regression test for the 2026-04-28 incident.

    On the F1 Ocon market, the YES token's orderbook collapsed to a
    single ask near 0.99 while the real midpoint (per Polymarket's
    UI, last trade, and outcome_prices) was ~0.495.  The previous
    implementation fell through to ``return best_ask`` for any
    one-sided in-range quote, producing a phantom mid of 0.99 that
    surfaced in the UI as a +1314% unrealized P&L.  Take-profit
    would have triggered immediately had the SELL retry path been
    healthy at the time.

    A lone quote near 0.99 (or near 0.01) is overwhelmingly likely
    to be a stale upside limit / thin liquidity provider quote
    rather than a real market signal.  Refusing to derive a mid
    from it forces upstream callers to either skip the mark update
    or use a different source (last trade, cached outcome_prices),
    which is the correct institutional behaviour."""
    # Lone ask near the upside extreme — must NOT be returned as mid.
    assert ws_feeds._safe_binary_mid(0.0, 0.99) is None
    assert ws_feeds._safe_binary_mid(0.0, 0.95) is None
    # Lone bid near the downside extreme — same refusal.
    assert ws_feeds._safe_binary_mid(0.04, 0.0) is None
    assert ws_feeds._safe_binary_mid(0.08, 0.0) is None
    # The boundaries — exactly at the cutoff is refused; just inside
    # is accepted.
    assert ws_feeds._safe_binary_mid(0.0, 0.10) is None
    assert ws_feeds._safe_binary_mid(0.0, 0.11) == pytest.approx(0.11)
    assert ws_feeds._safe_binary_mid(0.0, 0.90) is None
    assert ws_feeds._safe_binary_mid(0.0, 0.89) == pytest.approx(0.89)
    # Two-sided books are unaffected by the new one-sided guard; the
    # existing spread check still gates them.
    assert ws_feeds._safe_binary_mid(0.49, 0.50) == pytest.approx(0.495)
    # Spread > 0.50 is rejected (existing behaviour).
    assert ws_feeds._safe_binary_mid(0.40, 0.99) is None  # spread = 0.59


def test_price_cache_skips_callbacks_on_degenerate_mid():
    cache = ws_feeds.PriceCache()
    update_calls: list = []
    change_calls: list = []
    cache.add_on_update_callback(lambda *a: update_calls.append(a))
    cache.add_on_change_callback(lambda *a: change_calls.append(a))

    OBL = ws_feeds.OrderBookLevel
    # Establish a healthy mid first.
    cache.update("tok", [OBL(price=0.44, size=10)], [OBL(price=0.56, size=10)])
    assert len(update_calls) == 1
    assert cache.get_mid_price("tok") == pytest.approx(0.50)

    # Degenerate update: spread > 50¢. Cache is replaced (raw bid/ask
    # preserved) but no callback fires and the query method returns None.
    update_calls.clear()
    change_calls.clear()
    cache.update("tok", [OBL(price=0.10, size=1)], [OBL(price=0.80, size=1)])
    assert update_calls == []
    assert change_calls == []
    assert cache.get_mid_price("tok") is None
    bid_ask = cache.get_best_bid_ask("tok")
    assert bid_ask == (0.10, 0.80)


@pytest.mark.asyncio
async def test_feed_manager_start_is_idempotent_under_concurrency(monkeypatch):
    manager = ws_feeds.FeedManager()
    manager._polymarket_feed.start = AsyncMock(return_value=None)
    manager._kalshi_feed.start = AsyncMock(return_value=None)
    marker = object()
    monkeypatch.setattr(
        "services.position_mark_state.get_position_mark_state",
        lambda: SimpleNamespace(on_price_update=marker, _on_marks_changed=None),
    )

    await asyncio.gather(manager.start(), manager.start(), manager.start())

    manager._polymarket_feed.start.assert_awaited_once()
    manager._kalshi_feed.start.assert_awaited_once()
    assert manager._cache._on_update_callbacks.count(marker) == 1
