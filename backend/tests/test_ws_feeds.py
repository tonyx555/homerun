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
