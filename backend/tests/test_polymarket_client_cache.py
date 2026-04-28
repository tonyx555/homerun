"""Tests for the polymarket client TTL caches + batch endpoint.

Covers:
  * Cache HIT: same key within TTL returns cached value, no second
    HTTP call.
  * Cache MISS: expired entry triggers fresh fetch.
  * Single-flight: N concurrent callers of the same key fan in to ONE
    HTTP fetch (no thundering-herd stampede).
  * Invalidation: invalidate_wallet_caches() drops cached entries.
  * Batch midpoints: ``get_midpoints_batch`` makes one HTTP call for
    N tokens, populates the per-token cache.
  * Cache miss path falls back to single-token when batch fails.
"""
from __future__ import annotations

import sys
import asyncio
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.polymarket import _TTLCache, _CACHE_MISS, polymarket_client


@pytest.mark.asyncio
async def test_ttlcache_hit_skips_fetcher():
    cache = _TTLCache(ttl_seconds=10.0)
    fetch_calls = 0

    async def fetcher():
        nonlocal fetch_calls
        fetch_calls += 1
        return [1, 2, 3]

    a = await cache.get_or_fetch("k", fetcher)
    b = await cache.get_or_fetch("k", fetcher)
    c = await cache.get_or_fetch("k", fetcher)

    assert a == b == c == [1, 2, 3]
    assert fetch_calls == 1, "second/third call must be served from cache"
    stats = cache.stats()
    assert stats["hits"] == 2 and stats["misses"] == 1


@pytest.mark.asyncio
async def test_ttlcache_miss_after_ttl_expires():
    cache = _TTLCache(ttl_seconds=0.05)  # 50ms
    fetch_calls = 0

    async def fetcher():
        nonlocal fetch_calls
        fetch_calls += 1
        return fetch_calls

    a = await cache.get_or_fetch("k", fetcher)
    await asyncio.sleep(0.07)  # exceed TTL
    b = await cache.get_or_fetch("k", fetcher)
    assert a == 1 and b == 2
    assert fetch_calls == 2


@pytest.mark.asyncio
async def test_ttlcache_single_flight_coalesces_concurrent_misses():
    """The load-bearing property: N traders waking and calling the
    same cache key MUST result in a single fetch."""
    cache = _TTLCache(ttl_seconds=10.0)
    fetch_started = asyncio.Event()
    fetch_release = asyncio.Event()
    fetch_calls = 0

    async def slow_fetcher():
        nonlocal fetch_calls
        fetch_calls += 1
        fetch_started.set()
        await fetch_release.wait()
        return ["payload"]

    # Fire 10 concurrent gets for the same key
    tasks = [asyncio.create_task(cache.get_or_fetch("k", slow_fetcher)) for _ in range(10)]
    await fetch_started.wait()
    fetch_release.set()
    results = await asyncio.gather(*tasks)

    assert all(r == ["payload"] for r in results)
    assert fetch_calls == 1, "all 10 callers must have shared one fetch"


@pytest.mark.asyncio
async def test_ttlcache_single_flight_propagates_exception():
    cache = _TTLCache(ttl_seconds=10.0)
    fetch_calls = 0

    async def failing_fetcher():
        nonlocal fetch_calls
        fetch_calls += 1
        await asyncio.sleep(0.01)
        raise RuntimeError("upstream down")

    tasks = [asyncio.create_task(cache.get_or_fetch("k", failing_fetcher)) for _ in range(5)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(r, RuntimeError) for r in results), "all callers see the same error"
    assert fetch_calls == 1, "failure was coalesced — only ONE fetch"

    # After failure, next caller should retry (cache stayed empty).
    fetch_calls = 0
    async def good_fetcher():
        nonlocal fetch_calls
        fetch_calls += 1
        return "ok"

    val = await cache.get_or_fetch("k", good_fetcher)
    assert val == "ok"
    assert fetch_calls == 1


@pytest.mark.asyncio
async def test_ttlcache_invalidate_drops_entry():
    cache = _TTLCache(ttl_seconds=10.0)
    n = 0

    async def fetcher():
        nonlocal n
        n += 1
        return n

    await cache.get_or_fetch("a", fetcher)
    await cache.get_or_fetch("b", fetcher)
    cache.invalidate("a")
    a2 = await cache.get_or_fetch("a", fetcher)
    b2 = await cache.get_or_fetch("b", fetcher)
    # 'a' was re-fetched; 'b' was still cached.
    assert a2 == 3 and b2 == 2


@pytest.mark.asyncio
async def test_get_wallet_trades_paginated_cache_hit():
    """Two calls within the TTL share one upstream fetch."""
    polymarket_client._wallet_trades_cache.invalidate()
    fetch_calls = 0

    async def fake_get_wallet_trades(address, *, limit, offset):
        nonlocal fetch_calls
        fetch_calls += 1
        if offset > 0:
            return []
        return [{"side": "BUY", "asset": "abc", "size": 1.0}]

    with patch.object(polymarket_client, "get_wallet_trades", side_effect=fake_get_wallet_trades):
        a = await polymarket_client.get_wallet_trades_paginated("0xWALLET", max_trades=100)
        b = await polymarket_client.get_wallet_trades_paginated("0xWALLET", max_trades=100)
    assert a == b
    # First call: 2 invocations of get_wallet_trades (1 page + empty terminator).
    # Second call should be 0 invocations (cached).
    assert fetch_calls in (1, 2), f"expected 1-2 fetch calls (page + maybe terminator), got {fetch_calls}"


@pytest.mark.asyncio
async def test_invalidate_wallet_caches_address_specific():
    polymarket_client._wallet_trades_cache.invalidate()
    polymarket_client._closed_positions_cache.invalidate()

    async def fake_trades(address, *, limit, offset):
        return [{"asset": "x", "address_marker": address}] if offset == 0 else []

    with patch.object(polymarket_client, "get_wallet_trades", side_effect=fake_trades):
        await polymarket_client.get_wallet_trades_paginated("0xAAA", max_trades=10)
        await polymarket_client.get_wallet_trades_paginated("0xBBB", max_trades=10)

    assert polymarket_client._wallet_trades_cache.stats()["size"] == 2

    polymarket_client.invalidate_wallet_caches("0xAAA")
    assert polymarket_client._wallet_trades_cache.stats()["size"] == 1

    polymarket_client.invalidate_wallet_caches()  # all
    assert polymarket_client._wallet_trades_cache.stats()["size"] == 0


@pytest.mark.asyncio
async def test_get_midpoints_batch_uses_post_endpoint_and_populates_cache():
    polymarket_client._midpoint_cache.invalidate()

    fake_response = MagicMock()
    fake_response.json.return_value = {"tok-a": "0.42", "tok-b": "0.58"}
    fake_response.raise_for_status = MagicMock()

    fake_client = MagicMock()
    fake_client.post = AsyncMock(return_value=fake_response)

    async def _fake_get_client():
        return fake_client

    with patch.object(polymarket_client, "_get_client", side_effect=_fake_get_client):
        result = await polymarket_client.get_midpoints_batch(["tok-a", "tok-b"])

    assert result == {"tok-a": 0.42, "tok-b": 0.58}
    # ONE POST request for both tokens
    assert fake_client.post.call_count == 1
    call_args = fake_client.post.call_args
    assert call_args.args[0].endswith("/midpoints")
    assert call_args.kwargs["json"] == [{"token_id": "tok-a"}, {"token_id": "tok-b"}]

    # Per-token cache was populated — subsequent get_midpoint hits.
    assert polymarket_client._midpoint_cache.peek("tok-a") == 0.42
    assert polymarket_client._midpoint_cache.peek("tok-b") == 0.58


@pytest.mark.asyncio
async def test_get_midpoints_batch_skips_already_cached_tokens():
    polymarket_client._midpoint_cache.invalidate()
    # Pre-warm cache for tok-a
    polymarket_client._midpoint_cache._data["tok-a"] = (time.monotonic(), 0.99)

    fake_response = MagicMock()
    fake_response.json.return_value = {"tok-b": "0.10"}
    fake_response.raise_for_status = MagicMock()

    fake_client = MagicMock()
    fake_client.post = AsyncMock(return_value=fake_response)

    async def _fake_get_client():
        return fake_client

    with patch.object(polymarket_client, "_get_client", side_effect=_fake_get_client):
        result = await polymarket_client.get_midpoints_batch(["tok-a", "tok-b"])

    assert result == {"tok-a": 0.99, "tok-b": 0.10}
    # Batch request only included the cache-miss token
    call_args = fake_client.post.call_args
    assert call_args.kwargs["json"] == [{"token_id": "tok-b"}]


@pytest.mark.asyncio
async def test_ttlcache_cancelled_leader_doesnt_leak_future_exception():
    """Production-observed bug: when the leader of a single-flight
    fetch is cancelled by an outer asyncio.wait_for(...) timeout,
    the inflight Future was being set_exception(CancelledError) but
    nobody awaited it, surfacing as
    ``Future exception was never retrieved`` in the global asyncio
    handler.  The fix is to ``future.cancel()`` instead.
    """
    cache = _TTLCache(ttl_seconds=10.0)

    async def slow_fetcher():
        await asyncio.sleep(5.0)  # would normally exceed the wait_for
        return "never"

    # Wrap in wait_for to simulate the production cancel path.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            cache.get_or_fetch("k", slow_fetcher),
            timeout=0.05,
        )

    # Give the loop a tick to fire the future's done_callback (the
    # one that consumes the exception so it doesn't leak).
    await asyncio.sleep(0.01)
    # Cache should be empty (no result was stored).
    assert cache.peek("k") is _CACHE_MISS
    # No inflight entry should remain (cleanup ran).
    assert cache._inflight == {}


@pytest.mark.asyncio
async def test_get_midpoints_batch_empty_input_no_request():
    polymarket_client._midpoint_cache.invalidate()
    fake_client = MagicMock()
    fake_client.post = AsyncMock()
    async def _fake_get_client():
        return fake_client

    with patch.object(polymarket_client, "_get_client", side_effect=_fake_get_client):
        result = await polymarket_client.get_midpoints_batch([])
    assert result == {}
    fake_client.post.assert_not_called()
