"""Tests for the direct Binance WebSocket feed (services/binance_feed.py).

Covers the staleness watchdog and reconnect behavior — see
``BinanceFeed._run_loop`` for the contract.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services import binance_feed as binance_feed_module
from services.binance_feed import BinanceFeed


def _btc_ticker(price: float) -> str:
    return json.dumps(
        {"s": "BTCUSDT", "b": f"{price - 0.5:.2f}", "a": f"{price + 0.5:.2f}"}
    )


def _combined_ticker(symbol_lower: str, price: float) -> str:
    """Combined-stream wrapper: ``{"stream": "...", "data": {...}}``."""
    return json.dumps(
        {
            "stream": f"{symbol_lower}@bookTicker",
            "data": {
                "s": symbol_lower.upper(),
                "b": f"{price - 0.5:.2f}",
                "a": f"{price + 0.5:.2f}",
            },
        }
    )


class _FakeWebSocket:
    """In-memory websocket that yields pre-loaded messages then stalls."""

    def __init__(self, messages: list[str], stall_forever: bool = True) -> None:
        self._messages = list(messages)
        self._stall_forever = stall_forever

    async def recv(self) -> str:
        if self._messages:
            return self._messages.pop(0)
        if self._stall_forever:
            # Sleep long enough for wait_for(timeout) to fire.
            await asyncio.sleep(60)
        raise asyncio.CancelledError


class _ConnectFactory:
    """Yields a fresh _FakeWebSocket per connect call, recording attempts."""

    def __init__(self, sockets: list[_FakeWebSocket]) -> None:
        self._sockets = sockets
        self.connect_count = 0

    def __call__(self, *_args, **_kwargs):
        self.connect_count += 1
        try:
            ws = self._sockets.pop(0)
        except IndexError:
            ws = _FakeWebSocket([], stall_forever=True)

        @asynccontextmanager
        async def _cm():
            yield ws

        return _cm()


@pytest.mark.asyncio
async def test_stale_data_triggers_reconnect(monkeypatch):
    """If no message arrives within the stale-data window, force reconnect."""
    monkeypatch.setattr(binance_feed_module, "RECONNECT_AFTER_STALE_MS", 10)
    monkeypatch.setattr(binance_feed_module, "_stale_data_timeout_s", lambda: 0.1)

    socket_a = _FakeWebSocket([_btc_ticker(70_000.0)], stall_forever=True)
    socket_b = _FakeWebSocket([_btc_ticker(70_001.0)], stall_forever=True)
    factory = _ConnectFactory([socket_a, socket_b])
    monkeypatch.setattr(binance_feed_module.websockets, "connect", factory)

    received: list[tuple[str, float]] = []
    feed = BinanceFeed(ws_url="ws://test")
    feed.on_update(lambda asset, mid, *_: received.append((asset, mid)))

    await feed.start()
    # Wait long enough for: connect → recv message → wait_for(timeout=0.1)
    # → stale break → reconnect → recv message.  Two iterations comfortably
    # in 0.4s.
    await asyncio.sleep(0.5)
    await feed.stop()

    assert factory.connect_count >= 2, (
        f"watchdog should reconnect after stall, got {factory.connect_count} connect(s)"
    )
    assert len(received) >= 2, f"expected ≥2 messages across reconnects, got {received}"
    assert received[0][0] == "BTC"


@pytest.mark.asyncio
async def test_handle_message_updates_last_update_ms(monkeypatch):
    """First-message path should stamp _last_update_ms and fire callback."""
    feed = BinanceFeed(ws_url="ws://test")
    captured: list[tuple[str, float, float, float, int]] = []
    feed.on_update(lambda *args: captured.append(args))

    before_ms = int(time.time() * 1000) - 1
    feed._handle_message(_btc_ticker(72_500.0))

    assert len(captured) == 1
    asset, mid, bid, ask, ts_ms = captured[0]
    assert asset == "BTC"
    assert mid == pytest.approx(72_500.0)
    assert bid == pytest.approx(72_499.5)
    assert ask == pytest.approx(72_500.5)
    assert ts_ms >= before_ms
    assert feed.last_update_ms == ts_ms


def test_combined_stream_payloads_unwrap_correctly():
    """Combined `/stream?streams=` payloads wrap ticker in {stream, data}."""
    feed = BinanceFeed(ws_url="ws://test")
    captured: list[tuple[str, float]] = []
    feed.on_update(lambda asset, mid, *_: captured.append((asset, mid)))

    feed._handle_message(_combined_ticker("btcusdt", 70_000.0))
    feed._handle_message(_combined_ticker("ethusdt", 3_200.0))
    feed._handle_message(_combined_ticker("solusdt", 150.0))
    feed._handle_message(_combined_ticker("xrpusdt", 0.55))

    assert [c[0] for c in captured] == ["BTC", "ETH", "SOL", "XRP"]


def test_per_asset_status_tracks_message_counts():
    """Per-asset diagnostic surfaces would catch a partial subscription."""
    feed = BinanceFeed(ws_url="ws://test")
    feed.on_update(lambda *_: None)

    for _ in range(3):
        feed._handle_message(_combined_ticker("btcusdt", 70_000.0))
    feed._handle_message(_combined_ticker("ethusdt", 3_200.0))

    status = feed.get_per_asset_status()
    assert set(status.keys()) == {"BTC", "ETH"}
    assert status["BTC"]["message_count"] == 3
    assert status["ETH"]["message_count"] == 1
    assert status["BTC"]["last_update_ms"] > 0
    assert status["BTC"]["age_ms"] >= 0


def test_stale_data_timeout_floor(monkeypatch):
    """Floor at 0.5s prevents reconnect-storm if user mis-configures the knob."""

    class _S:
        BINANCE_WS_STALE_DATA_TIMEOUT_SECONDS = 0.05

    monkeypatch.setattr(binance_feed_module, "settings", _S)
    assert binance_feed_module._stale_data_timeout_s() == 0.5

    class _S2:
        BINANCE_WS_STALE_DATA_TIMEOUT_SECONDS = 5.0

    monkeypatch.setattr(binance_feed_module, "settings", _S2)
    assert binance_feed_module._stale_data_timeout_s() == 5.0
