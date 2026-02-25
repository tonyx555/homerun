"""Direct Binance WebSocket price feed for BTC/ETH/SOL/XRP.

Connects to Binance's combined ``bookTicker`` stream to receive best
bid/ask updates with sub-100ms latency.  This is significantly faster
than the RTDS-relayed Binance prices in :mod:`chainlink_feed` (which
pass through Polymarket's relay, adding 500ms-2s).

The feed writes prices into :class:`chainlink_feed.ChainlinkFeed` via
its ``update_from_binance_direct()`` method, tagged with source
``"binance_direct"`` to distinguish from the RTDS relay (``"binance"``).

See: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Callable, Optional

import websockets

from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

RECONNECT_BASE_MS = 500
RECONNECT_MAX_MS = 30_000

_BINANCE_SYMBOL_MAP = {
    "btcusdt": "BTC",
    "ethusdt": "ETH",
    "solusdt": "SOL",
    "xrpusdt": "XRP",
}


class BinanceFeed:
    """Direct WebSocket connection to Binance bookTicker stream.

    Fires ``on_update`` callback with ``(asset, mid, bid, ask, timestamp_ms)``
    on every book update.  Source tag is ``"binance_direct"`` to distinguish
    from the RTDS-relayed ``"binance"`` prices in ChainlinkFeed.

    Usage::

        feed = BinanceFeed()
        feed.on_update(lambda asset, mid, bid, ask, ts: print(asset, mid))
        await feed.start()
        # ...
        await feed.stop()
    """

    def __init__(self, ws_url: Optional[str] = None):
        self._ws_url = ws_url or settings.BINANCE_WS_URL
        self._task: Optional[asyncio.Task] = None
        self._stopped = False
        self._on_update: Optional[Callable] = None
        self._last_update_ms: int = 0

    @property
    def started(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def last_update_ms(self) -> int:
        return self._last_update_ms

    def on_update(self, callback: Callable) -> None:
        """Register a callback for price updates.

        Signature: ``callback(asset: str, mid: float, bid: float, ask: float, timestamp_ms: int)``
        """
        self._on_update = callback

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stopped = False
        self._task = asyncio.create_task(self._run_loop())
        logger.info("BinanceFeed: starting connection to %s", self._ws_url)

    async def stop(self) -> None:
        self._stopped = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("BinanceFeed: stopped")

    async def _run_loop(self) -> None:
        """Reconnecting WebSocket loop with exponential backoff."""
        reconnect_ms = RECONNECT_BASE_MS

        while not self._stopped:
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    reconnect_ms = RECONNECT_BASE_MS
                    logger.info("BinanceFeed: connected to bookTicker stream")

                    async for raw in ws:
                        if self._stopped:
                            break
                        self._handle_message(raw)

            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._stopped:
                    break
                logger.debug("BinanceFeed: connection error: %s", e)
                await asyncio.sleep(reconnect_ms / 1000.0)
                reconnect_ms = min(int(reconnect_ms * 1.5), RECONNECT_MAX_MS)

    def _handle_message(self, raw: str | bytes) -> None:
        """Parse a Binance bookTicker message and fire callback."""
        msg_str = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
        if not msg_str.strip():
            return

        try:
            data = json.loads(msg_str)
        except json.JSONDecodeError:
            return

        # Combined stream format wraps ticker in {"stream": "...", "data": {...}}
        if isinstance(data, dict) and "data" in data:
            ticker = data["data"]
        else:
            ticker = data

        if not isinstance(ticker, dict):
            return

        symbol = str(ticker.get("s") or "").lower()
        asset = _BINANCE_SYMBOL_MAP.get(symbol)
        if not asset:
            return

        try:
            bid = float(ticker["b"])
            ask = float(ticker["a"])
        except (KeyError, TypeError, ValueError):
            return
        if bid <= 0 or ask <= 0:
            return

        mid = (bid + ask) / 2.0
        now_ms = int(time.time() * 1000)
        self._last_update_ms = now_ms

        if self._on_update:
            try:
                self._on_update(asset, mid, bid, ask, now_ms)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_instance: Optional[BinanceFeed] = None


def get_binance_feed() -> BinanceFeed:
    global _instance
    if _instance is None:
        _instance = BinanceFeed()
    return _instance
