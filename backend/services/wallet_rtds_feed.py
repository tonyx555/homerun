"""RTDS-based wallet trade fast path.

Subscribes to Polymarket's Real-Time Data Socket activity firehose and
emits :class:`~services.wallet_ws_monitor.WalletTradeEvent` instances
**ahead of block confirmation** for any wallet currently tracked by
:class:`~services.wallet_ws_monitor.WalletWebSocketMonitor`.

Why this exists
---------------

The on-chain RPC path (``wallet_ws_monitor``) is the source of truth, but
it can only see a trade after the block confirms — typically 1–3 seconds
of latency, plus whatever RPC backoff is in play. The Polymarket activity
firehose surfaces the same trade as soon as the matching engine fills it,
which is consistently faster.

This module does NOT replace ``wallet_ws_monitor``. It produces
**preliminary** events (``confirmed=False``) that the frontend should
render as "pending"; the canonical, persisted record still comes from the
on-chain path. Consumers should dedupe by ``tx_hash`` so each trade only
shows up once.

Operational notes
-----------------

- Server-side filters on the activity topic are unreliable
  (https://github.com/Polymarket/real-time-data-client/issues/34) — we
  subscribe unfiltered and discard everything except tracked wallets on
  the client.
- Trade IDs use a deterministic key (``asset|timestamp|tx_hash`` with
  field-fallback when ``tx_hash`` is empty) so reconnect replays don't
  produce duplicate events.
- Ping cadence is 30s to match the upstream keep-alive. Faster pings just
  burn outbound bandwidth without reducing disconnect rate.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Callable, Iterable, Optional

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore[assignment]

from services.wallet_ws_monitor import WalletTradeEvent
from utils.logger import get_logger

logger = get_logger("wallet_rtds_feed")


RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
ACTIVITY_TOPIC = "activity"
ACTIVITY_TYPE_TRADES = "trades"

# Match upstream keep-alive. The reference UpDownWalletMonitor commit
# notes 5s pings produced ~12x outbound traffic for no benefit.
PING_INTERVAL_SECONDS = 30.0
RECONNECT_BASE_SECONDS = 1.0
RECONNECT_MAX_SECONDS = 30.0


def _coerce_side(raw: object) -> str:
    if isinstance(raw, str):
        normalized = raw.strip().upper()
        if normalized in ("BUY", "SELL"):
            return normalized
    return "BUY"


def _coerce_float(raw: object) -> float:
    try:
        return float(raw) if raw is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _coerce_int(raw: object) -> int:
    try:
        return int(raw) if raw is not None else 0
    except (TypeError, ValueError):
        return 0


class WalletRTDSFeed:
    """WebSocket client for Polymarket activity/trades, filtered locally.

    Wire after constructing your :class:`WalletWebSocketMonitor`::

        rtds = WalletRTDSFeed(
            get_tracked_wallets=lambda: monitor.tracked_wallets(),
        )
        rtds.add_callback(monitor._emit_callbacks)  # forward to same subscribers
        await rtds.start()
    """

    def __init__(
        self,
        get_tracked_wallets: Callable[[], Iterable[str]],
        ws_url: str = RTDS_WS_URL,
    ) -> None:
        self._get_tracked_wallets = get_tracked_wallets
        self._ws_url = ws_url
        self._task: Optional[asyncio.Task] = None
        self._stopped = False
        self._callbacks: list[Callable[[WalletTradeEvent], object]] = []
        self._seen_ids: dict[str, float] = {}
        self._seen_ids_max = 4096
        self._stats = {
            "events_emitted": 0,
            "events_filtered_no_match": 0,
            "events_filtered_dedup": 0,
            "ws_reconnects": 0,
        }

    @property
    def started(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    def add_callback(self, callback: Callable[[WalletTradeEvent], object]) -> None:
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def remove_callback(self, callback: Callable[[WalletTradeEvent], object]) -> None:
        self._callbacks = [cb for cb in self._callbacks if cb != callback]

    async def start(self) -> None:
        if websockets is None:
            logger.warning(
                "WalletRTDSFeed: 'websockets' not installed, RTDS fast path disabled"
            )
            return
        if self._task and not self._task.done():
            return
        self._stopped = False
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"WalletRTDSFeed: starting connection to {self._ws_url}")

    async def stop(self) -> None:
        self._stopped = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _run_loop(self) -> None:
        reconnect = RECONNECT_BASE_SECONDS
        while not self._stopped:
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=None,  # we send manual pings on PING_INTERVAL_SECONDS
                    close_timeout=5,
                ) as ws:
                    reconnect = RECONNECT_BASE_SECONDS
                    sub_msg = json.dumps(
                        {
                            "action": "subscribe",
                            "subscriptions": [
                                {"topic": ACTIVITY_TOPIC, "type": ACTIVITY_TYPE_TRADES}
                            ],
                        }
                    )
                    await ws.send(sub_msg)
                    logger.info(
                        "WalletRTDSFeed: subscribed to activity/trades firehose"
                    )

                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        async for raw in ws:
                            if self._stopped:
                                break
                            await self._handle_message(raw)
                    finally:
                        ping_task.cancel()
                        with self._suppress_cancelled():
                            await ping_task
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._stopped:
                    break
                self._stats["ws_reconnects"] += 1
                logger.debug(f"WalletRTDSFeed: connection error: {exc}")
                await asyncio.sleep(reconnect)
                reconnect = min(reconnect * 1.5, RECONNECT_MAX_SECONDS)

    @staticmethod
    def _suppress_cancelled():
        # Local context-manager helper avoids importing contextlib.suppress
        # at module top in a hot path file.
        from contextlib import suppress
        return suppress(asyncio.CancelledError)

    async def _ping_loop(self, ws) -> None:
        try:
            while not self._stopped:
                await asyncio.sleep(PING_INTERVAL_SECONDS)
                try:
                    await ws.send("ping")
                except Exception:
                    return
        except asyncio.CancelledError:
            return

    async def _handle_message(self, raw) -> None:
        text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
        text_stripped = text.strip()
        if not text_stripped or text_stripped == "pong":
            return
        try:
            msg = json.loads(text_stripped)
        except json.JSONDecodeError:
            return

        if msg.get("topic") != ACTIVITY_TOPIC or msg.get("type") != ACTIVITY_TYPE_TRADES:
            return

        payload = msg.get("payload") or {}
        wallet = str(payload.get("proxyWallet") or "").lower()
        if not wallet:
            return

        # Filter against currently tracked wallets. Re-read each message
        # so wallets added/removed mid-flight take effect immediately.
        tracked = {w.lower() for w in (self._get_tracked_wallets() or [])}
        if wallet not in tracked:
            self._stats["events_filtered_no_match"] += 1
            return

        token_id = payload.get("asset")
        side = payload.get("side")
        price_raw = payload.get("price")
        if not token_id or side is None or price_raw is None:
            return

        # Deterministic ID: (token_id, timestamp, tx_hash) with field-
        # fallback when tx_hash is missing. Random IDs broke client
        # dedup on the upstream — see the JS reference's bug-fix comment.
        ts_seconds = _coerce_int(payload.get("timestamp")) or int(time.time())
        tx_hash = str(payload.get("transactionHash") or "")
        if tx_hash:
            event_key = f"{token_id}|{ts_seconds}|{tx_hash}"
        else:
            shares = _coerce_float(payload.get("size"))
            event_key = (
                f"{token_id}|{ts_seconds}|{wallet}|{side}|"
                f"{price_raw}|{shares}"
            )
        if event_key in self._seen_ids:
            self._stats["events_filtered_dedup"] += 1
            return
        self._record_seen(event_key)

        size = _coerce_float(payload.get("size"))
        price = _coerce_float(price_raw)
        block_dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
        detected_at = datetime.now(timezone.utc)
        latency_ms = max(
            (detected_at - block_dt).total_seconds() * 1000.0,
            0.0,
        )

        event = WalletTradeEvent(
            wallet_address=wallet,
            token_id=str(token_id),
            side=_coerce_side(side),
            size=size,
            price=price,
            tx_hash=tx_hash,
            order_hash="",
            log_index=0,
            block_number=0,
            timestamp=block_dt,
            detected_at=detected_at,
            latency_ms=latency_ms,
            confirmed=False,
            source="rtds",
        )

        self._stats["events_emitted"] += 1
        await self._emit(event)

    def _record_seen(self, key: str) -> None:
        # Bound the dedup cache so a long-running connection doesn't grow
        # the dict without limit. Drop the oldest 25% when full — cheap
        # since this is a hot path.
        if len(self._seen_ids) >= self._seen_ids_max:
            cutoff = sorted(self._seen_ids.values())[int(self._seen_ids_max * 0.25)]
            self._seen_ids = {
                k: v for k, v in self._seen_ids.items() if v > cutoff
            }
        self._seen_ids[key] = time.time()

    async def _emit(self, event: WalletTradeEvent) -> None:
        for cb in list(self._callbacks):
            try:
                result = cb(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("WalletRTDSFeed callback raised")


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_instance: Optional[WalletRTDSFeed] = None


def get_wallet_rtds_feed(
    get_tracked_wallets: Optional[Callable[[], Iterable[str]]] = None,
) -> WalletRTDSFeed:
    """Return the process-wide WalletRTDSFeed.

    First call must supply ``get_tracked_wallets``. Subsequent calls
    return the same instance.
    """
    global _instance
    if _instance is None:
        if get_tracked_wallets is None:
            raise RuntimeError(
                "WalletRTDSFeed not yet initialized; pass get_tracked_wallets "
                "on first call"
            )
        _instance = WalletRTDSFeed(get_tracked_wallets=get_tracked_wallets)
    return _instance
