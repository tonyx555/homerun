import asyncio
import json
import time
from datetime import datetime
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from config import settings
from models.database import AsyncSessionLocal, EventsSnapshot
from sqlalchemy import select
from services import shared_state
from services.news import shared_state as news_shared_state
from services.position_mark_state import get_position_mark_state
from services.signal_bus import read_trade_signal_source_stats
from services.trader_orchestrator_state import (
    list_serialized_execution_sessions,
    list_traders,
    read_orchestrator_snapshot,
)
from services.wallet_tracker import wallet_tracker
from services.worker_state import list_worker_snapshots, summarize_worker_stats
from services.ui_lock import UI_LOCK_SESSION_COOKIE, ui_lock_service
from services.weather import shared_state as weather_shared_state
from services.ws_feeds import get_feed_manager
from utils.logger import get_logger
from utils.market_urls import serialize_opportunity_with_links

logger = get_logger("websocket")


class ConnectionManager:
    """Manages WebSocket connections"""

    def __init__(self):
        self.active_connections: dict[WebSocket, dict[str, Any]] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[websocket] = {
            "channels": {"*"},
            "topics": {"*"},
            "visible": True,
            "price_market_ids": set(),
            "all_price_topics": False,
            "price_signatures": {},
        }

    def disconnect(self, websocket: WebSocket):
        state = self.active_connections.pop(websocket, None)
        if state:
            # Unregister from FeedManager price push
            try:
                get_feed_manager().unregister_ws_client(id(websocket))
            except Exception:
                pass

    def update_presence(self, websocket: WebSocket, *, visible: bool | None = None) -> None:
        state = self.active_connections.get(websocket)
        if state is None:
            return
        if visible is not None:
            state["visible"] = bool(visible)

    def update_channels(self, websocket: WebSocket, channels: list[str] | None) -> list[str]:
        state = self.active_connections.get(websocket)
        if state is None:
            return []
        if not isinstance(channels, list):
            state["channels"] = {"*"}
            return ["*"]
        normalized = {
            str(channel or "").strip().lower()
            for channel in channels
            if str(channel or "").strip()
        }
        state["channels"] = normalized or {"*"}
        return sorted(state["channels"])

    def update_topics(self, websocket: WebSocket, topics: list[str] | None) -> list[str]:
        state = self.active_connections.get(websocket)
        if state is None:
            return []
        if not isinstance(topics, list):
            state["topics"] = {"*"}
            state["price_market_ids"] = set()
            state["all_price_topics"] = False
            return ["*"]
        normalized = {
            str(topic or "").strip().lower()
            for topic in topics
            if str(topic or "").strip()
        }
        state["topics"] = normalized or {"*"}
        all_prices = False
        price_market_ids: set[str] = set()
        for topic in state["topics"]:
            if topic in {"prices", "prices:*"}:
                all_prices = True
                continue
            if topic.startswith("prices:"):
                market_id = str(topic.split(":", 1)[1] or "").strip().lower()
                if market_id:
                    price_market_ids.add(market_id)
        state["all_price_topics"] = all_prices
        state["price_market_ids"] = price_market_ids
        state["price_signatures"] = {}
        return sorted(state["topics"])

    @staticmethod
    def _message_channel(message_type: str) -> str | None:
        mapping = {
            "scanner_status": "core",
            "scanner_activity": "opportunities",
            "opportunities_update": "opportunities",
            "opportunity_events": "opportunities",
            "opportunity_update": "opportunities",
            "prices_update": "opportunities",
            "position_marks_update": "trading",
            "crypto_markets_update": "crypto",
            "weather_update": "weather",
            "weather_status": "weather",
            "news_update": "news",
            "news_workflow_status": "news",
            "news_workflow_update": "news",
            "events_status": "events",
            "events_update": "events",
            "worker_status_update": "workers",
            "signals_update": "signals",
            "wallet_trade": "wallet",
            "trader_orchestrator_status": "core",
            "trader_decision": "trading",
            "trader_order": "trading",
            "execution_order": "trading",
            "trader_event": "trading",
        }
        return mapping.get(message_type)

    @staticmethod
    def _default_message_topic(message_type: str) -> str | None:
        if message_type in {"scanner_status", "scanner_activity", "opportunities_update", "opportunity_events"}:
            return "opportunities.summary"
        if message_type == "opportunity_update":
            return "opportunities.detail"
        if message_type == "prices_update":
            return "prices"
        if message_type == "position_marks_update":
            return "position_marks"
        return None

    @staticmethod
    def _requires_visible_tab(message_type: str) -> bool:
        return message_type in {
            "scanner_activity",
            "opportunities_update",
            "opportunity_events",
            "opportunity_update",
            "prices_update",
            "position_marks_update",
            "crypto_markets_update",
            "weather_update",
            "news_workflow_update",
            "events_update",
        }

    @staticmethod
    def _channel_allowed(channels: set[str], message_channel: str | None) -> bool:
        if not channels or "*" in channels:
            return True
        if message_channel is None:
            return True
        return message_channel in channels

    @staticmethod
    def _topic_allowed(topics: set[str], message_topic: str | None) -> bool:
        if not topics or "*" in topics:
            return True
        if message_topic is None:
            return True
        normalized_topic = str(message_topic or "").strip().lower()
        if not normalized_topic:
            return True
        if normalized_topic in topics:
            return True
        if ":" in normalized_topic:
            prefix = normalized_topic.split(":", 1)[0]
            return f"{prefix}:*" in topics or prefix in topics
        return False

    async def broadcast(self, message: dict):
        """Send message to all connected clients"""
        if not self.active_connections:
            return

        message_json = json.dumps(message, default=str)
        disconnected: list[WebSocket] = []
        message_type = str(message.get("type", "") or "")
        channel = self._message_channel(message_type)
        topic = str(message.get("topic", "") or "").strip().lower() or self._default_message_topic(message_type)
        require_visible = self._requires_visible_tab(message_type)

        for connection, state in list(self.active_connections.items()):
            try:
                channels = state.get("channels")
                channels = channels if isinstance(channels, set) else {"*"}
                topics = state.get("topics")
                topics = topics if isinstance(topics, set) else {"*"}
                if not self._channel_allowed(channels, channel):
                    continue
                if not self._topic_allowed(topics, topic):
                    continue
                if require_visible and not bool(state.get("visible", True)):
                    continue
                await asyncio.wait_for(connection.send_text(message_json), timeout=5.0)
            except Exception:
                disconnected.append(connection)

        # Clean up disconnected clients
        for websocket in disconnected:
            self.disconnect(websocket)

    async def send_personal(self, websocket: WebSocket, message: dict):
        """Send message to specific client"""
        try:
            await asyncio.wait_for(
                websocket.send_text(json.dumps(message, default=str)),
                timeout=5.0,
            )
        except Exception:
            self.disconnect(websocket)


# Global connection manager
manager = ConnectionManager()
_market_token_cache: dict[str, tuple[str, str]] = {}
_market_token_cache_loaded_at_mono = 0.0
_market_token_cache_updated_at: datetime | None = None
_market_token_cache_lock = asyncio.Lock()


async def _get_market_token_cache() -> dict[str, tuple[str, str]]:
    global _market_token_cache
    global _market_token_cache_loaded_at_mono
    global _market_token_cache_updated_at

    now_mono = time.monotonic()
    if _market_token_cache and (now_mono - _market_token_cache_loaded_at_mono) < 2.0:
        return _market_token_cache

    async with _market_token_cache_lock:
        now_mono = time.monotonic()
        if _market_token_cache and (now_mono - _market_token_cache_loaded_at_mono) < 2.0:
            return _market_token_cache

        async with AsyncSessionLocal() as session:
            _, markets, metadata = await shared_state.read_market_catalog(
                session,
                include_events=False,
                include_markets=True,
                validate=False,
            )
            updated_at = metadata.get("updated_at")
            if (
                _market_token_cache
                and _market_token_cache_updated_at is not None
                and updated_at is not None
                and updated_at <= _market_token_cache_updated_at
            ):
                _market_token_cache_loaded_at_mono = now_mono
                return _market_token_cache

            rebuilt: dict[str, tuple[str, str]] = {}
            for market in markets:
                if not isinstance(market, dict):
                    continue
                market_id = str(market.get("condition_id") or market.get("id") or "").strip().lower()
                raw_tokens = list(market.get("clob_token_ids") or [])
                if not market_id:
                    continue
                clean_tokens = [str(token or "").strip() for token in raw_tokens if str(token or "").strip()]
                if len(clean_tokens) < 2:
                    continue
                rebuilt[market_id] = (clean_tokens[0], clean_tokens[1])

        _market_token_cache = rebuilt
        _market_token_cache_loaded_at_mono = now_mono
        _market_token_cache_updated_at = updated_at if isinstance(updated_at, datetime) else _market_token_cache_updated_at
        return _market_token_cache


# Reverse lookup: token_id -> market_id (built lazily from _market_token_cache)
_token_to_market: dict[str, str] = {}
_token_to_market_built_at_mono = 0.0


def _rebuild_token_to_market(token_cache: dict[str, tuple[str, str]]) -> dict[str, str]:
    """Build reverse map from token_id -> market_id."""
    global _token_to_market, _token_to_market_built_at_mono
    now = time.monotonic()
    if _token_to_market and (now - _token_to_market_built_at_mono) < 5.0:
        return _token_to_market
    result: dict[str, str] = {}
    for market_id, (yes_tok, no_tok) in token_cache.items():
        if yes_tok:
            result[yes_tok] = market_id
        if no_tok:
            result[no_tok] = market_id
    _token_to_market = result
    _token_to_market_built_at_mono = now
    return result


async def _resolve_subscribed_token_ids(websocket: WebSocket) -> set[str]:
    """Resolve the set of token_ids this websocket wants price updates for."""
    state = manager.active_connections.get(websocket)
    if state is None:
        return set()
    market_ids: set[str] = set()
    explicit_ids = state.get("price_market_ids")
    if isinstance(explicit_ids, set):
        market_ids.update(str(mid or "").strip().lower() for mid in explicit_ids if str(mid or "").strip())
    token_cache = await _get_market_token_cache()
    all_market_cap = 250
    if bool(state.get("all_price_topics", False)):
        market_ids.update(list(token_cache.keys())[:all_market_cap])
    token_ids: set[str] = set()
    for mid in market_ids:
        tokens = token_cache.get(mid, ())
        for t in tokens:
            if t:
                token_ids.add(t)
    return token_ids


async def _on_price_push(websocket: WebSocket, token_updates: dict[str, dict[str, Any]]) -> None:
    """Callback from FeedManager: push coalesced price updates to one frontend WS.

    *token_updates* is ``{token_id: {mid, bid, ask, exchange_ts, ingest_ts, sequence, is_fresh}}``.
    We group by market and send one ``prices_update`` per market, with signature dedup.
    """
    state = manager.active_connections.get(websocket)
    if state is None:
        return
    if not bool(state.get("visible", True)):
        return

    token_cache = await _get_market_token_cache()
    token_to_market = _rebuild_token_to_market(token_cache)

    signatures = state.get("price_signatures")
    if not isinstance(signatures, dict):
        signatures = {}
        state["price_signatures"] = signatures

    # Collect which markets were affected
    affected_markets: set[str] = set()
    for token_id in token_updates:
        mid = token_to_market.get(token_id)
        if mid:
            affected_markets.add(mid)

    # Check topic filtering
    topics = state.get("topics")
    topics = topics if isinstance(topics, set) else {"*"}

    for market_id in affected_markets:
        if not manager._topic_allowed(topics, f"prices:{market_id}"):
            continue
        yes_token, no_token = token_cache.get(market_id, ("", ""))

        # Get price data from the push batch or from cache
        feed_manager = get_feed_manager()
        cache = feed_manager.cache

        yes_data = token_updates.get(yes_token)
        no_data = token_updates.get(no_token)

        # For tokens not in this batch, read from cache
        if yes_data is None and yes_token:
            entry_mid = cache.get_mid_price(yes_token)
            if entry_mid is not None:
                ba = cache.get_best_bid_ask(yes_token)
                yes_data = {
                    "mid": entry_mid,
                    "bid": ba[0] if ba else entry_mid,
                    "ask": ba[1] if ba else entry_mid,
                    "ingest_ts": cache.get_observed_at_epoch(yes_token) or 0.0,
                    "exchange_ts": 0.0,
                    "sequence": cache.get_sequence(yes_token) or 0,
                    "is_fresh": cache.is_fresh(yes_token),
                }
        if no_data is None and no_token:
            entry_mid = cache.get_mid_price(no_token)
            if entry_mid is not None:
                ba = cache.get_best_bid_ask(no_token)
                no_data = {
                    "mid": entry_mid,
                    "bid": ba[0] if ba else entry_mid,
                    "ask": ba[1] if ba else entry_mid,
                    "ingest_ts": cache.get_observed_at_epoch(no_token) or 0.0,
                    "exchange_ts": 0.0,
                    "sequence": cache.get_sequence(no_token) or 0,
                    "is_fresh": cache.is_fresh(no_token),
                }

        yes_mid = float(yes_data["mid"]) if yes_data and yes_data.get("mid") is not None else None
        no_mid = float(no_data["mid"]) if no_data and no_data.get("mid") is not None else None
        yes_seq = int(yes_data.get("sequence") or 0) if yes_data else 0
        no_seq = int(no_data.get("sequence") or 0) if no_data else 0

        # Signature dedup using actual cache sequence numbers (not time.time())
        signature = (yes_mid, no_mid, yes_seq, no_seq)
        if signatures.get(market_id) == signature:
            continue
        signatures[market_id] = signature

        yes_ingest = float(yes_data.get("ingest_ts") or 0) if yes_data else None
        no_ingest = float(no_data.get("ingest_ts") or 0) if no_data else None
        yes_exchange = float(yes_data.get("exchange_ts") or 0) if yes_data else None
        no_exchange = float(no_data.get("exchange_ts") or 0) if no_data else None

        await manager.send_personal(
            websocket,
            {
                "type": "prices_update",
                "topic": f"prices:{market_id}",
                "data": {
                    "market_id": market_id,
                    "yes_token_id": yes_token,
                    "no_token_id": no_token,
                    "yes_price": yes_mid,
                    "no_price": no_mid,
                    "yes_ingest_ts": yes_ingest,
                    "no_ingest_ts": no_ingest,
                    "yes_exchange_ts": yes_exchange,
                    "no_exchange_ts": no_exchange,
                    "yes_sequence": yes_seq,
                    "no_sequence": no_seq,
                    "is_fresh": bool(
                        yes_data and no_data
                        and yes_data.get("is_fresh")
                        and no_data.get("is_fresh")
                    ),
                },
            },
        )


async def _register_price_push(websocket: WebSocket) -> None:
    """Register this websocket with FeedManager for event-driven price pushes."""
    token_ids = await _resolve_subscribed_token_ids(websocket)
    feed_manager = get_feed_manager()

    async def push_callback(token_updates: dict[str, dict[str, Any]]) -> None:
        await _on_price_push(websocket, token_updates)

    feed_manager.register_ws_client(id(websocket), token_ids, push_callback)


async def _update_price_push_tokens(websocket: WebSocket) -> None:
    """Update the FeedManager registration after topic changes."""
    token_ids = await _resolve_subscribed_token_ids(websocket)
    get_feed_manager().update_ws_client_tokens(id(websocket), token_ids)


async def _price_safety_net_loop(websocket: WebSocket) -> None:
    """Slow safety-net fallback that pushes prices every 30s.

    The primary path is event-driven via FeedManager._on_price_update ->
    coalesced push. This loop exists only to catch any prices that slip
    through (e.g. if FeedManager is not started).
    """
    interval = max(10.0, float(settings.MARKET_DATA_PRICE_STREAM_INTERVAL_SECONDS or 30.0))
    all_market_cap = 250
    while True:
        await asyncio.sleep(interval)
        state = manager.active_connections.get(websocket)
        if state is None:
            return
        if not bool(state.get("visible", True)):
            continue

        market_ids: set[str] = set()
        explicit_ids = state.get("price_market_ids")
        if isinstance(explicit_ids, set):
            market_ids.update(str(mid or "").strip().lower() for mid in explicit_ids if str(mid or "").strip())

        token_cache = await _get_market_token_cache()
        if bool(state.get("all_price_topics", False)):
            market_ids.update(list(token_cache.keys())[:all_market_cap])

        if not market_ids:
            continue

        requested_market_ids = sorted(mid for mid in market_ids if mid in token_cache)
        if not requested_market_ids:
            continue

        feed_manager = get_feed_manager()
        if not getattr(feed_manager, "_started", False):
            continue

        signatures = state.get("price_signatures")
        if not isinstance(signatures, dict):
            signatures = {}
            state["price_signatures"] = signatures

        for market_id in requested_market_ids:
            yes_token, no_token = token_cache.get(market_id, ("", ""))
            cache = feed_manager.cache

            yes_mid = cache.get_mid_price(yes_token) if yes_token else None
            no_mid = cache.get_mid_price(no_token) if no_token else None
            yes_seq = cache.get_sequence(yes_token) or 0 if yes_token else 0
            no_seq = cache.get_sequence(no_token) or 0 if no_token else 0

            signature = (yes_mid, no_mid, yes_seq, no_seq)
            if signatures.get(market_id) == signature:
                continue
            signatures[market_id] = signature

            yes_ingest = cache.get_observed_at_epoch(yes_token) if yes_token else None
            no_ingest = cache.get_observed_at_epoch(no_token) if no_token else None

            await manager.send_personal(
                websocket,
                {
                    "type": "prices_update",
                    "topic": f"prices:{market_id}",
                    "data": {
                        "market_id": market_id,
                        "yes_token_id": yes_token,
                        "no_token_id": no_token,
                        "yes_price": float(yes_mid) if yes_mid is not None else None,
                        "no_price": float(no_mid) if no_mid is not None else None,
                        "yes_ingest_ts": float(yes_ingest) if yes_ingest else None,
                        "no_ingest_ts": float(no_ingest) if no_ingest else None,
                        "yes_sequence": yes_seq,
                        "no_sequence": no_seq,
                        "is_fresh": bool(
                            yes_token and no_token
                            and cache.is_fresh(yes_token)
                            and cache.is_fresh(no_token)
                        ),
                    },
                },
            )


# ---------------------------------------------------------------------------
# Position marks push via PositionMarkState
# ---------------------------------------------------------------------------

_marks_push_task: asyncio.Task | None = None
_marks_push_pending = False
_marks_loop: asyncio.AbstractEventLoop | None = None

_marks_callback_count = 0
_marks_push_count = 0
_marks_callback_registered = False


def set_marks_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Set the event loop for position marks push.

    Must be called from the API process lifespan BEFORE FeedManager starts,
    so that price callbacks can schedule mark pushes immediately.
    """
    global _marks_loop
    _marks_loop = loop
    _ensure_position_marks_push_registered()
    if _marks_push_pending:
        try:
            _marks_loop.call_soon_threadsafe(_schedule_marks_push)
        except RuntimeError:
            pass
    logger.info("Position marks event loop captured")


def _on_position_marks_changed(changed_marks: list) -> None:
    """Synchronous callback from PositionMarkState. Schedules an async push.

    NOTE: This is called from WS receive threads (not the asyncio loop),
    so we must use call_soon_threadsafe to schedule the push task.
    """
    global _marks_push_pending, _marks_callback_count
    _marks_callback_count += 1
    _marks_push_pending = True
    if _marks_loop is None:
        return
    try:
        _marks_loop.call_soon_threadsafe(_schedule_marks_push)
    except RuntimeError:
        pass  # loop closed


def _schedule_marks_push() -> None:
    """Create the marks push task from the event loop thread."""
    global _marks_push_task
    if _marks_loop is None:
        return
    if _marks_push_task is None or _marks_push_task.done():
        _marks_push_task = _marks_loop.create_task(_push_position_marks())


async def _push_position_marks() -> None:
    """Coalesced push of position marks to all subscribed frontends."""
    global _marks_push_pending, _marks_push_count
    # Brief coalescing delay to batch rapid-fire updates
    await asyncio.sleep(0.1)
    _marks_push_pending = False
    _do_push_marks()


def _do_push_marks() -> None:
    """Actually broadcast marks to all clients. Shared by event push + periodic refresh."""
    global _marks_push_count
    pms = get_position_mark_state()
    marks = pms.get_marks()
    if not marks:
        return
    if not manager.active_connections:
        return
    _marks_push_count += 1
    if _marks_push_count <= 5:
        logger.info("Pushing position marks to %d clients (%d marks, push #%d)",
                     len(manager.active_connections), len(marks), _marks_push_count)
    asyncio.ensure_future(manager.broadcast(
        {
            "type": "position_marks_update",
            "data": {
                "marks": list(marks.values()),
            },
        }
    ))


_marks_refresh_task: asyncio.Task | None = None

async def _marks_periodic_refresh() -> None:
    """Re-broadcast current marks every 2s even when prices haven't changed.

    For illiquid markets, Polymarket WS may not send new orderbook data
    for 10-60+ seconds. Without this refresh, the UI would show growing
    mark ages even though the price data IS the latest available.
    """
    while True:
        await asyncio.sleep(2)
        try:
            _do_push_marks()
        except Exception:
            pass


def start_marks_refresh_loop() -> None:
    """Start the periodic marks refresh. Call from lifespan after event loop is running."""
    global _marks_refresh_task
    _ensure_position_marks_push_registered()
    if _marks_refresh_task is None or _marks_refresh_task.done():
        _marks_refresh_task = asyncio.ensure_future(_marks_periodic_refresh())


def _ensure_position_marks_push_registered() -> None:
    global _marks_callback_registered
    if _marks_callback_registered:
        return
    pms = get_position_mark_state()
    pms.set_on_marks_changed(_on_position_marks_changed)
    _marks_callback_registered = True


async def handle_websocket(websocket: WebSocket):
    """Main WebSocket handler"""

    await manager.connect(websocket)

    # Register for event-driven price push
    await _register_price_push(websocket)

    # Safety-net fallback loop (slow, 30s cadence)
    safety_net_task = asyncio.create_task(
        _price_safety_net_loop(websocket), name="ws-price-safety-net"
    )

    # Send current state from the in-process runtimes plus durable projections.
    async with AsyncSessionLocal() as session:
        opportunities, status = await shared_state.read_scanner_snapshot(session)
        weather_opportunities = await weather_shared_state.get_weather_opportunities_from_db(session)
        weather_status = await weather_shared_state.get_weather_status_from_db(session)
        news_workflow_status = await news_shared_state.get_news_status_from_db(session)
        worker_statuses = await list_worker_snapshots(session, include_stats=True, stats_mode="summary")
        orchestrator_status = await read_orchestrator_snapshot(session)
        traders = await list_traders(session)
        execution_sessions = await list_serialized_execution_sessions(
            session,
            trader_id=None,
            status=None,
            limit=100,
        )
        signal_sources = await read_trade_signal_source_stats(session)
        world_snapshot = (
            (await session.execute(select(EventsSnapshot).where(EventsSnapshot.id == "latest"))).scalars().one_or_none()
        )
        events_status = {
            "status": (world_snapshot.status if world_snapshot else {}) or {},
            "stats": (world_snapshot.stats if world_snapshot else {}) or {},
            "updated_at": (
                world_snapshot.updated_at.isoformat() if world_snapshot and world_snapshot.updated_at else None
            ),
        }
    crypto_markets: list[dict[str, Any]] = []
    for worker in worker_statuses:
        if worker.get("worker_name") != "crypto":
            continue
        stats = worker.get("stats") if isinstance(worker.get("stats"), dict) else {}
        if isinstance(stats.get("markets"), list):
            crypto_markets = list(stats.get("markets") or [])
        break
    if crypto_markets:
        for worker in worker_statuses:
            if worker.get("worker_name") == "crypto":
                worker["stats"] = {"markets": crypto_markets}
                break
    if isinstance(orchestrator_status, dict):
        orchestrator_status = dict(orchestrator_status)
        orchestrator_status["stats"] = summarize_worker_stats(orchestrator_status.get("stats"))
    await manager.send_personal(
        websocket,
        {
            "type": "init",
            "data": {
                "opportunities": [serialize_opportunity_with_links(o) for o in opportunities[:20]],
                "weather_opportunities": [serialize_opportunity_with_links(o) for o in weather_opportunities[:20]],
                "scanner_status": {
                    "running": status.get("running", False),
                    "last_scan": status.get("last_scan"),
                    "last_fast_scan": status.get("last_fast_scan"),
                    "last_heavy_scan": status.get("last_heavy_scan"),
                    "opportunities_count": status.get("opportunities_count", len(opportunities)),
                    "current_activity": status.get("current_activity"),
                    "lane_watchdogs": status.get("lane_watchdogs"),
                },
                "weather_status": weather_status,
                "news_workflow_status": news_workflow_status,
                "workers_status": worker_statuses,
                "trader_orchestrator_status": orchestrator_status,
                "signals_status": {"sources": signal_sources},
                "traders": traders,
                "execution_sessions": execution_sessions,
                "events_status": events_status,
                "crypto_markets": crypto_markets,
            },
        },
    )

    try:
        while True:
            # Wait for messages from client
            data = await websocket.receive_text()
            token = websocket.cookies.get(UI_LOCK_SESSION_COOKIE)
            unlocked = await ui_lock_service.is_token_unlocked(token)
            if not unlocked:
                status = await ui_lock_service.status(token)
                if status.get("enabled"):
                    await manager.send_personal(
                        websocket,
                        {
                            "type": "ui_locked",
                            "data": {"message": "UI lock is active.", "ui_lock": status},
                        },
                    )
                    await websocket.close(code=4403)
                    break
            message = json.loads(data)

            # Handle different message types
            if message.get("type") == "subscribe":
                subscribed_channels = manager.update_channels(websocket, message.get("channels"))
                subscribed_topics = manager.update_topics(websocket, message.get("topics"))
                visible = message.get("visible")
                if isinstance(visible, bool):
                    manager.update_presence(websocket, visible=visible)
                # Update FeedManager token subscriptions for event-driven push
                await _update_price_push_tokens(websocket)
                await manager.send_personal(
                    websocket,
                    {
                        "type": "subscribed",
                        "data": {
                            "channels": subscribed_channels,
                            "topics": subscribed_topics,
                            "visible": bool(manager.active_connections.get(websocket, {}).get("visible", True)),
                        },
                    },
                )

            elif message.get("type") == "ui_presence":
                visible = message.get("visible")
                if isinstance(visible, bool):
                    manager.update_presence(websocket, visible=visible)
                await manager.send_personal(
                    websocket,
                    {
                        "type": "presence_ack",
                        "data": {"visible": bool(manager.active_connections.get(websocket, {}).get("visible", True))},
                    },
                )

            elif message.get("type") == "ping":
                await manager.send_personal(websocket, {"type": "pong"})

            elif message.get("type") == "scan":
                # Request one scan; worker will run it
                async with AsyncSessionLocal() as session:
                    await shared_state.request_one_scan(session)
                async with AsyncSessionLocal() as session:
                    opportunities, _ = await shared_state.read_scanner_snapshot(session)
                await manager.send_personal(
                    websocket,
                    {
                        "type": "scan_requested",
                        "data": {
                            "message": "Scan requested; results will update when worker runs.",
                            "current_count": len(opportunities),
                        },
                    },
                )

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error("WebSocket error", exc_info=e)
        manager.disconnect(websocket)
    finally:
        safety_net_task.cancel()
        try:
            await safety_net_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass


async def broadcast_opportunities(opportunities):
    """Callback to broadcast new opportunities"""
    await manager.broadcast(
        {
            "type": "opportunities_update",
            "topic": "opportunities.summary",
            "data": {
                "count": len(opportunities),
                "opportunities": [serialize_opportunity_with_links(o) for o in opportunities[:20]],
            },
        }
    )


async def broadcast_wallet_trade(trade):
    """Callback to broadcast new wallet trades"""
    await manager.broadcast({"type": "wallet_trade", "data": trade})


async def broadcast_wallet_ws_event(event) -> None:
    """Push a wallet_ws_monitor trade event to subscribed clients.

    Fires for both on-chain confirmations (``confirmed=True``,
    ``source="onchain"``) and the RTDS fast-path preliminary detections
    (``confirmed=False``, ``source="rtds"``). Frontend consumers should
    dedupe by ``tx_hash``: render preliminary events optimistically,
    then upgrade to confirmed when the on-chain version arrives.

    Topic ``wallet_trades`` so subscribers can opt in via
    ``{action: "subscribe_topics", topics: ["wallet_trades"]}``.
    """
    try:
        token_id = getattr(event, "token_id", None) or ""
        detected_at = getattr(event, "detected_at", None)
        block_dt = getattr(event, "timestamp", None)
        payload = {
            "wallet_address": getattr(event, "wallet_address", "") or "",
            "token_id": token_id,
            "side": getattr(event, "side", "") or "",
            "size": float(getattr(event, "size", 0.0) or 0.0),
            "price": float(getattr(event, "price", 0.0) or 0.0),
            "tx_hash": getattr(event, "tx_hash", "") or "",
            "order_hash": getattr(event, "order_hash", "") or "",
            "log_index": int(getattr(event, "log_index", 0) or 0),
            "block_number": int(getattr(event, "block_number", 0) or 0),
            "latency_ms": float(getattr(event, "latency_ms", 0.0) or 0.0),
            "detected_at_ms": (
                int(detected_at.timestamp() * 1000)
                if detected_at is not None
                else None
            ),
            "trade_at_ms": (
                int(block_dt.timestamp() * 1000)
                if block_dt is not None
                else None
            ),
            "confirmed": bool(getattr(event, "confirmed", True)),
            "source": str(getattr(event, "source", "onchain") or "onchain"),
        }
    except Exception:
        # Never let a serialization edge case break the on-chain emit
        # path — that would silently lose trades.
        return
    await manager.broadcast(
        {"type": "wallet_trade_event", "topic": "wallet_trades", "data": payload}
    )


async def broadcast_scanner_status(status):
    """Callback to broadcast scanner status changes"""
    await manager.broadcast({"type": "scanner_status", "topic": "opportunities.summary", "data": status})


async def broadcast_scanner_activity(activity: str):
    """Callback to broadcast scanning activity updates (live status line)"""
    await manager.broadcast(
        {"type": "scanner_activity", "topic": "opportunities.summary", "data": {"activity": activity}}
    )


async def broadcast_news_update(article_count: int):
    """Callback to broadcast new news articles arriving"""
    await manager.broadcast({"type": "news_update", "data": {"new_count": article_count}})


async def broadcast_crypto_markets(markets_data: list[dict]):
    """Broadcast live crypto market data to all connected clients."""
    await manager.broadcast({"type": "crypto_markets_update", "data": {"markets": markets_data}})


async def broadcast_weather_update(opportunities: list[dict], status: dict):
    """Broadcast weather workflow opportunity updates to all clients."""
    await manager.broadcast(
        {
            "type": "weather_update",
            "data": {
                "count": len(opportunities),
                "opportunities": opportunities[:100],
                "status": status,
            },
        }
    )


async def broadcast_weather_status(status: dict):
    """Broadcast weather workflow status changes to all clients."""
    await manager.broadcast({"type": "weather_status", "data": status})


async def broadcast_events_update(signals: list[dict], summary: dict):
    """Broadcast events signal updates to all clients."""
    await manager.broadcast(
        {
            "type": "events_update",
            "data": {
                "count": len(signals),
                "signals": signals[:50],
                "summary": summary,
            },
        }
    )


async def broadcast_events_status(status: dict):
    """Broadcast events worker status changes."""
    await manager.broadcast({"type": "events_status", "data": status})


# Register callbacks (scanner runs in worker process; no scanner callbacks here)
# Frontend gets opportunities/status via polling or init; or add API polling->broadcast later
wallet_tracker.add_callback(broadcast_wallet_trade)


# Subscribe the wallet_ws_monitor (Polygon on-chain + Polymarket RTDS fast-
# path) to push WalletTradeEvent instances to the frontend in real time.
# Done in a try/except so a missing module doesn't take down the whole WS
# layer at import time.
try:
    from services.wallet_ws_monitor import wallet_ws_monitor as _wallet_ws_monitor

    _wallet_ws_monitor.add_callback(broadcast_wallet_ws_event)
except Exception as _wallet_ws_register_err:  # pragma: no cover
    import logging as _logging
    _logging.getLogger(__name__).warning(
        "Failed to register broadcast_wallet_ws_event callback: %s",
        _wallet_ws_register_err,
    )
