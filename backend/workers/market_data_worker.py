"""Market data worker: dedicated WS ingestion + stale-gap price poller.

Owns Polymarket/Kalshi feed-manager lifecycle, keeps subscriptions synced to
the canonical market catalog, and continuously writes canonical token prices
into Redis via ``redis_price_cache``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from config import apply_runtime_settings_overrides, settings
from models.database import AsyncSessionLocal
from services.kalshi_client import kalshi_client
from services.optimization.vwap import OrderBook, OrderBookLevel
from services.polymarket import polymarket_client
from services.redis_price_cache import redis_price_cache
from services.shared_state import read_market_catalog
from services.worker_state import clear_worker_run_request, read_worker_control, write_worker_snapshot
from services.ws_feeds import get_feed_manager
from utils.logger import setup_logging
from utils.utcnow import utcnow

setup_logging(level=os.environ.get("LOG_LEVEL", "INFO"), json_format=False)
logger = logging.getLogger("market_data_worker")


def _is_polymarket_token(token_id: str) -> bool:
    token = str(token_id or "").strip()
    if not token:
        return False
    lower = token.lower()
    return lower.startswith("0x") or len(token) > 20


def _normalize_market_id(raw_id: object) -> str:
    return str(raw_id or "").strip().lower()


def _normalize_token_id(raw_token: object) -> str:
    return str(raw_token or "").strip()


def _parse_level(raw: Any) -> OrderBookLevel | None:
    if isinstance(raw, dict):
        price = raw.get("price")
        size = raw.get("size", raw.get("quantity", raw.get("amount", 0)))
    elif isinstance(raw, (list, tuple)) and len(raw) >= 2:
        price = raw[0]
        size = raw[1]
    else:
        return None
    try:
        px = float(price)
        sz = float(size)
    except (TypeError, ValueError):
        return None
    if px < 0:
        return None
    if px > 1 and px <= 100:
        px = px / 100.0
    return OrderBookLevel(price=max(0.0, min(1.0, px)), size=max(0.0, sz))


def _book_from_raw(raw: Any) -> OrderBook | None:
    if not isinstance(raw, dict):
        return None
    bids_raw = raw.get("bids") or raw.get("buy") or []
    asks_raw = raw.get("asks") or raw.get("sell") or []
    bids = [_parse_level(item) for item in bids_raw]
    asks = [_parse_level(item) for item in asks_raw]
    bids = [level for level in bids if level is not None]
    asks = [level for level in asks if level is not None]
    bids.sort(key=lambda level: level.price, reverse=True)
    asks.sort(key=lambda level: level.price)
    if not bids and not asks:
        return None
    return OrderBook(bids=bids, asks=asks)


def _market_mid_order_book(mid: float) -> OrderBook:
    mid_px = max(0.0, min(1.0, float(mid)))
    level = OrderBookLevel(price=mid_px, size=1.0)
    return OrderBook(bids=[level], asks=[level])


async def _http_fallback_order_book(token_id: str) -> OrderBook | None:
    token = _normalize_token_id(token_id)
    if not token:
        return None

    if _is_polymarket_token(token):
        raw = await polymarket_client.get_order_book(token)
        return _book_from_raw(raw)

    if token.endswith("_yes") or token.endswith("_no"):
        ticker = token.rsplit("_", 1)[0]
        market = await kalshi_client.get_market(ticker)
        if market is None:
            return None
        mid = market.yes_price if token.endswith("_yes") else market.no_price
        return _market_mid_order_book(mid)

    return None


def _split_token_universe(tokens: set[str]) -> tuple[list[str], list[str]]:
    polymarket_tokens: list[str] = []
    kalshi_tokens: list[str] = []
    for token in sorted(tokens):
        if _is_polymarket_token(token):
            polymarket_tokens.append(token)
        else:
            kalshi_tokens.append(token)
    return polymarket_tokens, kalshi_tokens


def _kalshi_tickers_from_tokens(tokens: set[str]) -> set[str]:
    out: set[str] = set()
    for token in tokens:
        if _is_polymarket_token(token):
            continue
        if token.endswith("_yes") or token.endswith("_no"):
            out.add(token.rsplit("_", 1)[0])
        elif token:
            out.add(token)
    return out


def _collect_catalog_index(markets: list) -> tuple[set[str], dict[str, tuple[str, str]]]:
    token_ids: set[str] = set()
    market_tokens: dict[str, tuple[str, str]] = {}
    for market in markets:
        market_id = _normalize_market_id(getattr(market, "condition_id", None) or getattr(market, "id", None))
        if not market_id:
            continue
        raw_tokens = list(getattr(market, "clob_token_ids", None) or [])
        clean_tokens = [_normalize_token_id(token) for token in raw_tokens if _normalize_token_id(token)]
        if len(clean_tokens) < 2:
            continue
        yes_token = clean_tokens[0]
        no_token = clean_tokens[1]
        token_ids.add(yes_token)
        token_ids.add(no_token)
        market_tokens[market_id] = (yes_token, no_token)
    return token_ids, market_tokens


def _fallback_updates_from_prices(
    prices: dict[str, dict],
    sequence_start: int,
    now_ts: float,
) -> tuple[dict[str, tuple[float | None, float | None, float | None, float, float, int]], int]:
    updates: dict[str, tuple[float | None, float | None, float | None, float, float, int]] = {}
    seq = int(sequence_start)
    for token_id, payload in prices.items():
        if not isinstance(payload, dict):
            continue
        mid = payload.get("mid")
        if mid is None:
            continue
        try:
            mid_val = float(mid)
        except (TypeError, ValueError):
            continue
        if not (0.0 <= mid_val <= 1.0):
            continue
        seq += 1
        updates[str(token_id)] = (
            mid_val,
            float(payload.get("bid", mid_val)),
            float(payload.get("ask", mid_val)),
            now_ts,
            now_ts,
            seq,
        )
    return updates, seq


async def _run_loop() -> None:
    worker_name = "market_data"
    heartbeat_interval = max(
        1.0,
        float(getattr(settings, "MARKET_DATA_HEARTBEAT_INTERVAL_SECONDS", 3.0) or 3.0),
    )
    sync_interval = max(
        1,
        int(getattr(settings, "MARKET_DATA_REFRESH_INTERVAL_SECONDS", 5) or 5),
    )
    stale_poll_interval = max(
        1,
        int(getattr(settings, "MARKET_DATA_STALE_POLL_INTERVAL_SECONDS", 3) or 3),
    )
    stale_poll_batch_size = max(
        10,
        int(getattr(settings, "MARKET_DATA_STALE_POLL_BATCH_SIZE", 400) or 400),
    )
    ws_subscription_cap = max(
        100,
        int(getattr(settings, "MARKET_DATA_WS_SUBSCRIPTION_CAP", 8000) or 8000),
    )

    state: dict[str, Any] = {
        "enabled": True,
        "interval_seconds": sync_interval,
        "activity": "Market data worker started; awaiting catalog snapshot.",
        "last_error": None,
        "last_run_at": None,
        "run_id": None,
        "phase": "idle",
        "progress": 0.0,
        "catalog_markets": 0,
        "catalog_tokens": 0,
        "fresh_tokens": 0,
        "stale_tokens": 0,
        "stale_markets": 0,
        "fallback_polled_tokens": 0,
        "fallback_updates": 0,
        "ws_started": False,
        "polymarket_subscriptions": 0,
        "kalshi_subscriptions": 0,
    }
    heartbeat_stop_event = asyncio.Event()

    feed_manager = get_feed_manager()
    fallback_sequence = 0
    desired_tokens: set[str] = set()
    market_tokens: dict[str, tuple[str, str]] = {}
    subscribed_poly_tokens: set[str] = set()
    subscribed_kalshi_tickers: set[str] = set()
    next_stale_poll_at = datetime.now(timezone.utc)

    async def _write_heartbeat() -> None:
        async with AsyncSessionLocal() as session:
            await write_worker_snapshot(
                session,
                worker_name,
                running=True,
                enabled=bool(state.get("enabled", True)),
                current_activity=str(state.get("activity") or "Idle"),
                interval_seconds=int(state.get("interval_seconds") or sync_interval),
                last_run_at=state.get("last_run_at"),
                last_error=(str(state["last_error"]) if state.get("last_error") is not None else None),
                stats={
                    "run_id": state.get("run_id"),
                    "phase": state.get("phase"),
                    "progress": float(state.get("progress", 0.0) or 0.0),
                    "catalog_markets": int(state.get("catalog_markets", 0) or 0),
                    "catalog_tokens": int(state.get("catalog_tokens", 0) or 0),
                    "fresh_tokens": int(state.get("fresh_tokens", 0) or 0),
                    "stale_tokens": int(state.get("stale_tokens", 0) or 0),
                    "stale_markets": int(state.get("stale_markets", 0) or 0),
                    "fallback_polled_tokens": int(state.get("fallback_polled_tokens", 0) or 0),
                    "fallback_updates": int(state.get("fallback_updates", 0) or 0),
                    "ws_started": bool(state.get("ws_started", False)),
                    "polymarket_subscriptions": int(state.get("polymarket_subscriptions", 0) or 0),
                    "kalshi_subscriptions": int(state.get("kalshi_subscriptions", 0) or 0),
                },
            )

    async def _heartbeat_loop() -> None:
        while not heartbeat_stop_event.is_set():
            try:
                await _write_heartbeat()
            except Exception as exc:
                state["last_error"] = str(exc)
                logger.warning("Market data heartbeat snapshot write failed: %s", exc)
            try:
                await asyncio.wait_for(heartbeat_stop_event.wait(), timeout=heartbeat_interval)
            except asyncio.TimeoutError:
                continue

    async def _sync_ws_subscriptions() -> None:
        nonlocal subscribed_poly_tokens
        nonlocal subscribed_kalshi_tickers

        desired_poly_tokens, desired_kalshi_tokens = _split_token_universe(desired_tokens)
        desired_poly = set(desired_poly_tokens[:ws_subscription_cap])
        desired_kalshi = _kalshi_tickers_from_tokens(set(desired_kalshi_tokens[:ws_subscription_cap]))

        add_poly = sorted(desired_poly - subscribed_poly_tokens)
        remove_poly = sorted(subscribed_poly_tokens - desired_poly)
        if add_poly:
            await feed_manager.polymarket_feed.subscribe(token_ids=add_poly)
        if remove_poly:
            await feed_manager.polymarket_feed.unsubscribe(token_ids=remove_poly)
        subscribed_poly_tokens = desired_poly

        add_kalshi = sorted(desired_kalshi - subscribed_kalshi_tickers)
        remove_kalshi = sorted(subscribed_kalshi_tickers - desired_kalshi)
        if add_kalshi:
            await feed_manager.kalshi_feed.subscribe(add_kalshi)
        if remove_kalshi:
            await feed_manager.kalshi_feed.unsubscribe(remove_kalshi)
        subscribed_kalshi_tickers = desired_kalshi

        state["polymarket_subscriptions"] = len(subscribed_poly_tokens)
        state["kalshi_subscriptions"] = len(subscribed_kalshi_tickers)

    async def _run_stale_gap_filler(now_dt: datetime) -> None:
        nonlocal fallback_sequence
        all_tokens = sorted(desired_tokens)
        if not all_tokens:
            state["fresh_tokens"] = 0
            state["stale_tokens"] = 0
            state["stale_markets"] = 0
            state["fallback_polled_tokens"] = 0
            state["fallback_updates"] = 0
            return

        fresh_prices = await redis_price_cache.read_prices(all_tokens)
        fresh_tokens = set(fresh_prices.keys())
        stale_tokens = [token for token in all_tokens if token not in fresh_tokens]
        state["fresh_tokens"] = len(fresh_tokens)
        state["stale_tokens"] = len(stale_tokens)

        stale_market_count = 0
        for yes_token, no_token in market_tokens.values():
            if yes_token not in fresh_tokens or no_token not in fresh_tokens:
                stale_market_count += 1
        state["stale_markets"] = stale_market_count

        if not bool(getattr(settings, "MARKET_DATA_WORKER_OWNS_WS", True)):
            state["fallback_polled_tokens"] = 0
            state["fallback_updates"] = 0
            return
        if not stale_tokens:
            state["fallback_polled_tokens"] = 0
            state["fallback_updates"] = 0
            return

        poll_tokens = stale_tokens[:stale_poll_batch_size]
        poly_poll = [token for token in poll_tokens if _is_polymarket_token(token)]
        kalshi_poll = [token for token in poll_tokens if not _is_polymarket_token(token)]
        fallback_prices: dict[str, dict] = {}

        if poly_poll:
            try:
                fallback_prices.update(await polymarket_client.get_prices_batch(poly_poll))
            except Exception as exc:
                logger.warning("Polymarket stale-gap poll failed: %s", exc)
        if kalshi_poll:
            try:
                fallback_prices.update(await kalshi_client.get_prices_batch(kalshi_poll))
            except Exception as exc:
                logger.warning("Kalshi stale-gap poll failed: %s", exc)

        now_ts = max(0.0, now_dt.timestamp())
        updates, fallback_sequence = _fallback_updates_from_prices(fallback_prices, fallback_sequence, now_ts)
        if updates:
            await redis_price_cache.write_prices(updates)
        state["fallback_polled_tokens"] = len(poll_tokens)
        state["fallback_updates"] = len(updates)

    heartbeat_task = asyncio.create_task(_heartbeat_loop(), name="market-data-heartbeat")
    logger.info("Market data worker started")

    try:
        if settings.WS_FEED_ENABLED:
            feed_manager.set_http_fallback(_http_fallback_order_book)
            if not feed_manager._started:
                await feed_manager.start()
            state["ws_started"] = True
            state["activity"] = "Market data feeds connected; syncing catalog subscriptions."
        else:
            state["ws_started"] = False
            state["activity"] = "WS feeds disabled; stale-gap poller active only."

        while True:
            async with AsyncSessionLocal() as session:
                control = await read_worker_control(
                    session,
                    worker_name,
                    default_interval=sync_interval,
                )
                try:
                    await apply_runtime_settings_overrides()
                except Exception as exc:
                    logger.warning("Market data runtime settings refresh failed: %s", exc)

            interval_seconds = max(1, int(control.get("interval_seconds") or sync_interval))
            paused = bool(control.get("is_paused", False))
            requested = control.get("requested_run_at") is not None
            enabled = bool(control.get("is_enabled", True)) and not paused
            state["enabled"] = enabled
            state["interval_seconds"] = interval_seconds
            state["progress"] = 0.0
            state["phase"] = "idle"
            if state.get("run_id") is None:
                state["run_id"] = uuid.uuid4().hex[:16]

            if not enabled and not requested:
                state["activity"] = "Paused"
                await asyncio.sleep(heartbeat_interval)
                continue

            cycle_run_id = uuid.uuid4().hex[:16]
            state["run_id"] = cycle_run_id
            state["phase"] = "catalog_sync"
            state["progress"] = 0.1
            state["activity"] = "Syncing market catalog and WS subscriptions..."
            now_dt = datetime.now(timezone.utc)

            try:
                async with AsyncSessionLocal() as session:
                    _, markets, metadata = await read_market_catalog(session)
                    await clear_worker_run_request(session, worker_name)
                desired_tokens, market_tokens = _collect_catalog_index(markets)
                state["catalog_markets"] = int(metadata.get("market_count") or len(markets) or 0)
                state["catalog_tokens"] = len(desired_tokens)

                if settings.WS_FEED_ENABLED:
                    state["phase"] = "subscription_sync"
                    state["progress"] = 0.45
                    await _sync_ws_subscriptions()

                if now_dt >= next_stale_poll_at:
                    state["phase"] = "stale_gap_fill"
                    state["progress"] = 0.75
                    await _run_stale_gap_filler(now_dt)
                    next_stale_poll_at = now_dt + timedelta(seconds=stale_poll_interval)

                state["last_error"] = None
                state["last_run_at"] = utcnow()
                state["phase"] = "idle"
                state["progress"] = 1.0
                state["activity"] = (
                    f"Market data synced: {state['catalog_markets']} markets, {state['catalog_tokens']} tokens, "
                    f"{state['stale_tokens']} stale tokens."
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state["last_error"] = str(exc)
                state["phase"] = "error"
                state["activity"] = f"Market data cycle error: {exc}"
                logger.exception("Market data cycle failed: %s", exc)

            await asyncio.sleep(float(interval_seconds))
    finally:
        heartbeat_stop_event.set()
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

        if feed_manager._started:
            try:
                await feed_manager.stop()
            except Exception:
                pass


async def start_loop() -> None:
    try:
        await _run_loop()
    except asyncio.CancelledError:
        logger.info("Market data worker shutting down")
