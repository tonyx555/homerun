"""Market data tools — search, discovery, prices, order books, regime detection.

Uses three Polymarket APIs:
- Gamma API (gamma-api.polymarket.com) — market/event metadata, no auth
- CLOB API  (clob.polymarket.com)     — orderbook, prices, spreads, no auth for reads
- Public search (/public-search)       — keyword search across all content
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"
_HTTP_TIMEOUT = 15

_shared_client: httpx.AsyncClient | None = None


def _http() -> httpx.AsyncClient:
    global _shared_client
    if _shared_client is None or _shared_client.is_closed:
        _shared_client = httpx.AsyncClient(timeout=_HTTP_TIMEOUT)
    return _shared_client


def build_tools() -> list:
    from services.ai.agent import AgentTool

    return [
        # -- Discovery & Search --
        AgentTool(
            name="search_markets",
            description=(
                "Keyword search across all Polymarket content (markets, events, descriptions). "
                "Uses the /public-search endpoint for real full-text search. "
                "Use specific topic keywords like 'Bitcoin', 'NBA finals', 'Fed rate', 'Trump'. "
                "Returns matching events (which group related markets together) with prices, "
                "volume, liquidity, and end dates."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search keywords (e.g. 'Bitcoin price', 'NBA playoffs', 'Fed rate cut')",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 10)",
                        "default": 10,
                    },
                },
                "required": ["query"],
            },
            handler=_search_markets,
            max_calls=10,
            category="market_data",
        ),
        AgentTool(
            name="discover_events",
            description=(
                "Browse and filter Polymarket events with rich criteria. Use this for "
                "discovery queries like 'trending markets', 'expiring soon', 'high volume', "
                "or category-specific browsing. Events group related markets together. "
                "Supports filtering by end date range, volume, liquidity, tags, and sorting."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "tag_slug": {
                        "type": "string",
                        "description": "Filter by category tag slug (e.g. 'politics', 'crypto', 'sports', 'science', 'finance', 'pop-culture')",
                    },
                    "end_date_min": {
                        "type": "string",
                        "description": "Earliest end date (ISO 8601, e.g. '2026-03-21T00:00:00Z'). Use for 'expiring after' filters.",
                    },
                    "end_date_max": {
                        "type": "string",
                        "description": "Latest end date (ISO 8601). Use for 'expiring before' / 'expiring soon' filters.",
                    },
                    "volume_min": {
                        "type": "number",
                        "description": "Minimum total volume in USD (e.g. 10000 for $10K+)",
                    },
                    "liquidity_min": {
                        "type": "number",
                        "description": "Minimum liquidity in USD",
                    },
                    "order": {
                        "type": "string",
                        "description": "Sort: volume_24hr (default/trending), volume, liquidity, end_date, start_date, competitive",
                        "default": "volume_24hr",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 15)",
                        "default": 15,
                    },
                },
                "required": [],
            },
            handler=_discover_events,
            max_calls=10,
            category="market_data",
        ),
        AgentTool(
            name="get_market_tags",
            description=(
                "List all available category tags on Polymarket with their IDs and slugs. "
                "Use this to discover valid tag slugs for filtering with discover_events."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_tags,
            max_calls=5,
            category="market_data",
        ),
        # -- Market Details --
        AgentTool(
            name="get_market_details",
            description=(
                "Fetch detailed information about a specific Polymarket market including "
                "question, description, resolution rules, current prices, volume, and liquidity."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "market_id": {
                        "type": "string",
                        "description": "The market's condition ID or slug",
                    },
                },
                "required": ["market_id"],
            },
            handler=_get_market_details,
            max_calls=10,
            category="market_data",
        ),
        AgentTool(
            name="get_event_details",
            description=(
                "Fetch a Polymarket event and ALL its child markets. Events group related "
                "markets (e.g. 'NBA Finals 2026' event contains markets for each game). "
                "Use event slug or ID."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "event_id": {
                        "type": "string",
                        "description": "Event ID or slug",
                    },
                },
                "required": ["event_id"],
            },
            handler=_get_event_details,
            max_calls=10,
            category="market_data",
        ),
        # -- Price & Orderbook --
        AgentTool(
            name="get_live_prices",
            description=(
                "Get current live mid-prices for all active markets the system is tracking. "
                "Returns a mapping of token_id to mid price. Useful for a quick snapshot "
                "of where prices stand across the portfolio."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_live_prices,
            max_calls=3,
            category="market_data",
        ),
        AgentTool(
            name="get_price_history",
            description=(
                "Get historical price data for a market token. Returns OHLC-style "
                "candles or price points over a time range. Useful for trend analysis "
                "and detecting momentum."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "token_id": {
                        "type": "string",
                        "description": "Token ID to get price history for",
                    },
                    "lookback_hours": {
                        "type": "integer",
                        "description": "Hours of history to fetch (default 24)",
                        "default": 24,
                    },
                },
                "required": ["token_id"],
            },
            handler=_get_price_history,
            max_calls=10,
            category="market_data",
        ),
        AgentTool(
            name="check_orderbook",
            description=(
                "Check the order book depth and liquidity for a market token. "
                "Returns bid/ask spreads, depth, and available liquidity at different "
                "price levels. Note: use /price and /midpoint for reliable live prices "
                "as /book can sometimes return stale data."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "token_id": {
                        "type": "string",
                        "description": "The token ID to check",
                    },
                },
                "required": ["token_id"],
            },
            handler=_check_orderbook,
            max_calls=10,
            category="market_data",
        ),
        AgentTool(
            name="get_market_prices",
            description=(
                "Get reliable live prices for one or more market tokens from the CLOB API. "
                "Returns best bid, best ask, midpoint, and spread for each token. "
                "More reliable than orderbook for current prices."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "token_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of CLOB token IDs to price",
                    },
                },
                "required": ["token_ids"],
            },
            handler=_get_market_prices,
            max_calls=10,
            category="market_data",
        ),
        # -- Analysis --
        AgentTool(
            name="get_market_regime",
            description=(
                "Get the current market regime for a specific market — volatile, "
                "trending, mean-reverting, or calm. Includes regime parameters and "
                "confidence level."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "market_id": {
                        "type": "string",
                        "description": "Market condition ID",
                    },
                },
                "required": ["market_id"],
            },
            handler=_get_market_regime,
            max_calls=10,
            category="market_data",
        ),
        AgentTool(
            name="get_active_markets_summary",
            description=(
                "Get a summary of all currently active markets the system is monitoring "
                "(from the internal runtime, not Polymarket API). Returns count, top markets "
                "by volume. Only useful if the system's market scanner is running."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_active_markets_summary,
            max_calls=5,
            category="market_data",
        ),
    ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_market(m: dict) -> dict:
    """Extract a consistent market summary dict from a Gamma API market object."""
    return {
        "condition_id": m.get("conditionId") or m.get("condition_id", ""),
        "question": m.get("question", ""),
        "slug": m.get("slug", ""),
        "outcomes": m.get("outcomes", []),
        "outcome_prices": m.get("outcomePrices", []),
        "volume": m.get("volume", 0),
        "volume_24hr": m.get("volume24hr", 0),
        "liquidity": m.get("liquidity", 0),
        "end_date": m.get("endDate", ""),
        "active": m.get("active", False),
        "clob_token_ids": m.get("clobTokenIds", []),
    }


def _format_event(e: dict, include_markets: bool = True) -> dict:
    """Extract a consistent event summary dict from a Gamma API event object."""
    result: dict[str, Any] = {
        "event_id": e.get("id", ""),
        "title": e.get("title", ""),
        "slug": e.get("slug", ""),
        "category": e.get("category", ""),
        "volume": e.get("volume", 0),
        "volume_24hr": e.get("volume24hr", 0),
        "liquidity": e.get("liquidity", 0),
        "end_date": e.get("endDate", ""),
        "start_date": e.get("startDate", ""),
        "active": e.get("active", False),
        "market_count": len(e.get("markets", [])),
    }
    if include_markets:
        result["markets"] = [_format_market(m) for m in e.get("markets", [])[:10]]
    return result


# ---------------------------------------------------------------------------
# Implementations — Search & Discovery
# ---------------------------------------------------------------------------


async def _search_markets(args: dict) -> dict:
    """Keyword search via /events with _q (Strapi full-text) and slug_contains."""
    try:
        query = args["query"]
        limit = args.get("limit", 10)
        client = _http()
        slug_query = query.lower().replace(" ", "-")

        base_params: dict[str, Any] = {
            "closed": "false",
            "active": "true",
            "limit": limit,
        }

        # Run both search strategies concurrently
        resp_q, resp_slug = await asyncio.gather(
            client.get(f"{GAMMA_API_URL}/events", params={**base_params, "_q": query.strip()}),
            client.get(f"{GAMMA_API_URL}/events", params={**base_params, "slug_contains": slug_query}),
        )

        seen_ids: set[str] = set()
        events: list[dict] = []

        for resp in (resp_q, resp_slug):
            if resp.status_code != 200:
                continue
            for e in resp.json():
                if not isinstance(e, dict):
                    continue
                eid = e.get("id", "")
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
                events.append(_format_event(e))
                if len(events) >= limit:
                    break

        return {"events": events[:limit], "count": len(events), "query": query}
    except Exception as exc:
        logger.error("search_markets failed: %s", exc)
        return {"error": str(exc)}


async def _discover_events(args: dict) -> dict:
    """Browse/filter events via /events. Client-side filters for volume/liquidity/dates."""
    try:
        limit = args.get("limit", 15)

        # Only pass params the Gamma API actually supports
        params: dict[str, Any] = {
            "active": "true",
            "closed": "false",
            "limit": 100,  # fetch more, filter client-side
        }

        if args.get("tag_slug"):
            params["tag_slug"] = args["tag_slug"]

        client = _http()
        resp = await client.get(f"{GAMMA_API_URL}/events", params=params)
        if resp.status_code != 200:
            return {"error": f"Events API returned {resp.status_code}"}

        events_raw = resp.json()

        # Client-side filtering for params the API doesn't support
        volume_min = args.get("volume_min")
        liquidity_min = args.get("liquidity_min")
        end_date_min = args.get("end_date_min")
        end_date_max = args.get("end_date_max")

        filtered = []
        for e in events_raw:
            if not isinstance(e, dict):
                continue
            if volume_min is not None:
                vol = float(e.get("volume", 0) or 0)
                if vol < volume_min:
                    continue
            if liquidity_min is not None:
                liq = float(e.get("liquidity", 0) or 0)
                if liq < liquidity_min:
                    continue
            if end_date_min and e.get("endDate"):
                if e["endDate"] < end_date_min:
                    continue
            if end_date_max and e.get("endDate"):
                if e["endDate"] > end_date_max:
                    continue
            filtered.append(e)

        # Sort by volume (24hr) descending by default
        order = args.get("order", "volume_24hr")
        sort_key = {
            "volume_24hr": "volume24hr",
            "volume": "volume",
            "liquidity": "liquidity",
            "end_date": "endDate",
        }.get(order, "volume24hr")
        filtered.sort(key=lambda e: float(e.get(sort_key, 0) or 0), reverse=(order != "end_date"))

        events = [_format_event(e) for e in filtered[:limit]]

        return {
            "events": events,
            "count": len(events),
        }
    except Exception as exc:
        logger.error("discover_events failed: %s", exc)
        return {"error": str(exc)}


async def _get_tags(args: dict) -> dict:
    """List all available category tags."""
    try:
        client = _http()
        resp = await client.get(f"{GAMMA_API_URL}/tags")
        if resp.status_code != 200:
            return {"error": f"Tags API returned {resp.status_code}"}

        tags = resp.json()
        return {
            "tags": [
                {"id": t.get("id"), "label": t.get("label", ""), "slug": t.get("slug", "")}
                for t in tags[:50]
            ],
            "count": len(tags),
        }
    except Exception as exc:
        logger.error("get_tags failed: %s", exc)
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Implementations — Market/Event Details
# ---------------------------------------------------------------------------


async def _get_market_details(args: dict) -> dict:
    try:
        market_id = args["market_id"]
        client = _http()

        # Try direct fetch first
        resp = await client.get(f"{GAMMA_API_URL}/markets/{market_id}")
        if resp.status_code == 200:
            data = resp.json()
        else:
            # Fallback: try by slug, then by condition_id
            data = None
            for param in ("slug", "condition_id"):
                resp2 = await client.get(
                    f"{GAMMA_API_URL}/markets",
                    params={param: market_id, "limit": 1},
                )
                if resp2.status_code == 200:
                    results = resp2.json()
                    if results:
                        data = results[0] if isinstance(results, list) else results
                        break
            if data is None:
                return {"error": f"Market not found: {market_id}"}

        return {
            "question": data.get("question", ""),
            "description": data.get("description", ""),
            "resolution_source": data.get("resolutionSource", ""),
            "end_date": data.get("endDate", ""),
            "outcomes": data.get("outcomes", []),
            "outcome_prices": data.get("outcomePrices", []),
            "volume": data.get("volume", 0),
            "volume_24hr": data.get("volume24hr", 0),
            "liquidity": data.get("liquidity", 0),
            "active": data.get("active", False),
            "closed": data.get("closed", False),
            "condition_id": data.get("conditionId", ""),
            "slug": data.get("slug", ""),
            "tokens": data.get("clobTokenIds", []),
        }
    except Exception as exc:
        logger.error("get_market_details failed: %s", exc)
        return {"error": str(exc)}


async def _get_event_details(args: dict) -> dict:
    """Fetch an event and all its child markets."""
    try:
        event_id = args["event_id"]
        client = _http()

        # Try direct ID fetch
        resp = await client.get(f"{GAMMA_API_URL}/events/{event_id}")
        if resp.status_code == 200:
            data = resp.json()
        else:
            # Fallback: try by slug
            resp2 = await client.get(
                f"{GAMMA_API_URL}/events",
                params={"slug": event_id, "limit": 1},
            )
            if resp2.status_code == 200:
                results = resp2.json()
                if results:
                    data = results[0] if isinstance(results, list) else results
                else:
                    return {"error": f"Event not found: {event_id}"}
            else:
                return {"error": f"Event not found: {event_id}"}

        return _format_event(data, include_markets=True)
    except Exception as exc:
        logger.error("get_event_details failed: %s", exc)
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Implementations — Prices & Orderbook
# ---------------------------------------------------------------------------


async def _get_live_prices(args: dict) -> dict:
    try:
        from services.market_runtime import get_market_runtime

        runtime = get_market_runtime()
        if runtime is None:
            return {"error": "MarketRuntime not initialized"}

        markets = runtime.get_crypto_markets()
        prices = {}
        for m in markets:
            cid = m.get("conditionId") or m.get("condition_id", "")
            best_bid = m.get("bestBid") or m.get("best_bid")
            best_ask = m.get("bestAsk") or m.get("best_ask")
            if best_bid is not None and best_ask is not None:
                try:
                    prices[cid] = round((float(best_bid) + float(best_ask)) / 2, 4)
                except (ValueError, TypeError):
                    pass

        return {"prices": prices, "count": len(prices)}
    except Exception as exc:
        logger.error("get_live_prices failed: %s", exc)
        return {"error": str(exc)}


async def _get_price_history(args: dict) -> dict:
    try:
        from services.strategy_sdk import StrategySDK

        token_id = args["token_id"]
        lookback_hours = args.get("lookback_hours", 24)

        import time
        end_ts = int(time.time())
        start_ts = end_ts - (lookback_hours * 3600)

        history = StrategySDK.get_price_history(
            token_id=token_id,
            start_time=start_ts,
            end_time=end_ts,
        )
        return {"token_id": token_id, "lookback_hours": lookback_hours, "data": history}
    except Exception as exc:
        logger.error("get_price_history failed: %s", exc)
        return {"error": str(exc)}


async def _check_orderbook(args: dict) -> dict:
    try:
        token_id = args["token_id"]
        client = _http()
        resp = await client.get(f"{CLOB_API_URL}/book", params={"token_id": token_id})
        if resp.status_code != 200:
            return {"error": f"Order book request failed (HTTP {resp.status_code})"}

        book = resp.json()
        bids = book.get("bids", [])
        asks = book.get("asks", [])

        bid_levels = [{"price": float(b["price"]), "size": float(b["size"])} for b in bids[:10]]
        ask_levels = [{"price": float(a["price"]), "size": float(a["size"])} for a in asks[:10]]

        best_bid = max((b["price"] for b in bid_levels), default=0.0)
        best_ask = min((a["price"] for a in ask_levels), default=1.0)
        spread = best_ask - best_bid if best_ask > best_bid else 0.0
        mid_price = (best_bid + best_ask) / 2 if (best_bid + best_ask) > 0 else 0.0

        total_bid_liq = sum(b["price"] * b["size"] for b in bid_levels)
        total_ask_liq = sum(a["price"] * a["size"] for a in ask_levels)

        return {
            "token_id": token_id,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": round(spread, 4),
            "spread_pct": round(spread / mid_price * 100, 2) if mid_price > 0 else 0.0,
            "mid_price": round(mid_price, 4),
            "bid_levels": bid_levels[:5],
            "ask_levels": ask_levels[:5],
            "total_bid_liquidity_usd": round(total_bid_liq, 2),
            "total_ask_liquidity_usd": round(total_ask_liq, 2),
            "bid_depth": len(bids),
            "ask_depth": len(asks),
        }
    except Exception as exc:
        logger.error("check_orderbook failed: %s", exc)
        return {"error": str(exc)}


async def _get_market_prices(args: dict) -> dict:
    """Get reliable live prices from CLOB /price and /midpoint endpoints."""
    try:
        token_ids = args["token_ids"]
        client = _http()
        results = []

        for tid in token_ids[:10]:  # Cap at 10 to avoid abuse
            try:
                # Fetch midpoint + bid/ask prices in parallel
                mid_resp, bid_resp, ask_resp = await asyncio.gather(
                    client.get(f"{CLOB_API_URL}/midpoint", params={"token_id": tid}),
                    client.get(f"{CLOB_API_URL}/price", params={"token_id": tid, "side": "BUY"}),
                    client.get(f"{CLOB_API_URL}/price", params={"token_id": tid, "side": "SELL"}),
                )
                mid = float(mid_resp.json().get("mid", 0)) if mid_resp.status_code == 200 else None
                bid = float(bid_resp.json().get("price", 0)) if bid_resp.status_code == 200 else None
                ask = float(ask_resp.json().get("price", 0)) if ask_resp.status_code == 200 else None
                spread = round(ask - bid, 4) if bid is not None and ask is not None else None

                results.append({
                    "token_id": tid,
                    "midpoint": mid,
                    "best_bid": bid,
                    "best_ask": ask,
                    "spread": spread,
                })
            except Exception:
                results.append({"token_id": tid, "error": "Failed to fetch price"})

        return {"prices": results, "count": len(results)}
    except Exception as exc:
        logger.error("get_market_prices failed: %s", exc)
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Implementations — Analysis
# ---------------------------------------------------------------------------


async def _get_market_regime(args: dict) -> dict:
    try:
        from services.strategy_sdk import StrategySDK

        market_id = args["market_id"]
        regime = StrategySDK.get_market_regime(market_id)
        return {"market_id": market_id, "regime": regime}
    except Exception as exc:
        logger.error("get_market_regime failed: %s", exc)
        return {"error": str(exc)}


async def _get_active_markets_summary(args: dict) -> dict:
    try:
        from services.market_runtime import get_market_runtime

        runtime = get_market_runtime()
        if runtime is None:
            return {"error": "MarketRuntime not initialized"}

        markets = runtime.get_crypto_markets()
        active = [m for m in markets if m.get("active")]

        # Sort by volume descending
        by_volume = sorted(active, key=lambda m: float(m.get("volume", 0) or 0), reverse=True)

        top_markets = []
        for m in by_volume[:15]:
            top_markets.append({
                "question": m.get("question", ""),
                "condition_id": m.get("conditionId") or m.get("condition_id", ""),
                "volume": m.get("volume", 0),
                "liquidity": m.get("liquidity", 0),
            })

        return {
            "total_active": len(active),
            "total_tracked": len(markets),
            "top_by_volume": top_markets,
        }
    except Exception as exc:
        logger.error("get_active_markets_summary failed: %s", exc)
        return {"error": str(exc)}


