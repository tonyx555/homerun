"""Market data tools — search, prices, order books, regime detection."""

from __future__ import annotations

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
        AgentTool(
            name="search_markets",
            description=(
                "Search Polymarket for active prediction markets matching a query. "
                "Returns market question, current prices, volume, liquidity, and IDs. "
                "Use this to discover markets before deeper analysis."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (e.g. 'Trump', 'Bitcoin price', 'Fed rate')",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (default 10)",
                        "default": 10,
                    },
                    "active_only": {
                        "type": "boolean",
                        "description": "Only return active (non-closed) markets",
                        "default": True,
                    },
                },
                "required": ["query"],
            },
            handler=_search_markets,
            max_calls=5,
            category="market_data",
        ),
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
            max_calls=5,
            category="market_data",
        ),
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
            max_calls=5,
            category="market_data",
        ),
        AgentTool(
            name="check_orderbook",
            description=(
                "Check the order book depth and liquidity for a market token. "
                "Returns bid/ask spreads, depth, and available liquidity at different "
                "price levels."
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
            max_calls=5,
            category="market_data",
        ),
        AgentTool(
            name="find_related_markets",
            description=(
                "Find other Polymarket markets related to the same event or topic. "
                "Useful for cross-market analysis, correlation detection, and finding "
                "arbitrage opportunities."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "event_id": {
                        "type": "string",
                        "description": "Event ID to find sibling markets",
                    },
                    "search_query": {
                        "type": "string",
                        "description": "Text search for related markets",
                    },
                },
            },
            handler=_find_related_markets,
            max_calls=3,
            category="market_data",
        ),
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
            max_calls=5,
            category="market_data",
        ),
        AgentTool(
            name="get_active_markets_summary",
            description=(
                "Get a summary of all currently active markets the system is monitoring. "
                "Returns count, top markets by volume, recent price movers, and overall "
                "market health metrics."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_active_markets_summary,
            max_calls=2,
            category="market_data",
        ),
    ]


# ---------------------------------------------------------------------------
# Implementations
# ---------------------------------------------------------------------------


async def _search_markets(args: dict) -> dict:
    try:
        query = args["query"]
        limit = args.get("limit", 10)
        active_only = args.get("active_only", True)

        # Fetch more results for client-side filtering when a query is specified.
        fetch_limit = max(limit * 5, 100) if query else limit
        params: dict[str, Any] = {"limit": fetch_limit}
        if active_only:
            params["active"] = "true"
            params["closed"] = "false"

        client = _http()
        resp = await client.get(f"{GAMMA_API_URL}/markets", params=params)
        if resp.status_code != 200:
            return {"error": f"Gamma API returned {resp.status_code}"}

        all_markets = resp.json()
        # Client-side filter by query (each word must match)
        if query:
            query_words = query.lower().split()
            matched = []
            for m in all_markets:
                text = (m.get("question", "") + " " + m.get("description", "")).lower()
                if all(w in text for w in query_words):
                    matched.append(m)
                if len(matched) >= limit:
                    break
        else:
            matched = all_markets[:limit]

        results = []
        for m in matched:
            results.append({
                "condition_id": m.get("conditionId", ""),
                "question": m.get("question", ""),
                "outcome_prices": m.get("outcomePrices", []),
                "volume": m.get("volume", 0),
                "liquidity": m.get("liquidity", 0),
                "end_date": m.get("endDate", ""),
                "slug": m.get("slug", ""),
                "active": m.get("active", False),
            })

        return {"markets": results, "count": len(results), "query": query}
    except Exception as exc:
        logger.error("search_markets failed: %s", exc)
        return {"error": str(exc)}


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


async def _find_related_markets(args: dict) -> dict:
    try:
        event_id = args.get("event_id")
        search_query = args.get("search_query")
        results: list[dict] = []
        client = _http()

        if event_id:
            resp = await client.get(
                f"{GAMMA_API_URL}/markets",
                params={"event_id": event_id, "limit": 20},
            )
            if resp.status_code == 200:
                for m in resp.json():
                    results.append({
                        "condition_id": m.get("conditionId", ""),
                        "question": m.get("question", ""),
                        "outcome_prices": m.get("outcomePrices", []),
                        "volume": m.get("volume", 0),
                        "liquidity": m.get("liquidity", 0),
                        "slug": m.get("slug", ""),
                    })

        if search_query:
            resp = await client.get(
                f"{GAMMA_API_URL}/markets",
                params={"active": "true", "closed": "false", "limit": 20},
            )
            if resp.status_code == 200:
                q = search_query.lower()
                for m in resp.json():
                    if q in m.get("question", "").lower():
                        results.append({
                            "condition_id": m.get("conditionId", ""),
                            "question": m.get("question", ""),
                            "outcome_prices": m.get("outcomePrices", []),
                            "volume": m.get("volume", 0),
                            "liquidity": m.get("liquidity", 0),
                            "slug": m.get("slug", ""),
                        })

        # Deduplicate
        seen = set()
        unique = []
        for r in results:
            cid = r["condition_id"]
            if cid and cid not in seen:
                seen.add(cid)
                unique.append(r)

        return {"related_markets": unique, "count": len(unique)}
    except Exception as exc:
        logger.error("find_related_markets failed: %s", exc)
        return {"error": str(exc)}


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
