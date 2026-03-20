"""Web search & fetch tools — give agents access to the internet."""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = 20


def build_tools() -> list:
    from services.ai.agent import AgentTool

    return [
        AgentTool(
            name="web_search",
            description=(
                "Search the web using a search engine. Returns titles, snippets, "
                "and URLs for the top results. Useful for researching current events, "
                "verifying facts, or finding information not in the local database."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Max results to return (default 5)",
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
            handler=_web_search,
            max_calls=5,
            category="web",
        ),
        AgentTool(
            name="fetch_webpage",
            description=(
                "Fetch the text content of a webpage by URL. Extracts the main "
                "body text, stripping HTML. Useful for reading articles, docs, "
                "or data pages found via web_search."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "URL to fetch",
                    },
                },
                "required": ["url"],
            },
            handler=_fetch_webpage,
            max_calls=5,
            category="web",
        ),
    ]


# ---------------------------------------------------------------------------
# Implementations
# ---------------------------------------------------------------------------


async def _web_search(args: dict) -> dict:
    """Search the web using available search providers."""
    query = args["query"]
    max_results = args.get("max_results", 5)

    # Try multiple search backends in priority order
    # 1. SerpAPI (if key available)
    try:
        result = await _search_serpapi(query, max_results)
        if result and not result.get("error"):
            return result
    except Exception:
        pass

    # 2. Brave Search API (if key available)
    try:
        result = await _search_brave(query, max_results)
        if result and not result.get("error"):
            return result
    except Exception:
        pass

    # 3. DuckDuckGo HTML (no API key needed)
    try:
        result = await _search_duckduckgo(query, max_results)
        if result and not result.get("error"):
            return result
    except Exception:
        pass

    return {"error": "No search provider available. Configure SERPAPI_KEY or BRAVE_SEARCH_KEY in settings.", "query": query}


async def _search_serpapi(query: str, max_results: int) -> dict:
    from models.database import AsyncSessionLocal, AppSettings
    from sqlalchemy import select

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AppSettings).where(AppSettings.key == "serpapi_key")
        )
        row = result.scalar_one_or_none()

    if not row or not row.value:
        return {"error": "SERPAPI_KEY not configured"}

    api_key = row.value
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        resp = await client.get(
            "https://serpapi.com/search",
            params={"q": query, "api_key": api_key, "num": max_results, "engine": "google"},
        )
        if resp.status_code != 200:
            return {"error": f"SerpAPI returned {resp.status_code}"}

        data = resp.json()
        results = []
        for item in data.get("organic_results", [])[:max_results]:
            results.append({
                "title": item.get("title", ""),
                "snippet": item.get("snippet", ""),
                "url": item.get("link", ""),
            })

        return {"results": results, "count": len(results), "query": query, "provider": "serpapi"}


async def _search_brave(query: str, max_results: int) -> dict:
    from models.database import AsyncSessionLocal, AppSettings
    from sqlalchemy import select

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AppSettings).where(AppSettings.key == "brave_search_key")
        )
        row = result.scalar_one_or_none()

    if not row or not row.value:
        return {"error": "BRAVE_SEARCH_KEY not configured"}

    api_key = row.value
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        resp = await client.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={"X-Subscription-Token": api_key, "Accept": "application/json"},
            params={"q": query, "count": max_results},
        )
        if resp.status_code != 200:
            return {"error": f"Brave Search returned {resp.status_code}"}

        data = resp.json()
        results = []
        for item in data.get("web", {}).get("results", [])[:max_results]:
            results.append({
                "title": item.get("title", ""),
                "snippet": item.get("description", ""),
                "url": item.get("url", ""),
            })

        return {"results": results, "count": len(results), "query": query, "provider": "brave"}


async def _search_duckduckgo(query: str, max_results: int) -> dict:
    """DuckDuckGo Instant Answer API — no API key needed."""
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        resp = await client.get(
            "https://api.duckduckgo.com/",
            params={"q": query, "format": "json", "no_html": "1", "skip_disambig": "1"},
        )
        if resp.status_code != 200:
            return {"error": f"DuckDuckGo returned {resp.status_code}"}

        data = resp.json()
        results = []

        # Abstract (main result)
        if data.get("Abstract"):
            results.append({
                "title": data.get("Heading", query),
                "snippet": data.get("Abstract", ""),
                "url": data.get("AbstractURL", ""),
            })

        # Related topics
        for item in data.get("RelatedTopics", [])[:max_results]:
            if isinstance(item, dict) and item.get("Text"):
                results.append({
                    "title": item.get("Text", "")[:80],
                    "snippet": item.get("Text", ""),
                    "url": item.get("FirstURL", ""),
                })

        if not results:
            return {"error": "No results from DuckDuckGo", "query": query}

        return {"results": results[:max_results], "count": len(results[:max_results]), "query": query, "provider": "duckduckgo"}


async def _fetch_webpage(args: dict) -> dict:
    """Fetch and extract text from a URL."""
    try:
        url = args["url"]

        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0 HomerunBot/1.0"})
            if resp.status_code != 200:
                return {"error": f"HTTP {resp.status_code} fetching {url}"}

            content_type = resp.headers.get("content-type", "")
            if "json" in content_type:
                return {"url": url, "content_type": "json", "data": resp.json()}

            html = resp.text

        # Basic HTML to text extraction
        text = _html_to_text(html)

        # Truncate to avoid blowing up context
        if len(text) > 6000:
            text = text[:6000] + "\n\n[... truncated ...]"

        return {"url": url, "content_type": "text", "text": text, "length": len(text)}
    except Exception as exc:
        logger.error("fetch_webpage failed: %s", exc)
        return {"error": str(exc)}


def _html_to_text(html: str) -> str:
    """Simple HTML-to-text extraction without external dependencies."""
    import re

    # Remove script and style blocks
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Decode common entities
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#39;", "'").replace("&nbsp;", " ")
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # Restore some structure with line breaks
    text = re.sub(r"\s{3,}", "\n\n", text)
    return text
