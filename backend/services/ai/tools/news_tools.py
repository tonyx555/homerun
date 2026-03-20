"""News & sentiment analysis tools."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def build_tools() -> list:
    from services.ai.agent import AgentTool

    return [
        AgentTool(
            name="search_news",
            description=(
                "Search for recent news articles related to a topic or market. "
                "Returns headlines, summaries, sources, and sentiment analysis."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query for news articles",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum articles to return (default 5)",
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
            handler=_search_news,
            max_calls=5,
            category="news",
        ),
        AgentTool(
            name="get_news_edges",
            description=(
                "Get detected 'news edges' — breaking news events that are likely "
                "to move prediction market prices. The edge detector matches news "
                "to markets and estimates impact."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_news_edges,
            max_calls=3,
            category="news",
        ),
        AgentTool(
            name="analyze_market_sentiment",
            description=(
                "Perform deep sentiment analysis on a market by aggregating recent "
                "news, social signals, and price action. Returns an overall sentiment "
                "score and breakdown."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Market topic or question to analyze sentiment for",
                    },
                    "max_articles": {
                        "type": "integer",
                        "description": "Max articles to analyze (default 10)",
                        "default": 10,
                    },
                },
                "required": ["query"],
            },
            handler=_analyze_market_sentiment,
            max_calls=3,
            category="news",
        ),
        AgentTool(
            name="analyze_resolution",
            description=(
                "Deep analysis of a market's resolution criteria — checks for "
                "ambiguities, edge cases, and risks that could lead to unexpected outcomes."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "market_id": {"type": "string", "description": "Market condition ID"},
                    "question": {"type": "string", "description": "Market question text"},
                    "description": {"type": "string", "default": ""},
                    "resolution_source": {"type": "string", "default": ""},
                },
                "required": ["market_id", "question"],
            },
            handler=_analyze_resolution,
            max_calls=3,
            category="news",
        ),
    ]


# ---------------------------------------------------------------------------
# Implementations
# ---------------------------------------------------------------------------


async def _search_news(args: dict) -> dict:
    try:
        query = args["query"]
        max_results = args.get("max_results", 5)

        try:
            from services.ai.news_sentiment import NewsSentimentAnalyzer
            analyzer = NewsSentimentAnalyzer()
            result = await analyzer.search_and_analyze(query=query, max_articles=max_results)
            return result
        except ImportError:
            pass

        # Fallback: try the feed service directly
        try:
            from services.news.feed_service import FeedService
            svc = FeedService.instance()
            if svc:
                articles = await svc.search_articles(query=query, limit=max_results)
                return {"articles": articles, "count": len(articles), "query": query}
        except Exception:
            pass

        return {"error": "News service not available", "query": query}
    except Exception as exc:
        logger.error("search_news failed: %s", exc)
        return {"error": str(exc)}


async def _get_news_edges(args: dict) -> dict:
    try:
        from services.news.edge_detector import EdgeDetector

        detector = EdgeDetector.instance()
        if detector is None:
            return {"error": "EdgeDetector not initialized"}

        edges = await detector.get_cached_edges()
        if isinstance(edges, list):
            return {"edges": edges, "count": len(edges)}
        return {"edges": edges}
    except Exception as exc:
        logger.error("get_news_edges failed: %s", exc)
        return {"error": str(exc)}


async def _analyze_market_sentiment(args: dict) -> dict:
    try:
        from services.ai.news_sentiment import NewsSentimentAnalyzer

        query = args["query"]
        max_articles = args.get("max_articles", 10)

        analyzer = NewsSentimentAnalyzer()
        result = await analyzer.search_and_analyze(query=query, max_articles=max_articles)
        return result
    except Exception as exc:
        logger.error("analyze_market_sentiment failed: %s", exc)
        return {"error": str(exc)}


async def _analyze_resolution(args: dict) -> dict:
    try:
        from services.ai.resolution_analyzer import ResolutionAnalyzer

        analyzer = ResolutionAnalyzer()
        result = await analyzer.analyze_market(
            market_id=args["market_id"],
            question=args["question"],
            description=args.get("description", ""),
            resolution_source=args.get("resolution_source", ""),
        )
        return result
    except ImportError:
        return {"error": "ResolutionAnalyzer not available"}
    except Exception as exc:
        logger.error("analyze_resolution failed: %s", exc)
        return {"error": str(exc)}
