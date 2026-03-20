"""Wallet discovery & trader intelligence tools."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def build_tools() -> list:
    from services.ai.agent import AgentTool

    return [
        AgentTool(
            name="get_wallet_profile",
            description=(
                "Get a detailed profile for a specific wallet address — win rate, ROI, "
                "Sharpe ratio, recent trades, position count, and tags."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "wallet_address": {
                        "type": "string",
                        "description": "The 0x... wallet address",
                    },
                },
                "required": ["wallet_address"],
            },
            handler=_get_wallet_profile,
            max_calls=5,
            category="wallets",
        ),
        AgentTool(
            name="get_wallet_leaderboard",
            description=(
                "Get the top-performing wallets ranked by various metrics. "
                "Categories: overall, profit, win_rate, volume, sharpe. "
                "Time periods: 7d, 30d, 90d, all."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "enum": ["overall", "profit", "win_rate", "volume", "sharpe"],
                        "description": "Ranking category (default: overall)",
                        "default": "overall",
                    },
                    "time_period": {
                        "type": "string",
                        "enum": ["7d", "30d", "90d", "all"],
                        "description": "Time period (default: 30d)",
                        "default": "30d",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of wallets to return (default 20)",
                        "default": 20,
                    },
                },
                "required": [],
            },
            handler=_get_wallet_leaderboard,
            max_calls=3,
            category="wallets",
        ),
        AgentTool(
            name="get_smart_pool_stats",
            description=(
                "Get current Smart Pool composition, health metrics, and member stats. "
                "The Smart Pool is the curated set of top-performing wallets the system "
                "copy-trades from."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_smart_pool_stats,
            max_calls=2,
            category="wallets",
        ),
        AgentTool(
            name="get_confluence_signals",
            description=(
                "Get active confluence signals — when multiple tracked wallets are "
                "trading the same market in the same direction. Strong indicator of "
                "smart money agreement."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_confluence_signals,
            max_calls=3,
            category="wallets",
        ),
        AgentTool(
            name="get_tracked_wallets",
            description=(
                "List all wallets currently being tracked by the system, including "
                "their tier (low/medium/high/extreme), tags, and recent activity."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Max wallets to return (default 50)",
                        "default": 50,
                    },
                },
                "required": [],
            },
            handler=_get_tracked_wallets,
            max_calls=3,
            category="wallets",
        ),
        AgentTool(
            name="get_trader_groups",
            description=(
                "List all trader groups/clusters — named collections of wallets "
                "grouped by trading behavior, strategy, or manual curation."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_trader_groups,
            max_calls=2,
            category="wallets",
        ),
        AgentTool(
            name="get_whale_clusters",
            description=(
                "Get whale cluster analysis — groups of high-volume wallets that "
                "trade in coordinated patterns. Useful for detecting institutional "
                "or whale activity."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_whale_clusters,
            max_calls=2,
            category="wallets",
        ),
    ]


# ---------------------------------------------------------------------------
# Implementations
# ---------------------------------------------------------------------------


async def _get_wallet_profile(args: dict) -> dict:
    try:
        from services.wallet_discovery import WalletDiscoveryEngine

        engine = WalletDiscoveryEngine.instance()
        if engine is None:
            return {"error": "WalletDiscoveryEngine not initialized"}

        wallet = args["wallet_address"]
        profile = await engine.get_wallet_profile(wallet)
        return profile if isinstance(profile, dict) else {"profile": profile}
    except Exception as exc:
        logger.error("get_wallet_profile failed: %s", exc)
        return {"error": str(exc)}


async def _get_wallet_leaderboard(args: dict) -> dict:
    try:
        from services.wallet_discovery import WalletDiscoveryEngine

        engine = WalletDiscoveryEngine.instance()
        if engine is None:
            return {"error": "WalletDiscoveryEngine not initialized"}

        category = args.get("category", "overall")
        time_period = args.get("time_period", "30d")
        limit = args.get("limit", 20)

        page = await engine.get_leaderboard_page(
            category=category,
            time_period=time_period,
            sort="rank",
            page=1,
        )
        if isinstance(page, dict):
            wallets = page.get("wallets", page.get("results", []))[:limit]
            return {"wallets": wallets, "count": len(wallets), "category": category, "time_period": time_period}
        return {"wallets": [], "count": 0}
    except Exception as exc:
        logger.error("get_wallet_leaderboard failed: %s", exc)
        return {"error": str(exc)}


async def _get_smart_pool_stats(args: dict) -> dict:
    try:
        from models.database import AsyncSessionLocal, DiscoveredWallet
        from sqlalchemy import select, func

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(func.count(DiscoveredWallet.address)).where(
                    DiscoveredWallet.in_pool == True  # noqa: E712
                )
            )
            pool_count = result.scalar() or 0

            result2 = await session.execute(
                select(DiscoveredWallet).where(
                    DiscoveredWallet.in_pool == True  # noqa: E712
                ).limit(30)
            )
            members = result2.scalars().all()

        pool_members = []
        for m in members:
            pool_members.append({
                "address": m.address,
                "tier": getattr(m, "tier", None),
                "win_rate": float(m.win_rate) if getattr(m, "win_rate", None) else None,
                "roi": float(m.roi) if getattr(m, "roi", None) else None,
                "rank_score": float(m.rank_score) if getattr(m, "rank_score", None) else None,
            })

        return {"pool_size": pool_count, "members": pool_members}
    except Exception as exc:
        logger.error("get_smart_pool_stats failed: %s", exc)
        return {"error": str(exc)}


async def _get_confluence_signals(args: dict) -> dict:
    try:
        from services.strategy_sdk import StrategySDK

        signals = StrategySDK.get_trader_confluence_signals()
        return {"signals": signals if isinstance(signals, list) else [], "count": len(signals) if isinstance(signals, list) else 0}
    except Exception as exc:
        logger.error("get_confluence_signals failed: %s", exc)
        return {"error": str(exc)}


async def _get_tracked_wallets(args: dict) -> dict:
    try:
        from models.database import AsyncSessionLocal, TrackedWallet
        from sqlalchemy import select

        limit = args.get("limit", 50)

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(TrackedWallet).limit(limit)
            )
            rows = result.scalars().all()

        wallets = []
        for r in rows:
            wallets.append({
                "address": r.address,
                "label": getattr(r, "label", None),
                "tier": getattr(r, "tier", None),
                "tags": getattr(r, "tags", []),
                "active": getattr(r, "active", True),
            })

        return {"wallets": wallets, "count": len(wallets)}
    except Exception as exc:
        logger.error("get_tracked_wallets failed: %s", exc)
        return {"error": str(exc)}


async def _get_trader_groups(args: dict) -> dict:
    try:
        from services.strategy_sdk import StrategySDK

        groups = StrategySDK.get_trader_groups()
        return {"groups": groups if isinstance(groups, list) else [], "count": len(groups) if isinstance(groups, list) else 0}
    except Exception as exc:
        logger.error("get_trader_groups failed: %s", exc)
        return {"error": str(exc)}


async def _get_whale_clusters(args: dict) -> dict:
    try:
        from services.strategy_sdk import StrategySDK

        clusters = StrategySDK.get_whale_cohorts()
        return {"clusters": clusters if isinstance(clusters, list) else [], "count": len(clusters) if isinstance(clusters, list) else 0}
    except Exception as exc:
        logger.error("get_whale_clusters failed: %s", exc)
        return {"error": str(exc)}
