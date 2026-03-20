"""System tools — status, settings, diagnostics."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def build_tools() -> list:
    from services.ai.agent import AgentTool

    return [
        AgentTool(
            name="get_system_status",
            description=(
                "Get the overall system health status — running workers, WebSocket "
                "feed status, market runtime health, discovery engine status, and "
                "any active errors or warnings."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_system_status,
            max_calls=2,
            category="system",
        ),
        AgentTool(
            name="get_settings",
            description=(
                "Get current system settings for a specific category. Categories: "
                "polymarket, kalshi, llm, notifications, scanner, live_execution, "
                "discovery, trading_proxy, search_filter."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "description": "Settings category to retrieve",
                        "enum": [
                            "polymarket", "kalshi", "llm", "notifications",
                            "scanner", "live_execution", "discovery",
                            "trading_proxy", "search_filter",
                        ],
                    },
                },
                "required": ["category"],
            },
            handler=_get_settings,
            max_calls=5,
            category="system",
        ),
        AgentTool(
            name="update_setting",
            description=(
                "Update a specific setting value. Provide the setting key and "
                "new value. Use get_settings first to see current values."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "Setting key (e.g. 'max_position_size', 'min_liquidity')",
                    },
                    "value": {
                        "type": "string",
                        "description": "New value (will be type-coerced)",
                    },
                },
                "required": ["key", "value"],
            },
            handler=_update_setting,
            max_calls=5,
            category="system",
        ),
        AgentTool(
            name="get_llm_usage_stats",
            description=(
                "Get LLM API usage statistics — total calls, token counts, costs, "
                "and breakdown by provider and purpose. Useful for monitoring spend."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "hours": {
                        "type": "integer",
                        "description": "Lookback hours (default 24)",
                        "default": 24,
                    },
                },
                "required": [],
            },
            handler=_get_llm_usage_stats,
            max_calls=3,
            category="system",
        ),
        AgentTool(
            name="get_data_sources",
            description=(
                "List all configured data sources — custom data feeds that pull "
                "from external APIs, RSS feeds, or scrapers. Shows source name, "
                "schedule, last run status, and record count."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_data_sources,
            max_calls=2,
            category="system",
        ),
        AgentTool(
            name="query_data_source",
            description=(
                "Get recent records from a specific data source. Data sources are "
                "custom feeds that collect external data on a schedule."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "source_slug": {
                        "type": "string",
                        "description": "Data source slug identifier",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max records to return (default 20)",
                        "default": 20,
                    },
                },
                "required": ["source_slug"],
            },
            handler=_query_data_source,
            max_calls=5,
            category="system",
        ),
    ]


# ---------------------------------------------------------------------------
# Implementations
# ---------------------------------------------------------------------------


async def _get_system_status(args: dict) -> dict:
    try:
        status = {}

        # Market runtime
        try:
            from services.market_runtime import MarketRuntime
            runtime = MarketRuntime.instance()
            if runtime:
                crypto_status = runtime.get_crypto_status()
                status["market_runtime"] = {
                    "active": True,
                    "market_count": crypto_status.get("market_count", 0) if isinstance(crypto_status, dict) else 0,
                }
            else:
                status["market_runtime"] = {"active": False}
        except Exception as e:
            status["market_runtime"] = {"error": str(e)}

        # WS feeds
        try:
            from services.ws_feeds import WSFeedManager
            ws = WSFeedManager.instance()
            if ws:
                status["ws_feeds"] = {"active": True, "connected": getattr(ws, "connected", None)}
            else:
                status["ws_feeds"] = {"active": False}
        except Exception as e:
            status["ws_feeds"] = {"error": str(e)}

        # Strategy loader
        try:
            from services.strategy_loader import StrategyLoader
            loader = StrategyLoader.instance()
            if loader:
                all_statuses = loader.get_all_statuses()
                status["strategies"] = {
                    "loaded_count": len(all_statuses) if all_statuses else 0,
                }
            else:
                status["strategies"] = {"loaded_count": 0}
        except Exception as e:
            status["strategies"] = {"error": str(e)}

        return {"status": status}
    except Exception as exc:
        logger.error("get_system_status failed: %s", exc)
        return {"error": str(exc)}


async def _get_settings(args: dict) -> dict:
    try:
        from models.database import AppSettings, AsyncSessionLocal
        from sqlalchemy import select
        from sqlalchemy.inspection import inspect

        category = args["category"]

        # Map category to column name prefixes
        prefix_map = {
            "polymarket": "polymarket_",
            "kalshi": "kalshi_",
            "llm": ["llm_", "openai_", "anthropic_", "google_", "xai_", "deepseek_", "ollama_", "lmstudio_"],
            "notifications": "notification_",
            "scanner": "scanner_",
            "live_execution": "live_",
            "discovery": "discovery_",
            "trading_proxy": "trading_proxy_",
            "search_filter": "search_filter_",
        }

        prefixes = prefix_map.get(category)
        if not prefixes:
            return {"error": f"Unknown category: {category}"}
        if isinstance(prefixes, str):
            prefixes = [prefixes]

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(AppSettings).where(AppSettings.id == "default")
            )
            row = result.scalar_one_or_none()

        if not row:
            return {"category": category, "settings": {}}

        # Extract columns matching the prefix
        mapper = inspect(AppSettings)
        settings = {}
        sensitive_keywords = {"key", "secret", "token", "password"}
        for col in mapper.columns:
            col_name = col.key
            if any(col_name.startswith(p) for p in prefixes):
                val = getattr(row, col_name, None)
                # Mask sensitive values
                if any(s in col_name.lower() for s in sensitive_keywords) and val:
                    settings[col_name] = "***" + str(val)[-4:] if len(str(val)) > 4 else "***"
                else:
                    settings[col_name] = val

        return {"category": category, "settings": settings}
    except Exception as exc:
        logger.error("get_settings failed: %s", exc)
        return {"error": str(exc)}


async def _update_setting(args: dict) -> dict:
    try:
        from models.database import AppSettings, AsyncSessionLocal
        from sqlalchemy import select

        key = args["key"]
        value = args["value"]

        # Block updating secrets via agent
        sensitive_keywords = {"api_key", "secret", "password", "private_key", "token"}
        if any(s in key.lower() for s in sensitive_keywords):
            return {"error": "Cannot update sensitive settings (API keys, secrets) via agent. Use the Settings UI."}

        # Verify the column exists on AppSettings
        if not hasattr(AppSettings, key):
            return {"error": f"Unknown setting: {key}. Use get_settings to see available settings."}

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(AppSettings).where(AppSettings.id == "default")
            )
            row = result.scalar_one_or_none()
            if not row:
                return {"error": "AppSettings not initialized"}

            old_value = getattr(row, key, None)
            setattr(row, key, value)
            await session.commit()

        return {"key": key, "old_value": old_value, "new_value": value, "updated": True}
    except Exception as exc:
        logger.error("update_setting failed: %s", exc)
        return {"error": str(exc)}


async def _get_llm_usage_stats(args: dict) -> dict:
    try:
        from datetime import datetime, timedelta, timezone

        from models.database import AsyncSessionLocal, LLMUsageLog
        from sqlalchemy import func, select

        hours = args.get("hours", 24)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        async with AsyncSessionLocal() as session:
            # Total stats
            result = await session.execute(
                select(
                    func.count(LLMUsageLog.id),
                    func.sum(LLMUsageLog.input_tokens),
                    func.sum(LLMUsageLog.output_tokens),
                    func.sum(LLMUsageLog.cost_usd),
                ).where(LLMUsageLog.requested_at >= cutoff)
            )
            row = result.one()
            total_calls, total_input, total_output, total_cost = row

            # By provider
            result2 = await session.execute(
                select(
                    LLMUsageLog.provider,
                    func.count(LLMUsageLog.id),
                    func.sum(LLMUsageLog.cost_usd),
                ).where(LLMUsageLog.requested_at >= cutoff)
                .group_by(LLMUsageLog.provider)
            )
            by_provider = [{"provider": r[0], "calls": r[1], "cost_usd": round(r[2] or 0, 4)} for r in result2.all()]

        return {
            "hours": hours,
            "total_calls": total_calls or 0,
            "total_input_tokens": total_input or 0,
            "total_output_tokens": total_output or 0,
            "total_cost_usd": round(total_cost or 0, 4),
            "by_provider": by_provider,
        }
    except Exception as exc:
        logger.error("get_llm_usage_stats failed: %s", exc)
        return {"error": str(exc)}


async def _get_data_sources(args: dict) -> dict:
    try:
        from models.database import AsyncSessionLocal, DataSource
        from sqlalchemy import select

        async with AsyncSessionLocal() as session:
            result = await session.execute(select(DataSource))
            rows = result.scalars().all()

        sources = []
        for r in rows:
            sources.append({
                "id": r.id,
                "name": getattr(r, "name", None),
                "slug": getattr(r, "slug", None),
                "enabled": getattr(r, "enabled", True),
                "schedule": getattr(r, "schedule", None),
                "last_run_at": getattr(r, "last_run_at", None).isoformat() if getattr(r, "last_run_at", None) else None,
                "last_run_status": getattr(r, "last_run_status", None),
            })

        return {"data_sources": sources, "count": len(sources)}
    except Exception as exc:
        logger.error("get_data_sources failed: %s", exc)
        return {"error": str(exc)}


async def _query_data_source(args: dict) -> dict:
    try:
        from services.strategy_sdk import StrategySDK

        source_slug = args["source_slug"]
        limit = args.get("limit", 20)

        records = StrategySDK.get_data_records(source_slug, limit=limit)
        return {
            "source_slug": source_slug,
            "records": records if isinstance(records, list) else [],
            "count": len(records) if isinstance(records, list) else 0,
        }
    except Exception as exc:
        logger.error("query_data_source failed: %s", exc)
        return {"error": str(exc)}
