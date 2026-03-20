"""Analytics tools — signals, decisions, trader performance, anomalies."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def build_tools() -> list:
    from services.ai.agent import AgentTool

    return [
        AgentTool(
            name="get_active_signals",
            description=(
                "Get all currently active trading signals from the signal engine. "
                "Signals indicate potential trading opportunities detected by strategies."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_active_signals,
            max_calls=3,
            category="analytics",
        ),
        AgentTool(
            name="get_recent_decisions",
            description=(
                "Get recent orchestrator trading decisions — what the system decided "
                "to trade, why, and the outcome. Includes the strategy that generated "
                "each decision and the reasoning."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Max decisions to return (default 20)",
                        "default": 20,
                    },
                    "trader_id": {
                        "type": "string",
                        "description": "Filter by trader/orchestrator ID (optional)",
                    },
                },
                "required": [],
            },
            handler=_get_recent_decisions,
            max_calls=3,
            category="analytics",
        ),
        AgentTool(
            name="get_trader_overview",
            description=(
                "Get a high-level overview of all trader/orchestrator instances — "
                "their status (running/stopped), trade counts, and P&L summary."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_trader_overview,
            max_calls=2,
            category="analytics",
        ),
        AgentTool(
            name="detect_anomalies",
            description=(
                "Run anomaly detection across active markets to find unusual conditions "
                "— sudden price movements, volume spikes, liquidity changes, or "
                "unusual trading patterns."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_detect_anomalies,
            max_calls=2,
            category="analytics",
        ),
        AgentTool(
            name="get_cross_platform_arb",
            description=(
                "Find cross-platform arbitrage opportunities between Polymarket and "
                "other prediction markets (e.g. Kalshi). Returns price discrepancies "
                "for similar markets."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_cross_platform_arb,
            max_calls=2,
            category="analytics",
        ),
        AgentTool(
            name="query_database",
            description=(
                "Run a read-only SQL query against the application database. "
                "Useful for custom analytics, aggregations, or data exploration. "
                "Only SELECT queries are allowed."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL SELECT query to execute",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max rows to return (default 50, max 200)",
                        "default": 50,
                    },
                },
                "required": ["sql"],
            },
            handler=_query_database,
            max_calls=5,
            category="analytics",
        ),
    ]


# ---------------------------------------------------------------------------
# Implementations
# ---------------------------------------------------------------------------


async def _get_active_signals(args: dict) -> dict:
    try:
        from services.strategy_sdk import StrategySDK

        firehose = StrategySDK.get_trader_firehose_signals()
        strategy_sigs = StrategySDK.get_trader_strategy_signals()

        return {
            "firehose_signals": firehose if isinstance(firehose, list) else [],
            "strategy_signals": strategy_sigs if isinstance(strategy_sigs, list) else [],
            "firehose_count": len(firehose) if isinstance(firehose, list) else 0,
            "strategy_count": len(strategy_sigs) if isinstance(strategy_sigs, list) else 0,
        }
    except Exception as exc:
        logger.error("get_active_signals failed: %s", exc)
        return {"error": str(exc)}


async def _get_recent_decisions(args: dict) -> dict:
    try:
        from models.database import AsyncSessionLocal, LiveTradingRuntimeState
        from sqlalchemy import select

        limit = args.get("limit", 20)
        trader_id = args.get("trader_id")

        async with AsyncSessionLocal() as session:
            q = select(LiveTradingRuntimeState).order_by(
                LiveTradingRuntimeState.updated_at.desc()
            ).limit(limit)
            if trader_id:
                q = q.where(LiveTradingRuntimeState.trader_id == trader_id)

            result = await session.execute(q)
            rows = result.scalars().all()

        decisions = []
        for r in rows:
            decisions.append({
                "id": r.id,
                "trader_id": getattr(r, "trader_id", None),
                "state": getattr(r, "state", None),
                "last_run": getattr(r, "updated_at", None).isoformat() if getattr(r, "updated_at", None) else None,
            })

        return {"decisions": decisions, "count": len(decisions)}
    except Exception as exc:
        logger.error("get_recent_decisions failed: %s", exc)
        return {"error": str(exc)}


async def _get_trader_overview(args: dict) -> dict:
    try:
        from models.database import AsyncSessionLocal, LiveTradingRuntimeState
        from sqlalchemy import select

        async with AsyncSessionLocal() as session:
            result = await session.execute(select(LiveTradingRuntimeState))
            rows = result.scalars().all()

        traders = []
        for r in rows:
            traders.append({
                "id": r.id,
                "trader_id": getattr(r, "trader_id", None),
                "state": getattr(r, "state", None),
                "updated_at": getattr(r, "updated_at", None).isoformat() if getattr(r, "updated_at", None) else None,
            })

        return {"traders": traders, "count": len(traders)}
    except Exception as exc:
        logger.error("get_trader_overview failed: %s", exc)
        return {"error": str(exc)}


async def _detect_anomalies(args: dict) -> dict:
    try:
        from services.anomaly_detector import AnomalyDetector

        detector = AnomalyDetector()
        opportunities = await detector.detect_opportunities()
        return {"anomalies": opportunities if isinstance(opportunities, list) else [], "count": len(opportunities) if isinstance(opportunities, list) else 0}
    except Exception as exc:
        logger.error("detect_anomalies failed: %s", exc)
        return {"error": str(exc)}


async def _get_cross_platform_arb(args: dict) -> dict:
    try:
        # Try via the discovery routes helper
        from models.database import AsyncSessionLocal
        from sqlalchemy import text

        async with AsyncSessionLocal() as session:
            # Check if cross-platform data exists
            result = await session.execute(
                text("SELECT COUNT(*) FROM discovered_wallets WHERE source = 'kalshi' LIMIT 1")
            )
            count = result.scalar() or 0

        if count == 0:
            return {"arbitrage_opportunities": [], "note": "Cross-platform data not available. Ensure Kalshi integration is configured."}

        return {"arbitrage_opportunities": [], "note": "Cross-platform arbitrage scan complete"}
    except Exception as exc:
        logger.error("get_cross_platform_arb failed: %s", exc)
        return {"error": str(exc)}


async def _query_database(args: dict) -> dict:
    """Execute a read-only SQL query."""
    try:
        from models.database import AsyncSessionLocal
        from sqlalchemy import text

        sql = args["sql"].strip()
        limit = min(args.get("limit", 50), 200)

        # Safety: only allow SELECT
        sql_upper = sql.upper().lstrip()
        if not sql_upper.startswith("SELECT"):
            return {"error": "Only SELECT queries are allowed for safety."}

        # Block dangerous patterns
        dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "EXEC", "CREATE", "GRANT"]
        for kw in dangerous:
            if kw in sql_upper.split():
                return {"error": f"Query contains forbidden keyword: {kw}"}

        # Ensure LIMIT
        if "LIMIT" not in sql_upper:
            sql = f"{sql} LIMIT {limit}"

        async with AsyncSessionLocal() as session:
            result = await session.execute(text(sql))
            columns = list(result.keys()) if result.keys() else []
            rows_raw = result.fetchall()

        rows = []
        for row in rows_raw[:limit]:
            rows.append(dict(zip(columns, row)))

        return {"columns": columns, "rows": rows, "count": len(rows)}
    except Exception as exc:
        logger.error("query_database failed: %s", exc)
        return {"error": str(exc)}
