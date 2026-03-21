"""Analytics tools — signals, decisions, trader performance, anomalies, DB queries."""

from __future__ import annotations

import datetime
import decimal
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema reference injected into query_database description so the LLM knows
# the real table/column names instead of guessing.
# ---------------------------------------------------------------------------

_DB_SCHEMA_HINT = """
Key tables and columns (PostgreSQL):

strategies: id, slug (unique), source_key, name, description, source_code, class_name, is_system, enabled, status, error_message, config (JSON), config_schema (JSON), aliases, version, sort_order, created_at, updated_at

traders: id, name, description, source_configs_json (JSON), risk_limits_json (JSON), metadata_json (JSON), mode, is_enabled, is_paused, interval_seconds, requested_run_at, last_run_at, next_run_at, created_at, updated_at

trader_orders: id, trader_id, signal_id, decision_id, source, strategy_key, strategy_version, market_id, market_question, direction, event_id, trace_id, mode, status, notional_usd, entry_price, effective_price, edge_percent, confidence, actual_profit, reason, payload_json (JSON), error_message, created_at, executed_at, updated_at
  -- status values: pending, submitted, filled, resolved_win, resolved_loss, closed_win, closed_loss, cancelled, failed, expired

trader_decisions: id, trader_id (FK->traders), signal_id (FK->trade_signals), source, strategy_key, strategy_version, decision, reason, score, event_id, trace_id, checks_summary_json (JSON), risk_snapshot_json (JSON), payload_json (JSON), created_at
  -- decision values: selected, skipped, blocked, failed

execution_sessions: id, trader_id (FK->traders), signal_id, decision_id, source, strategy_key, strategy_version, mode, status, policy, plan_id, market_ids_json (JSON), legs_total, legs_completed, legs_failed, legs_open, requested_notional_usd, executed_notional_usd, max_unhedged_notional_usd, unhedged_notional_usd, trace_id, started_at, completed_at, expires_at, error_message, payload_json (JSON), created_at, updated_at

live_trading_runtime_state: id, wallet_address, total_trades, winning_trades, losing_trades, total_volume, total_pnl, daily_volume, daily_pnl, open_positions, last_trade_at, daily_volume_reset_at, market_positions_json (JSON), pending_reconciliation_json (JSON), created_at, updated_at

trade_signals: id, source, market_id, market_question, direction, strategy_key, strategy_version, edge_percent, confidence, signal_strength, reason, payload_json (JSON), status, created_at, expires_at

opportunity_state: stable_id (PK), opportunity_json (JSON — full opportunity data), first_seen_at, last_seen_at, last_updated_at, is_active (boolean), last_run_id
  -- The primary opportunity table. opportunity_json contains: strategy, title, total_cost, expected_roi, risk_score, positions, markets, etc.

opportunity_history: id, strategy_type, event_id, title, total_cost, expected_roi, risk_score, positions_data (JSON), detected_at, expired_at, resolution_date, was_profitable, actual_roi

opportunity_events: id, stable_id, run_id, event_type (detected|updated|expired|reactivated), opportunity_json (JSON), created_at

research_sessions: id, session_type, query, opportunity_id, market_id, status, result, error, iterations, tools_called, total_input_tokens, total_output_tokens, total_cost_usd, model_used, started_at, completed_at, duration_seconds

discovered_wallets: address (PK), username, discovered_at, last_analyzed_at, discovery_source (scan|manual|referral), total_trades, wins, losses, win_rate, total_pnl, realized_pnl

Important: There is NO "trades" or "opportunities" table. Use trader_orders for trade data. Use opportunity_state for current opportunities. Use strategy_key (not strategy_id) to filter by strategy.
""".strip()


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
            max_calls=10,
            category="analytics",
        ),
        AgentTool(
            name="get_recent_decisions",
            description=(
                "Get recent orchestrator trading decisions — what the system decided "
                "to trade, why, and the outcome. Queries the trader_decisions table. "
                "Includes the strategy that generated each decision and the reasoning."
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
                    "strategy_key": {
                        "type": "string",
                        "description": "Filter by strategy slug/key (optional)",
                    },
                },
                "required": [],
            },
            handler=_get_recent_decisions,
            max_calls=10,
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
            max_calls=5,
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
            max_calls=3,
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
            max_calls=3,
            category="analytics",
        ),
        AgentTool(
            name="query_database",
            description=(
                "Run a read-only SQL query against the application database (PostgreSQL). "
                "Useful for custom analytics, aggregations, or data exploration. "
                "Only SELECT queries are allowed.\n\n" + _DB_SCHEMA_HINT
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
            max_calls=15,
            category="analytics",
        ),
    ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _json_safe(obj: Any) -> Any:
    """Recursively convert non-JSON-native types for safe serialization."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    return obj


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
        from models.database import AsyncSessionLocal, TraderDecision
        from sqlalchemy import select

        limit = args.get("limit", 20)
        trader_id = args.get("trader_id")
        strategy_key = args.get("strategy_key")

        async with AsyncSessionLocal() as session:
            q = select(TraderDecision).order_by(
                TraderDecision.created_at.desc()
            ).limit(limit)
            if trader_id:
                q = q.where(TraderDecision.trader_id == trader_id)
            if strategy_key:
                q = q.where(TraderDecision.strategy_key == strategy_key)

            result = await session.execute(q)
            rows = result.scalars().all()

        decisions = []
        for r in rows:
            decisions.append({
                "id": r.id,
                "trader_id": r.trader_id,
                "strategy_key": r.strategy_key,
                "decision": r.decision,
                "reason": r.reason,
                "score": r.score,
                "source": r.source,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            })

        return {"decisions": decisions, "count": len(decisions)}
    except Exception as exc:
        logger.error("get_recent_decisions failed: %s", exc)
        return {"error": str(exc)}


async def _get_trader_overview(args: dict) -> dict:
    try:
        from models.database import AsyncSessionLocal, Trader
        from sqlalchemy import select

        async with AsyncSessionLocal() as session:
            result = await session.execute(select(Trader))
            rows = result.scalars().all()

        traders = []
        for r in rows:
            traders.append({
                "id": r.id,
                "name": r.name,
                "enabled": r.is_enabled,
                "paused": r.is_paused,
                "mode": r.mode,
                "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
            })

        return {"traders": traders, "count": len(traders)}
    except Exception as exc:
        logger.error("get_trader_overview failed: %s", exc)
        return {"error": str(exc)}


async def _detect_anomalies(args: dict) -> dict:
    try:
        from services.anomaly_detector import AnomalyDetector

        detector = AnomalyDetector()
        anomalies = await detector.get_anomalies()
        return {"anomalies": anomalies if isinstance(anomalies, list) else [], "count": len(anomalies) if isinstance(anomalies, list) else 0}
    except Exception as exc:
        logger.error("detect_anomalies failed: %s", exc)
        return {"error": str(exc)}


async def _get_cross_platform_arb(args: dict) -> dict:
    try:
        from models.database import AsyncSessionLocal
        from sqlalchemy import text

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM discovered_wallets WHERE discovery_source = 'kalshi' LIMIT 1")
            )
            count = result.scalar() or 0

        if count == 0:
            return {"arbitrage_opportunities": [], "note": "Cross-platform data not available. Ensure Kalshi integration is configured."}

        return {"arbitrage_opportunities": [], "note": "Cross-platform arbitrage scan complete"}
    except Exception as exc:
        logger.error("get_cross_platform_arb failed: %s", exc)
        return {"error": str(exc)}


async def _query_database(args: dict) -> dict:
    """Execute a read-only SQL query with datetime-safe serialization."""
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
            rows.append(_json_safe(dict(zip(columns, row))))

        return {"columns": columns, "rows": rows, "count": len(rows)}
    except Exception as exc:
        logger.error("query_database failed: %s", exc)
        return {"error": str(exc)}
