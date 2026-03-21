"""Cortex fleet commander tools — memory, fleet control, orchestrator management."""

from __future__ import annotations

import logging
import uuid

logger = logging.getLogger(__name__)


def build_tools() -> list:
    from services.ai.agent import AgentTool

    return [
        # -- Memory tools --
        AgentTool(
            name="cortex_recall",
            description=(
                "Search your persistent memory for past observations, lessons, "
                "rules, or preferences. Use this to recall what you've learned "
                "about strategies, market regimes, and fleet behavior from prior runs."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query — keywords or natural language description of what to recall.",
                    },
                    "category": {
                        "type": "string",
                        "description": "Optional filter by category.",
                        "enum": ["observation", "lesson", "rule", "preference"],
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (default 10).",
                    },
                },
                "required": ["query"],
            },
            handler=_cortex_recall,
            max_calls=5,
            category="cortex",
        ),
        AgentTool(
            name="cortex_remember",
            description=(
                "Save a persistent memory for future agent runs. Use ONLY for durable "
                "cross-session insights: lessons learned from mistakes, rules derived "
                "from patterns, or user preferences. Do NOT save analysis results, "
                "chat responses, or per-request data — those belong in the conversation, "
                "not memory. Memory is for things that change how the agent behaves in "
                "future conversations."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The memory content — clear, concise, actionable.",
                    },
                    "category": {
                        "type": "string",
                        "description": "Memory category.",
                        "enum": ["observation", "lesson", "rule", "preference"],
                    },
                    "importance": {
                        "type": "number",
                        "description": "Importance score 0.0-1.0 (higher = more relevant in future).",
                    },
                    "context": {
                        "type": "object",
                        "description": "Structured metadata — strategy slugs, metrics, market conditions at time of writing.",
                    },
                    "update_id": {
                        "type": "string",
                        "description": "If updating an existing memory, provide its ID.",
                    },
                },
                "required": ["content", "category"],
            },
            handler=_cortex_remember,
            max_calls=10,
            category="cortex",
        ),
        AgentTool(
            name="cortex_expire_memory",
            description="Mark a memory as expired (soft-delete). Use when a lesson is no longer valid.",
            parameters={
                "type": "object",
                "properties": {
                    "memory_id": {"type": "string", "description": "ID of the memory to expire."},
                },
                "required": ["memory_id"],
            },
            handler=_cortex_expire_memory,
            max_calls=5,
            category="cortex",
        ),
        # -- Fleet control tools --
        AgentTool(
            name="cortex_get_fleet_status",
            description=(
                "Get comprehensive fleet status: orchestrator state, all traders "
                "(enabled/paused/mode), active strategies, portfolio P&L, exposure, "
                "recent decisions, and risk clamp settings. This is your primary "
                "observation tool."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_cortex_get_fleet_status,
            max_calls=3,
            category="cortex",
        ),
        AgentTool(
            name="cortex_pause_trader",
            description=(
                "Pause or unpause a specific trader bot. Paused traders stop "
                "evaluating signals but retain their positions."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "trader_id": {"type": "string", "description": "UUID of the trader to pause/unpause."},
                    "paused": {"type": "boolean", "description": "True to pause, False to unpause."},
                    "reason": {"type": "string", "description": "Why you are pausing/unpausing this trader."},
                },
                "required": ["trader_id", "paused", "reason"],
            },
            handler=_cortex_pause_trader,
            max_calls=5,
            category="cortex",
        ),
        AgentTool(
            name="cortex_enable_strategy",
            description=(
                "Enable or disable a strategy across the fleet. Disabled strategies "
                "stop detecting opportunities."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "strategy_slug": {"type": "string", "description": "Strategy slug identifier."},
                    "enabled": {"type": "boolean", "description": "True to enable, False to disable."},
                    "reason": {"type": "string", "description": "Why you are enabling/disabling this strategy."},
                },
                "required": ["strategy_slug", "enabled", "reason"],
            },
            handler=_cortex_enable_strategy,
            max_calls=5,
            category="cortex",
        ),
        AgentTool(
            name="cortex_update_risk_clamps",
            description=(
                "Update orchestrator-level risk clamps: max consecutive losses cap, "
                "max open positions cap, max trade notional USD cap. These are "
                "safety-critical — use conservatively."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "max_consecutive_losses_cap": {
                        "type": "integer",
                        "description": "Max consecutive losses before blocking trades (1-20).",
                    },
                    "max_open_positions_cap": {
                        "type": "integer",
                        "description": "Max open positions across all traders (1-100).",
                    },
                    "max_trade_notional_usd_cap": {
                        "type": "number",
                        "description": "Max notional per trade in USD (1-10000).",
                    },
                    "reason": {"type": "string", "description": "Why you are adjusting risk clamps."},
                },
                "required": ["reason"],
            },
            handler=_cortex_update_risk_clamps,
            max_calls=3,
            category="cortex",
        ),
        AgentTool(
            name="cortex_get_autoresearch_status",
            description=(
                "Get the status of autoresearch parameter optimization experiments "
                "across the fleet. Shows active experiments, best scores, iteration "
                "counts, and recent improvements for each trader. Use this to see "
                "what the autoresearch loop is doing before making param changes."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "trader_id": {
                        "type": "string",
                        "description": "Optional specific trader ID. If omitted, returns all recent experiments.",
                    },
                },
                "required": [],
            },
            handler=_cortex_get_autoresearch_status,
            max_calls=3,
            category="cortex",
        ),
    ]


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------


async def _cortex_recall(arguments: dict) -> dict:
    from sqlalchemy import select
    from models.database import AsyncSessionLocal, CortexMemory

    query = arguments.get("query", "")
    category = arguments.get("category")
    limit = min(arguments.get("limit", 10), 50)

    async with AsyncSessionLocal() as session:
        stmt = select(CortexMemory).where(CortexMemory.expired == False)  # noqa: E712
        if category:
            stmt = stmt.where(CortexMemory.category == category)

        # Keyword search — match any word in the query against content
        keywords = [kw.strip().lower() for kw in query.split() if kw.strip()]
        if keywords:
            from sqlalchemy import func, or_

            conditions = [func.lower(CortexMemory.content).contains(kw) for kw in keywords]
            stmt = stmt.where(or_(*conditions))

        stmt = stmt.order_by(CortexMemory.importance.desc(), CortexMemory.updated_at.desc())
        stmt = stmt.limit(limit)
        rows = (await session.execute(stmt)).scalars().all()

        # Bump access count
        for row in rows:
            row.access_count = (row.access_count or 0) + 1
        await session.commit()

        return {
            "memories": [
                {
                    "id": row.id,
                    "category": row.category,
                    "content": row.content,
                    "importance": row.importance,
                    "access_count": row.access_count,
                    "context": row.context_json,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                }
                for row in rows
            ],
            "count": len(rows),
        }


async def _cortex_remember(arguments: dict) -> dict:
    from models.database import AsyncSessionLocal, CortexMemory
    from utils.utcnow import utcnow

    content = arguments["content"]
    category = arguments.get("category", "observation")
    importance = max(0.0, min(1.0, arguments.get("importance", 0.5)))
    context = arguments.get("context")
    update_id = arguments.get("update_id")

    async with AsyncSessionLocal() as session:
        if update_id:
            from sqlalchemy import select

            row = (await session.execute(select(CortexMemory).where(CortexMemory.id == update_id))).scalar_one_or_none()
            if row is None:
                return {"error": f"Memory {update_id} not found"}
            row.content = content
            row.category = category
            row.importance = importance
            row.context_json = context
            row.updated_at = utcnow()
            await session.commit()
            return {"status": "updated", "id": update_id}

        memory = CortexMemory(
            id=str(uuid.uuid4()),
            category=category,
            content=content,
            importance=importance,
            context_json=context,
        )
        session.add(memory)
        await session.commit()
        return {"status": "saved", "id": memory.id, "category": category}


async def _cortex_expire_memory(arguments: dict) -> dict:
    from sqlalchemy import select
    from models.database import AsyncSessionLocal, CortexMemory

    memory_id = arguments["memory_id"]
    async with AsyncSessionLocal() as session:
        row = (await session.execute(select(CortexMemory).where(CortexMemory.id == memory_id))).scalar_one_or_none()
        if row is None:
            return {"error": f"Memory {memory_id} not found"}
        row.expired = True
        await session.commit()
        return {"status": "expired", "id": memory_id}


async def _cortex_get_fleet_status(arguments: dict) -> dict:
    from sqlalchemy import func, select
    from models.database import (
        AsyncSessionLocal,
        Strategy,
        Trader,
        TraderDecision,
        TraderPosition,
    )
    from services.trader_orchestrator_state import (
        read_orchestrator_control,
        read_orchestrator_snapshot,
    )

    async with AsyncSessionLocal() as session:
        # Orchestrator state
        control = await read_orchestrator_control(session)
        snapshot = await read_orchestrator_snapshot(session)

        # Traders
        traders_rows = (await session.execute(select(Trader))).scalars().all()
        traders = []
        for t in traders_rows:
            traders.append({
                "id": t.id,
                "name": t.name,
                "enabled": t.is_enabled,
                "paused": t.is_paused,
                "mode": t.mode,
            })

        # Strategies
        strategy_rows = (await session.execute(select(Strategy).where(Strategy.enabled == True))).scalars().all()  # noqa: E712
        strategies = [{"slug": s.slug, "source_key": s.source_key, "enabled": s.enabled} for s in strategy_rows]

        # Open positions summary
        open_positions = (
            await session.execute(
                select(
                    func.count(TraderPosition.id),
                    func.coalesce(func.sum(TraderPosition.total_notional_usd), 0),
                ).where(TraderPosition.status == "open")
            )
        ).one()

        # Recent decisions (last 20)
        recent_decisions = (
            await session.execute(
                select(TraderDecision)
                .order_by(TraderDecision.created_at.desc())
                .limit(20)
            )
        ).scalars().all()

        decisions_summary = []
        for d in recent_decisions:
            decisions_summary.append({
                "strategy": d.strategy_key,
                "decision": d.decision,
                "reason": d.reason,
                "score": d.score,
                "created_at": d.created_at.isoformat() if d.created_at else None,
            })

        return {
            "orchestrator": {
                "enabled": control.get("is_enabled"),
                "paused": control.get("is_paused"),
                "mode": control.get("mode"),
                "interval_seconds": control.get("run_interval_seconds"),
                "settings": control.get("settings_json", {}),
            },
            "snapshot": {
                "running": snapshot.get("running") if isinstance(snapshot, dict) else None,
                "traders_total": snapshot.get("traders_total") if isinstance(snapshot, dict) else None,
                "traders_running": snapshot.get("traders_running") if isinstance(snapshot, dict) else None,
                "gross_exposure_usd": snapshot.get("gross_exposure_usd") if isinstance(snapshot, dict) else None,
                "daily_pnl": snapshot.get("daily_pnl") if isinstance(snapshot, dict) else None,
                "last_run_at": snapshot.get("last_run_at") if isinstance(snapshot, dict) else None,
            },
            "traders": traders,
            "active_strategies": strategies,
            "portfolio": {
                "open_positions": open_positions[0],
                "total_notional_usd": float(open_positions[1]),
            },
            "recent_decisions": decisions_summary,
        }


async def _cortex_pause_trader(arguments: dict) -> dict:
    from models.database import AsyncSessionLocal
    from services.trader_orchestrator_state import set_trader_paused

    trader_id = arguments["trader_id"]
    paused = arguments["paused"]
    reason = arguments.get("reason", "")

    # Check write permission
    if not await _check_write_permission():
        return {"error": "Write actions are disabled. Enable 'cortex_write_actions_enabled' in settings."}

    async with AsyncSessionLocal() as session:
        result = await set_trader_paused(session, trader_id, paused)
        return {
            "status": "ok",
            "trader_id": trader_id,
            "paused": paused,
            "reason": reason,
            "result": result,
        }


async def _cortex_enable_strategy(arguments: dict) -> dict:
    from sqlalchemy import select
    from models.database import AsyncSessionLocal, Strategy

    slug = arguments["strategy_slug"]
    enabled = arguments["enabled"]
    reason = arguments.get("reason", "")

    if not await _check_write_permission():
        return {"error": "Write actions are disabled. Enable 'cortex_write_actions_enabled' in settings."}

    async with AsyncSessionLocal() as session:
        row = (await session.execute(select(Strategy).where(Strategy.slug == slug))).scalar_one_or_none()
        if row is None:
            return {"error": f"Strategy '{slug}' not found"}
        row.enabled = enabled
        await session.commit()
        return {
            "status": "ok",
            "strategy_slug": slug,
            "enabled": enabled,
            "reason": reason,
        }


async def _cortex_update_risk_clamps(arguments: dict) -> dict:
    from models.database import AsyncSessionLocal
    from services.trader_orchestrator_state import read_orchestrator_control, update_orchestrator_control

    if not await _check_write_permission():
        return {"error": "Write actions are disabled. Enable 'cortex_write_actions_enabled' in settings."}

    reason = arguments.get("reason", "")

    async with AsyncSessionLocal() as session:
        control = await read_orchestrator_control(session)
        settings_json = dict(control.get("settings_json") or {})
        clamps = dict(settings_json.get("live_risk_clamps") or {})

        updated_fields = []
        if "max_consecutive_losses_cap" in arguments:
            val = max(1, min(20, arguments["max_consecutive_losses_cap"]))
            clamps["max_consecutive_losses_cap"] = val
            updated_fields.append(f"max_consecutive_losses_cap={val}")
        if "max_open_positions_cap" in arguments:
            val = max(1, min(100, arguments["max_open_positions_cap"]))
            clamps["max_open_positions_cap"] = val
            updated_fields.append(f"max_open_positions_cap={val}")
        if "max_trade_notional_usd_cap" in arguments:
            val = max(1.0, min(10000.0, arguments["max_trade_notional_usd_cap"]))
            clamps["max_trade_notional_usd_cap"] = val
            updated_fields.append(f"max_trade_notional_usd_cap={val}")

        settings_json["live_risk_clamps"] = clamps
        await update_orchestrator_control(session, settings_json=settings_json)

        return {
            "status": "ok",
            "updated": updated_fields,
            "reason": reason,
            "new_clamps": clamps,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _cortex_get_autoresearch_status(arguments: dict) -> dict:
    from services.autoresearch_service import autoresearch_service

    trader_id = arguments.get("trader_id")
    try:
        experiments = await autoresearch_service.get_fleet_autoresearch_status(trader_id=trader_id)
        return {
            "experiments": experiments,
            "count": len(experiments),
            "active_count": sum(1 for e in experiments if e.get("status") == "running"),
        }
    except Exception as exc:
        return {"error": str(exc)}


async def _check_write_permission() -> bool:
    """Check if Cortex write actions are enabled in AppSettings."""
    from sqlalchemy import select
    from models.database import AppSettings, AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        row = (await session.execute(select(AppSettings).where(AppSettings.id == "default"))).scalar_one_or_none()
        if row is None:
            return False
        return bool(getattr(row, "cortex_write_actions_enabled", False))
