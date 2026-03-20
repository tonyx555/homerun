"""Cortex — autonomous fleet commander agent worker.

Runs on a configurable interval, instantiates a ReAct agent with fleet
management tools, and persists its reasoning, actions, and learnings.
Optionally notifies via Telegram.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid

from datetime import datetime, timezone

from models.database import AppSettings, AsyncSessionLocal, CortexMemory, CortexRunLog
from services.ai.agent import Agent, AgentEventType
from services.ai.tools import resolve_tools
from utils.logger import get_logger
from utils.utcnow import utcnow

logger = get_logger("cortex")

# Tools the Cortex agent has access to
CORTEX_TOOL_NAMES = [
    # Cortex-specific
    "cortex_recall",
    "cortex_remember",
    "cortex_expire_memory",
    "cortex_get_fleet_status",
    "cortex_pause_trader",
    "cortex_enable_strategy",
    "cortex_update_risk_clamps",
    # Existing observation tools
    "get_portfolio_performance",
    "get_open_positions",
    "get_trade_history",
    "list_strategies",
    "get_strategy_details",
    "get_strategy_performance",
    "get_recent_decisions",
    "update_strategy_config",
    "get_live_prices",
    "get_market_regime",
    "get_active_markets_summary",
    "get_system_status",
    "search_news",
    "get_news_edges",
    "analyze_market_sentiment",
    "get_confluence_signals",
    "get_whale_clusters",
]

DEFAULT_MANDATE = """\
You are Cortex, an autonomous fleet commander for a prediction market trading system.

Your job is to observe, reason, and act to optimize fleet performance while managing risk.

On each cycle you should:
1. **Observe** — Read fleet status: trader P&L, win rates, drawdowns, exposure, \
open positions, recent decisions.
2. **Contextualize** — Check market regime, volatility, news sentiment, smart money signals. \
Recall relevant memories from prior runs.
3. **Evaluate** — Assess each active strategy's recent performance in context. \
Is poor performance due to market conditions (temporary) or a fundamental issue?
4. **Act (conservatively)** — Only take action when evidence is clear:
   - Pause underperforming traders (consecutive losses, excessive drawdown)
   - Disable strategies unsuited to the current market regime
   - Adjust strategy parameters (min_edge, confidence thresholds) based on observed data
   - Tighten or loosen risk clamps as exposure warrants
5. **Learn** — Save observations and lessons to memory for future reference. \
Update or expire stale memories.

## Principles
- **Prefer observation over action.** Most cycles should result in 0 actions.
- **Think in terms of regimes.** A strategy that fails in low-vol may excel in high-vol.
- **Be data-driven.** Cite specific metrics when acting (e.g., "3 consecutive losses, \
-2.4% drawdown in 2h").
- **Never increase risk limits without very strong justification.**
- **Always explain your reasoning.**

## Output Format
End your response with a brief summary paragraph. If you saved any learnings, mention them.
"""


async def _load_cortex_settings() -> dict:
    """Load Cortex settings from AppSettings."""
    from sqlalchemy import select

    async with AsyncSessionLocal() as session:
        row = (await session.execute(select(AppSettings).where(AppSettings.id == "default"))).scalar_one_or_none()
        if row is None:
            return {"enabled": False}
        return {
            "enabled": bool(getattr(row, "cortex_enabled", False)),
            "model": getattr(row, "cortex_model", None),
            "interval_seconds": getattr(row, "cortex_interval_seconds", 300),
            "max_iterations": getattr(row, "cortex_max_iterations", 15),
            "temperature": getattr(row, "cortex_temperature", 0.1),
            "mandate": getattr(row, "cortex_mandate", None),
            "memory_limit": getattr(row, "cortex_memory_limit", 20),
            "write_actions_enabled": bool(getattr(row, "cortex_write_actions_enabled", False)),
            "notify_telegram": bool(getattr(row, "cortex_notify_telegram", False)),
            "telegram_bot_token": getattr(row, "telegram_bot_token", None),
            "telegram_chat_id": getattr(row, "telegram_chat_id", None),
        }


async def _load_memories(limit: int) -> list[dict]:
    """Load top memories by importance for injection into system prompt."""
    from sqlalchemy import select

    async with AsyncSessionLocal() as session:
        stmt = (
            select(CortexMemory)
            .where(CortexMemory.expired == False)  # noqa: E712
            .order_by(CortexMemory.importance.desc(), CortexMemory.updated_at.desc())
            .limit(limit)
        )
        rows = (await session.execute(stmt)).scalars().all()
        return [
            {
                "id": r.id,
                "category": r.category,
                "content": r.content,
                "importance": r.importance,
            }
            for r in rows
        ]


def _build_system_prompt(mandate: str | None, memories: list[dict], write_enabled: bool) -> str:
    """Build the full system prompt with memory context."""
    base = mandate or DEFAULT_MANDATE

    parts = [base]

    if not write_enabled:
        parts.append(
            "\n## IMPORTANT: Read-Only Mode\n"
            "Write actions are DISABLED. You may observe and save learnings, "
            "but cannot pause traders, enable/disable strategies, or adjust risk clamps. "
            "Tools that attempt mutations will return an error."
        )

    if memories:
        memory_lines = []
        for m in memories:
            memory_lines.append(f"- [{m['category']}] (importance={m['importance']:.2f}) {m['content']}")
        parts.append(
            "\n## Your Memories (from prior runs)\n"
            + "\n".join(memory_lines)
        )

    parts.append(f"\n## Current Time\n{utcnow().isoformat()}Z")

    return "\n".join(parts)


async def _send_telegram(token: str, chat_id: str, text: str) -> None:
    """Send a Telegram notification."""
    import httpx

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            await client.post(url, json=payload)
    except Exception as exc:
        logger.warning("Cortex Telegram notification failed", exc_info=exc)


async def run_cortex_cycle(*, trigger: str = "scheduled", settings_override: dict | None = None) -> dict:
    """Execute one Cortex agent cycle. Returns run summary dict."""
    cfg = settings_override or await _load_cortex_settings()

    run_id = str(uuid.uuid4())
    started_at = utcnow()

    # Create run log entry
    async with AsyncSessionLocal() as session:
        run_log = CortexRunLog(
            id=run_id,
            started_at=started_at,
            status="running",
            trigger=trigger,
            model_used=cfg.get("model"),
        )
        session.add(run_log)
        await session.commit()

    # Load memories
    memories = await _load_memories(cfg.get("memory_limit", 20))

    # Build system prompt
    system_prompt = _build_system_prompt(
        cfg.get("mandate"),
        memories,
        cfg.get("write_actions_enabled", False),
    )

    # Resolve tools
    tools = resolve_tools(CORTEX_TOOL_NAMES)

    # Create agent (reuses existing Agent class)
    agent = Agent(
        system_prompt=system_prompt,
        tools=tools,
        model=cfg.get("model"),
        max_iterations=cfg.get("max_iterations", 15),
        session_type="cortex",
        temperature=cfg.get("temperature", 0.1),
    )

    # Run agent
    thinking_parts: list[str] = []
    actions: list[dict] = []
    answer = ""
    total_tokens = 0
    error_msg = None

    try:
        async for event in agent.run("Perform your fleet observation and management cycle."):
            if event.type == AgentEventType.THINKING:
                thinking_parts.append(event.data.get("content", ""))
            elif event.type == AgentEventType.TOOL_START:
                actions.append({
                    "tool": event.data.get("tool", ""),
                    "input": event.data.get("input", {}),
                    "output": None,
                })
            elif event.type == AgentEventType.TOOL_END:
                if actions:
                    actions[-1]["output"] = event.data.get("output", {})
            elif event.type == AgentEventType.TOOL_ERROR:
                if actions:
                    actions[-1]["output"] = {"error": event.data.get("error", "")}
            elif event.type == AgentEventType.ANSWER_START:
                answer = event.data.get("content", "")
            elif event.type == AgentEventType.DONE:
                result_data = event.data.get("result", {})
                answer = result_data.get("answer", answer)
                total_tokens = result_data.get("total_tokens", 0)
            elif event.type == AgentEventType.ERROR:
                error_msg = event.data.get("error", "Unknown error")
    except Exception as exc:
        error_msg = str(exc)
        logger.error("Cortex cycle error", exc_info=exc)

    # Identify write actions taken (for Telegram)
    write_actions = [a for a in actions if a["tool"] in (
        "cortex_pause_trader", "cortex_enable_strategy",
        "cortex_update_risk_clamps", "update_strategy_config",
    )]

    # Identify learnings saved
    learnings = [a for a in actions if a["tool"] == "cortex_remember"]

    # Update run log
    finished_at = utcnow()
    status = "error" if error_msg else "completed"

    async with AsyncSessionLocal() as session:
        from sqlalchemy import select

        row = (await session.execute(select(CortexRunLog).where(CortexRunLog.id == run_id))).scalar_one_or_none()
        if row:
            row.finished_at = finished_at
            row.status = status
            row.thinking_log = "\n".join(thinking_parts)
            row.actions_taken = actions
            row.learnings_saved = [
                {"content": a.get("input", {}).get("content", ""), "category": a.get("input", {}).get("category", "")}
                for a in learnings
            ]
            row.summary = answer or error_msg
            row.tokens_used = total_tokens
            # Cost estimation will come from LLM usage log
            await session.commit()

    # Telegram notification
    if cfg.get("notify_telegram") and write_actions:
        token = cfg.get("telegram_bot_token")
        chat_id = cfg.get("telegram_chat_id")
        if token and chat_id:
            from utils.secrets import decrypt_secret

            decrypted_token = decrypt_secret(token) or token
            lines = ["<b>🧠 Cortex Fleet Commander</b>\n"]
            for a in write_actions:
                tool_name = a["tool"].replace("cortex_", "").replace("_", " ").title()
                reason = a.get("input", {}).get("reason", "")
                lines.append(f"• <b>{tool_name}</b>: {reason}")
            if answer:
                # Truncate for Telegram
                summary = answer[:500] + ("..." if len(answer) > 500 else "")
                lines.append(f"\n<i>{summary}</i>")
            await _send_telegram(decrypted_token, chat_id, "\n".join(lines))

    result = {
        "run_id": run_id,
        "status": status,
        "actions_count": len(write_actions),
        "learnings_count": len(learnings),
        "tokens_used": total_tokens,
        "duration_seconds": (finished_at - started_at).total_seconds(),
        "summary": answer[:200] if answer else error_msg,
    }
    logger.info("Cortex cycle complete", **{k: v for k, v in result.items() if k != "summary"})
    return result


# ---------------------------------------------------------------------------
# Orchestrator-integrated tick
# ---------------------------------------------------------------------------

_last_cortex_run_at: datetime | None = None
_cortex_task: asyncio.Task | None = None


async def maybe_run_cortex_tick() -> None:
    """Called by the orchestrator each cycle. Fires a Cortex cycle if due.

    Cortex runs as a background task so it doesn't block the orchestrator.
    Only one Cortex cycle runs at a time — if the previous one is still
    in progress, this is a no-op.
    """
    global _last_cortex_run_at, _cortex_task

    # Skip if a cycle is already running
    if _cortex_task is not None and not _cortex_task.done():
        return

    # Clean up finished task
    if _cortex_task is not None and _cortex_task.done():
        try:
            _cortex_task.result()
        except Exception as exc:
            logger.error("Previous Cortex cycle failed", exc_info=exc)
        _cortex_task = None

    try:
        cfg = await _load_cortex_settings()
    except Exception as exc:
        logger.warning("Cortex: failed to load settings", exc_info=exc)
        return

    if not cfg.get("enabled"):
        return

    interval = max(30, cfg.get("interval_seconds", 300))
    now = utcnow()

    if _last_cortex_run_at is not None:
        elapsed = (now - _last_cortex_run_at).total_seconds()
        if elapsed < interval:
            return

    _last_cortex_run_at = now
    _cortex_task = asyncio.create_task(
        _safe_cortex_cycle(cfg),
        name="cortex-cycle",
    )


async def _safe_cortex_cycle(cfg: dict) -> None:
    """Wrapper that catches exceptions so the task never propagates."""
    try:
        await run_cortex_cycle(trigger="orchestrator", settings_override=cfg)
    except Exception as exc:
        logger.error("Cortex cycle failed", exc_info=exc)
