"""Cortex — autonomous strategy and risk management agent worker.

Runs on a configurable interval, instantiates a ReAct agent with all
available tools, and persists output to the cortex chat session.
Optionally notifies via Telegram.
"""

from __future__ import annotations

import asyncio

from datetime import datetime

from models.database import AppSettings, AsyncSessionLocal, CortexMemory
from services.ai.agent import Agent, AgentEventType
from services.ai.tools import get_all_tools
from services.live_pressure import current_backpressure_level, is_db_pressure_active
from utils.logger import get_logger
from utils.utcnow import utcnow

logger = get_logger("cortex")


DEFAULT_MANDATE = """\
You are Cortex, an autonomous agent for a prediction market trading system.

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
5. **Manage memory** — Review your existing memories. Delete stale, redundant, or \
no-longer-relevant ones. Only save NEW memories when you discover something genuinely \
new that will be useful across future cycles.

## Principles
- **Prefer observation over action.** Most cycles should result in 0 actions.
- **Think in terms of regimes.** A strategy that fails in low-vol may excel in high-vol.
- **Be data-driven.** Cite specific metrics when acting (e.g., "3 consecutive losses, \
-2.4% drawdown in 2h").
- **Never increase risk limits without very strong justification.**
- **Always explain your reasoning.**

## Memory Guidelines
Memories are your long-term knowledge base across cycles. They are NOT a place to dump \
raw analysis output.

**Good memories** (save these):
- Durable lessons: "Strategy X loses money on single-stock markets due to 8:1 loss asymmetry"
- Rules you've derived: "Never increase position size when collateral ratio < 35%"
- Regime patterns: "Weather strategies underperform in spring transition months"
- Known bugs/issues: "Weather trader generates duplicate orders — needs dedup guard"

**Bad memories** (do NOT save these):
- Verbose analysis results or P&L breakdowns (these belong in your response, not memory)
- Observations that duplicate existing memories with slightly updated numbers
- Temporary state that will change by next cycle (e.g., current cash balance)

**Memory hygiene every cycle:**
- Recall existing memories before saving new ones
- If a new observation updates an existing memory, UPDATE the existing one (expire old, save new)
- Delete memories about resolved issues or outdated conditions
- Consolidate multiple related memories into one when possible
- Keep total memory count lean — 5-15 high-quality memories, not 50 verbose ones

## Output Format
Structure your response as:

**Summary** — 2-3 sentence overview of what you observed and concluded.

**Actions Taken** — If you took any actions (paused traders, adjusted configs, etc.), \
list them with the reason. If no actions, say "No actions taken."

**Memory Updates** — If you saved, updated, or deleted any memories, briefly note what changed.
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


async def _get_or_create_cortex_session() -> str:
    """Get or create the single Cortex chat session. Returns session_id."""
    from services.ai.chat_memory import chat_memory_service

    existing = await chat_memory_service.find_latest_for_context("cortex", "cortex")
    if existing:
        return existing["session_id"]
    created = await chat_memory_service.create_session(
        context_type="cortex",
        context_id="cortex",
        title="Cortex Activity",
    )
    return created["session_id"]


async def run_cortex_cycle(*, trigger: str = "scheduled", settings_override: dict | None = None) -> dict:
    """Execute one Cortex agent cycle. Persists output to chat session."""
    from services.ai.chat_memory import chat_memory_service

    cfg = settings_override or await _load_cortex_settings()

    started_at = utcnow()
    session_id = await _get_or_create_cortex_session()

    # Load memories
    memories = await _load_memories(cfg.get("memory_limit", 20))

    # Build system prompt
    system_prompt = _build_system_prompt(
        cfg.get("mandate"),
        memories,
        cfg.get("write_actions_enabled", False),
    )

    # Resolve tools
    tools = list(get_all_tools().values())

    # Create agent
    agent = Agent(
        system_prompt=system_prompt,
        tools=tools,
        model=cfg.get("model"),
        max_iterations=cfg.get("max_iterations", 15),
        session_type="cortex",
        temperature=cfg.get("temperature", 0.1),
    )

    # Run agent — track only the final answer and write actions
    write_tool_names = {
        "cortex_pause_trader", "cortex_enable_strategy",
        "cortex_update_risk_clamps", "update_strategy_config",
    }
    write_actions: list[dict] = []
    answer = ""
    total_tokens = 0
    error_msg = None

    try:
        async for event in agent.run("Perform your fleet observation and management cycle."):
            if event.type == AgentEventType.TOOL_START:
                tool_name = event.data.get("tool", "")
                if tool_name in write_tool_names:
                    write_actions.append({
                        "tool": tool_name,
                        "input": event.data.get("input", {}),
                    })
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

    # Persist only the final answer text
    answer_text = answer or error_msg or ""

    if answer_text.strip():
        await chat_memory_service.append_message(
            session_id=session_id,
            role="assistant",
            content=answer_text,
            model_used=cfg.get("model"),
        )

    if cfg.get("notify_telegram") and write_actions:
        token = cfg.get("telegram_bot_token")
        chat_id = cfg.get("telegram_chat_id")
        if token and chat_id:
            from utils.secrets import decrypt_secret

            decrypted_token = decrypt_secret(token) or token
            lines = ["<b>Cortex</b>\n"]
            for a in write_actions:
                tool_name = a.get("tool", "").replace("cortex_", "").replace("_", " ").title()
                reason = a.get("input", {}).get("reason", "")
                lines.append(f"- <b>{tool_name}</b>: {reason}")
            if answer:
                summary = answer[:500] + ("..." if len(answer) > 500 else "")
                lines.append(f"\n<i>{summary}</i>")
            await _send_telegram(decrypted_token, chat_id, "\n".join(lines))

    finished_at = utcnow()
    result = {
        "status": "error" if error_msg else "completed",
        "tokens_used": total_tokens,
        "duration_seconds": (finished_at - started_at).total_seconds(),
    }
    logger.info("Cortex cycle complete", **result)
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

    # Defer cortex cycles when DB pressure is active or backpressure is
    # elevated.  Cortex is COLD — heavy LLM/tool calls + DB reads — and
    # should yield to the orchestrator hot path.
    if is_db_pressure_active() or current_backpressure_level() > 0.6:
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
