"""API routes for the Cortex autonomous fleet commander agent.

Provides endpoints for:
- Status and run history
- Manual run triggering (with SSE streaming)
- Memory CRUD (browse, edit, delete)
- Settings read/write
"""

import json
import uuid

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select
from typing import Optional

from models.database import (
    AppSettings,
    AsyncSessionLocal,
    CortexMemory,
    CortexRunLog,
)
from utils.logger import get_logger
from utils.utcnow import utcnow

logger = get_logger(__name__)

router = APIRouter(prefix="/cortex", tags=["cortex"])


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------


class CortexSettingsRequest(BaseModel):
    enabled: Optional[bool] = None
    model: Optional[str] = None
    interval_seconds: Optional[int] = Field(default=None, ge=30, le=3600)
    max_iterations: Optional[int] = Field(default=None, ge=1, le=50)
    temperature: Optional[float] = Field(default=None, ge=0.0, le=2.0)
    mandate: Optional[str] = None
    memory_limit: Optional[int] = Field(default=None, ge=1, le=100)
    write_actions_enabled: Optional[bool] = None
    notify_telegram: Optional[bool] = None


class UpdateMemoryRequest(BaseModel):
    content: Optional[str] = None
    category: Optional[str] = Field(default=None, pattern="^(observation|lesson|rule|preference)$")
    importance: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    expired: Optional[bool] = None


class CreateMemoryRequest(BaseModel):
    content: str = Field(..., min_length=1)
    category: str = Field(default="observation", pattern="^(observation|lesson|rule|preference)$")
    importance: float = Field(default=0.5, ge=0.0, le=1.0)
    context: Optional[dict] = None


# ---------------------------------------------------------------------------
# Status & Runs
# ---------------------------------------------------------------------------


@router.get("/status")
async def get_cortex_status() -> dict:
    """Current Cortex state: enabled, last run, memory count, etc."""
    async with AsyncSessionLocal() as session:
        # Settings
        settings_row = (
            await session.execute(select(AppSettings).where(AppSettings.id == "default"))
        ).scalar_one_or_none()

        enabled = bool(getattr(settings_row, "cortex_enabled", False)) if settings_row else False
        interval = getattr(settings_row, "cortex_interval_seconds", 300) if settings_row else 300
        write_enabled = bool(getattr(settings_row, "cortex_write_actions_enabled", False)) if settings_row else False

        # Last run
        last_run = (
            await session.execute(
                select(CortexRunLog).order_by(CortexRunLog.started_at.desc()).limit(1)
            )
        ).scalar_one_or_none()

        # Memory count
        memory_count = (
            await session.execute(
                select(func.count(CortexMemory.id)).where(CortexMemory.expired == False)  # noqa: E712
            )
        ).scalar() or 0

        # Total runs
        total_runs = (await session.execute(select(func.count(CortexRunLog.id)))).scalar() or 0

        return {
            "enabled": enabled,
            "write_actions_enabled": write_enabled,
            "interval_seconds": interval,
            "memory_count": memory_count,
            "total_runs": total_runs,
            "last_run": {
                "id": last_run.id,
                "started_at": last_run.started_at.isoformat() if last_run.started_at else None,
                "finished_at": last_run.finished_at.isoformat() if last_run.finished_at else None,
                "status": last_run.status,
                "actions_count": len(last_run.actions_taken or []),
                "learnings_count": len(last_run.learnings_saved or []),
                "tokens_used": last_run.tokens_used,
                "summary": last_run.summary,
                "trigger": last_run.trigger,
            }
            if last_run
            else None,
        }


@router.get("/runs")
async def list_runs(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: Optional[str] = Query(default=None),
) -> dict:
    """Paginated run history."""
    async with AsyncSessionLocal() as session:
        stmt = select(CortexRunLog).order_by(CortexRunLog.started_at.desc())
        count_stmt = select(func.count(CortexRunLog.id))

        if status:
            stmt = stmt.where(CortexRunLog.status == status)
            count_stmt = count_stmt.where(CortexRunLog.status == status)

        total = (await session.execute(count_stmt)).scalar() or 0
        rows = (await session.execute(stmt.offset(offset).limit(limit))).scalars().all()

        return {
            "runs": [
                {
                    "id": r.id,
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                    "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                    "status": r.status,
                    "actions_count": len(r.actions_taken or []),
                    "learnings_count": len(r.learnings_saved or []),
                    "tokens_used": r.tokens_used,
                    "cost_usd": r.cost_usd,
                    "model_used": r.model_used,
                    "trigger": r.trigger,
                    "summary": r.summary,
                }
                for r in rows
            ],
            "total": total,
            "limit": limit,
            "offset": offset,
        }


@router.get("/runs/{run_id}")
async def get_run(run_id: str) -> dict:
    """Full detail for a single run including thinking log and actions."""
    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(select(CortexRunLog).where(CortexRunLog.id == run_id))
        ).scalar_one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Run not found")

        return {
            "id": row.id,
            "started_at": row.started_at.isoformat() if row.started_at else None,
            "finished_at": row.finished_at.isoformat() if row.finished_at else None,
            "status": row.status,
            "thinking_log": row.thinking_log,
            "actions_taken": row.actions_taken or [],
            "learnings_saved": row.learnings_saved or [],
            "summary": row.summary,
            "tokens_used": row.tokens_used,
            "cost_usd": row.cost_usd,
            "model_used": row.model_used,
            "trigger": row.trigger,
        }


@router.post("/runs/trigger")
async def trigger_run() -> dict:
    """Manually trigger an immediate Cortex cycle."""
    from workers.cortex_worker import run_cortex_cycle

    result = await run_cortex_cycle(trigger="manual")
    return result


@router.post("/runs/trigger/stream")
async def trigger_run_stream():
    """Manually trigger a Cortex cycle with SSE streaming of agent events."""
    from services.ai.agent import Agent, AgentEventType
    from services.ai.tools import resolve_tools
    from workers.cortex_worker import (
        CORTEX_TOOL_NAMES,
        _build_system_prompt,
        _load_cortex_settings,
        _load_memories,
        _send_telegram,
    )

    cfg = await _load_cortex_settings()
    if not cfg.get("enabled"):
        raise HTTPException(status_code=400, detail="Cortex is disabled")

    memories = await _load_memories(cfg.get("memory_limit", 20))
    system_prompt = _build_system_prompt(cfg.get("mandate"), memories, cfg.get("write_actions_enabled", False))
    tools = resolve_tools(CORTEX_TOOL_NAMES)

    agent = Agent(
        system_prompt=system_prompt,
        tools=tools,
        model=cfg.get("model"),
        max_iterations=cfg.get("max_iterations", 15),
        session_type="cortex",
        temperature=cfg.get("temperature", 0.1),
    )

    run_id = str(uuid.uuid4())

    # Create run log
    async with AsyncSessionLocal() as session:
        session.add(CortexRunLog(
            id=run_id,
            started_at=utcnow(),
            status="running",
            trigger="manual",
            model_used=cfg.get("model"),
        ))
        await session.commit()

    async def event_stream():
        actions = []
        thinking_parts = []
        answer = ""
        total_tokens = 0
        error_msg = None

        try:
            async for event in agent.run("Perform your fleet observation and management cycle."):
                payload = json.dumps({"type": event.type.value, "data": event.data}, default=str)
                yield f"data: {payload}\n\n"

                if event.type == AgentEventType.THINKING:
                    thinking_parts.append(event.data.get("content", ""))
                elif event.type == AgentEventType.TOOL_START:
                    actions.append({"tool": event.data.get("tool", ""), "input": event.data.get("input", {}), "output": None})
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

                if event.type in (AgentEventType.DONE, AgentEventType.ERROR):
                    break
        except Exception as exc:
            error_msg = str(exc)
            error_payload = json.dumps({"type": "error", "data": {"error": error_msg}})
            yield f"data: {error_payload}\n\n"

        # Persist run log
        learnings = [a for a in actions if a["tool"] == "cortex_remember"]
        status = "error" if error_msg else "completed"

        async with AsyncSessionLocal() as session:
            row = (await session.execute(select(CortexRunLog).where(CortexRunLog.id == run_id))).scalar_one_or_none()
            if row:
                row.finished_at = utcnow()
                row.status = status
                row.thinking_log = "\n".join(thinking_parts)
                row.actions_taken = actions
                row.learnings_saved = [
                    {"content": a.get("input", {}).get("content", ""), "category": a.get("input", {}).get("category", "")}
                    for a in learnings
                ]
                row.summary = answer or error_msg
                row.tokens_used = total_tokens
                await session.commit()

        # Telegram
        write_actions = [a for a in actions if a["tool"] in (
            "cortex_pause_trader", "cortex_enable_strategy",
            "cortex_update_risk_clamps", "update_strategy_config",
        )]
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
                await _send_telegram(decrypted_token, chat_id, "\n".join(lines))

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Memory CRUD
# ---------------------------------------------------------------------------


@router.get("/memory")
async def list_memories(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    category: Optional[str] = Query(default=None),
    include_expired: bool = Query(default=False),
) -> dict:
    """Browse all Cortex memories."""
    async with AsyncSessionLocal() as session:
        stmt = select(CortexMemory).order_by(CortexMemory.importance.desc(), CortexMemory.updated_at.desc())
        count_stmt = select(func.count(CortexMemory.id))

        if not include_expired:
            stmt = stmt.where(CortexMemory.expired == False)  # noqa: E712
            count_stmt = count_stmt.where(CortexMemory.expired == False)  # noqa: E712
        if category:
            stmt = stmt.where(CortexMemory.category == category)
            count_stmt = count_stmt.where(CortexMemory.category == category)

        total = (await session.execute(count_stmt)).scalar() or 0
        rows = (await session.execute(stmt.offset(offset).limit(limit))).scalars().all()

        return {
            "memories": [
                {
                    "id": r.id,
                    "category": r.category,
                    "content": r.content,
                    "importance": r.importance,
                    "access_count": r.access_count,
                    "context": r.context_json,
                    "expired": r.expired,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                }
                for r in rows
            ],
            "total": total,
        }


@router.post("/memory")
async def create_memory(request: CreateMemoryRequest) -> dict:
    """Manually create a memory."""
    async with AsyncSessionLocal() as session:
        memory = CortexMemory(
            id=str(uuid.uuid4()),
            category=request.category,
            content=request.content,
            importance=request.importance,
            context_json=request.context,
        )
        session.add(memory)
        await session.commit()
        return {
            "id": memory.id,
            "category": memory.category,
            "content": memory.content,
            "importance": memory.importance,
        }


@router.put("/memory/{memory_id}")
async def update_memory(memory_id: str, request: UpdateMemoryRequest) -> dict:
    """Update a memory's content, category, importance, or expired status."""
    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(select(CortexMemory).where(CortexMemory.id == memory_id))
        ).scalar_one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Memory not found")

        if request.content is not None:
            row.content = request.content
        if request.category is not None:
            row.category = request.category
        if request.importance is not None:
            row.importance = request.importance
        if request.expired is not None:
            row.expired = request.expired
        row.updated_at = utcnow()
        await session.commit()

        return {
            "id": row.id,
            "category": row.category,
            "content": row.content,
            "importance": row.importance,
            "expired": row.expired,
        }


@router.delete("/memory/{memory_id}")
async def delete_memory(memory_id: str) -> dict:
    """Permanently delete a memory."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            delete(CortexMemory).where(CortexMemory.id == memory_id)
        )
        await session.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Memory not found")
        return {"deleted": True, "id": memory_id}


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@router.get("/settings")
async def get_cortex_settings() -> dict:
    """Read Cortex settings."""
    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(select(AppSettings).where(AppSettings.id == "default"))
        ).scalar_one_or_none()
        if not row:
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
        }


@router.put("/settings")
async def update_cortex_settings(request: CortexSettingsRequest) -> dict:
    """Update Cortex settings."""
    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(select(AppSettings).where(AppSettings.id == "default"))
        ).scalar_one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Settings not found")

        field_map = {
            "enabled": "cortex_enabled",
            "model": "cortex_model",
            "interval_seconds": "cortex_interval_seconds",
            "max_iterations": "cortex_max_iterations",
            "temperature": "cortex_temperature",
            "mandate": "cortex_mandate",
            "memory_limit": "cortex_memory_limit",
            "write_actions_enabled": "cortex_write_actions_enabled",
            "notify_telegram": "cortex_notify_telegram",
        }

        updated = []
        for req_field, db_col in field_map.items():
            val = getattr(request, req_field, None)
            if val is not None:
                setattr(row, db_col, val)
                updated.append(req_field)

        await session.commit()
        return {"status": "ok", "updated": updated}
