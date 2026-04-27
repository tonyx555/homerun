"""API routes for the Autoresearch continuous parameter optimization loop.

Provides endpoints for:
- Experiment status and history per trader
- SSE streaming for real-time experiment progress
- Start/stop experiments
- Settings read/write
"""

import json

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Optional

from utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/autoresearch", tags=["Autoresearch"])


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------


class AutoresearchSettingsRequest(BaseModel):
    model: Optional[str] = None
    max_iterations: Optional[int] = Field(default=None, ge=1, le=500)
    interval_seconds: Optional[int] = Field(default=None, ge=60, le=7200)
    temperature: Optional[float] = Field(default=None, ge=0.0, le=2.0)
    mandate: Optional[str] = None
    auto_apply: Optional[bool] = None
    walk_forward_windows: Optional[int] = Field(default=None, ge=1, le=20)
    train_ratio: Optional[float] = Field(default=None, ge=0.5, le=0.9)
    mode: Optional[str] = Field(default=None, pattern="^(params|code)$")


class AutoresearchStartRequest(BaseModel):
    model: Optional[str] = None
    max_iterations: Optional[int] = Field(default=None, ge=1, le=500)
    mandate: Optional[str] = None
    mode: Optional[str] = Field(default=None, pattern="^(params|code)$")
    strategy_id: Optional[str] = None  # required for code mode


# ---------------------------------------------------------------------------
# Status & History
# ---------------------------------------------------------------------------


@router.get("/status/{trader_id}")
async def get_autoresearch_status(trader_id: str) -> dict:
    """Get the latest experiment status for a trader."""
    from services.autoresearch_service import autoresearch_service
    return await autoresearch_service.get_experiment_status(trader_id)


@router.get("/history/{trader_id}")
async def get_autoresearch_history(
    trader_id: str,
    experiment_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict:
    """Get iteration log for a trader's experiment."""
    from services.autoresearch_service import autoresearch_service
    iterations = await autoresearch_service.get_experiment_history(
        trader_id, experiment_id=experiment_id, limit=limit
    )
    return {"iterations": iterations}


@router.get("/experiments/{trader_id}")
async def list_experiments(
    trader_id: str,
    limit: int = Query(default=20, ge=1, le=100),
) -> dict:
    """List all experiments for a trader."""
    from services.autoresearch_service import autoresearch_service
    experiments = await autoresearch_service.list_experiments(trader_id, limit=limit)
    return {"experiments": experiments}


# ---------------------------------------------------------------------------
# Start / Stop / Stream
# ---------------------------------------------------------------------------


@router.post("/stream/{trader_id}")
async def stream_autoresearch_experiment(
    trader_id: str,
    request: Optional[AutoresearchStartRequest] = None,
):
    """Start an autoresearch experiment with SSE streaming.

    Streams real-time events: experiment_start, iteration_start, proposal,
    decision, done, error.
    """
    from services.autoresearch_service import autoresearch_service

    settings_override = {}
    if request:
        if request.model is not None:
            settings_override["model"] = request.model
        if request.max_iterations is not None:
            settings_override["max_iterations"] = request.max_iterations
        if request.mandate is not None:
            settings_override["mandate"] = request.mandate
        if request.mode is not None:
            settings_override["mode"] = request.mode
        if request.strategy_id is not None:
            settings_override["strategy_id"] = request.strategy_id

    async def event_stream():
        try:
            async for event_dict in autoresearch_service.run_experiment_stream(
                trader_id=trader_id,
                settings_override=settings_override or None,
            ):
                event_type = event_dict.get("event", "progress")
                data = event_dict.get("data", {})
                payload = json.dumps({"type": event_type, "data": data}, default=str)
                yield f"event: {event_type}\ndata: {payload}\n\n"
        except Exception as exc:
            logger.exception("Autoresearch stream error: %s", exc)
            error_payload = json.dumps({"type": "error", "data": {"error": str(exc)}})
            yield f"event: error\ndata: {error_payload}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/stop/{trader_id}")
async def stop_autoresearch_experiment(trader_id: str) -> dict:
    """Stop a running autoresearch experiment."""
    from services.autoresearch_service import autoresearch_service
    return await autoresearch_service.stop_experiment(trader_id)


# ---------------------------------------------------------------------------
# Strategy-scoped routes — code evolution against the backtest data plane.
# These mirror the trader-scoped status/history/stream/stop endpoints but
# don't require a bot context. Code experiments operate on the strategy's
# source code only and the kept versions land on the Strategy record.
# ---------------------------------------------------------------------------


class StrategyAutoresearchStartRequest(BaseModel):
    model: Optional[str] = None
    max_iterations: Optional[int] = Field(default=None, ge=1, le=500)
    mandate: Optional[str] = None


@router.get("/strategy/{strategy_id}/status")
async def get_strategy_autoresearch_status(strategy_id: str) -> dict:
    """Latest code-evolution experiment status for a strategy."""
    from services.autoresearch_service import autoresearch_service
    return await autoresearch_service.get_strategy_experiment_status(strategy_id)


@router.get("/strategy/{strategy_id}/history")
async def get_strategy_autoresearch_history(
    strategy_id: str,
    experiment_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict:
    """Iteration log for a strategy's code-evolution experiment."""
    from services.autoresearch_service import autoresearch_service
    iterations = await autoresearch_service.get_strategy_experiment_history(
        strategy_id, experiment_id=experiment_id, limit=limit
    )
    return {"iterations": iterations}


@router.post("/strategy/{strategy_id}/stream")
async def stream_strategy_autoresearch_experiment(
    strategy_id: str,
    request: Optional[StrategyAutoresearchStartRequest] = None,
):
    """Start a strategy-scoped code-evolution experiment with SSE.

    No trader/bot is involved — the experiment evolves the strategy's
    source code against the backtest data plane, validates with AST,
    and persists kept versions on the Strategy record.
    """
    from services.autoresearch_service import autoresearch_service

    settings_override: dict = {}
    if request:
        if request.model is not None:
            settings_override["model"] = request.model
        if request.max_iterations is not None:
            settings_override["max_iterations"] = request.max_iterations
        if request.mandate is not None:
            settings_override["mandate"] = request.mandate

    async def event_stream():
        try:
            async for event_dict in autoresearch_service.run_code_evolution_stream(
                trader_id=None,
                strategy_id=strategy_id,
                settings_override=settings_override or None,
            ):
                event_type = event_dict.get("event", "progress")
                data = event_dict.get("data", {})
                payload = json.dumps({"type": event_type, "data": data}, default=str)
                yield f"event: {event_type}\ndata: {payload}\n\n"
        except Exception as exc:
            logger.exception("Strategy autoresearch stream error: %s", exc)
            error_payload = json.dumps({"type": "error", "data": {"error": str(exc)}})
            yield f"event: error\ndata: {error_payload}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/strategy/{strategy_id}/stop")
async def stop_strategy_autoresearch_experiment(strategy_id: str) -> dict:
    """Stop a running strategy-scoped code experiment."""
    from services.autoresearch_service import autoresearch_service
    return await autoresearch_service.stop_strategy_experiment(strategy_id)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@router.get("/settings")
async def get_autoresearch_settings() -> dict:
    """Read autoresearch settings."""
    from services.autoresearch_service import load_autoresearch_settings
    return await load_autoresearch_settings()


@router.put("/settings")
async def update_autoresearch_settings(request: AutoresearchSettingsRequest) -> dict:
    """Update autoresearch settings."""
    from services.autoresearch_service import save_autoresearch_settings
    updates = {k: v for k, v in request.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No settings to update")
    return await save_autoresearch_settings(updates)


# ---------------------------------------------------------------------------
# A/B Experiment from Code Evolution
# ---------------------------------------------------------------------------


@router.post("/create-ab-experiment/{experiment_id}")
async def create_ab_from_autoresearch(experiment_id: str) -> dict:
    """Create a strategy A/B experiment from a completed code evolution autoresearch experiment."""
    from models.database import AsyncSessionLocal, AutoresearchExperiment, Strategy
    from services.strategy_experiments import create_strategy_experiment

    async with AsyncSessionLocal() as session:
        exp = await session.get(AutoresearchExperiment, experiment_id)
        if exp is None:
            raise HTTPException(status_code=404, detail="Experiment not found")
        if getattr(exp, "mode", "params") != "code":
            raise HTTPException(status_code=400, detail="Only code evolution experiments can create A/B tests")
        strategy_id = getattr(exp, "strategy_id", None)
        best_version = getattr(exp, "best_version", None)
        if not strategy_id or not best_version:
            raise HTTPException(status_code=400, detail="Experiment has no best version to test")

        strategy = await session.get(Strategy, strategy_id)
        if strategy is None:
            raise HTTPException(status_code=404, detail="Strategy not found")

        # Baseline version is the one before best_version
        control_version = best_version - 1
        if control_version < 1:
            control_version = 1

        ab_experiment = await create_strategy_experiment(
            session,
            name=f"autoresearch-{exp.name}",
            source_key=str(strategy.source_key or "scanner"),
            strategy_key=str(strategy.slug or ""),
            control_version=control_version,
            candidate_version=best_version,
            candidate_allocation_pct=50.0,
            created_by="autoresearch",
            notes=f"Auto-created from autoresearch experiment {experiment_id}",
            commit=True,
        )

        return {
            "ab_experiment_id": ab_experiment.id,
            "control_version": control_version,
            "candidate_version": best_version,
            "strategy_slug": strategy.slug,
        }
