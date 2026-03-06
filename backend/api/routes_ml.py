"""ML Training Pipeline API Routes

Provides endpoints for:
- Recording control (start/stop/configure snapshots)
- Training (trigger, status, list models)
- Model management (promote, archive, delete)
- Data stats & pruning
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select

from models.database import (
    AsyncSessionLocal,
    MLRecorderConfig,
    MLTrainedModel,
    MLTrainingJob,
    MLTrainingSnapshot,
)
from services.ml_recorder import ml_recorder
from services.ml_trainer import archive_model, promote_model, run_training_job
from utils.utcnow import utcnow

router = APIRouter(prefix="/ml", tags=["ML Training Pipeline"])


# ── Pydantic request/response models ──────────────────────────────────────


class RecorderConfigUpdate(BaseModel):
    is_recording: Optional[bool] = None
    interval_seconds: Optional[int] = Field(None, ge=5, le=3600)
    retention_days: Optional[int] = Field(None, ge=1, le=365)
    assets: Optional[list[str]] = None
    timeframes: Optional[list[str]] = None
    schedule_enabled: Optional[bool] = None
    schedule_start_utc: Optional[str] = None
    schedule_end_utc: Optional[str] = None


class TrainRequest(BaseModel):
    model_config = {"protected_namespaces": ()}
    model_type: str = Field("xgboost", pattern=r"^(xgboost|lightgbm)$")
    assets: Optional[list[str]] = None
    timeframes: Optional[list[str]] = None
    days_lookback: int = Field(30, ge=1, le=365)
    hyperparams: Optional[dict] = None


class PromoteRequest(BaseModel):
    model_config = {"protected_namespaces": ()}
    model_id: str


class PruneRequest(BaseModel):
    older_than_days: Optional[int] = Field(None, ge=1, le=365)
    confirm: bool = False


class ExportRequest(BaseModel):
    asset: Optional[str] = None
    timeframe: Optional[str] = None
    days: int = Field(7, ge=1, le=90)


# ── Recorder config endpoints ─────────────────────────────────────────────


@router.get("/recorder/config")
async def get_recorder_config():
    """Get current recorder configuration and recording stats."""
    stats = await ml_recorder.get_stats()
    return stats


@router.put("/recorder/config")
async def update_recorder_config(body: RecorderConfigUpdate):
    """Update recorder configuration (start/stop recording, change interval, etc)."""
    async with AsyncSessionLocal() as session:
        row = (await session.execute(select(MLRecorderConfig).where(MLRecorderConfig.id == "default"))).scalar_one_or_none()

        if row is None:
            row = MLRecorderConfig(id="default")
            session.add(row)

        if body.is_recording is not None:
            row.is_recording = body.is_recording
        if body.interval_seconds is not None:
            row.interval_seconds = body.interval_seconds
        if body.retention_days is not None:
            row.retention_days = body.retention_days
        if body.assets is not None:
            allowed = {"btc", "eth", "sol", "xrp"}
            row.assets = [a for a in body.assets if a in allowed]
        if body.timeframes is not None:
            allowed = {"5m", "15m", "1h", "4h"}
            row.timeframes = [t for t in body.timeframes if t in allowed]
        if body.schedule_enabled is not None:
            row.schedule_enabled = body.schedule_enabled
        if body.schedule_start_utc is not None:
            row.schedule_start_utc = body.schedule_start_utc
        if body.schedule_end_utc is not None:
            row.schedule_end_utc = body.schedule_end_utc

        row.updated_at = utcnow()
        await session.commit()

    ml_recorder.invalidate_config_cache()

    return await ml_recorder.get_stats()


# ── Data stats & management ───────────────────────────────────────────────


@router.get("/data/stats")
async def get_data_stats():
    """Get detailed snapshot statistics."""
    async with AsyncSessionLocal() as session:
        total = (await session.execute(select(func.count(MLTrainingSnapshot.id)))).scalar() or 0
        oldest = (await session.execute(select(func.min(MLTrainingSnapshot.timestamp)))).scalar()
        newest = (await session.execute(select(func.max(MLTrainingSnapshot.timestamp)))).scalar()

        # Per asset+timeframe
        breakdown = (
            await session.execute(
                select(
                    MLTrainingSnapshot.asset,
                    MLTrainingSnapshot.timeframe,
                    func.count(MLTrainingSnapshot.id),
                    func.min(MLTrainingSnapshot.timestamp),
                    func.max(MLTrainingSnapshot.timestamp),
                ).group_by(MLTrainingSnapshot.asset, MLTrainingSnapshot.timeframe)
            )
        ).all()

        groups = [
            {
                "asset": row[0],
                "timeframe": row[1],
                "count": row[2],
                "oldest": row[3].isoformat() if row[3] else None,
                "newest": row[4].isoformat() if row[4] else None,
            }
            for row in breakdown
        ]

    return {
        "total_snapshots": total,
        "oldest": oldest.isoformat() if oldest else None,
        "newest": newest.isoformat() if newest else None,
        "groups": groups,
    }


@router.post("/data/prune")
async def prune_data(body: PruneRequest):
    """Delete old snapshots.  Requires confirm=true."""
    if not body.confirm:
        # Dry-run: show what would be deleted
        days = body.older_than_days
        if days is None:
            config = await ml_recorder._get_config()
            days = config.get("retention_days", 90)

        from datetime import timedelta

        cutoff = utcnow() - timedelta(days=days)
        async with AsyncSessionLocal() as session:
            count = (
                await session.execute(
                    select(func.count(MLTrainingSnapshot.id)).where(MLTrainingSnapshot.timestamp < cutoff)
                )
            ).scalar() or 0

        return {"action": "dry_run", "would_delete": count, "older_than_days": days, "cutoff": cutoff.isoformat()}

    deleted = await ml_recorder.prune_old_snapshots()
    return {"action": "pruned", "deleted": deleted}


@router.delete("/data/all")
async def delete_all_data(confirm: bool = Query(False)):
    """Delete ALL training snapshots.  Requires confirm=true."""
    if not confirm:
        raise HTTPException(400, "Pass confirm=true to delete all ML training data")

    async with AsyncSessionLocal() as session:
        result = await session.execute(delete(MLTrainingSnapshot))
        await session.commit()
        return {"deleted": result.rowcount}


# ── Training endpoints ────────────────────────────────────────────────────


_training_tasks: set[asyncio.Task] = set()


@router.post("/train")
async def start_training(body: TrainRequest):
    """Start an ML training job (async).  Returns job ID for polling."""
    job_id = str(uuid.uuid4())

    # Create job row
    async with AsyncSessionLocal() as session:
        job = MLTrainingJob(
            id=job_id,
            status="queued",
            model_type=body.model_type,
            assets=body.assets or ["btc", "eth", "sol", "xrp"],
            timeframes=body.timeframes or ["5m", "15m", "1h", "4h"],
            created_at=utcnow(),
        )
        session.add(job)
        await session.commit()

    # Launch training in background — hold a strong reference so
    # the task isn't GC'd (which would leak DB connections).
    task = asyncio.create_task(
        run_training_job(
            job_id=job_id,
            model_type=body.model_type,
            assets=body.assets,
            timeframes=body.timeframes,
            hyperparams=body.hyperparams,
            days_lookback=body.days_lookback,
        )
    )
    _training_tasks.add(task)
    task.add_done_callback(_training_tasks.discard)

    return {"job_id": job_id, "status": "queued"}


@router.get("/train/jobs")
async def list_training_jobs(
    status: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    """List training jobs, newest first."""
    async with AsyncSessionLocal() as session:
        query = select(MLTrainingJob).order_by(MLTrainingJob.created_at.desc()).limit(limit)
        if status:
            query = query.where(MLTrainingJob.status == status)
        rows = (await session.execute(query)).scalars().all()

    return [
        {
            "id": r.id,
            "status": r.status,
            "model_type": r.model_type,
            "assets": r.assets,
            "timeframes": r.timeframes,
            "progress": r.progress,
            "message": r.message,
            "error": r.error,
            "trained_model_id": r.trained_model_id,
            "result_summary": r.result_summary,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        }
        for r in rows
    ]


@router.get("/train/jobs/{job_id}")
async def get_training_job(job_id: str):
    """Get a specific training job's status and results."""
    async with AsyncSessionLocal() as session:
        job = (await session.execute(select(MLTrainingJob).where(MLTrainingJob.id == job_id))).scalar_one_or_none()
        if not job:
            raise HTTPException(404, "Training job not found")

    return {
        "id": job.id,
        "status": job.status,
        "model_type": job.model_type,
        "assets": job.assets,
        "timeframes": job.timeframes,
        "progress": job.progress,
        "message": job.message,
        "error": job.error,
        "trained_model_id": job.trained_model_id,
        "result_summary": job.result_summary,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
    }


# ── Model management endpoints ────────────────────────────────────────────


@router.get("/models")
async def list_models(
    status: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    """List trained models, newest first."""
    async with AsyncSessionLocal() as session:
        query = select(MLTrainedModel).order_by(MLTrainedModel.created_at.desc()).limit(limit)
        if status:
            query = query.where(MLTrainedModel.status == status)
        rows = (await session.execute(query)).scalars().all()

    return [
        {
            "id": r.id,
            "name": r.name,
            "model_type": r.model_type,
            "version": r.version,
            "status": r.status,
            "assets": r.assets,
            "timeframes": r.timeframes,
            "train_accuracy": r.train_accuracy,
            "test_accuracy": r.test_accuracy,
            "test_auc": r.test_auc,
            "feature_importance": r.feature_importance,
            "train_samples": r.train_samples,
            "test_samples": r.test_samples,
            "training_date_range": r.training_date_range,
            "walkforward_results": r.walkforward_results,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "promoted_at": r.promoted_at.isoformat() if r.promoted_at else None,
            "notes": r.notes,
        }
        for r in rows
    ]


@router.get("/models/{model_id}")
async def get_model(model_id: str):
    """Get a specific model's details (including walk-forward results)."""
    async with AsyncSessionLocal() as session:
        model = (await session.execute(select(MLTrainedModel).where(MLTrainedModel.id == model_id))).scalar_one_or_none()
        if not model:
            raise HTTPException(404, "Model not found")

    return {
        "id": model.id,
        "name": model.name,
        "model_type": model.model_type,
        "version": model.version,
        "status": model.status,
        "assets": model.assets,
        "timeframes": model.timeframes,
        "train_accuracy": model.train_accuracy,
        "test_accuracy": model.test_accuracy,
        "test_auc": model.test_auc,
        "feature_importance": model.feature_importance,
        "train_samples": model.train_samples,
        "test_samples": model.test_samples,
        "training_date_range": model.training_date_range,
        "walkforward_results": model.walkforward_results,
        "hyperparams": model.hyperparams,
        "feature_names": model.feature_names,
        "created_at": model.created_at.isoformat() if model.created_at else None,
        "promoted_at": model.promoted_at.isoformat() if model.promoted_at else None,
        "notes": model.notes,
    }


@router.post("/models/{model_id}/promote")
async def promote_model_endpoint(model_id: str):
    """Promote a trained model to 'active' status (demotes any currently active)."""
    result = await promote_model(model_id)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@router.post("/models/{model_id}/archive")
async def archive_model_endpoint(model_id: str):
    """Archive a model (remove from active use)."""
    result = await archive_model(model_id)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@router.delete("/models/{model_id}")
async def delete_model(model_id: str, confirm: bool = Query(False)):
    """Permanently delete a model.  Cannot delete 'active' models."""
    if not confirm:
        raise HTTPException(400, "Pass confirm=true to delete the model")

    async with AsyncSessionLocal() as session:
        model = (await session.execute(select(MLTrainedModel).where(MLTrainedModel.id == model_id))).scalar_one_or_none()
        if not model:
            raise HTTPException(404, "Model not found")
        if model.status == "active":
            raise HTTPException(400, "Cannot delete an active model. Archive it first.")

        await session.delete(model)
        await session.commit()
        return {"deleted": model_id}


# ── Active model (for strategies to consume) ──────────────────────────────


@router.get("/active-model")
async def get_active_model(model_type: Optional[str] = Query(None)):
    """Get the currently active model (used by strategies at runtime)."""
    async with AsyncSessionLocal() as session:
        query = select(MLTrainedModel).where(MLTrainedModel.status == "active")
        if model_type:
            query = query.where(MLTrainedModel.model_type == model_type)
        query = query.order_by(MLTrainedModel.promoted_at.desc()).limit(1)
        model = (await session.execute(query)).scalar_one_or_none()

    if not model:
        return {"active": False, "model": None}

    return {
        "active": True,
        "model": {
            "id": model.id,
            "name": model.name,
            "model_type": model.model_type,
            "version": model.version,
            "test_accuracy": model.test_accuracy,
            "test_auc": model.test_auc,
            "promoted_at": model.promoted_at.isoformat() if model.promoted_at else None,
        },
    }
