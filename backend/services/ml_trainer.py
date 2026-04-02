from __future__ import annotations

import math
import uuid
from datetime import timedelta
from typing import Any

import numpy as np
from sqlalchemy import func, select

from models.database import AsyncSessionLocal, MLTrainedModel, MLTrainingJob, MLTrainingSnapshot
from services.ml_runtime import crypto_ml_runtime
from utils.logger import get_logger
from utils.utcnow import utcnow

logger = get_logger(__name__)

SUPPORTED_MODEL_TYPE = "logistic"


def is_training_backend_available(model_type: str) -> tuple[bool, str | None]:
    normalized = str(model_type or "").strip().lower()
    if normalized != SUPPORTED_MODEL_TYPE:
        return False, f"Unsupported model_type '{model_type}'. Supported: {SUPPORTED_MODEL_TYPE}"
    try:
        from sklearn.linear_model import LogisticRegression  # noqa: F401
        from sklearn.preprocessing import StandardScaler  # noqa: F401
    except Exception as exc:
        return False, str(exc)
    return True, None


def _series_price(row: dict[str, Any]) -> float | None:
    up_price = row.get("up_price")
    if up_price is not None:
        return up_price
    return row.get("mid_price")


def _normalize_asset(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_timeframe(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"5m", "5min", "5-minute", "5minutes"}:
        return "5m"
    if text in {"15m", "15min", "15-minute", "15minutes"}:
        return "15m"
    if text in {"1h", "1hr", "1hour", "60m"}:
        return "1h"
    if text in {"4h", "4hr", "4hour", "240m"}:
        return "4h"
    return text


def _compute_features(rows: list[dict[str, Any]], lookback: int = 5) -> tuple[np.ndarray, np.ndarray, list[str]]:
    if len(rows) < lookback + 2:
        return np.array([]), np.array([]), []

    feature_names = [
        "price",
        "spread",
        "combined",
        "liquidity_log",
        "volume_24h_log",
        "seconds_left_norm",
        "oracle_distance",
        "ptb_distance",
    ]
    for lag in range(1, lookback + 1):
        feature_names.append(f"return_{lag}")
    feature_names.append("spread_change_1")

    X_rows: list[list[float]] = []
    y_rows: list[int] = []

    for idx in range(lookback, len(rows) - 1):
        current = rows[idx]
        next_row = rows[idx + 1]

        price = _series_price(current)
        next_price = _series_price(next_row)
        if price is None or next_price is None or price == 0:
            continue

        spread = float(current.get("spread") or 0.0)
        combined = float(current.get("combined") or 1.0)
        liquidity = max(1.0, float(current.get("liquidity") or 1.0))
        volume_24h = max(1.0, float(current.get("volume_24h") or current.get("volume") or 1.0))
        seconds_left = float(current.get("seconds_left") or 900.0)
        oracle_price = float(current.get("oracle_price") or 0.0)
        price_to_beat = float(current.get("price_to_beat") or 0.0)

        liquidity_log = math.log(liquidity)
        volume_log = math.log(volume_24h)
        seconds_left_norm = min(1.0, seconds_left / 3600.0)
        oracle_distance = ((price - oracle_price) / max(abs(oracle_price), 1.0)) if oracle_price > 0.0 else 0.0
        ptb_distance = ((oracle_price - price_to_beat) / max(abs(price_to_beat), 1.0)) if price_to_beat > 0.0 else 0.0

        returns: list[float] = []
        for lag in range(1, lookback + 1):
            previous_price = _series_price(rows[idx - lag]) or price
            returns.append((price - previous_price) / max(abs(previous_price), 1e-6))

        previous_spread = float(rows[idx - 1].get("spread") or 0.0)
        spread_change_1 = spread - previous_spread

        X_rows.append(
            [price, spread, combined, liquidity_log, volume_log, seconds_left_norm, oracle_distance, ptb_distance]
            + returns
            + [spread_change_1]
        )
        y_rows.append(1 if next_price > price else 0)

    if not X_rows:
        return np.array([]), np.array([]), feature_names

    return np.array(X_rows, dtype=np.float64), np.array(y_rows, dtype=np.int32), feature_names


def _manual_auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    pos = y_score[y_true == 1]
    neg = y_score[y_true == 0]
    if len(pos) == 0 or len(neg) == 0:
        return 0.5
    total = 0.0
    for score in pos:
        total += float(np.sum(score > neg) + (0.5 * np.sum(score == neg)))
    return float(total / (len(pos) * len(neg)))


def _train_logistic(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
    feature_names: list[str],
    hyperparams: dict | None = None,
) -> dict[str, Any]:
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import roc_auc_score
        from sklearn.preprocessing import StandardScaler
    except Exception as exc:
        raise RuntimeError(f"scikit-learn logistic backend unavailable: {exc}") from exc

    params = {
        "C": 1.0,
        "max_iter": 1000,
        "class_weight": "balanced",
        "solver": "lbfgs",
        "random_state": 0,
    }
    if hyperparams:
        params.update(hyperparams)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    classifier = LogisticRegression(
        C=float(params.get("C", 1.0)),
        max_iter=max(100, int(params.get("max_iter", 1000))),
        class_weight=params.get("class_weight", "balanced"),
        solver=str(params.get("solver", "lbfgs")),
        random_state=int(params.get("random_state", 0)),
    )
    classifier.fit(X_train_scaled, y_train)

    train_proba = classifier.predict_proba(X_train_scaled)[:, 1]
    test_proba = classifier.predict_proba(X_test_scaled)[:, 1]
    train_pred = (train_proba >= 0.5).astype(int)
    test_pred = (test_proba >= 0.5).astype(int)

    train_accuracy = float(np.mean(train_pred == y_train))
    test_accuracy = float(np.mean(test_pred == y_test))
    test_auc = float(roc_auc_score(y_test, test_proba)) if len(np.unique(y_test)) > 1 else _manual_auc(y_test, test_proba)

    coefficients = [float(value) for value in classifier.coef_[0]]
    feature_importance = dict(zip(feature_names, [abs(value) for value in coefficients]))

    return {
        "model_json": {
            "format": "logistic_regression_v1",
            "coef": coefficients,
            "intercept": float(classifier.intercept_[0]),
            "means": [float(value) for value in scaler.mean_],
            "scales": [max(1e-9, float(value)) for value in scaler.scale_],
        },
        "params": {
            "C": float(classifier.C),
            "max_iter": int(classifier.max_iter),
            "class_weight": classifier.class_weight,
            "solver": classifier.solver,
            "random_state": int(params.get("random_state", 0)),
        },
        "train_accuracy": train_accuracy,
        "test_accuracy": test_accuracy,
        "test_auc": test_auc,
        "feature_importance": feature_importance,
        "train_samples": len(y_train),
        "test_samples": len(y_test),
    }


def _walk_forward_train(
    all_rows: list[dict[str, Any]],
    n_folds: int = 5,
    min_train_size: int = 200,
    hyperparams: dict | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    X_all, y_all, feature_names = _compute_features(all_rows)
    if len(X_all) < min_train_size + 50:
        return None, [{"error": f"Not enough data: {len(X_all)} rows (need {min_train_size + 50})"}]

    fold_size = len(X_all) // n_folds
    if fold_size < 20:
        n_folds = max(2, len(X_all) // 50)
        fold_size = len(X_all) // max(1, n_folds)

    fold_results: list[dict[str, Any]] = []
    best_result = None
    best_auc = -1.0

    for fold in range(1, n_folds):
        train_end = fold * fold_size
        test_end = min((fold + 1) * fold_size, len(X_all))
        if train_end < min_train_size:
            continue

        X_train = X_all[:train_end]
        y_train = y_all[:train_end]
        X_test = X_all[train_end:test_end]
        y_test = y_all[train_end:test_end]
        if len(X_test) < 10:
            continue

        try:
            result = _train_logistic(X_train, y_train, X_test, y_test, feature_names, hyperparams)
            fold_results.append(
                {
                    "fold": fold,
                    "train_size": len(X_train),
                    "test_size": len(X_test),
                    "train_accuracy": result["train_accuracy"],
                    "test_accuracy": result["test_accuracy"],
                    "test_auc": result["test_auc"],
                }
            )
            if result["test_auc"] > best_auc:
                best_auc = result["test_auc"]
                best_result = result
        except Exception as exc:
            fold_results.append({"fold": fold, "error": str(exc)})

    return best_result, fold_results


def _build_group_feature_blocks(
    groups: dict[tuple[str, str], list[dict[str, Any]]],
) -> tuple[list[np.ndarray], list[np.ndarray], list[str]]:
    feature_names: list[str] = []
    X_blocks: list[np.ndarray] = []
    y_blocks: list[np.ndarray] = []
    for group_rows in groups.values():
        X_group, y_group, group_feature_names = _compute_features(group_rows)
        if len(X_group) == 0 or len(y_group) == 0:
            continue
        if not feature_names:
            feature_names = group_feature_names
        X_blocks.append(X_group)
        y_blocks.append(y_group)
    return X_blocks, y_blocks, feature_names


async def run_training_job(
    job_id: str,
    model_type: str = SUPPORTED_MODEL_TYPE,
    assets: list[str] | None = None,
    timeframes: list[str] | None = None,
    hyperparams: dict | None = None,
    days_lookback: int = 30,
) -> dict[str, Any]:
    assets = [_normalize_asset(value) for value in (assets or ["btc", "eth", "sol", "xrp"])]
    timeframes = [_normalize_timeframe(value) for value in (timeframes or ["5m", "15m", "1h", "4h"])]

    backend_ok, backend_error = is_training_backend_available(model_type)
    if not backend_ok:
        return {"error": backend_error or "Training backend unavailable"}

    async def _update_job(status: str, progress: float, message: str, **extra: Any) -> None:
        try:
            async with AsyncSessionLocal() as session:
                job = (await session.execute(select(MLTrainingJob).where(MLTrainingJob.id == job_id))).scalar_one_or_none()
                if job is None:
                    return
                job.status = status
                job.progress = progress
                job.message = message
                if status == "running" and job.started_at is None:
                    job.started_at = utcnow()
                if status in {"completed", "failed"}:
                    job.finished_at = utcnow()
                for key, value in extra.items():
                    if hasattr(job, key):
                        setattr(job, key, value)
                await session.commit()
        except Exception:
            pass

    await _update_job("running", 0.05, "Loading recorded crypto snapshots...")

    cutoff = utcnow() - timedelta(days=days_lookback)
    try:
        async with AsyncSessionLocal() as session:
            rows_raw = (
                await session.execute(
                    select(MLTrainingSnapshot)
                    .where(
                        MLTrainingSnapshot.asset.in_(assets),
                        MLTrainingSnapshot.timeframe.in_(timeframes),
                        MLTrainingSnapshot.timestamp >= cutoff,
                    )
                    .order_by(MLTrainingSnapshot.timestamp)
                )
            ).scalars().all()
    except Exception as exc:
        await _update_job("failed", 0.0, f"DB query failed: {exc}", error=str(exc))
        return {"error": str(exc)}

    if not rows_raw:
        await _update_job("failed", 0.0, "No training snapshots found", error="No data available")
        return {"error": "No training snapshots found"}

    await _update_job("running", 0.15, f"Loaded {len(rows_raw)} snapshots. Grouping by asset and timeframe...")

    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows_raw:
        groups.setdefault((row.asset, row.timeframe), []).append(
            {
                "up_price": row.up_price,
                "mid_price": row.mid_price,
                "spread": row.spread,
                "combined": row.combined,
                "liquidity": row.liquidity,
                "volume": row.volume,
                "volume_24h": row.volume_24h,
                "seconds_left": row.seconds_left,
                "oracle_price": row.oracle_price,
                "price_to_beat": row.price_to_beat,
            }
        )

    await _update_job("running", 0.25, f"Training across {len(groups)} asset/timeframe groups...")

    all_fold_results: list[dict[str, Any]] = []
    best_overall_result = None
    best_overall_auc = -1.0

    for index, ((asset, timeframe), group_rows) in enumerate(groups.items(), start=1):
        progress = 0.25 + (0.6 * (index / max(1, len(groups))))
        await _update_job("running", progress, f"Training {asset}/{timeframe} ({len(group_rows)} rows)...")

        result, folds = _walk_forward_train(group_rows, hyperparams=hyperparams)
        for fold in folds:
            fold["asset"] = asset
            fold["timeframe"] = timeframe
        all_fold_results.extend(folds)

        if result is not None and float(result.get("test_auc", 0.0)) > best_overall_auc:
            best_overall_auc = float(result["test_auc"])
            best_overall_result = result

    if best_overall_result is None:
        await _update_job("failed", 0.9, "All training folds failed or lacked enough data", error="No successful training")
        return {"error": "All training folds failed", "fold_results": all_fold_results}

    await _update_job("running", 0.88, "Training final logistic model on all available data...")

    X_blocks, y_blocks, feature_names = _build_group_feature_blocks(groups)
    total_feature_rows = sum(len(block) for block in X_blocks)
    if total_feature_rows >= 50 and feature_names:
        X_train_parts: list[np.ndarray] = []
        X_test_parts: list[np.ndarray] = []
        y_train_parts: list[np.ndarray] = []
        y_test_parts: list[np.ndarray] = []

        for X_group, y_group in zip(X_blocks, y_blocks):
            if len(X_group) < 10:
                continue
            split_idx = max(1, int(len(X_group) * 0.8))
            if split_idx >= len(X_group):
                split_idx = len(X_group) - 1
            if split_idx <= 0:
                continue
            X_train_parts.append(X_group[:split_idx])
            X_test_parts.append(X_group[split_idx:])
            y_train_parts.append(y_group[:split_idx])
            y_test_parts.append(y_group[split_idx:])

        if not X_train_parts or not X_test_parts:
            final_result = best_overall_result
        else:
            X_train = np.vstack(X_train_parts)
            X_test = np.vstack(X_test_parts)
            y_train = np.concatenate(y_train_parts)
            y_test = np.concatenate(y_test_parts)
            try:
                final_result = _train_logistic(X_train, y_train, X_test, y_test, feature_names, hyperparams)
            except Exception as exc:
                await _update_job("failed", 0.9, f"Final model training failed: {exc}", error=str(exc))
                return {"error": str(exc), "fold_results": all_fold_results}
    else:
        final_result = best_overall_result

    await _update_job("running", 0.93, "Saving trained model artifact...")

    model_id = str(uuid.uuid4())
    try:
        async with AsyncSessionLocal() as session:
            max_version = (
                await session.execute(
                    select(func.max(MLTrainedModel.version)).where(MLTrainedModel.name == f"crypto_directional_{model_type}")
                )
            ).scalar() or 0
    except Exception:
        max_version = 0
    new_version = int(max_version) + 1

    date_range = {
        "start": rows_raw[0].timestamp.isoformat() if rows_raw and rows_raw[0].timestamp else None,
        "end": rows_raw[-1].timestamp.isoformat() if rows_raw and rows_raw[-1].timestamp else None,
    }

    trained_model = MLTrainedModel(
        id=model_id,
        name=f"crypto_directional_{model_type}",
        model_type=model_type,
        version=new_version,
        status="trained",
        weights_json=final_result["model_json"],
        feature_names=feature_names,
        hyperparams=final_result.get("params"),
        assets=assets,
        timeframes=timeframes,
        train_accuracy=final_result["train_accuracy"],
        test_accuracy=final_result["test_accuracy"],
        test_auc=final_result["test_auc"],
        feature_importance=final_result.get("feature_importance"),
        train_samples=final_result["train_samples"],
        test_samples=final_result["test_samples"],
        training_date_range=date_range,
        walkforward_results=all_fold_results,
        created_at=utcnow(),
    )

    try:
        async with AsyncSessionLocal() as session:
            session.add(trained_model)
            await session.commit()
    except Exception as exc:
        await _update_job("failed", 0.95, f"Failed to save model: {exc}", error=str(exc))
        return {"error": str(exc)}

    summary = {
        "model_id": model_id,
        "model_type": model_type,
        "version": new_version,
        "train_accuracy": final_result["train_accuracy"],
        "test_accuracy": final_result["test_accuracy"],
        "test_auc": final_result["test_auc"],
        "train_samples": final_result["train_samples"],
        "test_samples": final_result["test_samples"],
        "groups_trained": len(groups),
        "total_snapshots": len(rows_raw),
        "walkforward_folds": len(all_fold_results),
        "date_range": date_range,
    }

    await _update_job(
        "completed",
        1.0,
        f"Model v{new_version}: test_acc={final_result['test_accuracy']:.3f} auc={final_result['test_auc']:.3f}",
        trained_model_id=model_id,
        result_summary=summary,
    )

    logger.info(
        "ML training complete",
        model_type=model_type,
        version=new_version,
        test_accuracy=final_result["test_accuracy"],
        test_auc=final_result["test_auc"],
        snapshot_count=len(rows_raw),
    )
    return summary


async def promote_model(model_id: str) -> dict[str, Any]:
    async with AsyncSessionLocal() as session:
        model = (await session.execute(select(MLTrainedModel).where(MLTrainedModel.id == model_id))).scalar_one_or_none()
        if model is None:
            return {"error": "Model not found"}

        active_models = (
            await session.execute(select(MLTrainedModel).where(MLTrainedModel.status == "active"))
        ).scalars().all()
        for active_model in active_models:
            active_model.status = "archived"

        model.status = "active"
        model.promoted_at = utcnow()
        await session.commit()

    crypto_ml_runtime.invalidate_cache()
    return {
        "model_id": model.id,
        "name": model.name,
        "version": model.version,
        "status": "active",
        "promoted_at": model.promoted_at.isoformat(),
    }


async def archive_model(model_id: str) -> dict[str, Any]:
    async with AsyncSessionLocal() as session:
        model = (await session.execute(select(MLTrainedModel).where(MLTrainedModel.id == model_id))).scalar_one_or_none()
        if model is None:
            return {"error": "Model not found"}
        model.status = "archived"
        await session.commit()

    crypto_ml_runtime.invalidate_cache()
    return {"model_id": model.id, "status": "archived"}
