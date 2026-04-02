from __future__ import annotations

import math
import time
from typing import Any

import numpy as np
from sqlalchemy import select

from models.database import AsyncSessionLocal, MLTrainedModel
from utils.utcnow import utcnow


def _to_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


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


def _current_price(market: dict[str, Any]) -> float | None:
    up_price = _to_float(market.get("up_price"))
    if up_price is not None:
        return up_price
    down_price = _to_float(market.get("down_price"))
    mid_price = _to_float(market.get("mid_price"))
    if mid_price is not None:
        return mid_price
    if down_price is not None:
        return 1.0 - down_price
    return None


def _current_spread(market: dict[str, Any]) -> float:
    spread = _to_float(market.get("spread"))
    if spread is not None:
        return max(0.0, spread)
    best_bid = _to_float(market.get("best_bid"))
    best_ask = _to_float(market.get("best_ask"))
    if best_bid is None or best_ask is None or best_ask < best_bid:
        return 0.0
    return best_ask - best_bid


def _prior_history_snapshots(market: dict[str, Any], current_price: float) -> list[dict[str, Any]]:
    history_tail = market.get("history_tail")
    if not isinstance(history_tail, list):
        return []
    history = [dict(snapshot) for snapshot in history_tail if isinstance(snapshot, dict)]
    if not history:
        return []
    first_mid = _to_float(history[0].get("mid"))
    if first_mid is not None and abs(first_mid - current_price) <= 1e-4:
        return history[1:]
    return history


def _feature_vector(market: dict[str, Any], feature_names: list[str]) -> np.ndarray | None:
    current_price = _current_price(market)
    if current_price is None or current_price <= 0.0:
        return None

    prior_history = _prior_history_snapshots(market, current_price)
    prior_prices = [
        price
        for price in (_to_float(snapshot.get("mid")) for snapshot in prior_history)
        if price is not None and price > 0.0
    ]

    current_spread = _current_spread(market)
    prior_spread = None
    for snapshot in prior_history:
        bid = _to_float(snapshot.get("bid"))
        ask = _to_float(snapshot.get("ask"))
        if bid is None or ask is None or ask < bid:
            continue
        prior_spread = ask - bid
        break
    spread_change_1 = current_spread - prior_spread if prior_spread is not None else 0.0

    combined = _to_float(market.get("combined"))
    if combined is None:
        up_price = _to_float(market.get("up_price"))
        down_price = _to_float(market.get("down_price"))
        combined = (up_price + down_price) if up_price is not None and down_price is not None else 1.0

    liquidity = max(1.0, _to_float(market.get("liquidity")) or 1.0)
    volume_24h = max(1.0, _to_float(market.get("volume_24h")) or _to_float(market.get("volume")) or 1.0)
    seconds_left = _to_float(market.get("seconds_left")) or 900.0
    oracle_price = _to_float(market.get("oracle_price")) or 0.0
    price_to_beat = _to_float(market.get("price_to_beat")) or 0.0

    values_by_name: dict[str, float] = {
        "mid_price": current_price,
        "price": current_price,
        "spread": current_spread,
        "combined": combined,
        "liquidity_log": math.log(liquidity),
        "volume_24h_log": math.log(volume_24h),
        "seconds_left_norm": min(1.0, seconds_left / 3600.0),
        "oracle_distance": ((current_price - oracle_price) / max(abs(oracle_price), 1.0)) if oracle_price > 0.0 else 0.0,
        "ptb_distance": ((oracle_price - price_to_beat) / max(abs(price_to_beat), 1.0)) if price_to_beat > 0.0 else 0.0,
        "spread_change_1": spread_change_1,
    }

    max_return_lag = 0
    for feature_name in feature_names:
        if feature_name.startswith("return_"):
            try:
                max_return_lag = max(max_return_lag, int(feature_name.split("_", 1)[1]))
            except (TypeError, ValueError):
                continue

    for lag in range(1, max_return_lag + 1):
        if len(prior_prices) >= lag:
            previous_price = prior_prices[lag - 1]
        else:
            previous_price = current_price
        values_by_name[f"return_{lag}"] = (current_price - previous_price) / max(abs(previous_price), 1e-6)

    try:
        return np.array([float(values_by_name.get(name, 0.0)) for name in feature_names], dtype=np.float64)
    except Exception:
        return None


class CryptoMLRuntime:
    def __init__(self) -> None:
        self._cache_ttl_seconds = 15.0
        self._cached_model_id: str | None = None
        self._cached_model_fetched_at = 0.0
        self._cached_model_payload: dict[str, Any] | None = None

    def invalidate_cache(self) -> None:
        self._cached_model_id = None
        self._cached_model_fetched_at = 0.0
        self._cached_model_payload = None

    async def _get_active_model(self) -> dict[str, Any] | None:
        now_mono = time.monotonic()
        if (
            self._cached_model_payload is not None
            and (now_mono - self._cached_model_fetched_at) < self._cache_ttl_seconds
        ):
            return self._cached_model_payload

        async with AsyncSessionLocal() as session:
            row = (
                await session.execute(
                    select(MLTrainedModel)
                    .where(
                        MLTrainedModel.status == "active",
                        MLTrainedModel.model_type == "logistic",
                    )
                    .order_by(MLTrainedModel.promoted_at.desc(), MLTrainedModel.created_at.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()

        self._cached_model_fetched_at = now_mono
        if row is None:
            self._cached_model_id = None
            self._cached_model_payload = None
            return None

        self._cached_model_id = row.id
        self._cached_model_payload = {
            "id": row.id,
            "name": row.name,
            "model_type": row.model_type,
            "version": row.version,
            "assets": [_normalize_asset(value) for value in list(row.assets or [])],
            "timeframes": [_normalize_timeframe(value) for value in list(row.timeframes or [])],
            "feature_names": list(row.feature_names or []),
            "weights_json": dict(row.weights_json or {}),
            "test_accuracy": row.test_accuracy,
            "test_auc": row.test_auc,
            "promoted_at": row.promoted_at.isoformat() if row.promoted_at else None,
        }
        return self._cached_model_payload

    @staticmethod
    def _predict_probability(feature_vector: np.ndarray, payload: dict[str, Any]) -> float | None:
        weights_json = payload.get("weights_json")
        if not isinstance(weights_json, dict):
            return None
        if str(weights_json.get("format") or "") != "logistic_regression_v1":
            return None

        coef = weights_json.get("coef") or []
        means = weights_json.get("means") or []
        scales = weights_json.get("scales") or []
        intercept = _to_float(weights_json.get("intercept"))
        if intercept is None:
            return None
        if not isinstance(coef, list) or not isinstance(means, list) or not isinstance(scales, list):
            return None
        if len(coef) != len(feature_vector) or len(means) != len(feature_vector) or len(scales) != len(feature_vector):
            return None

        feature_means = np.array([float(value) for value in means], dtype=np.float64)
        feature_scales = np.array(
            [max(1e-9, float(value)) for value in scales],
            dtype=np.float64,
        )
        weights = np.array([float(value) for value in coef], dtype=np.float64)
        z_value = float(np.dot((feature_vector - feature_means) / feature_scales, weights) + intercept)
        bounded = max(-60.0, min(60.0, z_value))
        return 1.0 / (1.0 + math.exp(-bounded))

    async def predict_market(self, market: dict[str, Any]) -> dict[str, Any] | None:
        payload = await self._get_active_model()
        if payload is None:
            return None

        asset = _normalize_asset(market.get("asset"))
        timeframe = _normalize_timeframe(market.get("timeframe"))
        if asset not in set(payload.get("assets") or []):
            return None
        if timeframe not in set(payload.get("timeframes") or []):
            return None

        feature_names = payload.get("feature_names") or []
        if not isinstance(feature_names, list) or not feature_names:
            return None

        feature_vector = _feature_vector(market, feature_names)
        if feature_vector is None:
            return None

        probability_yes = self._predict_probability(feature_vector, payload)
        if probability_yes is None:
            return None

        confidence = abs(probability_yes - 0.5) * 2.0
        return {
            "model_id": payload["id"],
            "model_name": payload["name"],
            "model_type": payload["model_type"],
            "model_version": payload["version"],
            "probability_yes": round(float(probability_yes), 6),
            "probability_no": round(float(1.0 - probability_yes), 6),
            "confidence": round(float(confidence), 6),
            "feature_count": len(feature_names),
            "predicted_at": utcnow().isoformat(),
            "test_accuracy": payload.get("test_accuracy"),
            "test_auc": payload.get("test_auc"),
            "promoted_at": payload.get("promoted_at"),
        }

    async def annotate_markets(self, markets: list[dict[str, Any]]) -> None:
        if not markets:
            return
        for market in markets:
            if not isinstance(market, dict):
                continue
            prediction = await self.predict_market(market)
            if prediction is None:
                market.pop("ml_prediction", None)
                continue
            market["ml_prediction"] = prediction


crypto_ml_runtime = CryptoMLRuntime()
