"""ML training data recorder for live crypto market snapshots."""

from __future__ import annotations

import math
import time
import uuid

from sqlalchemy import delete, func, select

from models.database import AsyncSessionLocal, MLRecorderConfig, MLTrainingSnapshot
from utils.logger import get_logger
from utils.utcnow import utcnow

logger = get_logger(__name__)


def _to_float(value: object) -> float | None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _normalize_asset(value: object) -> str:
    return str(value or "").strip().lower()


def _normalize_timeframe(value: object) -> str:
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


class MLRecorder:
    """Singleton that the crypto worker calls each cycle to optionally record snapshots."""

    def __init__(self) -> None:
        self._last_record_mono: float = 0.0
        self._config_cache: dict | None = None
        self._config_fetched_mono: float = 0.0
        self._CONFIG_CACHE_TTL = 30.0  # re-read config from DB every 30s

    async def _get_config(self) -> dict:
        now = time.monotonic()
        if self._config_cache is not None and (now - self._config_fetched_mono) < self._CONFIG_CACHE_TTL:
            return self._config_cache

        try:
            async with AsyncSessionLocal() as session:
                row = (await session.execute(select(MLRecorderConfig).where(MLRecorderConfig.id == "default"))).scalar_one_or_none()
                if row is None:
                    self._config_cache = {"is_recording": False, "interval_seconds": 60, "retention_days": 90, "assets": ["btc", "eth", "sol", "xrp"], "timeframes": ["5m", "15m", "1h", "4h"], "schedule_enabled": False}
                else:
                    self._config_cache = {
                        "is_recording": bool(row.is_recording),
                        "interval_seconds": max(5, int(row.interval_seconds or 60)),
                        "retention_days": max(1, int(row.retention_days or 90)),
                        "assets": [_normalize_asset(value) for value in list(row.assets or ["btc", "eth", "sol", "xrp"])],
                        "timeframes": [_normalize_timeframe(value) for value in list(row.timeframes or ["5m", "15m", "1h", "4h"])],
                        "schedule_enabled": bool(row.schedule_enabled),
                        "schedule_start_utc": row.schedule_start_utc,
                        "schedule_end_utc": row.schedule_end_utc,
                    }
                self._config_fetched_mono = now
        except Exception as exc:
            logger.debug("MLRecorder config fetch failed", exc_info=exc)
            if self._config_cache is None:
                self._config_cache = {"is_recording": False, "interval_seconds": 60, "retention_days": 90, "assets": ["btc", "eth", "sol", "xrp"], "timeframes": ["5m", "15m", "1h", "4h"], "schedule_enabled": False}
        return self._config_cache

    async def get_config(self) -> dict:
        return dict(await self._get_config())

    def _in_schedule(self, config: dict) -> bool:
        if not config.get("schedule_enabled"):
            return True

        now = utcnow()
        current_minutes = now.hour * 60 + now.minute

        start_str = config.get("schedule_start_utc") or "00:00"
        end_str = config.get("schedule_end_utc") or "23:59"

        try:
            sh, sm = (int(x) for x in start_str.split(":"))
            eh, em = (int(x) for x in end_str.split(":"))
        except (ValueError, AttributeError):
            return True

        start_min = sh * 60 + sm
        end_min = eh * 60 + em

        if start_min <= end_min:
            return start_min <= current_minutes <= end_min
        else:
            # Wraps midnight (e.g. 22:00 → 06:00)
            return current_minutes >= start_min or current_minutes <= end_min

    async def maybe_record(self, markets_payload: list[dict]) -> int:
        """Record snapshots when enabled and the configured interval has elapsed."""
        if not markets_payload:
            return 0

        config = await self._get_config()
        if not config.get("is_recording"):
            return 0

        # Throttle by interval
        now_mono = time.monotonic()
        interval = config.get("interval_seconds", 60)
        if (now_mono - self._last_record_mono) < interval:
            return 0

        if not self._in_schedule(config):
            return 0

        allowed_assets = {_normalize_asset(value) for value in config.get("assets", [])}
        allowed_timeframes = {_normalize_timeframe(value) for value in config.get("timeframes", [])}

        snapshots: list[MLTrainingSnapshot] = []
        now = utcnow()

        for market in markets_payload:
            asset = _normalize_asset(market.get("asset"))
            timeframe = _normalize_timeframe(market.get("timeframe"))

            if asset not in allowed_assets or timeframe not in allowed_timeframes:
                continue

            up_price = _to_float(market.get("up_price"))
            down_price = _to_float(market.get("down_price"))

            if up_price is None and down_price is None:
                continue

            if up_price is not None and down_price is not None:
                mid = (up_price + (1.0 - down_price)) / 2.0
            elif up_price is not None:
                mid = up_price
            else:
                mid = 1.0 - down_price

            snapshots.append(
                MLTrainingSnapshot(
                    id=str(uuid.uuid4()),
                    asset=asset,
                    timeframe=timeframe,
                    timestamp=now,
                    mid_price=round(mid, 6),
                    up_price=up_price,
                    down_price=down_price,
                    best_bid=_to_float(market.get("best_bid")),
                    best_ask=_to_float(market.get("best_ask")),
                    spread=_to_float(market.get("spread")),
                    combined=_to_float(market.get("combined")),
                    liquidity=_to_float(market.get("liquidity")),
                    volume=_to_float(market.get("volume")),
                    volume_24h=_to_float(market.get("volume_24h")),
                    oracle_price=_to_float(market.get("oracle_price")),
                    price_to_beat=_to_float(market.get("price_to_beat")),
                    seconds_left=int(market["seconds_left"]) if _to_float(market.get("seconds_left")) is not None else None,
                    is_live=bool(market.get("is_live")),
                )
            )

        if not snapshots:
            return 0

        try:
            async with AsyncSessionLocal() as session:
                session.add_all(snapshots)
                await session.commit()
            self._last_record_mono = now_mono
            logger.debug("MLRecorder wrote snapshots", snapshot_count=len(snapshots))
            return len(snapshots)
        except Exception as exc:
            logger.warning("MLRecorder write failed", exc_info=exc)
            return 0

    async def prune_old_snapshots(self, *, older_than_days: int | None = None) -> int:
        config = await self._get_config()
        retention_days = max(1, int(older_than_days or config.get("retention_days", 90)))

        from datetime import timedelta

        cutoff = utcnow() - timedelta(days=retention_days)

        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    delete(MLTrainingSnapshot).where(MLTrainingSnapshot.timestamp < cutoff)
                )
                await session.commit()
                deleted = result.rowcount
                if deleted:
                    logger.info("MLRecorder pruned snapshots", deleted=deleted, retention_days=retention_days)
                return deleted
        except Exception as exc:
            logger.warning("MLRecorder prune failed", exc_info=exc)
            return 0

    async def get_stats(self) -> dict:
        config = await self._get_config()
        try:
            async with AsyncSessionLocal() as session:
                total_rows = (await session.execute(select(func.count(MLTrainingSnapshot.id)))).scalar() or 0
                oldest = (await session.execute(select(func.min(MLTrainingSnapshot.timestamp)))).scalar()
                newest = (await session.execute(select(func.max(MLTrainingSnapshot.timestamp)))).scalar()

                # Per-asset counts
                asset_counts_raw = (
                    await session.execute(
                        select(MLTrainingSnapshot.asset, func.count(MLTrainingSnapshot.id))
                        .group_by(MLTrainingSnapshot.asset)
                    )
                ).all()
                asset_counts = {row[0]: row[1] for row in asset_counts_raw}

            return {
                "config": config,
                "total_snapshots": total_rows,
                "oldest_snapshot": oldest.isoformat() if oldest else None,
                "newest_snapshot": newest.isoformat() if newest else None,
                "snapshots_by_asset": asset_counts,
            }
        except Exception as exc:
            logger.warning("MLRecorder stats failed", exc_info=exc)
            return {"config": config, "total_snapshots": 0, "error": str(exc)}

    def invalidate_config_cache(self) -> None:
        self._config_cache = None
        self._config_fetched_mono = 0.0


# Module-level singleton
ml_recorder = MLRecorder()
