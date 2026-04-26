from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Optional

from services.binance_feed import get_binance_feed
from services.chainlink_feed import get_chainlink_feed
from services.chainlink_direct_feed import (
    ChainlinkDirectFeed,
    ChainlinkDirectPrice,
)
from utils.logger import get_logger

logger = get_logger(__name__)


class ReferenceRuntime:
    def __init__(self) -> None:
        self._started = False
        self._start_lock = asyncio.Lock()
        self._binance_feed = get_binance_feed()
        self._chainlink_feed = get_chainlink_feed()
        self._callbacks: list[Callable[[str], None]] = []
        self._binance_feed.on_update(self._on_binance_update)
        self._chainlink_feed.on_update(self._on_chainlink_update)
        # Direct Chainlink Data Streams feed. Initialized lazily in start()
        # so we can read API credentials from AppSettings — without them
        # the feed latches DISABLED and never connects.
        self._chainlink_direct_feed: Optional[ChainlinkDirectFeed] = None

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        if self._started:
            return
        async with self._start_lock:
            if self._started:
                return
            await self._chainlink_feed.start()
            await self._binance_feed.start()
            await self._start_chainlink_direct()
            self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        await self._binance_feed.stop()
        await self._chainlink_feed.stop()
        if self._chainlink_direct_feed is not None:
            await self._chainlink_direct_feed.stop()
        self._started = False

    async def _start_chainlink_direct(self) -> None:
        """Boot the direct Chainlink Data Streams feed if credentials exist.

        Reads ``chainlink_direct_api_key`` / ``chainlink_direct_user_secret``
        from AppSettings on each call — when missing the feed latches
        DISABLED and the connect loop never runs (avoids 401 storms).
        """
        try:
            from sqlalchemy import select
            from models.database import AppSettings, AsyncSessionLocal
            from utils.secrets import decrypt_secret
        except Exception:
            return

        # Snapshot creds once at start. The feed reads them via getters
        # each request so a settings-update + rearm() will pick up new
        # values without restarting the runtime.
        cached_api_key = ""
        cached_user_secret = ""

        async def _refresh_credentials() -> None:
            nonlocal cached_api_key, cached_user_secret
            try:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(select(AppSettings).limit(1))
                    row = result.scalar_one_or_none()
                if row is None:
                    return
                cached_api_key = decrypt_secret(getattr(row, "chainlink_direct_api_key", None)) or ""
                cached_user_secret = decrypt_secret(getattr(row, "chainlink_direct_user_secret", None)) or ""
            except Exception as exc:
                logger.debug("ChainlinkDirectFeed credential refresh failed", error=str(exc))

        await _refresh_credentials()
        self._refresh_chainlink_direct_credentials = _refresh_credentials  # exposed for settings-update path

        feed = ChainlinkDirectFeed(
            get_api_key=lambda: cached_api_key,
            get_user_secret=lambda: cached_user_secret,
        )
        feed.on_update(self._on_chainlink_direct_update)
        self._chainlink_direct_feed = feed
        await feed.start()

    async def rearm_chainlink_direct(self) -> None:
        """Re-evaluate Chainlink Data Streams credentials and restart the feed.

        Call from the settings-update handler after a user saves new
        Chainlink API credentials. Safe to call when the feed isn't
        running — it will be created if needed.
        """
        if self._chainlink_direct_feed is None:
            await self._start_chainlink_direct()
            return
        if hasattr(self, "_refresh_chainlink_direct_credentials"):
            await self._refresh_chainlink_direct_credentials()
        self._chainlink_direct_feed.rearm()
        if not self._chainlink_direct_feed.started:
            await self._chainlink_direct_feed.start()

    def _on_chainlink_direct_update(self, price: ChainlinkDirectPrice) -> None:
        """Forward a direct Chainlink price into the shared OraclePrice map."""
        try:
            self._chainlink_feed.update_from_chainlink_direct(
                price.asset,
                price.price,
                price.bid,
                price.ask,
                price.observations_timestamp_ms,
            )
        except Exception:
            logger.exception("update_from_chainlink_direct failed")
        self._notify_update(price.asset)

    def on_update(self, callback: Callable[[str], None]) -> None:
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def remove_on_update(self, callback: Callable[[str], None]) -> None:
        self._callbacks = [registered for registered in self._callbacks if registered != callback]

    def _on_binance_update(
        self,
        asset: str,
        mid: float,
        bid: float,
        ask: float,
        timestamp_ms: int,
    ) -> None:
        self._chainlink_feed.update_from_binance_direct(asset, mid, bid, ask, timestamp_ms)
        self._notify_update(asset)

    def _on_chainlink_update(self, oracle: Any) -> None:
        if str(getattr(oracle, "source", "") or "").strip().lower() == "binance_direct":
            return
        asset = str(getattr(oracle, "asset", "") or "").strip().upper()
        if asset:
            self._notify_update(asset)

    def _notify_update(self, asset: str) -> None:
        normalized = str(asset or "").strip().upper()
        if not normalized:
            return
        for callback in tuple(self._callbacks):
            try:
                callback(normalized)
            except Exception:
                pass

    def get_oracle_price(self, asset: str) -> dict[str, Any] | None:
        oracle = self._chainlink_feed.get_price(asset)
        if oracle is None:
            return None
        now_ms = int(time.time() * 1000)
        updated_at_ms = int(getattr(oracle, "updated_at_ms", 0) or 0)
        return {
            "asset": str(asset or "").upper(),
            "price": float(oracle.price),
            "updated_at_ms": updated_at_ms or None,
            "age_seconds": (round((now_ms - updated_at_ms) / 1000.0, 3) if updated_at_ms > 0 else None),
            "source": str(getattr(oracle, "source", "") or ""),
        }

    def get_oracle_prices_by_source(self, asset: str) -> dict[str, dict[str, Any]]:
        now_ms = int(time.time() * 1000)
        out: dict[str, dict[str, Any]] = {}
        for source, oracle in self._chainlink_feed.get_prices_by_source(asset).items():
            updated_at_ms = int(getattr(oracle, "updated_at_ms", 0) or 0)
            out[str(source)] = {
                "source": str(source),
                "price": float(getattr(oracle, "price", 0.0)),
                "updated_at_ms": updated_at_ms or None,
                "age_seconds": (round((now_ms - updated_at_ms) / 1000.0, 3) if updated_at_ms > 0 else None),
            }
        return out

    def get_oracle_history(self, asset: str, *, points: int = 80) -> list[dict[str, Any]]:
        history = getattr(self._chainlink_feed, "_history", {}).get(str(asset or "").upper())
        if not history:
            return []
        return [
            {"t": int(ts_ms), "p": round(float(price), 6)}
            for ts_ms, price in list(history)[-max(1, int(points)) :]
        ]

    def get_oracle_motion_summary(
        self,
        asset: str,
        *,
        min_coverage_ratio: float = 0.6,
    ) -> dict[str, Any]:
        history = getattr(self._chainlink_feed, "_history", {}).get(str(asset or "").upper())
        if not history:
            return {}

        latest_ts_ms, latest_price = history[-1]
        try:
            latest_ts_ms = int(latest_ts_ms)
            latest_price = float(latest_price)
        except (TypeError, ValueError):
            return {}
        if latest_ts_ms <= 0 or latest_price <= 0:
            return {}

        summary: dict[str, Any] = {
            "latest_ts_ms": latest_ts_ms,
            "latest_price": latest_price,
        }
        try:
            oldest_ts_ms = int(history[0][0])
            summary["history_coverage_seconds"] = max(0.0, (latest_ts_ms - oldest_ts_ms) / 1000.0)
        except (TypeError, ValueError, IndexError):
            summary["history_coverage_seconds"] = None

        for key, lookback_seconds in (
            ("move_5m", 300.0),
            ("move_30m", 1800.0),
            ("move_2h", 7200.0),
        ):
            move = self._oracle_move_from_history(
                history,
                latest_ts_ms=latest_ts_ms,
                latest_price=latest_price,
                lookback_seconds=lookback_seconds,
                min_coverage_ratio=min_coverage_ratio,
            )
            if move is not None:
                summary[key] = move
        return summary

    @staticmethod
    def _oracle_move_from_history(
        history: Any,
        *,
        latest_ts_ms: int,
        latest_price: float,
        lookback_seconds: float,
        min_coverage_ratio: float,
    ) -> dict[str, Any] | None:
        target_ts_ms = latest_ts_ms - int(max(1.0, float(lookback_seconds)) * 1000.0)
        prior_ts_ms: int | None = None
        prior_price: float | None = None

        for raw_ts_ms, raw_price in reversed(history):
            try:
                ts_ms = int(raw_ts_ms)
                price = float(raw_price)
            except (TypeError, ValueError):
                continue
            if ts_ms <= target_ts_ms and price > 0:
                prior_ts_ms = ts_ms
                prior_price = price
                break

        if prior_ts_ms is None or prior_price is None:
            for raw_ts_ms, raw_price in history:
                try:
                    ts_ms = int(raw_ts_ms)
                    price = float(raw_price)
                except (TypeError, ValueError):
                    continue
                if price <= 0:
                    continue
                actual_lookback_seconds = max(0.0, (latest_ts_ms - ts_ms) / 1000.0)
                if actual_lookback_seconds < max(1.0, float(lookback_seconds)) * max(0.0, min(1.0, min_coverage_ratio)):
                    return None
                prior_ts_ms = ts_ms
                prior_price = price
                break

        if prior_ts_ms is None or prior_price is None or prior_price <= 0:
            return None

        actual_lookback_seconds = max(0.0, (latest_ts_ms - prior_ts_ms) / 1000.0)
        percent = ((latest_price - prior_price) / prior_price) * 100.0
        return {
            "percent": percent,
            "actual_lookback_seconds": actual_lookback_seconds,
            "coverage_ratio": actual_lookback_seconds / max(1.0, float(lookback_seconds)),
            "prior_ts_ms": prior_ts_ms,
            "prior_price": prior_price,
            "latest_ts_ms": latest_ts_ms,
            "latest_price": latest_price,
        }

    def get_status(self) -> dict[str, Any]:
        chainlink_prices = self._chainlink_feed.get_all_prices()
        latest_chainlink_ms = max(
            (int(getattr(price, "updated_at_ms", 0) or 0) for price in chainlink_prices.values()),
            default=0,
        )
        now_ms = int(time.time() * 1000)
        last_binance_ms = int(getattr(self._binance_feed, "last_update_ms", 0) or 0)
        return {
            "started": bool(self._started),
            "chainlink_feed": {
                "started": bool(getattr(self._chainlink_feed, "started", False)),
                "latest_update_ms": latest_chainlink_ms or None,
                "age_seconds": (
                    round((now_ms - latest_chainlink_ms) / 1000.0, 3)
                    if latest_chainlink_ms > 0
                    else None
                ),
                "assets": sorted(chainlink_prices.keys()),
            },
            "binance_feed": {
                "started": bool(getattr(self._binance_feed, "started", False)),
                "latest_update_ms": last_binance_ms or None,
                "age_seconds": (
                    round((now_ms - last_binance_ms) / 1000.0, 3)
                    if last_binance_ms > 0
                    else None
                ),
            },
        }


_reference_runtime: ReferenceRuntime | None = None


def get_reference_runtime() -> ReferenceRuntime:
    global _reference_runtime
    if _reference_runtime is None:
        _reference_runtime = ReferenceRuntime()
    return _reference_runtime
