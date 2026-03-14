from __future__ import annotations

import asyncio
import time
from typing import Any, Callable

from services.binance_feed import get_binance_feed
from services.chainlink_feed import get_chainlink_feed


class ReferenceRuntime:
    def __init__(self) -> None:
        self._started = False
        self._start_lock = asyncio.Lock()
        self._binance_feed = get_binance_feed()
        self._chainlink_feed = get_chainlink_feed()
        self._callbacks: list[Callable[[str], None]] = []
        self._binance_feed.on_update(self._on_binance_update)
        self._chainlink_feed.on_update(self._on_chainlink_update)

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
            self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        await self._binance_feed.stop()
        await self._chainlink_feed.stop()
        self._started = False

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
