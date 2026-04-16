from __future__ import annotations

import copy
from typing import Any

from utils.utcnow import utcnow

_UNSET = object()


def _clone(value: Any) -> Any:
    return copy.deepcopy(value)


class RuntimeStatus:
    def __init__(self) -> None:
        self._crypto: dict[str, Any] = {
            "worker_name": "crypto",
            "running": False,
            "enabled": False,
            "current_activity": "Starting",
            "interval_seconds": 1,
            "last_run_at": None,
            "lag_seconds": None,
            "last_error": None,
            "updated_at": None,
            "stats": {},
            "control": {},
        }
        self._orchestrators: dict[str, dict[str, Any]] = {
            "general": {
                "worker_name": "trader_orchestrator",
                "running": False,
                "enabled": False,
                "current_activity": "Starting",
                "interval_seconds": 5,
                "last_run_at": None,
                "lag_seconds": None,
                "last_error": None,
                "updated_at": None,
                "stats": {},
                "control": {},
            },
            "crypto": {
                "worker_name": "trader_orchestrator_crypto",
                "running": False,
                "enabled": False,
                "current_activity": "Starting",
                "interval_seconds": 5,
                "last_run_at": None,
                "lag_seconds": None,
                "last_error": None,
                "updated_at": None,
                "stats": {},
                "control": {},
            },
        }

    def update_crypto(
        self,
        *,
        running: Any = _UNSET,
        enabled: Any = _UNSET,
        current_activity: Any = _UNSET,
        interval_seconds: Any = _UNSET,
        last_run_at: Any = _UNSET,
        lag_seconds: Any = _UNSET,
        last_error: Any = _UNSET,
        stats: Any = _UNSET,
        control: Any = _UNSET,
    ) -> None:
        self._update(
            self._crypto,
            running=running,
            enabled=enabled,
            current_activity=current_activity,
            interval_seconds=interval_seconds,
            last_run_at=last_run_at,
            lag_seconds=lag_seconds,
            last_error=last_error,
            stats=stats,
            control=control,
        )

    def update_orchestrator(
        self,
        *,
        lane: str = "general",
        running: Any = _UNSET,
        enabled: Any = _UNSET,
        current_activity: Any = _UNSET,
        interval_seconds: Any = _UNSET,
        last_run_at: Any = _UNSET,
        lag_seconds: Any = _UNSET,
        last_error: Any = _UNSET,
        stats: Any = _UNSET,
        control: Any = _UNSET,
    ) -> None:
        lane_key = self._normalize_orchestrator_lane(lane)
        self._update(
            self._orchestrators[lane_key],
            running=running,
            enabled=enabled,
            current_activity=current_activity,
            interval_seconds=interval_seconds,
            last_run_at=last_run_at,
            lag_seconds=lag_seconds,
            last_error=last_error,
            stats=stats,
            control=control,
        )

    def _normalize_orchestrator_lane(self, lane: Any) -> str:
        lane_key = str(lane or "general").strip().lower()
        return lane_key if lane_key in self._orchestrators else "general"

    def _update(
        self,
        target: dict[str, Any],
        *,
        running: Any = _UNSET,
        enabled: Any = _UNSET,
        current_activity: Any = _UNSET,
        interval_seconds: Any = _UNSET,
        last_run_at: Any = _UNSET,
        lag_seconds: Any = _UNSET,
        last_error: Any = _UNSET,
        stats: Any = _UNSET,
        control: Any = _UNSET,
    ) -> None:
        if running is not _UNSET:
            target["running"] = bool(running)
        if enabled is not _UNSET:
            target["enabled"] = bool(enabled)
        if current_activity is not _UNSET:
            target["current_activity"] = None if current_activity is None else str(current_activity)
        if interval_seconds is not _UNSET:
            target["interval_seconds"] = int(interval_seconds)
        if last_run_at is not _UNSET:
            target["last_run_at"] = last_run_at
        if lag_seconds is not _UNSET:
            target["lag_seconds"] = None if lag_seconds is None else float(lag_seconds)
        if last_error is not _UNSET:
            target["last_error"] = last_error
        if stats is not _UNSET:
            target["stats"] = _clone(stats)
        if control is not _UNSET:
            target["control"] = _clone(control)
        target["updated_at"] = utcnow().isoformat().replace("+00:00", "Z")

    def get_crypto(self) -> dict[str, Any]:
        return _clone(self._crypto)

    def get_orchestrator(self, lane: str = "general") -> dict[str, Any]:
        return _clone(self._orchestrators[self._normalize_orchestrator_lane(lane)])

    def list_runtime_rows(self) -> list[dict[str, Any]]:
        return [
            self.get_crypto(),
            self.get_orchestrator("general"),
            self.get_orchestrator("crypto"),
        ]


runtime_status = RuntimeStatus()
