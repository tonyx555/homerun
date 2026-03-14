from __future__ import annotations

import copy
from typing import Any

from utils.utcnow import utcnow


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
        self._orchestrator: dict[str, Any] = {
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
        }

    def update_crypto(
        self,
        *,
        running: bool | None = None,
        enabled: bool | None = None,
        current_activity: str | None = None,
        interval_seconds: int | None = None,
        last_run_at: str | None = None,
        lag_seconds: float | None = None,
        last_error: str | None = None,
        stats: dict[str, Any] | None = None,
        control: dict[str, Any] | None = None,
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
        running: bool | None = None,
        enabled: bool | None = None,
        current_activity: str | None = None,
        interval_seconds: int | None = None,
        last_run_at: str | None = None,
        lag_seconds: float | None = None,
        last_error: str | None = None,
        stats: dict[str, Any] | None = None,
        control: dict[str, Any] | None = None,
    ) -> None:
        self._update(
            self._orchestrator,
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

    def _update(
        self,
        target: dict[str, Any],
        *,
        running: bool | None = None,
        enabled: bool | None = None,
        current_activity: str | None = None,
        interval_seconds: int | None = None,
        last_run_at: str | None = None,
        lag_seconds: float | None = None,
        last_error: str | None = None,
        stats: dict[str, Any] | None = None,
        control: dict[str, Any] | None = None,
    ) -> None:
        if running is not None:
            target["running"] = bool(running)
        if enabled is not None:
            target["enabled"] = bool(enabled)
        if current_activity is not None:
            target["current_activity"] = str(current_activity)
        if interval_seconds is not None:
            target["interval_seconds"] = int(interval_seconds)
        if last_run_at is not None:
            target["last_run_at"] = last_run_at
        if lag_seconds is not None:
            target["lag_seconds"] = float(lag_seconds)
        if last_error is not None or last_error is None:
            target["last_error"] = last_error
        if stats is not None:
            target["stats"] = _clone(stats)
        if control is not None:
            target["control"] = _clone(control)
        target["updated_at"] = utcnow().isoformat().replace("+00:00", "Z")

    def get_crypto(self) -> dict[str, Any]:
        return _clone(self._crypto)

    def get_orchestrator(self) -> dict[str, Any]:
        return _clone(self._orchestrator)

    def list_runtime_rows(self) -> list[dict[str, Any]]:
        return [self.get_crypto(), self.get_orchestrator()]


runtime_status = RuntimeStatus()
