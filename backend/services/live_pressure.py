from __future__ import annotations

import time
from typing import Any

from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError, PendingRollbackError, ResourceClosedError

_DB_PRESSURE_UNTIL_MONO = 0.0
_DB_PRESSURE_REASON = ""
_DB_PRESSURE_COMPONENT = ""
_DB_PRESSURE_COUNT = 0

_PRESSURE_ERROR_FRAGMENTS = (
    "another operation",
    "canceling statement due to statement timeout",
    "cannot switch to state",
    "connection is closed",
    "current transaction is aborted",
    "deadlock",
    "infailedsqltransaction",
    "interfaceerror",
    "lock timeout",
    "pendingrollback",
    "querycancelederror",
    "resourceclosed",
    "statement timeout",
    "timeout",
    "transaction is closed",
)


def mark_db_pressure(reason: str, *, component: str = "", ttl_seconds: float = 60.0) -> None:
    global _DB_PRESSURE_UNTIL_MONO, _DB_PRESSURE_REASON, _DB_PRESSURE_COMPONENT, _DB_PRESSURE_COUNT

    now_mono = time.monotonic()
    ttl = max(1.0, min(float(ttl_seconds or 60.0), 300.0))
    until_mono = now_mono + ttl
    if until_mono >= _DB_PRESSURE_UNTIL_MONO:
        _DB_PRESSURE_UNTIL_MONO = until_mono
        _DB_PRESSURE_REASON = str(reason or "db_pressure").strip() or "db_pressure"
        _DB_PRESSURE_COMPONENT = str(component or "").strip()
    _DB_PRESSURE_COUNT += 1


def db_pressure_remaining_seconds() -> float:
    return max(0.0, _DB_PRESSURE_UNTIL_MONO - time.monotonic())


def is_db_pressure_active() -> bool:
    return db_pressure_remaining_seconds() > 0.0


def db_pressure_snapshot() -> dict[str, Any]:
    remaining = db_pressure_remaining_seconds()
    return {
        "active": remaining > 0.0,
        "remaining_seconds": round(remaining, 3),
        "reason": _DB_PRESSURE_REASON if remaining > 0.0 else "",
        "component": _DB_PRESSURE_COMPONENT if remaining > 0.0 else "",
        "count": int(_DB_PRESSURE_COUNT),
    }


def maybe_mark_db_pressure(exc: BaseException, *, component: str, ttl_seconds: float = 60.0) -> bool:
    if isinstance(exc, (DBAPIError, InterfaceError, OperationalError, PendingRollbackError, ResourceClosedError)):
        mark_db_pressure(type(exc).__name__, component=component, ttl_seconds=ttl_seconds)
        return True

    text = f"{type(exc).__name__} {exc}".lower()
    if any(fragment in text for fragment in _PRESSURE_ERROR_FRAGMENTS):
        mark_db_pressure(type(exc).__name__, component=component, ttl_seconds=ttl_seconds)
        return True
    return False
