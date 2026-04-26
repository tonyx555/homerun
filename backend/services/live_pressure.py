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


# =====================================================================
# PROACTIVE BACKPRESSURE
# =====================================================================
#
# mark_db_pressure / maybe_mark_db_pressure above are REACTIVE — they
# fire only after a query has actually failed with a contention or
# timeout error. By that point the cascade is usually already in
# motion (queue grew unbounded; backends locked up; freshness checks
# fired).
#
# Backpressure level is PROACTIVE — published by saturation observers
# (intent runtime queue depth, audit buffer depth, pool wait time
# samples) before any failure occurs, and consumed by load producers
# (trader cycles, scanner, anyone iterating quickly) so they can
# voluntarily slow down before the system breaks.
#
# Range: 0.0 (clean) to 1.0 (critical, shed load aggressively).
# Producers should treat ~0.5 as "extend intervals", ~0.75 as "skip
# non-essential work", ~1.0 as "pause for next interval".
#
# Components publish under their own key; the global level is the max
# across components (any one component saturated → backpressure
# elevated). Stale entries (no update for >30s) decay to 0.
_BACKPRESSURE_BY_COMPONENT: dict[str, tuple[float, float, str]] = {}
_BACKPRESSURE_DECAY_SECONDS = 30.0


def publish_backpressure(component: str, *, level: float, reason: str = "") -> None:
    """Publish a 0.0-1.0 backpressure level for *component*.

    Producers query :func:`current_backpressure_level` to pace themselves.
    Pass level=0.0 to clear.
    """
    component_key = str(component or "").strip().lower()
    if not component_key:
        return
    clamped = max(0.0, min(1.0, float(level)))
    if clamped <= 0.0:
        _BACKPRESSURE_BY_COMPONENT.pop(component_key, None)
        return
    _BACKPRESSURE_BY_COMPONENT[component_key] = (
        clamped,
        time.monotonic(),
        str(reason or "").strip(),
    )


def current_backpressure_level() -> float:
    """Return the current global backpressure level (max across components).

    Returns 0.0 (clean) up to 1.0 (critical). Stale component entries
    (no update for >30s) are evicted, so a crashed publisher cannot
    leave the system stuck pretending to be busy.
    """
    if not _BACKPRESSURE_BY_COMPONENT:
        return 0.0
    now_mono = time.monotonic()
    expired = [
        key
        for key, (_, ts, _) in _BACKPRESSURE_BY_COMPONENT.items()
        if (now_mono - ts) > _BACKPRESSURE_DECAY_SECONDS
    ]
    for key in expired:
        _BACKPRESSURE_BY_COMPONENT.pop(key, None)
    if not _BACKPRESSURE_BY_COMPONENT:
        return 0.0
    return max(level for level, _, _ in _BACKPRESSURE_BY_COMPONENT.values())


def backpressure_snapshot() -> dict[str, Any]:
    """Detailed view of the current backpressure state.  For diagnostics."""
    now_mono = time.monotonic()
    components: dict[str, dict[str, Any]] = {}
    for key, (level, ts, reason) in _BACKPRESSURE_BY_COMPONENT.items():
        age = now_mono - ts
        if age > _BACKPRESSURE_DECAY_SECONDS:
            continue
        components[key] = {
            "level": round(level, 3),
            "age_seconds": round(age, 1),
            "reason": reason,
        }
    return {
        "level": round(current_backpressure_level(), 3),
        "components": components,
    }


def backpressure_extra_sleep_seconds(base_interval_seconds: float) -> float:
    """How long to additionally sleep beyond *base_interval_seconds* given
    the current backpressure level.

    The curve: 0% at level<=0.4, ramps to 1× the base interval at level
    0.7, ramps to 3× the base interval at level=1.0. So a trader on a
    5s interval sleeps 5s clean, 10s at level 0.7, 20s at level 1.0.
    Producers needing finer control can call ``current_backpressure_level``
    directly.
    """
    level = current_backpressure_level()
    if level <= 0.4:
        return 0.0
    # Linear ramp: 0 at 0.4, 3.0 at 1.0
    multiplier = (level - 0.4) / 0.6 * 3.0
    return max(0.0, multiplier) * max(0.0, float(base_interval_seconds))
