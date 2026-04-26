"""Per-task-type concurrency budgets for DB sessions.

Background:
    The main worker pool has 80 connections shared by every background
    loop on the worker plane (orchestrator, reconciler, intent-runtime
    projection, discovery, events, news, scanner, ...).  If any one of
    those loops over-parallelizes — even briefly, even by accident in
    a future change — it can monopolize the pool and starve the other
    consumers.  The cascading failure pattern in production was usually
    downstream of exactly that kind of imbalance.

Usage:
    Wrap session opens with ``async with session_gate(name, limit):``
    so the gate's per-name asyncio.Semaphore enforces a max concurrent
    count for that task type.  Tasks of other types are unaffected.

        async def _reconcile_one(trader):
            async with session_gate("trader_reconciliation", limit=4):
                async with AsyncSessionLocal() as session:
                    await reconcile_live_positions(session, ...)

    The gate is OPT-IN.  Existing call sites that don't wrap continue
    to work and just don't get the budget enforcement.  New code and
    high-risk hot paths should adopt it.

Observability:
    ``session_gate_snapshot()`` returns counts and queue depths per
    name for diagnostics / metrics export.

Cancellation safety:
    The semaphore is released in the ``finally`` of the context
    manager, so a cancelled task always returns its slot.  The
    enclosed session-open / DB work is unaffected by the gate.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator


_GATES: dict[str, asyncio.Semaphore] = {}
_LIMITS: dict[str, int] = {}
_INFLIGHT: dict[str, int] = {}
_WAITING: dict[str, int] = {}
_TOTAL_WAIT_SECONDS: dict[str, float] = {}
_TOTAL_HOLDS: dict[str, int] = {}


def _normalize_name(name: str) -> str:
    return str(name or "").strip().lower() or "unnamed"


def _get_or_create_gate(name: str, limit: int) -> asyncio.Semaphore:
    """Return the existing gate for *name*, creating it with *limit* if absent.

    If an existing gate has a different limit, the existing limit wins
    (gates are assumed long-lived; surprise re-sizing would be racy).
    The mismatch is silently ignored to keep callers simple — tests can
    assert via ``session_gate_snapshot()`` if they care.
    """
    gate = _GATES.get(name)
    if gate is None:
        gate = asyncio.Semaphore(max(1, int(limit)))
        _GATES[name] = gate
        _LIMITS[name] = max(1, int(limit))
        _INFLIGHT[name] = 0
        _WAITING[name] = 0
        _TOTAL_WAIT_SECONDS[name] = 0.0
        _TOTAL_HOLDS[name] = 0
    return gate


@asynccontextmanager
async def session_gate(name: str, *, limit: int) -> AsyncIterator[None]:
    """Bounded entry to a worker's DB-session region.

    Acquires a per-name semaphore slot before the ``yield`` and
    releases it after.  Use to wrap an ``async with AsyncSessionLocal()
    as session:`` block in a background worker so a single worker
    cannot monopolize the pool.

    The gate is global per (process, name); creating a new gate with
    the same name reuses the existing semaphore (limit fixed at first
    creation — see _get_or_create_gate).

    Cancellation: if the calling task is cancelled while waiting on
    the semaphore OR while inside the gate, the slot is released in
    the finally block.
    """
    gate_name = _normalize_name(name)
    sem = _get_or_create_gate(gate_name, limit)
    wait_started = time.monotonic()
    _WAITING[gate_name] = _WAITING.get(gate_name, 0) + 1
    try:
        await sem.acquire()
    finally:
        _WAITING[gate_name] = max(0, _WAITING.get(gate_name, 0) - 1)
    wait_seconds = time.monotonic() - wait_started
    _TOTAL_WAIT_SECONDS[gate_name] = _TOTAL_WAIT_SECONDS.get(gate_name, 0.0) + wait_seconds
    _TOTAL_HOLDS[gate_name] = _TOTAL_HOLDS.get(gate_name, 0) + 1
    _INFLIGHT[gate_name] = _INFLIGHT.get(gate_name, 0) + 1
    try:
        yield
    finally:
        _INFLIGHT[gate_name] = max(0, _INFLIGHT.get(gate_name, 0) - 1)
        sem.release()


def session_gate_snapshot() -> dict[str, dict[str, float | int]]:
    """Diagnostic view of all gates: limit, in-flight, waiting, average wait."""
    out: dict[str, dict[str, float | int]] = {}
    for name in sorted(_GATES.keys()):
        holds = _TOTAL_HOLDS.get(name, 0)
        total_wait = _TOTAL_WAIT_SECONDS.get(name, 0.0)
        avg_wait_ms = round((total_wait / holds) * 1000.0, 2) if holds > 0 else 0.0
        out[name] = {
            "limit": _LIMITS.get(name, 0),
            "inflight": _INFLIGHT.get(name, 0),
            "waiting": _WAITING.get(name, 0),
            "total_holds": holds,
            "avg_wait_ms": avg_wait_ms,
        }
    return out
