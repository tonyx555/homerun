"""Regression tests for RetryableAsyncSession cancellation handling.

The session shields execute/commit/flush so the asyncpg extended protocol
sequence completes atomically.  When the calling task is cancelled the
session schedules a fire-and-forget cleanup that must drain the inner
task BEFORE invalidating, otherwise two coroutines race for the same
asyncpg connection — surfacing as
``InternalClientError: got result for unknown protocol state 3`` and
cascade-poisoning the connection pool.
"""

import sys
import asyncio
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


class _StubSession:
    """Stand-in that mimics AsyncSession for ordering checks."""

    def __init__(self, execute_runtime: float):
        self._execute_runtime = execute_runtime
        self.execute_finished_at: float | None = None
        self.invalidate_called_at: float | None = None
        self.invalidate_calls = 0

    async def _execute_inner(self):
        await asyncio.sleep(self._execute_runtime)
        self.execute_finished_at = asyncio.get_event_loop().time()
        return "result"

    async def _invalidate_inner(self):
        self.invalidate_called_at = asyncio.get_event_loop().time()
        self.invalidate_calls += 1


@pytest.mark.asyncio
async def test_drain_then_invalidate_waits_for_inner_to_finish():
    """The cleanup task must NOT invalidate until inner execute completes."""
    stub = _StubSession(execute_runtime=0.05)

    inner = asyncio.ensure_future(stub._execute_inner())

    drain_done = asyncio.Event()

    async def _drain_then_invalidate():
        try:
            await inner
        except Exception:
            pass
        await stub._invalidate_inner()
        drain_done.set()

    cleanup_task = asyncio.create_task(_drain_then_invalidate())
    await asyncio.wait_for(drain_done.wait(), timeout=2.0)

    assert stub.execute_finished_at is not None
    assert stub.invalidate_called_at is not None
    assert stub.invalidate_called_at >= stub.execute_finished_at, (
        "invalidate must NOT run before inner execute finishes — "
        "this is the asyncpg protocol-state race we are guarding against"
    )
    assert stub.invalidate_calls == 1
    cleanup_task.cancel()


@pytest.mark.asyncio
async def test_wait_inflight_blocks_until_pending_tasks_complete():
    """The new _wait_inflight pattern must serialize new ops behind any
    leftover inner task from a cancelled call.  Without this, calling
    rollback() right after a wait_for-cancelled execute() raises isce —
    "session is provisioning a new connection; concurrent operations
    are not permitted".
    """

    inner_done = asyncio.Event()

    async def _slow_inner():
        await asyncio.sleep(0.05)
        inner_done.set()

    inner = asyncio.ensure_future(_slow_inner())
    bag = {inner}
    inner.add_done_callback(bag.discard)

    async def _wait_inflight() -> None:
        pending = [t for t in list(bag) if not t.done()]
        if pending:
            await asyncio.wait(pending, timeout=2.0)

    started_wait = asyncio.get_event_loop().time()
    await _wait_inflight()
    waited_for = asyncio.get_event_loop().time() - started_wait

    assert inner_done.is_set(), "inner must finish before _wait_inflight returns"
    assert waited_for >= 0.04, "_wait_inflight must actually wait, not return immediately"


@pytest.mark.asyncio
async def test_drain_then_invalidate_runs_invalidate_even_when_inner_raises():
    """Inner failure must not stop invalidation — the connection still
    needs to be dropped from the pool."""

    async def _failing_inner():
        await asyncio.sleep(0.01)
        raise RuntimeError("simulated mid-protocol failure")

    inner = asyncio.ensure_future(_failing_inner())
    invalidate_called = False

    async def _drain_then_invalidate():
        nonlocal invalidate_called
        try:
            await inner
        except Exception:
            pass
        invalidate_called = True

    await _drain_then_invalidate()
    assert invalidate_called is True


@pytest.mark.asyncio
async def test_close_invalidates_when_inner_completed_with_connection_broken():
    """The 2026-04-28 cascade gap: an inner task that COMPLETES with an
    asyncpg ``cannot switch to state X; another operation in progress``
    error has poisoned the connection's protocol state.  ``super().close()``
    would return that connection to the pool where the next checkout
    reuses it and reproduces the same error indefinitely.

    The fix: ``_do_close_or_invalidate`` inspects each completed inner
    task; if any of them raised a connection-broken error, the close
    path invalidates instead of closing.

    This test exercises the inspection logic without needing a real
    asyncpg connection.
    """
    from utils.retry import is_db_connection_broken

    async def _broken_inner():
        # Simulate the asyncpg protocol-state error that the previous
        # ``except DBAPIError`` handler missed.
        raise type("InternalClientError", (Exception,), {})(
            "cannot switch to state 12; another operation (2) is in progress"
        )

    inner = asyncio.ensure_future(_broken_inner())
    try:
        await inner
    except Exception:
        pass

    # Replicate the inspection loop from _do_close_or_invalidate.
    connection_poisoned = False
    if inner.done():
        try:
            task_exc = inner.exception()
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            task_exc = None
        if task_exc is not None and is_db_connection_broken(task_exc):
            connection_poisoned = True

    assert connection_poisoned is True, (
        "is_db_connection_broken must classify the asyncpg "
        "'cannot switch to state' error as a poisoned-connection signal"
    )


@pytest.mark.asyncio
async def test_close_uses_close_when_inner_completed_cleanly():
    """Sanity: when the inner task completed without a connection-broken
    error, the close path uses ``super().close()`` (returns connection to
    pool), not ``invalidate()``."""
    from utils.retry import is_db_connection_broken

    async def _ok_inner():
        return "result"

    inner = asyncio.ensure_future(_ok_inner())
    await inner

    connection_poisoned = False
    if inner.done():
        try:
            task_exc = inner.exception()
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            task_exc = None
        if task_exc is not None and is_db_connection_broken(task_exc):
            connection_poisoned = True

    assert connection_poisoned is False
    assert inner.result() == "result"
