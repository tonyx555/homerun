"""Regression test for the ``_graceful_timeout`` cancel-on-timeout fix.

Pre-2026-04-28 the helper abandoned the inner task on timeout —
which left it running in the background, accumulating dozens of
in-flight reconciles whenever the venue / DB was slow.  The
``stuck=210s`` telemetry from the cascade was that pile-up.

The fix: actually call ``task.cancel()`` on timeout, then wait up
to ``_TIMEOUT_CANCEL_GRACE_SECONDS`` for the task to honor cancel.
This test verifies the contract — that a hung task is cancelled,
not abandoned, when the outer timeout fires.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from workers import trader_reconciliation_worker as recon


@pytest.mark.asyncio
async def test_graceful_timeout_cancels_inner_task_on_timeout(monkeypatch):
    """Hung inner coro must be cancelled when the outer timeout
    fires — not left running in the background."""
    cancelled_flag = {"value": False}

    async def hung_task() -> None:
        try:
            await asyncio.sleep(60.0)
        except asyncio.CancelledError:
            cancelled_flag["value"] = True
            raise

    # Tighten the cancel grace so the test runs fast.
    monkeypatch.setattr(recon, "_TIMEOUT_CANCEL_GRACE_SECONDS", 0.5)

    started = asyncio.get_event_loop().time()
    with pytest.raises(asyncio.TimeoutError):
        await recon._graceful_timeout(hung_task(), timeout=0.05, label="t1")
    elapsed = asyncio.get_event_loop().time() - started

    # The inner task was cancelled, not abandoned.
    assert cancelled_flag["value"] is True, "inner task did not receive cancel"

    # The whole call returned within timeout + cancel grace, not the
    # 60 s ``asyncio.sleep``.
    assert elapsed < 2.0, f"call did not return promptly: {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_graceful_timeout_returns_result_on_normal_completion(monkeypatch):
    """Sanity: when the inner coro finishes within budget, the
    helper returns its value and does NOT cancel."""
    cancelled_flag = {"value": False}

    async def fast_task() -> int:
        try:
            await asyncio.sleep(0.01)
            return 42
        except asyncio.CancelledError:
            cancelled_flag["value"] = True
            raise

    monkeypatch.setattr(recon, "_TIMEOUT_CANCEL_GRACE_SECONDS", 0.5)

    result = await recon._graceful_timeout(fast_task(), timeout=2.0, label="t2")
    assert result == 42
    assert cancelled_flag["value"] is False


@pytest.mark.asyncio
async def test_graceful_timeout_falls_back_to_abandon_if_task_resists_cancel(monkeypatch):
    """If the inner coro doesn't honor cancel within the grace
    window we fall back to abandonment so the caller isn't blocked
    forever waiting for cleanup."""
    async def stubborn_task() -> None:
        try:
            await asyncio.sleep(60.0)
        except asyncio.CancelledError:
            # Simulate a task that ignores cancel and keeps running.
            await asyncio.sleep(60.0)
            raise

    monkeypatch.setattr(recon, "_TIMEOUT_CANCEL_GRACE_SECONDS", 0.05)

    # Snapshot the abandoned set so we can detect insertion.
    pre_size = len(recon._abandoned_timed_tasks)

    started = asyncio.get_event_loop().time()
    with pytest.raises(asyncio.TimeoutError):
        await recon._graceful_timeout(
            stubborn_task(), timeout=0.05, label="t3"
        )
    elapsed = asyncio.get_event_loop().time() - started

    # Even though the task ignored cancel, the helper returned within
    # timeout + cancel-grace (~0.1s) — it did not block forever.
    assert elapsed < 1.0, f"call hung waiting for stubborn cancel: {elapsed:.2f}s"

    # And the stubborn task is now in the abandoned set so nothing
    # else accidentally awaits it.
    assert len(recon._abandoned_timed_tasks) > pre_size

    # Clean up the abandoned task so it doesn't leak into other tests.
    for task in list(recon._abandoned_timed_tasks):
        if not task.done():
            task.cancel()
