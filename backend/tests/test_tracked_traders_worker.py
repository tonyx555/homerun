import contextlib
import asyncio

import pytest

from workers import tracked_traders_worker


@pytest.mark.asyncio
async def test_graceful_timeout_closes_rejected_coroutine_when_prior_task_still_running():
    gate = asyncio.Event()

    async def _long_running():
        try:
            await gate.wait()
        finally:
            gate.set()

    async def _rejected():
        return {"flagged_insiders": 0}

    tracked_traders_worker._inflight_timed_tasks.clear()
    tracked_traders_worker._abandoned_tasks.clear()
    existing = asyncio.create_task(_long_running(), name="tracked-traders-test-existing")
    tracked_traders_worker._inflight_timed_tasks["insider_rescore"] = existing
    rejected_coro = _rejected()
    try:
        with pytest.raises(tracked_traders_worker._TimedTaskStillRunningError):
            await tracked_traders_worker._graceful_timeout(
                rejected_coro,
                timeout=0.01,
                label="insider_rescore",
            )
        assert rejected_coro.cr_frame is None
    finally:
        existing.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await existing
        tracked_traders_worker._inflight_timed_tasks.clear()
        tracked_traders_worker._abandoned_tasks.clear()
