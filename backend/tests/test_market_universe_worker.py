import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from workers import market_universe_worker


@pytest.mark.asyncio
async def test_run_loop_refreshes_runtime_settings_after_control_session_closes(monkeypatch):
    session_state = {"active": False}

    class _SessionContext:
        async def __aenter__(self):
            assert session_state["active"] is False
            session_state["active"] = True
            return object()

        async def __aexit__(self, exc_type, exc, tb):
            session_state["active"] = False
            return False

    class _HeartbeatTask:
        def cancel(self):
            return None

        def __await__(self):
            async def _wait():
                raise asyncio.CancelledError()

            return _wait().__await__()

    def _create_task(coro, *, name=None):
        del name
        coro.close()
        return _HeartbeatTask()

    async def _apply_runtime_settings_overrides():
        assert session_state["active"] is False
        raise asyncio.CancelledError()

    monkeypatch.setattr(market_universe_worker, "AsyncSessionLocal", lambda: _SessionContext())
    monkeypatch.setattr(market_universe_worker.asyncio, "create_task", _create_task)
    monkeypatch.setattr(market_universe_worker.scanner, "load_settings", AsyncMock(return_value=None))
    monkeypatch.setattr(
        market_universe_worker.scanner,
        "load_plugins",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        market_universe_worker,
        "read_worker_control",
        AsyncMock(return_value={"interval_seconds": 120, "is_paused": False, "is_enabled": True}),
    )
    monkeypatch.setattr(
        market_universe_worker,
        "apply_runtime_settings_overrides",
        _apply_runtime_settings_overrides,
    )

    with pytest.raises(asyncio.CancelledError):
        await market_universe_worker._run_loop()
