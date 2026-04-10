from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.runtime_status import runtime_status
from services import trader_orchestrator_state as orchestrator_state
from services import worker_state


@pytest.mark.asyncio
async def test_read_worker_snapshot_uses_durable_orchestrator_snapshot(monkeypatch) -> None:
    runtime_status.update_orchestrator(
        running=False,
        enabled=False,
        current_activity="stale local runtime row",
        interval_seconds=5,
        last_run_at=None,
        last_error=None,
        stats={},
    )
    monkeypatch.setattr(
        orchestrator_state,
        "read_orchestrator_control",
        AsyncMock(
            return_value={
                "is_enabled": True,
                "is_paused": False,
                "run_interval_seconds": 7,
                "requested_run_at": None,
            }
        ),
    )
    monkeypatch.setattr(
        orchestrator_state,
        "read_orchestrator_snapshot",
        AsyncMock(
            return_value={
                "running": True,
                "enabled": True,
                "current_activity": "Cycle[live]",
                "interval_seconds": 7,
                "last_run_at": "2026-04-09T08:48:00Z",
                "last_error": None,
                "stats": {"orders_count": 3},
                "updated_at": "2026-04-09T08:48:01Z",
            }
        ),
    )

    snapshot = await worker_state.read_worker_snapshot(object(), "trader_orchestrator")

    assert snapshot["running"] is True
    assert snapshot["enabled"] is True
    assert snapshot["current_activity"] == "Cycle[live]"
    assert snapshot["stats"]["orders_count"] == 3
    assert snapshot["control"]["is_enabled"] is True
    assert snapshot["control"]["is_paused"] is False
