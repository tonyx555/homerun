from __future__ import annotations

import asyncio
import sys
from datetime import timedelta
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import main
from utils.utcnow import utcnow


@pytest.mark.asyncio
async def test_gui_health_db_queries_use_workers_status_cache(monkeypatch) -> None:
    class EmptySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def fake_scanner_status(session) -> dict:
        assert session is not None
        return {"running": True, "last_scan": "scan", "opportunities_count": 4}

    async def fake_workers_status() -> dict:
        return {
            "workers": [
                {
                    "worker_name": "scanner",
                    "running": True,
                    "enabled": True,
                    "current_activity": "Scanning",
                },
                {
                    "worker_name": "trader_orchestrator",
                    "running": False,
                    "enabled": False,
                    "current_activity": "Disabled",
                },
            ]
        }

    monkeypatch.setattr(main, "AsyncSessionLocal", lambda: EmptySession())
    monkeypatch.setattr(main.shared_state, "get_scanner_status_from_db", fake_scanner_status)
    monkeypatch.setattr(main, "get_workers_status_cached_or_fallback", fake_workers_status)

    db = await main._gui_health_db_queries()
    response = main._build_gui_health_response(db)

    assert response["workers"]["scanner"]["running"] is True
    assert response["workers"]["scanner"]["current_activity"] == "Scanning"
    assert response["services"]["trader_orchestrator"]["current_activity"] == "Disabled"


@pytest.mark.asyncio
async def test_gui_health_check_refreshes_stale_cache_synchronously(monkeypatch) -> None:
    stale_db = {
        "database": True,
        "scanner_status": {"running": True, "last_scan": "stale-scan", "opportunities_count": 1},
        "worker_status_rows": [],
        "orchestrator_snapshot": {
            "running": False,
            "current_activity": "stale",
            "last_run_at": "stale-run",
            "last_error": None,
        },
    }
    fresh_db = {
        "database": True,
        "scanner_status": {"running": True, "last_scan": "fresh-scan", "opportunities_count": 2},
        "worker_status_rows": [],
        "orchestrator_snapshot": {
            "running": True,
            "current_activity": "fresh",
            "last_run_at": "fresh-run",
            "last_error": None,
        },
    }

    async def _fake_gui_health_db_queries() -> dict:
        return fresh_db

    monkeypatch.setattr(main, "_gui_health_cache", stale_db)
    monkeypatch.setattr(main, "_gui_health_cache_updated_at", utcnow() - timedelta(seconds=30))
    monkeypatch.setattr(main, "_gui_health_db_queries", _fake_gui_health_db_queries)

    response = await main.gui_health_check()

    assert response["services"]["trader_orchestrator"]["running"] is True
    assert response["services"]["trader_orchestrator"]["current_activity"] == "fresh"
    assert response["services"]["scanner"]["opportunities_count"] == 2


@pytest.mark.asyncio
async def test_gui_health_check_reports_database_online_when_summary_times_out(monkeypatch) -> None:
    async def _fake_gui_health_db_queries() -> dict:
        raise asyncio.TimeoutError

    async def _fake_gui_health_database_ping() -> bool:
        return True

    monkeypatch.setattr(main, "_gui_health_cache", None)
    monkeypatch.setattr(main, "_gui_health_cache_updated_at", None)
    monkeypatch.setattr(main, "_gui_health_db_queries", _fake_gui_health_db_queries)
    monkeypatch.setattr(main, "_gui_health_database_ping", _fake_gui_health_database_ping)

    response = await main.gui_health_check()

    assert response["checks"]["database"] is True
    assert response["services"]["scanner"]["opportunities_count"] == 0
