import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from workers import trader_orchestrator_worker


@pytest.mark.asyncio
async def test_build_traders_scope_context_uses_cache_within_ttl(monkeypatch):
    class _NoRelease:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return list(self._rows)

    class _Session:
        def __init__(self):
            self.execute_calls = 0

        async def execute(self, _query):
            self.execute_calls += 1
            return _Result(["0x1111111111111111111111111111111111111111"])

    now = datetime(2026, 3, 12, 15, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(trader_orchestrator_worker, "utcnow", lambda: now)
    monkeypatch.setattr(trader_orchestrator_worker, "release_conn", lambda _session: _NoRelease())

    trader_orchestrator_worker._traders_scope_context_cache.clear()
    try:
        session = _Session()
        scope = {"modes": ["tracked"]}

        first = await trader_orchestrator_worker._build_traders_scope_context(session, scope)
        second = await trader_orchestrator_worker._build_traders_scope_context(session, scope)

        assert session.execute_calls == 1
        assert first == second
    finally:
        trader_orchestrator_worker._traders_scope_context_cache.clear()


@pytest.mark.asyncio
async def test_build_traders_scope_context_uses_stale_cache_on_refresh_failure(monkeypatch):
    class _NoRelease:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def scalars(self):
            return self

        def all(self):
            return list(self._rows)

    class _Session:
        def __init__(self):
            self.execute_calls = 0
            self.fail = False

        async def execute(self, _query):
            self.execute_calls += 1
            if self.fail:
                raise RuntimeError("transient database failure")
            return _Result(["0x2222222222222222222222222222222222222222"])

    timeline = [
        datetime(2026, 3, 12, 15, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 3, 12, 15, 0, 7, tzinfo=timezone.utc),
    ]
    monkeypatch.setattr(trader_orchestrator_worker, "utcnow", lambda: timeline.pop(0))
    monkeypatch.setattr(trader_orchestrator_worker, "release_conn", lambda _session: _NoRelease())

    trader_orchestrator_worker._traders_scope_context_cache.clear()
    try:
        session = _Session()
        scope = {"modes": ["tracked"]}

        baseline = await trader_orchestrator_worker._build_traders_scope_context(session, scope)
        session.fail = True
        recovered = await trader_orchestrator_worker._build_traders_scope_context(session, scope)

        assert session.execute_calls == 2
        assert recovered == baseline
    finally:
        trader_orchestrator_worker._traders_scope_context_cache.clear()
