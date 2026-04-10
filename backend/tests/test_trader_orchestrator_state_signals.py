import sys
from pathlib import Path

import pytest
from sqlalchemy.dialects import postgresql

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.trader_orchestrator_state import list_unconsumed_trade_signals


class _ScalarResult:
    def scalars(self):
        return self

    def all(self):
        return []


class _CaptureSession:
    def __init__(self):
        self.query = None

    async def execute(self, query):
        self.query = query
        return _ScalarResult()


@pytest.mark.asyncio
async def test_list_unconsumed_trade_signals_excludes_cold_scanner_rows_from_scheduled_polling():
    session = _CaptureSession()

    await list_unconsumed_trade_signals(
        session,
        trader_id="trader-1",
        sources=["scanner"],
        statuses=["pending"],
        strategy_types_by_source={"scanner": ["generic_strategy"]},
        cursor_runtime_sequence=None,
        cursor_created_at=None,
        cursor_signal_id=None,
        limit=10,
    )

    compiled = str(
        session.query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).lower()

    assert "coalesce(trade_signals.source, '')" in compiled
    assert "trade_signals.runtime_sequence is not null" in compiled
    assert "execution_activation" not in compiled
