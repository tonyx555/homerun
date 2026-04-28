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
    # The slow GROUP BY MAX subquery has been replaced with NOT EXISTS
    # against trader_signal_consumption — the unique constraint
    # (trader_id, signal_id) lets the planner anti-join row-by-row using
    # the index instead of materialising the entire ledger.
    assert "not (exists" in compiled
    assert "group by trader_signal_consumption.signal_id" not in compiled
    assert "max(trader_signal_consumption.consumed_at)" not in compiled
