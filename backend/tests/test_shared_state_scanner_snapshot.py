import asyncio
import sys
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import OpportunityState, ScannerRun, ScannerSnapshot  # noqa: E402
from models.opportunity import MispricingType, Opportunity  # noqa: E402
from services import shared_state  # noqa: E402
from utils.utcnow import utcnow  # noqa: E402


class _FakeScalarResult:
    def __init__(self, scalar_value=None, scalars_list=None):
        self._scalar_value = scalar_value
        self._scalars_list = list(scalars_list or [])

    def scalar_one_or_none(self):
        return self._scalar_value

    def scalar_one(self):
        return self._scalar_value

    def one_or_none(self):
        return self._scalar_value

    def scalars(self):
        return self

    def all(self):
        return list(self._scalars_list)


class _FakeSession:
    def __init__(self, *, snapshot_row=None, existing_rows=None, execute_results=None):
        self.snapshot_row = snapshot_row
        self.existing_rows = list(existing_rows or [])
        self.execute_results = list(execute_results or [])
        self.added = []
        self.execute_calls = 0

    async def execute(self, statement):
        del statement
        if self.execute_results:
            result = self.execute_results.pop(0)
            if isinstance(result, _FakeScalarResult):
                return result
            return _FakeScalarResult(
                scalar_value=result.get("scalar_value"),
                scalars_list=result.get("scalars_list"),
            )
        self.execute_calls += 1
        if self.execute_calls == 1:
            return _FakeScalarResult(scalar_value=self.snapshot_row)
        return _FakeScalarResult(scalars_list=self.existing_rows)

    def add(self, value):
        if isinstance(value, ScannerSnapshot):
            self.snapshot_row = value
        self.added.append(value)


def _build_opportunity(*, market_id: str) -> Opportunity:
    now = utcnow().replace(tzinfo=timezone.utc)
    return Opportunity(
        strategy="tail_end_carry",
        title=f"Tail carry {market_id}",
        description="scanner opportunity",
        total_cost=0.91,
        expected_payout=1.0,
        gross_profit=0.09,
        fee=0.01,
        net_profit=0.08,
        roi_percent=8.79,
        risk_score=0.2,
        risk_factors=["unit_test"],
        markets=[
            {
                "id": market_id,
                "condition_id": market_id,
                "question": f"Question {market_id}",
                "yes_price": 0.09,
                "no_price": 0.91,
                "liquidity": 25000,
                "price_history": [
                    {"t": 1, "yes": 0.1, "no": 0.9},
                    {"t": 2, "yes": 0.09, "no": 0.91},
                ],
            }
        ],
        positions_to_take=[
            {
                "action": "BUY",
                "outcome": "NO",
                "price": 0.91,
                "token_id": f"token-{market_id}",
            }
        ],
        event_id=f"evt-{market_id}",
        event_title=f"Event {market_id}",
        category="Sports",
        min_liquidity=25000.0,
        max_position_size=2500.0,
        detected_at=now,
        mispricing_type=MispricingType.SETTLEMENT_LAG,
    )


@pytest.mark.asyncio
async def test_write_scanner_snapshot_publishes_runtime_events_without_db_opportunity_event_inserts(monkeypatch):
    publish_mock = AsyncMock()
    commit_mock = AsyncMock()
    schedule_mock = Mock()
    history_upsert_mock = AsyncMock(return_value=0)
    monkeypatch.setattr(shared_state.event_bus, "publish", publish_mock)
    monkeypatch.setattr(shared_state, "_commit_with_retry", commit_mock)
    monkeypatch.setattr(shared_state, "_schedule_scanner_state_projection", schedule_mock)
    monkeypatch.setattr(shared_state, "upsert_scanner_market_history", history_upsert_mock)

    session = _FakeSession()
    opportunity = _build_opportunity(market_id="market-1")
    status = {
        "running": True,
        "enabled": True,
        "interval_seconds": 60,
        "current_activity": "Fast scan complete - 1 found, 1 total",
        "last_scan": utcnow(),
        "strategies": [{"name": "Tail End Carry", "type": "tail_end_carry"}],
    }

    await shared_state.write_scanner_snapshot(session, [opportunity], status)
    await asyncio.sleep(0)

    assert isinstance(session.snapshot_row, ScannerSnapshot)
    assert session.snapshot_row.opportunities_count == 1
    assert list(session.snapshot_row.opportunities_json or []) == []
    commit_mock.assert_awaited_once()
    schedule_mock.assert_called_once()
    scheduled = schedule_mock.call_args.kwargs["opportunities"]
    assert len(scheduled) == 1
    assert scheduled[0].stable_id == opportunity.stable_id
    history_upsert_mock.assert_not_awaited()

    published_types = [call.args[0] for call in publish_mock.await_args_list]
    assert "scanner_status" in published_types
    assert "scanner_activity" in published_types
    assert "opportunities_update" in published_types
    assert "opportunity_events" not in published_types
    assert "opportunity_update" not in published_types


@pytest.mark.asyncio
async def test_persist_incremental_state_updates_state_and_returns_runtime_events_without_db_event_rows():
    opportunity = _build_opportunity(market_id="market-2")
    payload = [opportunity.model_dump(mode="json")]
    completed_at = utcnow().replace(tzinfo=None)
    session = _FakeSession(execute_results=[{"scalars_list": []}, {"scalars_list": []}])

    event_messages = await shared_state._persist_incremental_state(
        session,
        payload,
        {"current_activity": "Fast scan complete - 1 found, 1 total"},
        completed_at,
    )

    added_types = tuple(type(row) for row in session.added)
    assert ScannerRun in added_types
    assert OpportunityState in added_types
    assert len(event_messages) == 1
    assert event_messages[0]["event_type"] == "detected"
    assert event_messages[0]["stable_id"] == opportunity.stable_id


@pytest.mark.asyncio
async def test_persist_incremental_state_fetches_missing_current_ids_without_loading_all_active_and_current_rows_together():
    opportunity = _build_opportunity(market_id="market-4")
    payload = [opportunity.model_dump(mode="json")]
    completed_at = utcnow().replace(tzinfo=None)
    existing_row = OpportunityState(
        stable_id=opportunity.stable_id,
        opportunity_json=opportunity.model_dump(mode="json"),
        first_seen_at=utcnow().replace(tzinfo=None),
        last_seen_at=utcnow().replace(tzinfo=None),
        last_updated_at=utcnow().replace(tzinfo=None),
        is_active=False,
        last_run_id=None,
    )
    session = _FakeSession(execute_results=[{"scalars_list": []}, {"scalars_list": [existing_row]}])

    event_messages = await shared_state._persist_incremental_state(
        session,
        payload,
        {"current_activity": "Fast scan complete - 1 found, 1 total"},
        completed_at,
    )

    assert existing_row.is_active is True
    assert len(event_messages) == 1
    assert event_messages[0]["event_type"] == "reactivated"
    assert event_messages[0]["stable_id"] == opportunity.stable_id


@pytest.mark.asyncio
async def test_persist_incremental_state_marks_missing_opportunities_expired_without_db_event_rows():
    opportunity = _build_opportunity(market_id="market-3")
    existing_row = OpportunityState(
        stable_id=opportunity.stable_id,
        opportunity_json=opportunity.model_dump(mode="json"),
        first_seen_at=utcnow().replace(tzinfo=None),
        last_seen_at=utcnow().replace(tzinfo=None),
        last_updated_at=utcnow().replace(tzinfo=None),
        is_active=True,
        last_run_id=None,
    )
    session = _FakeSession(execute_results=[{"scalars_list": [existing_row]}])
    completed_at = utcnow().replace(tzinfo=None)

    event_messages = await shared_state._persist_incremental_state(
        session,
        [],
        {"current_activity": "Fast scan complete - 0 found, 0 total"},
        completed_at,
    )

    assert existing_row.is_active is False
    assert len(event_messages) == 1
    assert event_messages[0]["event_type"] == "expired"
    assert event_messages[0]["stable_id"] == opportunity.stable_id


@pytest.mark.asyncio
async def test_read_scanner_snapshot_reads_active_opportunities_from_state_rows():
    opportunity = _build_opportunity(market_id="market-9")
    payload = opportunity.model_dump(mode="json")
    snapshot_row = SimpleNamespace(
        running=True,
        enabled=True,
        interval_seconds=60,
        last_scan_at=utcnow(),
        current_activity="Fast scan complete - 1 found, 1 total",
        strategies_json=[{"name": "Tail End Carry", "type": "tail_end_carry"}],
        strategy_diagnostics_json={},
        tiered_scanning_json={},
        ws_feeds_json={},
        opportunities_count=1,
    )
    session = _FakeSession(
        execute_results=[
            {"scalar_value": snapshot_row},
            {"scalars_list": [payload]},
            {
                "scalars_list": [
                    (
                        "market-9",
                        [
                            {"t": 1.0, "yes": 0.11, "no": 0.89},
                            {"t": 2.0, "yes": 0.09, "no": 0.91},
                        ],
                    )
                ]
            },
        ]
    )

    opportunities, status = await shared_state.read_scanner_snapshot(session)

    assert len(opportunities) == 1
    assert len(opportunities[0].markets[0].get("price_history") or []) == 2
    assert status["opportunities_count"] == 1
