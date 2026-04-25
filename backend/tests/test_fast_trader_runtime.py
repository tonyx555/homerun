import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import (  # noqa: E402
    Base,
    TradeSignal,
    Trader,
    TraderDecision,
    TraderEvent,
    TraderSignalConsumption,
)
from services.strategies.base import StrategyDecision  # noqa: E402
from services.trader_orchestrator_state import list_fast_traders  # noqa: E402
from tests.postgres_test_db import build_postgres_session_factory  # noqa: E402
from utils.utcnow import utcnow  # noqa: E402
from workers import fast_trader_runtime  # noqa: E402


def _fast_trader_config() -> dict:
    return {
        "id": "fast-trader",
        "name": "Fast Infrastructure Trader",
        "mode": "live",
        "risk_limits": {"max_trade_notional_usd": 7.5},
        "source_configs": [
            {
                "source_key": "generic-source",
                "strategy_key": "generic-fast-strategy",
                "enabled": True,
                "strategy_params": {"min_score": 1.0},
            }
        ],
        "is_enabled": True,
        "is_paused": False,
    }


def _trade_signal(signal_id: str = "signal-1") -> TradeSignal:
    now = utcnow().replace(tzinfo=None)
    return TradeSignal(
        id=signal_id,
        source="generic-source",
        signal_type="entry",
        strategy_type="generic-fast-strategy",
        market_id=f"market-{signal_id}",
        market_question="Generic fast market?",
        direction="buy_yes",
        entry_price=0.42,
        effective_price=0.42,
        edge_percent=4.2,
        confidence=0.77,
        liquidity=1000.0,
        status="pending",
        payload_json={
            "positions_to_take": [
                {
                    "action": "BUY",
                    "outcome": "YES",
                    "price": 0.42,
                    "market_id": f"market-{signal_id}",
                    "token_id": f"token-{signal_id}",
                }
            ]
        },
        dedupe_key=f"dedupe-{signal_id}",
        runtime_sequence=100,
        created_at=now,
        updated_at=now,
    )


async def _seed_trader_and_signal(session, signal_id: str = "signal-1") -> None:
    now = utcnow().replace(tzinfo=None)
    session.add(
        Trader(
            id="fast-trader",
            name="Fast Infrastructure Trader",
            source_configs_json=_fast_trader_config()["source_configs"],
            risk_limits_json={"max_trade_notional_usd": 7.5},
            metadata_json={},
            mode="live",
            latency_class="fast",
            is_enabled=True,
            is_paused=False,
            interval_seconds=1,
            created_at=now,
            updated_at=now,
        )
    )
    session.add(_trade_signal(signal_id))
    await session.commit()


@pytest.mark.asyncio
async def test_fast_trader_records_skipped_decision_and_consumption(monkeypatch):
    engine, session_factory = await build_postgres_session_factory(Base, "fast_runtime_skipped_decision")

    class SkippingStrategy:
        def evaluate(self, signal, context):
            assert context["fast_tier"] is True
            return StrategyDecision(
                decision="skipped",
                reason="shared fast filters not met",
                score=8.25,
                size_usd=3.5,
                checks=[
                    {
                        "key": "generic_gate",
                        "label": "Generic gate",
                        "passed": False,
                        "score": 8.25,
                        "detail": "Rejected by shared infrastructure test.",
                    }
                ],
            )

    monkeypatch.setattr(fast_trader_runtime.strategy_loader, "get_instance", lambda key: SkippingStrategy())
    runner = fast_trader_runtime._FastTraderTask(_fast_trader_config(), asyncio.Event())

    try:
        async with session_factory() as session:
            await _seed_trader_and_signal(session)
            signal = await session.get(TradeSignal, "signal-1")
            await runner._process_one(session, signal=signal, mode="live", default_size_usd=7.5)
            await session.commit()

        async with session_factory() as session:
            decision = (
                await session.execute(select(TraderDecision).where(TraderDecision.trader_id == "fast-trader"))
            ).scalar_one()
            consumption = (
                await session.execute(
                    select(TraderSignalConsumption).where(TraderSignalConsumption.trader_id == "fast-trader")
                )
            ).scalar_one()

        assert decision.decision == "skipped"
        assert decision.reason == "shared fast filters not met"
        assert decision.strategy_key == "generic-fast-strategy"
        assert decision.payload_json["fast_tier"] is True
        assert decision.payload_json["evaluated_size_usd"] == 3.5
        assert decision.checks_summary_json["checks"][0]["key"] == "generic_gate"
        assert consumption.decision_id == decision.id
        assert consumption.outcome == "skipped"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_fast_trader_selected_signal_records_no_order_failure(monkeypatch):
    engine, session_factory = await build_postgres_session_factory(Base, "fast_runtime_no_order_decision")

    class SelectingStrategy:
        def evaluate(self, signal, context):
            return StrategyDecision(decision="selected", reason="selected by shared fast test", score=12.0)

    async def fake_execute_fast_signal(session, **kwargs):
        assert session is not None
        assert kwargs["decision_id"]
        return SimpleNamespace(
            status="skipped",
            effective_price=None,
            error_message="pre-submit gate rejected order",
            orders_written=0,
            payload={"reason": "pre_submit_gate"},
            created_orders=[],
        )

    monkeypatch.setattr(fast_trader_runtime.strategy_loader, "get_instance", lambda key: SelectingStrategy())
    monkeypatch.setattr(fast_trader_runtime, "execute_fast_signal", fake_execute_fast_signal)
    runner = fast_trader_runtime._FastTraderTask(_fast_trader_config(), asyncio.Event())

    try:
        async with session_factory() as session:
            await _seed_trader_and_signal(session, "signal-2")
            signal = await session.get(TradeSignal, "signal-2")
            await runner._process_one(session, signal=signal, mode="live", default_size_usd=7.5)
            await session.commit()

        async with session_factory() as session:
            decision = (
                await session.execute(select(TraderDecision).where(TraderDecision.trader_id == "fast-trader"))
            ).scalar_one()
            event = (
                await session.execute(select(TraderEvent).where(TraderEvent.trader_id == "fast-trader"))
            ).scalar_one()
            consumption = (
                await session.execute(
                    select(TraderSignalConsumption).where(TraderSignalConsumption.trader_id == "fast-trader")
                )
            ).scalar_one()

        assert decision.decision == "skipped"
        assert decision.reason == "pre-submit gate rejected order"
        assert decision.payload_json["submit_result"]["reason"] == "pre_submit_gate"
        assert event.event_type == "fast_submit_no_order"
        assert consumption.decision_id == decision.id
        assert consumption.outcome == "skipped"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_fast_trader_idle_cycle_updates_last_run_and_emits_heartbeat():
    engine, session_factory = await build_postgres_session_factory(Base, "fast_runtime_idle_heartbeat")
    runner = fast_trader_runtime._FastTraderTask(_fast_trader_config(), asyncio.Event())
    runner._last_idle_event_at = -1_000_000.0

    try:
        async with session_factory() as session:
            now = utcnow().replace(tzinfo=None)
            session.add(
                Trader(
                    id="fast-trader",
                    name="Fast Infrastructure Trader",
                    source_configs_json=_fast_trader_config()["source_configs"],
                    risk_limits_json={"max_trade_notional_usd": 7.5},
                    metadata_json={},
                    mode="live",
                    latency_class="fast",
                    is_enabled=True,
                    is_paused=False,
                    interval_seconds=1,
                    created_at=now,
                    updated_at=now,
                )
            )
            await session.commit()

            touched = await runner._touch_trader_run(session, force=True)
            emitted = await runner._maybe_emit_idle_event(
                session,
                accepted_sources=["generic-source"],
                cursor_runtime_sequence=123,
                cursor_created_at=now,
                cursor_signal_id="signal-0",
            )
            await session.commit()

        async with session_factory() as session:
            trader = await session.get(Trader, "fast-trader")
            event = (
                await session.execute(select(TraderEvent).where(TraderEvent.trader_id == "fast-trader"))
            ).scalar_one()

        assert touched is True
        assert emitted is True
        assert trader.last_run_at is not None
        assert event.event_type == "fast_cycle_heartbeat"
        assert event.payload_json["accepted_sources"] == ["generic-source"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_fast_runtime_restarts_dead_per_trader_task(monkeypatch):
    runtime = fast_trader_runtime._FastRuntime()
    old_wake = asyncio.Event()
    old_runner = fast_trader_runtime._FastTraderTask(_fast_trader_config(), old_wake)
    old_task = asyncio.get_running_loop().create_future()
    old_task.set_result(None)
    runtime._wake_events["fast-trader"] = old_wake
    runtime._task_objs["fast-trader"] = old_runner
    runtime._tasks["fast-trader"] = old_task

    class EmptySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def fake_list_fast_traders(session):
        assert session is not None
        return [_fast_trader_config()]

    created = {"count": 0}

    def fake_create_task(coro, *, name=None):
        assert name == "fast-trader-fast-trader"
        coro.close()
        created["count"] += 1
        return asyncio.get_running_loop().create_future()

    monkeypatch.setattr(fast_trader_runtime, "AsyncSessionLocal", lambda: EmptySession())

    async def fake_control_enabled(session):
        assert session is not None
        return {
            "is_enabled": True,
            "is_paused": False,
            "kill_switch": False,
            "mode": "live",
        }

    monkeypatch.setattr(fast_trader_runtime, "read_orchestrator_control", fake_control_enabled)
    monkeypatch.setattr(fast_trader_runtime, "list_fast_traders", fake_list_fast_traders)
    monkeypatch.setattr(fast_trader_runtime.asyncio, "create_task", fake_create_task)

    await runtime._refresh_roster()

    assert old_runner._stopped is True
    assert created["count"] == 1
    assert runtime._task_objs["fast-trader"] is not old_runner
    assert runtime._tasks["fast-trader"] is not old_task


@pytest.mark.asyncio
async def test_fast_runtime_stops_trader_tasks_when_orchestrator_is_paused(monkeypatch):
    runtime = fast_trader_runtime._FastRuntime()
    old_wake = asyncio.Event()
    old_runner = fast_trader_runtime._FastTraderTask(_fast_trader_config(), old_wake)
    old_task = asyncio.get_running_loop().create_future()
    runtime._wake_events["fast-trader"] = old_wake
    runtime._task_objs["fast-trader"] = old_runner
    runtime._tasks["fast-trader"] = old_task

    class EmptySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def fake_list_fast_traders(session):
        raise AssertionError("paused orchestrator must not list fast traders")

    monkeypatch.setattr(fast_trader_runtime, "AsyncSessionLocal", lambda: EmptySession())

    async def fake_control_paused(session):
        assert session is not None
        return {
            "is_enabled": False,
            "is_paused": True,
            "kill_switch": False,
            "mode": "live",
        }

    monkeypatch.setattr(fast_trader_runtime, "read_orchestrator_control", fake_control_paused)
    monkeypatch.setattr(fast_trader_runtime, "list_fast_traders", fake_list_fast_traders)

    await runtime._refresh_roster()

    assert old_runner._stopped is True
    assert old_task.cancelled() is True
    assert "fast-trader" not in runtime._task_objs
    assert "fast-trader" not in runtime._tasks
    assert "fast-trader" not in runtime._wake_events


@pytest.mark.asyncio
async def test_list_fast_traders_excludes_disabled_and_paused_traders():
    engine, session_factory = await build_postgres_session_factory(Base, "fast_runtime_enabled_filter")
    try:
        async with session_factory() as session:
            now = utcnow().replace(tzinfo=None)
            for trader_id, enabled, paused in (
                ("fast-enabled", True, False),
                ("fast-disabled", False, False),
                ("fast-paused", True, True),
            ):
                session.add(
                    Trader(
                        id=trader_id,
                        name=trader_id,
                        source_configs_json=_fast_trader_config()["source_configs"],
                        risk_limits_json={},
                        metadata_json={},
                        mode="live",
                        latency_class="fast",
                        is_enabled=enabled,
                        is_paused=paused,
                        interval_seconds=1,
                        created_at=now,
                        updated_at=now,
                    )
                )
            await session.commit()

            traders = await list_fast_traders(session)

        assert [trader["id"] for trader in traders] == ["fast-enabled"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_fast_runtime_restarts_stale_per_trader_task(monkeypatch):
    runtime = fast_trader_runtime._FastRuntime()
    old_wake = asyncio.Event()
    old_runner = fast_trader_runtime._FastTraderTask(_fast_trader_config(), old_wake)
    now_mono = fast_trader_runtime.time.monotonic()
    old_runner._started_at_mono = now_mono - fast_trader_runtime._FAST_TASK_STALE_SECONDS - 10.0
    old_runner._last_cycle_started_at = now_mono - fast_trader_runtime._FAST_TASK_STALE_SECONDS - 5.0
    old_runner._last_cycle_finished_at = 0.0
    old_task = asyncio.get_running_loop().create_future()
    runtime._wake_events["fast-trader"] = old_wake
    runtime._task_objs["fast-trader"] = old_runner
    runtime._tasks["fast-trader"] = old_task

    class EmptySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def fake_list_fast_traders(session):
        assert session is not None
        return [_fast_trader_config()]

    created = {"count": 0}

    def fake_create_task(coro, *, name=None):
        assert name == "fast-trader-fast-trader"
        coro.close()
        created["count"] += 1
        return asyncio.get_running_loop().create_future()

    monkeypatch.setattr(fast_trader_runtime, "AsyncSessionLocal", lambda: EmptySession())

    async def fake_control_enabled(session):
        assert session is not None
        return {
            "is_enabled": True,
            "is_paused": False,
            "kill_switch": False,
            "mode": "live",
        }

    monkeypatch.setattr(fast_trader_runtime, "read_orchestrator_control", fake_control_enabled)
    monkeypatch.setattr(fast_trader_runtime, "list_fast_traders", fake_list_fast_traders)
    monkeypatch.setattr(fast_trader_runtime.asyncio, "create_task", fake_create_task)

    await runtime._refresh_roster()

    assert old_runner._stopped is True
    assert old_task.cancelled() is True
    assert created["count"] == 1
    assert runtime._task_objs["fast-trader"] is not old_runner
    assert runtime._tasks["fast-trader"] is not old_task


def test_fast_trader_filters_signal_strategy_types_by_source_config():
    task = object.__new__(fast_trader_runtime._FastTraderTask)
    task._trader = {
        "source_configs": [
            {
                "source_key": "feed",
                "strategy_key": "configured",
                "requested_strategy_key": "requested",
                "strategy_params": {
                    "accepted_signal_strategy_types": ["alternate", "configured"],
                },
            },
            {
                "source_key": "disabled",
                "strategy_key": "ignored",
                "enabled": False,
            },
        ]
    }

    assert task._accepted_strategy_types_by_source() == {
        "feed": ["configured", "requested", "alternate"]
    }
