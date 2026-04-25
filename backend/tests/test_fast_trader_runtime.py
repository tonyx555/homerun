import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import (  # noqa: E402
    Base,
    TradeSignal,
    Trader,
    TraderDecision,
    TraderEvent,
    TraderOrder,
    TraderSignalConsumption,
)
from services.strategies.base import StrategyDecision  # noqa: E402
import services.trader_hot_state as hot_state  # noqa: E402
from services.trader_orchestrator import fast_submit  # noqa: E402
from services.trader_orchestrator.order_manager import LegSubmitResult  # noqa: E402
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
    monkeypatch.setattr(hot_state, "AsyncSessionLocal", session_factory)

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
            await hot_state.flush_audit_buffer()

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
async def test_fast_trader_consumes_signal_from_unconfigured_strategy_without_decision(monkeypatch):
    engine, session_factory = await build_postgres_session_factory(Base, "fast_runtime_strategy_filter")
    monkeypatch.setattr(hot_state, "AsyncSessionLocal", session_factory)

    def _unexpected_strategy_lookup(key):
        raise AssertionError(f"unexpected strategy lookup for {key}")

    monkeypatch.setattr(fast_trader_runtime.strategy_loader, "get_instance", _unexpected_strategy_lookup)
    runner = fast_trader_runtime._FastTraderTask(_fast_trader_config(), asyncio.Event())

    try:
        async with session_factory() as session:
            await _seed_trader_and_signal(session, "signal-filtered")
            signal = await session.get(TradeSignal, "signal-filtered")
            signal.strategy_type = "other-fast-strategy"
            await session.commit()

            await runner._process_one(session, signal=signal, mode="live", default_size_usd=7.5)
            await session.commit()
            await hot_state.flush_audit_buffer()

        async with session_factory() as session:
            decision_count = (
                await session.execute(select(func.count()).select_from(TraderDecision))
            ).scalar_one()
            consumption = (
                await session.execute(
                    select(TraderSignalConsumption).where(TraderSignalConsumption.trader_id == "fast-trader")
                )
            ).scalar_one()

        assert decision_count == 0
        assert consumption.decision_id is None
        assert consumption.outcome == "skipped"
        assert str(consumption.reason or "").startswith("source_strategy_filter:")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_fast_trader_selected_signal_records_no_order_failure(monkeypatch):
    engine, session_factory = await build_postgres_session_factory(Base, "fast_runtime_no_order_decision")
    monkeypatch.setattr(hot_state, "AsyncSessionLocal", session_factory)

    class SelectingStrategy:
        def evaluate(self, signal, context):
            return StrategyDecision(decision="selected", reason="selected by shared fast test", score=12.0)

    async def fake_execute_fast_signal(session, **kwargs):
        assert session is not None
        assert kwargs["decision_id"]
        existing_decisions = (
            await session.execute(select(func.count()).select_from(TraderDecision))
        ).scalar_one()
        assert existing_decisions == 0
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
            await hot_state.flush_audit_buffer()

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
async def test_fast_trader_idle_cycle_updates_last_run_and_emits_heartbeat(monkeypatch):
    engine, session_factory = await build_postgres_session_factory(Base, "fast_runtime_idle_heartbeat")
    monkeypatch.setattr(hot_state, "AsyncSessionLocal", session_factory)
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
            await hot_state.flush_audit_buffer()

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
async def test_execute_fast_signal_links_deferred_decision_to_order(monkeypatch):
    engine, session_factory = await build_postgres_session_factory(Base, "fast_submit_deferred_decision_link")

    async def fake_submit_execution_leg(**_kwargs):
        return LegSubmitResult(
            leg_id="leg-1",
            status="failed",
            effective_price=0.42,
            error_message="venue rejected",
            payload={"provider_status": "rejected"},
            provider_order_id="provider-order-1",
            provider_clob_order_id="provider-clob-1",
            shares=7.0,
            notional_usd=3.0,
        )

    monkeypatch.setattr(fast_submit, "submit_execution_leg", fake_submit_execution_leg)

    try:
        async with session_factory() as session:
            await _seed_trader_and_signal(session, "signal-linked")
            signal = await session.get(TradeSignal, "signal-linked")
            result = await fast_submit.execute_fast_signal(
                session,
                trader_id="fast-trader",
                signal=signal,
                decision_id="decision-linked",
                decision_audit={
                    "decision": "selected",
                    "reason": "selected after provider submit",
                    "score": 9.0,
                    "checks_summary": {"fast_tier": True, "checks": []},
                    "risk_snapshot": {"fast_tier": True},
                    "payload": {"fast_tier": True},
                },
                strategy_key="generic-fast-strategy",
                strategy_version=None,
                strategy_params={},
                mode="live",
                size_usd=3.0,
                reason="selected after provider submit",
            )
            await session.commit()

        async with session_factory() as session:
            decision = await session.get(TraderDecision, "decision-linked")
            order = (
                await session.execute(select(TraderOrder).where(TraderOrder.signal_id == "signal-linked"))
            ).scalar_one()

        assert result.orders_written == 1
        assert result.status == "failed"
        assert decision is not None
        assert decision.decision == "selected"
        assert order.decision_id == "decision-linked"
        assert order.provider_order_id == "provider-order-1"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_execute_fast_signal_rolls_back_partial_decision_when_order_persist_fails(monkeypatch):
    engine, session_factory = await build_postgres_session_factory(Base, "fast_submit_persist_failure_rollback")

    async def fake_submit_execution_leg(**_kwargs):
        return LegSubmitResult(
            leg_id="leg-1",
            status="failed",
            effective_price=0.42,
            error_message="venue rejected",
            payload={"provider_status": "rejected"},
            notional_usd=3.0,
        )

    async def fake_create_trader_order(*_args, **_kwargs):
        raise RuntimeError("order write failed")

    monkeypatch.setattr(fast_submit, "submit_execution_leg", fake_submit_execution_leg)
    monkeypatch.setattr(fast_submit, "create_trader_order", fake_create_trader_order)

    try:
        async with session_factory() as session:
            await _seed_trader_and_signal(session, "signal-rollback")
            signal = await session.get(TradeSignal, "signal-rollback")
            result = await fast_submit.execute_fast_signal(
                session,
                trader_id="fast-trader",
                signal=signal,
                decision_id="decision-rollback",
                decision_audit={
                    "decision": "selected",
                    "reason": "selected after provider submit",
                    "payload": {"fast_tier": True},
                },
                strategy_key="generic-fast-strategy",
                strategy_version=None,
                strategy_params={},
                mode="live",
                size_usd=3.0,
                reason="selected after provider submit",
            )
            await session.commit()

        async with session_factory() as session:
            decision = await session.get(TraderDecision, "decision-rollback")

        assert result.orders_written == 0
        assert result.status == "failed"
        assert "order write failed" in str(result.error_message)
        assert decision is None
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
