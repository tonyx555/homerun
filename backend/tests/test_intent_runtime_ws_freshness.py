from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services.intent_runtime import IntentRuntime
from services.signal_bus import build_signal_contract_from_opportunity
from services.strategies.base import BaseStrategy
from models.market import Event, Market
from models.opportunity import Opportunity
from utils.utcnow import utcnow


def test_tokens_have_fresh_ws_quotes_uses_scanner_age_budget(monkeypatch):
    seen_max_age: list[float] = []

    class _Cache:
        def is_fresh(self, token_id: str, *, max_age_seconds: float | None = None) -> bool:
            assert token_id == "scanner-token"
            seen_max_age.append(float(max_age_seconds or 0.0))
            return True

        def get_mid_price(self, token_id: str):
            assert token_id == "scanner-token"
            return 0.42

    monkeypatch.setattr(
        "services.intent_runtime.get_feed_manager",
        lambda: SimpleNamespace(_started=True, cache=_Cache()),
    )

    runtime = IntentRuntime()
    assert runtime._tokens_have_fresh_ws_quotes(["scanner-token"], source="scanner") is True
    assert seen_max_age == [pytest.approx(30.0)]


@pytest.mark.asyncio
async def test_publish_opportunities_restamps_signal_emitted_at_to_actionable_publish_time(monkeypatch):
    published_batches: list[dict[str, object]] = []

    async def _publish_signal_batch(**kwargs):
        published_batches.append(kwargs)
        return "batch-1"

    monkeypatch.setattr(
        "services.intent_runtime.build_signal_contract_from_opportunity",
        lambda _opportunity: (
            "market-1",
            "buy_yes",
            0.42,
            "Will event happen?",
            {"markets": [{"id": "market-1"}]},
            {},
        ),
    )
    monkeypatch.setattr("services.intent_runtime.publish_signal_batch", _publish_signal_batch)
    monkeypatch.setattr("services.intent_runtime.event_bus.publish", AsyncMock(return_value=None))

    runtime = IntentRuntime()
    opportunity = SimpleNamespace(
        id="opp-1",
        stable_id="stable-1",
        strategy="tail_end_carry",
        roi_percent=7.5,
        confidence=0.8,
        min_liquidity=1000.0,
        resolution_date=None,
    )

    published = await runtime.publish_opportunities([opportunity], source="scanner")

    assert published == 1
    assert len(published_batches) == 1
    emitted_at = str(published_batches[0]["emitted_at"])
    snapshots = published_batches[0]["signal_snapshots"]
    snapshot = next(iter(snapshots.values()))
    assert snapshot["payload_json"]["signal_emitted_at"] == emitted_at


@pytest.mark.asyncio
async def test_publish_opportunities_reactivates_unchanged_scanner_terminal_signal_after_cooldown(monkeypatch):
    published_batches: list[dict[str, object]] = []

    async def _publish_signal_batch(**kwargs):
        published_batches.append(kwargs)
        return "batch-1"

    monkeypatch.setattr(
        "services.intent_runtime.build_signal_contract_from_opportunity",
        lambda _opportunity: (
            "market-1",
            "buy_yes",
            0.42,
            "Will event happen?",
            {"markets": [{"id": "market-1"}]},
            {},
        ),
    )
    monkeypatch.setattr("services.intent_runtime.publish_signal_batch", _publish_signal_batch)
    monkeypatch.setattr("services.intent_runtime.event_bus.publish", AsyncMock(return_value=None))

    runtime = IntentRuntime()
    opportunity = SimpleNamespace(
        id="opp-reactivate-1",
        stable_id="stable-reactivate-1",
        strategy="generic_strategy",
        roi_percent=7.5,
        confidence=0.8,
        min_liquidity=1000.0,
        resolution_date=None,
    )

    assert await runtime.publish_opportunities([opportunity], source="scanner") == 1
    signal_id = next(iter(runtime._signals_by_id))
    runtime._signals_by_id[signal_id]["status"] = "skipped"
    runtime._signals_by_id[signal_id]["runtime_sequence"] = None
    runtime._signals_by_id[signal_id]["updated_at"] = (
        utcnow() - timedelta(seconds=181)
    ).isoformat().replace("+00:00", "Z")
    published_batches.clear()

    published = await runtime.publish_opportunities([opportunity], source="scanner")

    assert published == 1
    assert len(published_batches) == 1
    assert published_batches[0]["event_type"] == "upsert_reactivated"
    refreshed = runtime._signals_by_id[signal_id]
    assert refreshed["status"] == "pending"
    assert refreshed["runtime_sequence"] is not None


@pytest.mark.asyncio
async def test_publish_opportunities_does_not_reactivate_unchanged_scanner_terminal_signal_inside_cooldown(monkeypatch):
    published_batches: list[dict[str, object]] = []

    async def _publish_signal_batch(**kwargs):
        published_batches.append(kwargs)
        return "batch-1"

    monkeypatch.setattr(
        "services.intent_runtime.build_signal_contract_from_opportunity",
        lambda _opportunity: (
            "market-1",
            "buy_yes",
            0.42,
            "Will event happen?",
            {"markets": [{"id": "market-1"}]},
            {},
        ),
    )
    monkeypatch.setattr("services.intent_runtime.publish_signal_batch", _publish_signal_batch)
    monkeypatch.setattr("services.intent_runtime.event_bus.publish", AsyncMock(return_value=None))

    runtime = IntentRuntime()
    opportunity = SimpleNamespace(
        id="opp-reactivate-2",
        stable_id="stable-reactivate-2",
        strategy="generic_strategy",
        roi_percent=7.5,
        confidence=0.8,
        min_liquidity=1000.0,
        resolution_date=None,
    )

    assert await runtime.publish_opportunities([opportunity], source="scanner") == 1
    signal_id = next(iter(runtime._signals_by_id))
    runtime._signals_by_id[signal_id]["status"] = "skipped"
    runtime._signals_by_id[signal_id]["runtime_sequence"] = None
    runtime._signals_by_id[signal_id]["updated_at"] = utcnow().isoformat().replace("+00:00", "Z")
    published_batches.clear()

    published = await runtime.publish_opportunities([opportunity], source="scanner")

    assert published == 0
    assert published_batches == []
    refreshed = runtime._signals_by_id[signal_id]
    assert refreshed["status"] == "skipped"
    assert refreshed["runtime_sequence"] is None


@pytest.mark.asyncio
async def test_project_upserts_scopes_source_sweep_by_strategy_type(monkeypatch):
    class _Session:
        async def commit(self) -> None:
            return None

    class _SessionContext:
        async def __aenter__(self) -> _Session:
            return _Session()

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            del exc_type, exc, tb
            return False

    async def _upsert_trade_signal(*args, **kwargs):
        del args, kwargs
        return SimpleNamespace(status="pending", runtime_sequence=None, effective_price=None)

    expire_mock = AsyncMock(return_value=0)
    monkeypatch.setattr("services.intent_runtime.AsyncSessionLocal", lambda: _SessionContext())
    monkeypatch.setattr("services.intent_runtime.upsert_trade_signal", _upsert_trade_signal)
    monkeypatch.setattr("services.intent_runtime.expire_source_signals_except", expire_mock)

    runtime = IntentRuntime()
    await runtime._project_upsert_batch(
        {
            "source": "scanner",
            "snapshots": {
                "sig-corr-1": {
                    "id": "sig-corr-1",
                    "source": "scanner",
                    "source_item_id": "stable-corr-1",
                    "signal_type": "scanner_opportunity",
                    "strategy_type": "generic_pair_strategy",
                    "market_id": "market-corr-1",
                    "market_question": "Question?",
                    "direction": "buy_yes",
                    "entry_price": 0.42,
                    "edge_percent": 6.0,
                    "confidence": 0.7,
                    "liquidity": 1500.0,
                    "status": "pending",
                    "dedupe_key": "dedupe-corr-1",
                    "payload_json": {},
                    "strategy_context_json": {},
                    "runtime_sequence": 101,
                    "updated_at": utcnow(),
                }
            },
            "sweep_missing": True,
            "keep_dedupe_keys": ["dedupe-corr-1"],
        }
    )

    assert expire_mock.await_count == 1
    kwargs = expire_mock.await_args.kwargs
    assert kwargs["source"] == "scanner"
    assert kwargs["keep_dedupe_keys"] == {"dedupe-corr-1"}
    assert kwargs["signal_types"] == ["scanner_opportunity"]
    assert kwargs["strategy_types"] == ["generic_pair_strategy"]


@pytest.mark.asyncio
async def test_publish_opportunities_uses_strategy_ttl_instead_of_resolution_date(monkeypatch):
    monkeypatch.setattr(
        "services.intent_runtime.build_signal_contract_from_opportunity",
        lambda _opportunity: (
            "market-1",
            "buy_yes",
            0.42,
            "Will event happen?",
            {"markets": [{"id": "market-1"}]},
            {},
        ),
    )
    monkeypatch.setattr("services.intent_runtime.publish_signal_batch", AsyncMock(return_value="batch-1"))
    monkeypatch.setattr("services.intent_runtime.event_bus.publish", AsyncMock(return_value=None))
    monkeypatch.setattr(
        "services.intent_runtime.strategy_loader.get_instance",
        lambda slug: SimpleNamespace(config={"retention_window": "15m"}) if slug == "ttl_strategy" else None,
    )

    runtime = IntentRuntime()
    resolution_date = utcnow() + timedelta(days=2)
    opportunity = SimpleNamespace(
        id="opp-ttl-1",
        stable_id="stable-ttl-1",
        strategy="ttl_strategy",
        roi_percent=7.5,
        confidence=0.8,
        min_liquidity=1000.0,
        resolution_date=resolution_date,
    )

    published = await runtime.publish_opportunities([opportunity], source="scanner")

    assert published == 1
    snapshot = next(iter(runtime._signals_by_id.values()))
    expires_at = datetime.fromisoformat(snapshot["expires_at"].replace("Z", "+00:00"))
    ttl_seconds = (expires_at - utcnow()).total_seconds()
    assert ttl_seconds < 20 * 60
    assert expires_at < resolution_date


def test_build_signal_contract_infers_runtime_metadata_from_loaded_strategy(monkeypatch):
    monkeypatch.setattr(
        "services.strategy_loader.strategy_loader.get_strategy",
        lambda slug: SimpleNamespace(
            instance=SimpleNamespace(
                source_key="scanner",
                subscriptions=["market_data_refresh"],
            )
        )
        if slug == "custom_runtime_strategy"
        else None,
    )

    opportunity = Opportunity(
        strategy="custom_runtime_strategy",
        title="Custom strategy signal",
        description="Test custom strategy runtime metadata",
        total_cost=0.42,
        expected_payout=1.0,
        gross_profit=0.1,
        fee=0.0,
        net_profit=0.1,
        roi_percent=10.0,
        markets=[{"id": "market-1", "question": "Will it happen?"}],
        positions_to_take=[
            {
                "market_id": "market-1",
                "token_id": "scanner-token",
                "outcome": "YES",
                "side": "buy",
                "price": 0.42,
            }
        ],
    )

    _market_id, _direction, _entry_price, _market_question, payload, strategy_context = (
        build_signal_contract_from_opportunity(opportunity)
    )

    assert payload["strategy_runtime"]["source_key"] == "scanner"
    assert payload["strategy_runtime"]["subscriptions"] == ["market_data_refresh"]
    assert payload["strategy_runtime"]["execution_activation"] == "ws_post_arm_tick"
    assert strategy_context["source_key"] == "scanner"
    assert strategy_context["execution_activation"] == "ws_post_arm_tick"


def test_build_signal_contract_persists_full_event_market_roster():
    class _TestStrategy(BaseStrategy):
        strategy_type = "test_roster"
        name = "Test Roster"
        description = "Test roster persistence"

    strategy = _TestStrategy()
    home = Market(
        id="market-home",
        condition_id="condition-home",
        question="Will home team win?",
        slug="event-home",
        event_slug="event-atomic",
        group_item_title="Home",
        sports_market_type="moneyline",
        neg_risk=True,
    )
    draw = Market(
        id="market-draw",
        condition_id="condition-draw",
        question="Will the match end in a draw?",
        slug="event-draw",
        event_slug="event-atomic",
        group_item_title="Draw",
        sports_market_type="moneyline",
        neg_risk=True,
    )
    away = Market(
        id="market-away",
        condition_id="condition-away",
        question="Will away team win?",
        slug="event-away",
        event_slug="event-atomic",
        group_item_title="Away",
        sports_market_type="moneyline",
        neg_risk=True,
    )
    event = Event(
        id="event-atomic",
        slug="event-atomic",
        title="Atomic roster event",
        category="Sports",
        markets=[home, draw, away],
        neg_risk=True,
    )

    opportunity = strategy.create_opportunity(
        title="Atomic roster opportunity",
        description="Test event roster persistence",
        total_cost=0.8,
        expected_payout=1.0,
        is_guaranteed=False,
        markets=[home, away],
        positions=[
            {"market_id": "market-home", "token_id": "token-home", "outcome": "YES", "side": "buy", "price": 0.4},
            {"market_id": "market-away", "token_id": "token-away", "outcome": "YES", "side": "buy", "price": 0.4},
        ],
        event=event,
    )

    _market_id, _direction, _entry_price, _market_question, payload, _strategy_context = build_signal_contract_from_opportunity(opportunity)

    assert payload["market_roster"]["scope"] == "event"
    assert payload["market_roster"]["market_count"] == 3
    assert {market["id"] for market in payload["market_roster"]["markets"]} == {
        "market-home",
        "market-draw",
        "market-away",
    }


@pytest.mark.asyncio
async def test_publish_opportunities_defers_scanner_market_refresh_until_post_arm_ws_tick(monkeypatch):
    published_batches: list[dict[str, object]] = []

    async def _publish_signal_batch(**kwargs):
        published_batches.append(kwargs)
        return "batch-1"

    class _Cache:
        def __init__(self) -> None:
            self.observed_at_epoch = datetime.now(timezone.utc).timestamp()

        def is_fresh(self, token_id: str, *, max_age_seconds: float | None = None) -> bool:
            assert token_id == "scanner-token"
            assert max_age_seconds == pytest.approx(30.0)
            return True

        def get_mid_price(self, token_id: str):
            assert token_id == "scanner-token"
            return 0.42

        def get_observed_at_epoch(self, token_id: str) -> float:
            assert token_id == "scanner-token"
            return self.observed_at_epoch

    cache = _Cache()

    monkeypatch.setattr(
        "services.intent_runtime.get_feed_manager",
        lambda: SimpleNamespace(
            _started=True,
            cache=cache,
            polymarket_feed=SimpleNamespace(subscribe=AsyncMock(return_value=None)),
        ),
    )
    monkeypatch.setattr("services.intent_runtime.publish_signal_batch", _publish_signal_batch)
    monkeypatch.setattr("services.intent_runtime.event_bus.publish", AsyncMock(return_value=None))
    monkeypatch.setattr(
        "services.intent_runtime.build_signal_contract_from_opportunity",
        lambda _opportunity: (
            "market-1",
            "buy_yes",
            0.42,
            "Will event happen?",
            {
                "markets": [{"id": "market-1"}],
                "strategy_runtime": {
                    "source_key": "scanner",
                    "subscriptions": ["market_data_refresh"],
                    "execution_activation": "ws_post_arm_tick",
                },
                "positions_to_take": [{"token_id": "scanner-token"}],
            },
            {},
        ),
    )

    runtime = IntentRuntime()
    runtime._ensure_hot_subscriptions = AsyncMock(return_value=None)
    opportunity = SimpleNamespace(
        id="opp-1",
        stable_id="stable-1",
        strategy="custom_runtime_strategy",
        roi_percent=7.5,
        confidence=0.8,
        min_liquidity=1000.0,
        resolution_date=None,
    )

    published = await runtime.publish_opportunities([opportunity], source="scanner")

    assert published == 0
    assert published_batches == []
    snapshot = next(iter(runtime._signals_by_id.values()))
    assert snapshot["deferred_until_ws"] is True
    assert snapshot["deferred_reason"] == "awaiting_post_arm_ws_tick"
    armed_at_epoch = datetime.fromisoformat(
        snapshot["payload_json"]["execution_armed_at"].replace("Z", "+00:00")
    ).timestamp()

    cache.observed_at_epoch = armed_at_epoch - 0.5
    await runtime._reactivate_deferred_signals_for_token("scanner-token")
    assert published_batches == []

    cache.observed_at_epoch = armed_at_epoch + 0.001
    await runtime._reactivate_deferred_signals_for_token("scanner-token")
    assert len(published_batches) == 1
    reactivated_snapshot = next(iter(published_batches[0]["signal_snapshots"].values()))
    assert reactivated_snapshot["payload_json"]["signal_emitted_at"] == published_batches[0]["emitted_at"]


@pytest.mark.asyncio
async def test_deferred_timeout_uses_stable_deferred_start_under_repeated_refresh(monkeypatch):
    published_batches: list[dict[str, object]] = []

    async def _publish_signal_batch(**kwargs):
        published_batches.append(kwargs)
        return "batch-1"

    class _Cache:
        def is_fresh(self, token_id: str, *, max_age_seconds: float | None = None) -> bool:
            assert token_id == "scanner-token"
            return False

        def get_mid_price(self, token_id: str):
            assert token_id == "scanner-token"
            return 0.42

    monkeypatch.setattr(
        "services.intent_runtime.get_feed_manager",
        lambda: SimpleNamespace(
            _started=True,
            cache=_Cache(),
            polymarket_feed=SimpleNamespace(subscribe=AsyncMock(return_value=None)),
        ),
    )
    monkeypatch.setattr("services.intent_runtime.publish_signal_batch", _publish_signal_batch)
    monkeypatch.setattr("services.intent_runtime.event_bus.publish", AsyncMock(return_value=None))
    monkeypatch.setattr("services.intent_runtime.settings.INTENT_RUNTIME_DEFERRED_MAX_AGE_SECONDS", 5.0)
    monkeypatch.setattr(
        "services.intent_runtime.build_signal_contract_from_opportunity",
        lambda _opportunity: (
            "market-1",
            "buy_yes",
            0.42,
            "Will event happen?",
            {
                "markets": [{"id": "market-1"}],
                "positions_to_take": [{"token_id": "scanner-token"}],
            },
            {},
        ),
    )

    runtime = IntentRuntime()
    runtime._ensure_hot_subscriptions = AsyncMock(return_value=None)
    opportunity = SimpleNamespace(
        id="opp-1",
        stable_id="stable-1",
        strategy="generic_scanner_strategy",
        roi_percent=7.5,
        confidence=0.8,
        min_liquidity=1000.0,
        resolution_date=None,
    )

    published = await runtime.publish_opportunities([opportunity], source="scanner")

    assert published == 0
    assert published_batches == []
    snapshot = next(iter(runtime._signals_by_id.values()))
    original_deferred_started_at = snapshot["deferred_started_at"]
    assert snapshot["deferred_until_ws"] is True
    assert original_deferred_started_at

    await runtime.publish_opportunities([opportunity], source="scanner")
    refreshed_snapshot = next(iter(runtime._signals_by_id.values()))
    assert refreshed_snapshot["deferred_started_at"] == original_deferred_started_at

    stale_start = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    refreshed_snapshot["deferred_started_at"] = stale_start
    refreshed_snapshot["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    await runtime._release_stale_deferred_signals()

    assert len(published_batches) == 1
    reactivated_snapshot = next(iter(published_batches[0]["signal_snapshots"].values()))
    assert reactivated_snapshot["deferred_until_ws"] is False
    assert reactivated_snapshot["deferred_reason"] is None
    assert reactivated_snapshot["runtime_sequence"] is not None
    assert reactivated_snapshot["payload_json"]["signal_emitted_at"] == published_batches[0]["emitted_at"]


def test_build_signal_contract_treats_trader_strategy_like_other_ws_driven_strategies(monkeypatch):
    monkeypatch.setattr(
        "services.strategy_loader.strategy_loader.get_strategy",
        lambda slug: SimpleNamespace(
            instance=SimpleNamespace(
                source_key="traders",
                subscriptions=["trader_activity"],
            )
        )
        if slug == "custom_copy_trade"
        else None,
    )

    opportunity = Opportunity(
        strategy="custom_copy_trade",
        title="Copy trade signal",
        description="Trader-source strategy still uses WS execution activation",
        total_cost=0.41,
        expected_payout=1.0,
        gross_profit=0.09,
        fee=0.0,
        net_profit=0.09,
        roi_percent=9.0,
        markets=[{"id": "market-1", "question": "Will it happen?"}],
        positions_to_take=[
            {
                "market_id": "market-1",
                "token_id": "trader-token",
                "outcome": "YES",
                "side": "buy",
                "price": 0.41,
            }
        ],
    )

    _market_id, _direction, _entry_price, _market_question, payload, strategy_context = (
        build_signal_contract_from_opportunity(opportunity)
    )

    assert payload["strategy_runtime"]["source_key"] == "traders"
    assert payload["strategy_runtime"]["execution_activation"] == "ws_post_arm_tick"
    assert strategy_context["execution_activation"] == "ws_post_arm_tick"


@pytest.mark.asyncio
async def test_publish_opportunities_defers_trader_signal_until_post_arm_ws_tick(monkeypatch):
    published_batches: list[dict[str, object]] = []

    async def _publish_signal_batch(**kwargs):
        published_batches.append(kwargs)
        return "batch-1"

    class _Cache:
        def __init__(self) -> None:
            self.observed_at_epoch = datetime.now(timezone.utc).timestamp()

        def is_fresh(self, token_id: str, *, max_age_seconds: float | None = None) -> bool:
            assert token_id == "trader-token"
            return True

        def get_mid_price(self, token_id: str):
            assert token_id == "trader-token"
            return 0.41

        def get_observed_at_epoch(self, token_id: str) -> float:
            assert token_id == "trader-token"
            return self.observed_at_epoch

    cache = _Cache()

    monkeypatch.setattr(
        "services.intent_runtime.get_feed_manager",
        lambda: SimpleNamespace(
            _started=True,
            cache=cache,
            polymarket_feed=SimpleNamespace(subscribe=AsyncMock(return_value=None)),
        ),
    )
    monkeypatch.setattr("services.intent_runtime.publish_signal_batch", _publish_signal_batch)
    monkeypatch.setattr("services.intent_runtime.event_bus.publish", AsyncMock(return_value=None))
    monkeypatch.setattr(
        "services.intent_runtime.build_signal_contract_from_opportunity",
        lambda _opportunity: (
            "market-1",
            "buy_yes",
            0.41,
            "Will event happen?",
            {
                "markets": [{"id": "market-1"}],
                "strategy_runtime": {
                    "source_key": "traders",
                    "subscriptions": ["trader_activity"],
                    "execution_activation": "ws_post_arm_tick",
                },
                "positions_to_take": [{"token_id": "trader-token"}],
            },
            {},
        ),
    )

    runtime = IntentRuntime()
    runtime._ensure_hot_subscriptions = AsyncMock(return_value=None)
    opportunity = SimpleNamespace(
        id="opp-2",
        stable_id="stable-2",
        strategy="custom_copy_trade",
        roi_percent=5.0,
        confidence=0.7,
        min_liquidity=900.0,
        resolution_date=None,
    )

    published = await runtime.publish_opportunities([opportunity], source="traders")

    assert published == 0
    assert published_batches == []
    snapshot = next(iter(runtime._signals_by_id.values()))
    assert snapshot["deferred_until_ws"] is True
    assert snapshot["deferred_reason"] == "awaiting_post_arm_ws_tick"
