from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services.trader_orchestrator.live_market_context import (
    RuntimeTradeSignalView,
    build_cached_live_signal_contexts,
    build_live_signal_contexts,
)


@pytest.fixture(autouse=True)
def _patch_ws_price_sources(monkeypatch):
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.get_feed_manager",
        lambda: SimpleNamespace(_started=False),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.get_market_runtime",
        lambda: SimpleNamespace(get_market_snapshot=lambda *_args, **_kwargs: None),
    )


@pytest.mark.asyncio
async def test_build_live_signal_contexts_uses_live_prices_and_history(monkeypatch):
    market_id = "0x" + ("1" * 64)
    yes_token = "123456789012345678"
    no_token = "987654321098765432"

    async def _fake_market_lookup(_market_id: str):
        return {
            "condition_id": market_id,
            "question": "Will it rain tomorrow?",
            "token_ids": [yes_token, no_token],
            "outcomes": ["Yes", "No"],
            "yes_price": 0.41,
            "no_price": 0.59,
        }

    async def _fake_prices_batch(token_ids: list[str]):
        assert set(token_ids) == {yes_token, no_token}
        return {
            yes_token: {"mid": 0.45},
            no_token: {"mid": 0.55},
        }

    async def _fake_history(
        token_id: str,
        interval=None,
        fidelity=None,
        start_ts=None,
        end_ts=None,
        use_trading_proxy: bool = False,
    ):
        del interval, fidelity, use_trading_proxy
        base = 0.40 if token_id == yes_token else 0.60
        return [
            {"t": (int(start_ts) + 5) * 1000, "p": base},
            {"t": (int(end_ts) - 5) * 1000, "p": base + 0.02},
        ]

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_market_by_condition_id",
        _fake_market_lookup,
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_batch",
        _fake_prices_batch,
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_history",
        _fake_history,
    )

    signals = [
        SimpleNamespace(
            id="sig_yes",
            market_id=market_id,
            market_question="Will it rain tomorrow?",
            source="weather",
            direction="buy_yes",
            entry_price=0.40,
            edge_percent=10.0,
            payload_json={},
        ),
        SimpleNamespace(
            id="sig_no",
            market_id=market_id,
            market_question="Will it rain tomorrow?",
            source="weather",
            direction="buy_no",
            entry_price=0.60,
            edge_percent=10.0,
            payload_json={},
        ),
    ]

    contexts = await build_live_signal_contexts(
        signals,
        history_window_seconds=1800,
        history_fidelity_seconds=300,
        max_history_points=20,
        history_tail_points=3,
    )

    yes_ctx = contexts["sig_yes"]
    assert yes_ctx["available"] is True
    assert yes_ctx["selected_outcome"] == "yes"
    assert yes_ctx["live_selected_price"] == pytest.approx(0.45)
    assert yes_ctx["live_edge_percent"] == pytest.approx(5.0)
    assert yes_ctx["market_data_source"] == "http_batch"
    assert yes_ctx["market_data_age_ms"] is not None
    assert yes_ctx["history_summary"]["points"] == 2
    assert len(yes_ctx["history_tail"]) <= 3

    no_ctx = contexts["sig_no"]
    assert no_ctx["available"] is True
    assert no_ctx["selected_outcome"] == "no"
    assert no_ctx["live_selected_price"] == pytest.approx(0.55)
    assert no_ctx["live_edge_percent"] == pytest.approx(15.0)
    assert no_ctx["entry_price_delta"] == pytest.approx(-0.05)
    assert no_ctx["adverse_price_move"] is False


@pytest.mark.asyncio
async def test_build_live_signal_contexts_uses_hot_cache_without_network(monkeypatch):
    yes_token = "123456789012345678"
    no_token = "987654321098765432"
    market_id = "btc-5m-hint"

    class _Cache:
        def get_mid_price(self, token_id: str):
            if token_id == yes_token:
                return 0.41
            if token_id == no_token:
                return 0.59
            return None

        def staleness(self, token_id: str):
            if token_id in {yes_token, no_token}:
                return 0.02
            return None

        def get_price_history(self, token_id: str, max_snapshots: int = 60):
            del max_snapshots
            if token_id != no_token:
                return []
            return [
                {"timestamp": "2026-03-10T02:39:40Z", "mid": 0.61, "bid": 0.60, "ask": 0.62},
                {"timestamp": "2026-03-10T02:39:50Z", "mid": 0.59, "bid": 0.58, "ask": 0.60},
            ]

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.get_feed_manager",
        lambda: SimpleNamespace(_started=True, cache=_Cache()),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.get_market_runtime",
        lambda: SimpleNamespace(
            get_market_snapshot=lambda lookup_id, hint=None: (
                {
                    "condition_id": "0x" + ("a" * 64),
                    "question": "BTC 5m Manip",
                    "clob_token_ids": [yes_token, no_token],
                    "outcome_labels": ["Yes", "No"],
                    "seconds_left": 42,
                    "liquidity": 12000,
                }
                if lookup_id == market_id
                else None
            )
        ),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_market_by_condition_id",
        AsyncMock(side_effect=AssertionError("network market lookup should not run")),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_batch",
        AsyncMock(side_effect=AssertionError("network price batch should not run")),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_history",
        AsyncMock(side_effect=AssertionError("network history fetch should not run")),
    )

    signal = SimpleNamespace(
        id="sig_hot_cache",
        market_id=market_id,
        market_question="BTC 5m Manip",
        source="crypto",
        direction="buy_no",
        entry_price=0.60,
        edge_percent=4.0,
        payload_json={},
    )

    contexts = await build_live_signal_contexts([signal], strict_ws_only=True)
    ctx = contexts["sig_hot_cache"]
    assert ctx["available"] is True
    assert ctx["market_data_source"] == "ws_strict"
    assert ctx["selected_token_id"] == no_token
    assert ctx["live_selected_price"] == pytest.approx(0.59)
    assert ctx["history_summary"]["points"] == 2
    assert ctx["seconds_left"] == pytest.approx(42)


@pytest.mark.asyncio
async def test_build_live_signal_contexts_scanner_strict_ws_uses_scanner_age_budget(monkeypatch):
    yes_token = "223456789012345678"
    no_token = "887654321098765432"
    market_id = "scanner-market-hint"

    class _Cache:
        def get_mid_price(self, token_id: str):
            if token_id == yes_token:
                return 0.41
            if token_id == no_token:
                return 0.59
            return None

        def staleness(self, token_id: str):
            if token_id in {yes_token, no_token}:
                return 4.0
            return None

        def get_price_history(self, token_id: str, max_snapshots: int = 60):
            del max_snapshots
            if token_id != yes_token:
                return []
            return [
                {"timestamp": "2026-03-10T02:39:40Z", "mid": 0.39},
                {"timestamp": "2026-03-10T02:39:50Z", "mid": 0.41},
            ]

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.get_feed_manager",
        lambda: SimpleNamespace(_started=True, cache=_Cache()),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.get_market_runtime",
        lambda: SimpleNamespace(
            get_market_snapshot=lambda lookup_id, hint=None: (
                {
                    "condition_id": "0x" + ("b" * 64),
                    "question": "Scanner strict market",
                    "clob_token_ids": [yes_token, no_token],
                    "outcome_labels": ["Yes", "No"],
                    "seconds_left": 120,
                }
                if lookup_id == market_id
                else None
            )
        ),
    )

    signal = SimpleNamespace(
        id="sig_scanner_hot_cache",
        market_id=market_id,
        market_question="Scanner strict market",
        source="scanner",
        direction="buy_yes",
        entry_price=0.40,
        edge_percent=4.0,
        payload_json={},
    )

    contexts = await build_live_signal_contexts([signal], strict_ws_only=True)
    ctx = contexts["sig_scanner_hot_cache"]
    assert ctx["available"] is True
    assert ctx["market_data_source"] == "ws_strict"
    assert ctx["selected_token_id"] == yes_token
    assert ctx["live_selected_price"] == pytest.approx(0.41)
    assert ctx["market_data_age_ms"] == pytest.approx(4000.0)


@pytest.mark.asyncio
async def test_build_live_signal_contexts_derives_model_probability_from_weather_payload(
    monkeypatch,
):
    market_id = "0x" + ("2" * 64)
    yes_token = "111111111111111111"
    no_token = "222222222222222222"

    async def _fake_market_lookup(_market_id: str):
        return {
            "condition_id": market_id,
            "question": "Highest temp?",
            "token_ids": [yes_token, no_token],
            "outcomes": ["Yes", "No"],
        }

    async def _fake_prices_batch(token_ids: list[str]):
        del token_ids
        return {no_token: {"mid": 0.55}}

    async def _fake_history(*args, **kwargs):
        del args, kwargs
        return []

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_market_by_condition_id",
        _fake_market_lookup,
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_batch",
        _fake_prices_batch,
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_history",
        _fake_history,
    )

    signal = SimpleNamespace(
        id="sig_payload",
        market_id=market_id,
        market_question="Highest temp?",
        source="weather",
        direction="buy_no",
        entry_price=None,
        edge_percent=None,
        payload_json={"metadata": {"weather": {"consensus_probability": 0.3}}},
    )

    contexts = await build_live_signal_contexts([signal])
    ctx = contexts["sig_payload"]
    assert ctx["model_probability"] == pytest.approx(0.7)
    assert ctx["live_edge_percent"] == pytest.approx(15.0)


@pytest.mark.asyncio
async def test_build_cached_live_signal_contexts_uses_payload_market_hint_without_network(monkeypatch):
    yes_token = "555555555555555555"
    no_token = "666666666666666666"

    class _Cache:
        def get_mid_price(self, token_id: str):
            if token_id == yes_token:
                return 0.44
            if token_id == no_token:
                return 0.56
            return None

        def staleness(self, token_id: str):
            if token_id in {yes_token, no_token}:
                return 0.01
            return None

        def get_price_history(self, token_id: str, max_snapshots: int = 60):
            del max_snapshots
            if token_id != yes_token:
                return []
            return [
                {"timestamp": "2026-03-10T02:39:40Z", "mid": 0.41},
                {"timestamp": "2026-03-10T02:39:50Z", "mid": 0.44},
            ]

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.get_feed_manager",
        lambda: SimpleNamespace(_started=True, cache=_Cache()),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.get_market_runtime",
        lambda: SimpleNamespace(get_market_snapshot=lambda lookup_id, hint=None: None),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_market_by_condition_id",
        AsyncMock(side_effect=AssertionError("network market lookup should not run")),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_batch",
        AsyncMock(side_effect=AssertionError("network price batch should not run")),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_history",
        AsyncMock(side_effect=AssertionError("network history fetch should not run")),
    )

    signal = SimpleNamespace(
        id="sig_cached_payload",
        market_id="payload-market-1",
        market_question="BTC 15m breakout",
        source="crypto",
        direction="buy_yes",
        entry_price=0.42,
        edge_percent=5.0,
        payload_json={
            "markets": [
                {
                    "id": "payload-market-1",
                    "question": "BTC 15m breakout",
                    "token_ids": [yes_token, no_token],
                    "outcomes": ["Yes", "No"],
                    "liquidity": 12500,
                }
            ]
        },
    )

    contexts = await build_cached_live_signal_contexts([signal], strict_ws_only=True)
    ctx = contexts["sig_cached_payload"]
    assert ctx["available"] is True
    assert ctx["market_data_source"] == "ws_strict"
    assert ctx["selected_token_id"] == yes_token
    assert ctx["live_selected_price"] == pytest.approx(0.44)
    assert ctx["history_summary"]["points"] == 2
    assert ctx["liquidity_usd"] == pytest.approx(12500)


@pytest.mark.asyncio
async def test_build_live_signal_contexts_uses_runtime_source_item_lookup_when_signal_market_id_is_non_lookup(
    monkeypatch,
):
    signal_market_id = "Bitcoin Up or Down - February 23, 9AM ET"
    condition_id = "0x" + ("3" * 64)
    yes_token = "33333333333333333333333333333333333333333333333333333333333333333"
    no_token = "44444444444444444444444444444444444444444444444444444444444444444"

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.get_market_runtime",
        lambda: SimpleNamespace(
            get_market_snapshot=lambda lookup_id, hint=None: (
                {
                    "condition_id": condition_id,
                    "question": signal_market_id,
                    "clob_token_ids": [yes_token, no_token],
                    "outcome_labels": ["Yes", "No"],
                    "up_price": 0.08,
                    "down_price": 0.92,
                    "price_updated_at": "2026-03-10T02:39:50Z",
                }
                if lookup_id == condition_id
                else None
            )
        ),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_market_by_condition_id",
        AsyncMock(side_effect=AssertionError("network market lookup should not run")),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_batch",
        AsyncMock(side_effect=AssertionError("network price batch should not run")),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_history",
        AsyncMock(side_effect=AssertionError("network history fetch should not run")),
    )

    signal = SimpleNamespace(
        id="sig_payload_hint",
        market_id=signal_market_id,
        source_item_id=condition_id,
        market_question=signal_market_id,
        source="scanner",
        direction="buy_no",
        entry_price=0.95,
        edge_percent=5.0,
        payload_json={},
    )

    contexts = await build_live_signal_contexts([signal])
    ctx = contexts["sig_payload_hint"]
    assert ctx["market_id"] == signal_market_id.lower()
    assert ctx["condition_id"] == condition_id
    assert ctx["available"] is True
    assert ctx["selected_outcome"] == "no"
    assert ctx["selected_token_id"] == no_token
    assert ctx["live_selected_price"] == pytest.approx(0.92)


@pytest.mark.asyncio
async def test_build_live_signal_contexts_returns_unavailable_when_runtime_and_market_lookup_fail(
    monkeypatch,
):
    signal_market_id = "unresolvable-market-name"

    async def _fake_market_lookup(_market_id: str):
        return None

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_market_by_condition_id",
        _fake_market_lookup,
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_batch",
        AsyncMock(side_effect=AssertionError("price batch should not run without token ids")),
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_history",
        AsyncMock(side_effect=AssertionError("history fetch should not run without token ids")),
    )

    signal = SimpleNamespace(
        id="sig_market_lookup_fails",
        market_id=signal_market_id,
        market_question="Fallback token hint path",
        source="scanner",
        direction="buy_no",
        entry_price=0.9,
        edge_percent=3.0,
        payload_json={},
    )

    contexts = await build_live_signal_contexts([signal])
    ctx = contexts["sig_market_lookup_fails"]
    assert ctx["available"] is False
    assert ctx["selected_outcome"] == "no"
    assert ctx["selected_token_id"] is None
    assert ctx["live_selected_price"] is None


@pytest.mark.asyncio
async def test_build_live_signal_contexts_rejects_relaxed_ws_prices(monkeypatch):
    market_id = "0x" + ("7" * 64)
    yes_token = "777777777777777777"
    no_token = "888888888888888888"

    class _Cache:
        def get_mid_price(self, token_id: str):
            return 0.42 if token_id == yes_token else 0.58

        def staleness(self, token_id: str):
            del token_id
            return 4.0

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.get_feed_manager",
        lambda: SimpleNamespace(_started=True, cache=_Cache()),
    )

    async def _fake_market_lookup(_market_id: str):
        return {
            "condition_id": market_id,
            "question": "BTC 5m",
            "token_ids": [yes_token, no_token],
            "outcomes": ["Yes", "No"],
        }

    async def _fake_prices_batch(token_ids: list[str]):
        assert set(token_ids) == {yes_token, no_token}
        return {
            yes_token: {"mid": 0.47},
            no_token: {"mid": 0.53},
        }

    async def _fake_history(*args, **kwargs):
        del args, kwargs
        return []

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_market_by_condition_id",
        _fake_market_lookup,
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_batch",
        _fake_prices_batch,
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_history",
        _fake_history,
    )

    signal = SimpleNamespace(
        id="sig_ws_relaxed",
        market_id=market_id,
        market_question="BTC 5m",
        source="crypto",
        direction="buy_yes",
        entry_price=0.45,
        edge_percent=4.0,
        payload_json={},
    )
    contexts = await build_live_signal_contexts([signal])
    ctx = contexts["sig_ws_relaxed"]
    assert ctx["market_data_source"] == "http_batch"
    assert ctx["live_selected_price"] == pytest.approx(0.47)
    assert ctx["market_data_age_ms"] is not None


@pytest.mark.asyncio
async def test_build_live_signal_contexts_strict_ws_only_disables_http_price_fallback(monkeypatch):
    market_id = "0x" + ("8" * 64)
    yes_token = "777777777777777777"
    no_token = "888888888888888888"

    class _Cache:
        def get_mid_price(self, token_id: str):
            return 0.42 if token_id == yes_token else 0.58

        def staleness(self, token_id: str):
            del token_id
            return 4.0

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.get_feed_manager",
        lambda: SimpleNamespace(_started=True, cache=_Cache()),
    )

    async def _fake_market_lookup(_market_id: str):
        return {
            "condition_id": market_id,
            "question": "BTC 5m",
            "token_ids": [yes_token, no_token],
            "outcomes": ["Yes", "No"],
        }

    async def _fake_prices_batch(token_ids: list[str]):
        assert set(token_ids) == {yes_token, no_token}
        return {
            yes_token: {"mid": 0.47},
            no_token: {"mid": 0.53},
        }

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_market_by_condition_id",
        _fake_market_lookup,
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_batch",
        _fake_prices_batch,
    )

    signal = SimpleNamespace(
        id="sig_strict_ws_only",
        market_id=market_id,
        market_question="BTC 5m",
        source="crypto",
        direction="buy_yes",
        entry_price=0.45,
        edge_percent=4.0,
        payload_json={},
    )
    contexts = await build_live_signal_contexts([signal], strict_ws_only=True)
    assert "sig_strict_ws_only" not in contexts


@pytest.mark.asyncio
async def test_build_live_signal_contexts_keeps_unknown_age_for_snapshot_without_timestamp(
    monkeypatch,
):
    market_id = "0x" + ("9" * 64)
    yes_token = "999999999999999999"
    no_token = "101010101010101010"

    async def _fake_market_lookup(_market_id: str):
        return {
            "condition_id": market_id,
            "question": "Snapshot-only market",
            "token_ids": [yes_token, no_token],
            "outcomes": ["Yes", "No"],
            "yes_price": 0.44,
            "no_price": 0.56,
        }

    async def _fake_prices_batch(token_ids: list[str]):
        del token_ids
        return {}

    async def _fake_history(*args, **kwargs):
        del args, kwargs
        return []

    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_market_by_condition_id",
        _fake_market_lookup,
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_batch",
        _fake_prices_batch,
    )
    monkeypatch.setattr(
        "services.trader_orchestrator.live_market_context.polymarket_client.get_prices_history",
        _fake_history,
    )

    signal = SimpleNamespace(
        id="sig_snapshot_age_unknown",
        market_id=market_id,
        market_question="Snapshot-only market",
        source="scanner",
        direction="buy_yes",
        entry_price=0.44,
        edge_percent=1.0,
        payload_json={},
    )
    contexts = await build_live_signal_contexts([signal])
    ctx = contexts["sig_snapshot_age_unknown"]
    assert ctx["market_data_source"] == "market_snapshot"
    assert ctx["live_selected_price"] == pytest.approx(0.44)
    assert ctx["source_observed_at"] is None
    assert ctx["market_data_age_ms"] is None


def test_runtime_trade_signal_view_overrides_runtime_fields():
    base = SimpleNamespace(
        id="sig_runtime",
        source="weather",
        market_id="0xabc",
        entry_price=0.22,
        edge_percent=18.0,
        payload_json={"foo": "bar"},
    )
    runtime = RuntimeTradeSignalView(
        base,
        live_context={
            "live_selected_price": 0.31,
            "live_edge_percent": 7.2,
            "selected_outcome": "yes",
        },
    )

    assert runtime.id == "sig_runtime"
    assert runtime.entry_price == pytest.approx(0.31)
    assert runtime.edge_percent == pytest.approx(7.2)
    assert runtime.payload_json["foo"] == "bar"
    assert runtime.payload_json["live_market"]["selected_outcome"] == "yes"
