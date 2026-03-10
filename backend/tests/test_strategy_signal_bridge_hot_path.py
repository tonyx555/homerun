from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services import strategy_signal_bridge


class _FakeSession:
    pass


@pytest.mark.asyncio
async def test_bridge_publishes_stream_before_commit(monkeypatch):
    call_order: list[str] = []
    session = _FakeSession()

    monkeypatch.setattr(
        strategy_signal_bridge,
        "build_signal_contract_from_opportunity",
        lambda opp: (
            "market-1",
            "buy_no",
            0.23,
            "BTC 5m Manip",
            {"asset": "BTC", "timeframe": "5m"},
            {"asset": "BTC", "timeframe": "5m"},
        ),
    )
    monkeypatch.setattr(strategy_signal_bridge, "expire_source_signals_except", AsyncMock(return_value=None))
    monkeypatch.setattr(strategy_signal_bridge, "event_bus", SimpleNamespace(publish=AsyncMock(return_value=None)))

    async def _fake_upsert(*args, **kwargs):
        call_order.append("upsert")
        return SimpleNamespace(
            id="signal-1",
            source="crypto",
            source_item_id="stable-1",
            signal_type="crypto_opportunity",
            strategy_type="btc_5m_threshold_flip",
            market_id="market-1",
            market_question="BTC 5m Manip",
            direction="buy_no",
            entry_price=0.23,
            effective_price=None,
            edge_percent=5.0,
            confidence=0.72,
            liquidity=5000.0,
            expires_at=datetime(2026, 3, 10, 3, 0, tzinfo=timezone.utc),
            status="pending",
            payload_json={"asset": "BTC", "timeframe": "5m"},
            strategy_context_json={"asset": "BTC", "timeframe": "5m"},
            quality_passed=True,
            dedupe_key="dedupe-1",
            created_at=datetime(2026, 3, 10, 2, 39, 50, tzinfo=timezone.utc),
            updated_at=datetime(2026, 3, 10, 2, 39, 50, tzinfo=timezone.utc),
        )

    async def _fake_publish_hot_path_signal_batch(**kwargs):
        call_order.append("stream")
        assert kwargs["signal_ids"] == ["signal-1"]
        assert kwargs["signal_snapshots"]["signal-1"]["payload_json"]["asset"] == "BTC"

    async def _fake_commit(_session):
        call_order.append("commit")

    monkeypatch.setattr(strategy_signal_bridge, "upsert_trade_signal", _fake_upsert)
    monkeypatch.setattr(strategy_signal_bridge, "_publish_hot_path_signal_batch", _fake_publish_hot_path_signal_batch)
    monkeypatch.setattr(strategy_signal_bridge, "_commit_with_retry", _fake_commit)
    monkeypatch.setattr(strategy_signal_bridge, "_schedule_trade_signal_snapshot_refresh", lambda: call_order.append("refresh"))

    opportunity = SimpleNamespace(
        id="opp-1",
        stable_id="stable-1",
        strategy="btc_5m_threshold_flip",
        resolution_date=datetime(2026, 3, 10, 3, 0, tzinfo=timezone.utc),
        roi_percent=5.0,
        confidence=0.72,
        min_liquidity=5000.0,
    )

    emitted = await strategy_signal_bridge.bridge_opportunities_to_signals(
        session,
        [opportunity],
        "crypto",
        refresh_prices=False,
    )

    assert emitted == 1
    assert call_order[:3] == ["upsert", "stream", "commit"]
