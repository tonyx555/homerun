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
    fake_runtime = SimpleNamespace(
        started=True,
        prewarm_source_tokens=AsyncMock(return_value=None),
        publish_opportunities=AsyncMock(return_value=1),
    )
    monkeypatch.setattr(strategy_signal_bridge, "get_intent_runtime", lambda: fake_runtime)

    opportunity = SimpleNamespace(
        id="opp-1",
        stable_id="stable-1",
        strategy="crypto_strategy",
        resolution_date=datetime(2026, 3, 10, 3, 0, tzinfo=timezone.utc),
        roi_percent=5.0,
        confidence=0.72,
        min_liquidity=5000.0,
    )

    emitted = await strategy_signal_bridge.bridge_opportunities_to_signals(
        [opportunity],
        "crypto",
        refresh_prices=False,
    )

    assert emitted == 1
    fake_runtime.prewarm_source_tokens.assert_called_once_with([opportunity], source="crypto")
    fake_runtime.publish_opportunities.assert_called_once()
