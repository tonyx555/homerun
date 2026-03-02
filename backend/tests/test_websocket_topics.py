import json

import pytest

from api.websocket import ConnectionManager


class DummyWebSocket:
    def __init__(self) -> None:
        self.messages: list[dict] = []
        self.accepted = False

    async def accept(self) -> None:
        self.accepted = True

    async def send_text(self, payload: str) -> None:
        self.messages.append(json.loads(payload))


@pytest.mark.asyncio
async def test_topic_filtering_and_visibility_gate() -> None:
    manager = ConnectionManager()
    summary_ws = DummyWebSocket()
    detail_ws = DummyWebSocket()
    hidden_ws = DummyWebSocket()

    await manager.connect(summary_ws)
    await manager.connect(detail_ws)
    await manager.connect(hidden_ws)

    manager.update_channels(summary_ws, ["core", "opportunities"])
    manager.update_channels(detail_ws, ["opportunities"])
    manager.update_channels(hidden_ws, ["core", "opportunities"])

    manager.update_topics(summary_ws, ["opportunities.summary"])
    manager.update_topics(detail_ws, ["opportunities.detail:*"])
    manager.update_topics(hidden_ws, ["opportunities.summary"])
    manager.update_presence(hidden_ws, visible=False)

    await manager.broadcast(
        {
            "type": "opportunities_update",
            "topic": "opportunities.summary",
            "data": {"count": 1},
        }
    )

    assert len(summary_ws.messages) == 1
    assert len(detail_ws.messages) == 0
    assert len(hidden_ws.messages) == 0

    await manager.broadcast(
        {
            "type": "opportunity_update",
            "topic": "opportunities.detail:abc123",
            "data": {"stable_id": "abc123"},
        }
    )
    assert len(detail_ws.messages) == 1
    assert len(summary_ws.messages) == 1

    await manager.broadcast(
        {
            "type": "scanner_status",
            "data": {"running": True},
        }
    )
    assert len(summary_ws.messages) == 2
    assert len(hidden_ws.messages) == 1


@pytest.mark.asyncio
async def test_price_topic_subscription_state() -> None:
    manager = ConnectionManager()
    ws = DummyWebSocket()
    await manager.connect(ws)

    manager.update_topics(
        ws,
        [
            "opportunities.summary",
            "prices:market_a",
            "prices:Market_B",
        ],
    )
    state = manager.active_connections[ws]
    assert state["all_price_topics"] is False
    assert state["price_market_ids"] == {"market_a", "market_b"}

    manager.update_topics(ws, ["prices:*"])
    state = manager.active_connections[ws]
    assert state["all_price_topics"] is True
    assert state["price_market_ids"] == set()


def test_trader_orchestrator_status_uses_core_channel() -> None:
    assert ConnectionManager._message_channel("trader_orchestrator_status") == "core"
