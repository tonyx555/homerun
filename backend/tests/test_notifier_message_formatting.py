import sys
import re
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.notifier import TelegramNotifier
from services import notifier as notifier_module


def _plain_text(value: str) -> str:
    return re.sub(r"</?[^>]+>", "", value)


class _FakeResult:
    def __init__(self, *, rows=None, scalar_value=None):
        self._rows = list(rows or [])
        self._scalar_value = scalar_value

    def all(self):
        return list(self._rows)

    def scalars(self):
        return self

    def scalar(self):
        return self._scalar_value


class _FakeSession:
    def __init__(self, results):
        self._results = list(results)

    async def execute(self, *_args, **_kwargs):
        if not self._results:
            raise AssertionError("No fake result available for execute()")
        return self._results.pop(0)


@pytest.mark.asyncio
async def test_position_close_alert_has_compact_layout_and_usd_totals(monkeypatch):
    notifier = TelegramNotifier()
    captured: list[str] = []

    async def _enqueue(text: str) -> None:
        captured.append(text)

    async def _name_map(_session, _trader_ids: set[str]) -> dict[str, str]:
        return {
            "trader-1": "Alpha",
            "trader-2": "Beta",
        }

    async def _persist(_session) -> None:
        return None

    monkeypatch.setattr(notifier, "_enqueue", _enqueue)
    monkeypatch.setattr(notifier, "_load_trader_name_map", _name_map)
    monkeypatch.setattr(notifier, "_persist_runtime_state", _persist)

    now = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc)
    orders = [
        SimpleNamespace(
            id="order-1",
            trader_id="trader-1",
            status="resolved_win",
            actual_profit=15.50,
            market_question="Will CPI come in below 3%?",
            market_id="market-1",
            payload_json={"position_close": {"close_trigger": "take_profit"}},
            updated_at=now,
            created_at=now,
            mode="shadow",
        ),
        SimpleNamespace(
            id="order-2",
            trader_id="trader-2",
            status="resolved_loss",
            actual_profit=-4.25,
            market_question="Will BTC close above $120k this week?",
            market_id="market-2",
            payload_json={"position_close": {"close_trigger": "stop_loss"}},
            updated_at=now,
            created_at=now,
            mode="shadow",
        ),
    ]

    await notifier._send_position_close_alerts(
        session=object(),
        orders=orders,
        mode="shadow",
    )

    assert len(captured) == 1
    plain = _plain_text(captured[0])
    assert "Won: $15.50" in plain
    assert "Lost: $4.25" in plain
    assert "Net: +$11.25" in plain
    assert "1) Alpha · WIN · +$15.50" in plain
    assert "2) Beta · LOSS · -$4.25" in plain
    assert "Triggers:" in plain
    assert "take_profit" in plain
    assert "stop_loss" in plain


@pytest.mark.asyncio
async def test_position_close_alert_skips_replayed_close_marker(monkeypatch):
    notifier = TelegramNotifier()
    captured: list[str] = []

    async def _enqueue(text: str) -> None:
        captured.append(text)

    async def _name_map(_session, _trader_ids: set[str]) -> dict[str, str]:
        return {"trader-1": "Alpha"}

    async def _persist(_session) -> None:
        return None

    monkeypatch.setattr(notifier, "_enqueue", _enqueue)
    monkeypatch.setattr(notifier, "_load_trader_name_map", _name_map)
    monkeypatch.setattr(notifier, "_persist_runtime_state", _persist)

    close_at = "2026-03-02T12:00:00Z"
    notifier._close_alert_markers = {"order-1": "resolved_win|pnl_cents:1550"}
    now = datetime(2026, 3, 2, 13, 0, tzinfo=timezone.utc)
    orders = [
        SimpleNamespace(
            id="order-1",
            trader_id="trader-1",
            status="resolved_win",
            actual_profit=15.50,
            market_question="Will CPI come in below 3%?",
            market_id="market-1",
            payload_json={"position_close": {"close_trigger": "take_profit", "closed_at": close_at}},
            updated_at=now,
            created_at=now,
            mode="shadow",
        ),
    ]

    await notifier._send_position_close_alerts(
        session=object(),
        orders=orders,
        mode="shadow",
    )

    assert captured == []


@pytest.mark.asyncio
async def test_position_close_alert_uses_wallet_activity_identity_not_updated_closed_at(monkeypatch):
    notifier = TelegramNotifier()
    captured: list[str] = []

    async def _enqueue(text: str) -> None:
        captured.append(text)

    async def _persist(_session) -> None:
        return None

    async def _name_map(_session, _trader_ids: set[str]) -> dict[str, str]:
        return {"trader-1": "Alpha"}

    monkeypatch.setattr(notifier, "_enqueue", _enqueue)
    monkeypatch.setattr(notifier, "_persist_runtime_state", _persist)
    monkeypatch.setattr(notifier, "_load_trader_name_map", _name_map)

    base_position_close = {
        "close_trigger": "wallet_activity",
        "price_source": "wallet_activity",
        "wallet_activity_transaction_hash": "0xclose",
        "wallet_activity_timestamp": "2026-03-02T12:00:00Z",
    }
    notifier._close_alert_markers = {
        "order-1": TelegramNotifier._close_alert_marker_for_order(
            SimpleNamespace(
                id="order-1",
                status="closed_loss",
                actual_profit=-0.74,
                payload_json={"position_close": {**base_position_close, "closed_at": "2026-03-02T12:05:00Z"}},
                updated_at=datetime(2026, 3, 2, 12, 5, tzinfo=timezone.utc),
                executed_at=datetime(2026, 3, 2, 11, 0, tzinfo=timezone.utc),
                created_at=datetime(2026, 3, 2, 10, 0, tzinfo=timezone.utc),
            )
        )
    }
    order = SimpleNamespace(
        id="order-1",
        trader_id="trader-1",
        status="closed_loss",
        actual_profit=-0.74,
        market_question="Will a repeated close stay quiet?",
        market_id="market-1",
        payload_json={"position_close": {**base_position_close, "closed_at": "2026-03-02T13:05:00Z"}},
        updated_at=datetime(2026, 3, 2, 13, 5, tzinfo=timezone.utc),
        executed_at=datetime(2026, 3, 2, 11, 0, tzinfo=timezone.utc),
        created_at=datetime(2026, 3, 2, 10, 0, tzinfo=timezone.utc),
        mode="live",
    )

    await notifier._send_position_close_alerts(
        session=object(),
        orders=[order],
        mode="live",
    )

    assert captured == []


@pytest.mark.asyncio
async def test_seed_close_alert_markers_rekeys_old_persisted_marker():
    notifier = TelegramNotifier()
    notifier._close_alert_markers = {"order-1": "closed_loss|2026-03-02T12:05:00Z"}
    session = _FakeSession(
        [
            _FakeResult(
                rows=[
                    SimpleNamespace(
                        id="order-1",
                        status="closed_loss",
                        payload_json={
                            "position_close": {
                                "close_trigger": "wallet_activity",
                                "wallet_activity_transaction_hash": "0xclose",
                                "wallet_activity_timestamp": "2026-03-02T12:00:00Z",
                                "closed_at": "2026-03-02T13:05:00Z",
                            }
                        },
                        market_id="market-1",
                        direction="buy_yes",
                        updated_at=datetime(2026, 3, 2, 13, 5, tzinfo=timezone.utc),
                        executed_at=datetime(2026, 3, 2, 11, 0, tzinfo=timezone.utc),
                        created_at=datetime(2026, 3, 2, 10, 0, tzinfo=timezone.utc),
                    )
                ]
            )
        ]
    )

    await notifier._seed_close_alert_markers(session, preserve_existing=True)

    assert notifier._close_alert_markers["order-1"] == "closed_loss|pnl_cents:0"
    assert any(key.startswith("evidence:") for key in notifier._close_alert_markers)


@pytest.mark.asyncio
async def test_position_close_alert_suppresses_zero_pnl(monkeypatch):
    notifier = TelegramNotifier()
    captured: list[str] = []
    persisted = False

    async def _enqueue(text: str) -> None:
        captured.append(text)

    async def _persist(_session) -> None:
        nonlocal persisted
        persisted = True
        return None

    async def _name_map(_session, _trader_ids: set[str]) -> dict[str, str]:
        return {"trader-1": "Alpha"}

    monkeypatch.setattr(notifier, "_enqueue", _enqueue)
    monkeypatch.setattr(notifier, "_persist_runtime_state", _persist)
    monkeypatch.setattr(notifier, "_load_trader_name_map", _name_map)

    now = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc)
    await notifier._send_position_close_alerts(
        session=object(),
        orders=[
            SimpleNamespace(
                id="order-flat",
                trader_id="trader-1",
                status="closed_win",
                actual_profit=0.0,
                market_question="Will flat closes be labelled flat?",
                market_id="market-flat",
                payload_json={"position_close": {"close_trigger": "wallet_activity", "closed_at": "2026-03-02T12:00:00Z"}},
                updated_at=now,
                created_at=now,
                mode="live",
            )
        ],
        mode="live",
    )

    assert captured == []
    assert persisted
    assert "order-flat" in notifier._close_alert_markers


@pytest.mark.asyncio
async def test_position_close_alert_dedupes_same_wallet_close_evidence(monkeypatch):
    notifier = TelegramNotifier()
    captured: list[str] = []

    async def _enqueue(text: str) -> None:
        captured.append(text)

    async def _persist(_session) -> None:
        return None

    async def _name_map(_session, _trader_ids: set[str]) -> dict[str, str]:
        return {"trader-1": "Alpha"}

    monkeypatch.setattr(notifier, "_enqueue", _enqueue)
    monkeypatch.setattr(notifier, "_persist_runtime_state", _persist)
    monkeypatch.setattr(notifier, "_load_trader_name_map", _name_map)

    now = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc)
    close_payload = {
        "close_trigger": "wallet_activity",
        "price_source": "wallet_activity",
        "wallet_activity_transaction_hash": "0xclose",
        "wallet_activity_timestamp": "2026-03-02T12:00:00Z",
        "closed_at": "2026-03-02T12:01:00Z",
    }
    orders = [
        SimpleNamespace(
            id=f"order-{idx}",
            trader_id="trader-1",
            status="closed_loss",
            actual_profit=-0.74,
            market_question="Will duplicate wallet evidence stay quiet?",
            market_id="market-1",
            direction="yes",
            payload_json={
                "token_id": "token-1",
                "position_close": dict(close_payload),
            },
            updated_at=now,
            created_at=now,
            mode="live",
        )
        for idx in range(1, 4)
    ]

    await notifier._send_position_close_alerts(
        session=object(),
        orders=orders,
        mode="live",
    )

    plain = _plain_text(captured[0])
    assert "Count: 1" in plain
    assert plain.count("Alpha") == 1
    assert {"order-1", "order-2", "order-3"}.issubset(set(notifier._close_alert_markers))
    assert any(key.startswith("evidence:") for key in notifier._close_alert_markers)


@pytest.mark.asyncio
async def test_position_close_alert_dedupes_same_wallet_close_evidence_across_traders(monkeypatch):
    notifier = TelegramNotifier()
    captured: list[str] = []

    async def _enqueue(text: str) -> None:
        captured.append(text)

    async def _persist(_session) -> None:
        return None

    async def _name_map(_session, _trader_ids: set[str]) -> dict[str, str]:
        return {"trader-1": "Alpha", "trader-2": "Beta"}

    monkeypatch.setattr(notifier, "_enqueue", _enqueue)
    monkeypatch.setattr(notifier, "_persist_runtime_state", _persist)
    monkeypatch.setattr(notifier, "_load_trader_name_map", _name_map)

    now = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc)
    close_payload = {
        "close_trigger": "wallet_activity",
        "price_source": "wallet_activity",
        "wallet_activity_transaction_hash": "0xsharedclose",
        "wallet_activity_timestamp": "2026-03-02T12:00:00Z",
        "closed_at": "2026-03-02T12:01:00Z",
    }
    orders = [
        SimpleNamespace(
            id="order-cross-1",
            trader_id="trader-1",
            status="closed_loss",
            actual_profit=-0.09,
            market_question="Will one wallet close alert once?",
            market_id="market-1",
            direction="buy_yes",
            payload_json={"token_id": "token-1", "position_close": dict(close_payload)},
            updated_at=now,
            created_at=now,
            mode="live",
        ),
        SimpleNamespace(
            id="order-cross-2",
            trader_id="trader-2",
            status="closed_loss",
            actual_profit=-0.09,
            market_question="Will one wallet close alert once?",
            market_id="market-1",
            direction="buy_yes",
            payload_json={"token_id": "token-1", "position_close": dict(close_payload)},
            updated_at=now,
            created_at=now,
            mode="live",
        ),
    ]

    await notifier._send_position_close_alerts(
        session=object(),
        orders=orders,
        mode="live",
    )

    plain = _plain_text(captured[0])
    assert "Count: 1" in plain
    assert "Alpha" in plain
    assert "Beta" not in plain
    assert {"order-cross-1", "order-cross-2"}.issubset(set(notifier._close_alert_markers))


@pytest.mark.asyncio
async def test_position_close_alert_uses_payload_pnl_when_actual_profit_is_stale_zero(monkeypatch):
    notifier = TelegramNotifier()
    captured: list[str] = []

    async def _enqueue(text: str) -> None:
        captured.append(text)

    async def _persist(_session) -> None:
        return None

    async def _name_map(_session, _trader_ids: set[str]) -> dict[str, str]:
        return {"trader-1": "Alpha"}

    monkeypatch.setattr(notifier, "_enqueue", _enqueue)
    monkeypatch.setattr(notifier, "_persist_runtime_state", _persist)
    monkeypatch.setattr(notifier, "_load_trader_name_map", _name_map)

    now = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc)
    await notifier._send_position_close_alerts(
        session=object(),
        orders=[
            SimpleNamespace(
                id="order-payload-pnl",
                trader_id="trader-1",
                status="closed_loss",
                actual_profit=0.0,
                market_question="Will payload PnL correct stale zero actual profit?",
                market_id="market-payload-pnl",
                payload_json={
                    "position_close": {
                        "close_trigger": "wallet_activity",
                        "closed_at": "2026-03-02T12:00:00Z",
                        "realized_pnl": -1.48,
                    }
                },
                updated_at=now,
                created_at=now,
                mode="live",
            )
        ],
        mode="live",
    )

    plain = _plain_text(captured[0])
    assert "LOSS" in plain
    assert "-$1.48" in plain
    assert "Lost: $1.48" in plain


@pytest.mark.asyncio
async def test_timeline_summary_has_compact_realized_won_lost_net():
    notifier = TelegramNotifier()
    session = _FakeSession(
        [
            _FakeResult(rows=[("selected", 6), ("blocked", 2)]),
            _FakeResult(rows=[("submitted", 4, 120.0), ("resolved_win", 2, 80.0)]),
            _FakeResult(
                rows=[
                    SimpleNamespace(
                        id="win-1",
                        status="resolved_win",
                        actual_profit=20.0,
                        payload_json={"position_close": {"closed_at": "2026-03-01T06:00:00Z"}},
                        updated_at=datetime(2026, 3, 1, 7, 0, tzinfo=timezone.utc),
                        executed_at=datetime(2026, 3, 1, 5, 0, tzinfo=timezone.utc),
                        created_at=datetime(2026, 3, 1, 4, 0, tzinfo=timezone.utc),
                    ),
                    SimpleNamespace(
                        id="win-2",
                        status="resolved_win",
                        actual_profit=15.0,
                        payload_json={"position_close": {"closed_at": "2026-03-01T08:00:00Z"}},
                        updated_at=datetime(2026, 3, 2, 1, 0, tzinfo=timezone.utc),
                        executed_at=datetime(2026, 3, 1, 7, 0, tzinfo=timezone.utc),
                        created_at=datetime(2026, 3, 1, 6, 0, tzinfo=timezone.utc),
                    ),
                    SimpleNamespace(
                        id="loss-1",
                        status="resolved_loss",
                        actual_profit=-12.5,
                        payload_json={"position_close": {"closed_at": "2026-03-01T09:00:00Z"}},
                        updated_at=datetime(2026, 3, 1, 9, 5, tzinfo=timezone.utc),
                        executed_at=datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc),
                        created_at=datetime(2026, 3, 1, 7, 0, tzinfo=timezone.utc),
                    ),
                    SimpleNamespace(
                        id="stale-reupdated",
                        status="resolved_loss",
                        actual_profit=-99.0,
                        payload_json={"position_close": {"wallet_activity_timestamp": "2026-02-27T09:00:00Z"}},
                        updated_at=datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc),
                        executed_at=datetime(2026, 2, 27, 8, 0, tzinfo=timezone.utc),
                        created_at=datetime(2026, 2, 27, 7, 0, tzinfo=timezone.utc),
                    ),
                ]
            ),
            _FakeResult(scalar_value=3),
        ]
    )

    start = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc)
    summary = await notifier._build_timeline_summary(
        session=session,
        start=start,
        end=end,
        control=SimpleNamespace(mode="shadow"),
        snapshot=SimpleNamespace(traders_running=2, traders_total=5),
        title="Autotrader Performance (24h)",
    )

    plain = _plain_text(summary)
    assert "Won: $35.00" in plain
    assert "Lost: $12.50" in plain
    assert "Net: +$22.50" in plain
    assert "Decisions:" in plain
    assert "Orders:" in plain
    assert "Realized:" in plain
    assert "$99.00" not in plain


@pytest.mark.asyncio
async def test_status_message_has_daily_and_realized_usd_breakdown(monkeypatch):
    notifier = TelegramNotifier()

    class _SessionContext:
        async def __aenter__(self):
            return _FakeSession(
                [
                    _FakeResult(
                        rows=[
                            SimpleNamespace(
                                id="status-win",
                                status="resolved_win",
                                actual_profit=20.0,
                                payload_json={"position_close": {"closed_at": "2026-03-02T10:00:00Z"}},
                                updated_at=datetime(2026, 3, 2, 10, 1, tzinfo=timezone.utc),
                                executed_at=datetime(2026, 3, 2, 9, 0, tzinfo=timezone.utc),
                                created_at=datetime(2026, 3, 2, 8, 0, tzinfo=timezone.utc),
                            ),
                            SimpleNamespace(
                                id="status-loss",
                                status="resolved_loss",
                                actual_profit=-8.5,
                                payload_json={"position_close": {"closed_at": "2026-03-02T10:30:00Z"}},
                                updated_at=datetime(2026, 3, 2, 10, 31, tzinfo=timezone.utc),
                                executed_at=datetime(2026, 3, 2, 9, 30, tzinfo=timezone.utc),
                                created_at=datetime(2026, 3, 2, 8, 30, tzinfo=timezone.utc),
                            ),
                        ]
                    ),
                ]
            )

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(notifier_module, "AsyncSessionLocal", lambda: _SessionContext())
    monkeypatch.setattr(
        notifier_module,
        "read_orchestrator_control",
        AsyncMock(
            return_value={
                "mode": "shadow",
                "is_enabled": True,
                "is_paused": False,
                "kill_switch": False,
            }
        ),
    )
    monkeypatch.setattr(
        notifier_module,
        "read_orchestrator_snapshot",
        AsyncMock(
            return_value={
                "running": True,
                "traders_running": 3,
                "traders_total": 6,
                "decisions_count": 42,
                "orders_count": 18,
                "open_orders": 5,
                "daily_pnl": -12.30,
                "last_run_at": datetime(2026, 3, 2, 11, 30, tzinfo=timezone.utc),
                "updated_at": datetime(2026, 3, 2, 11, 31, tzinfo=timezone.utc),
            }
        ),
    )
    monkeypatch.setattr(
        notifier_module,
        "get_gross_exposure",
        AsyncMock(return_value=500.0),
    )
    monkeypatch.setattr(
        notifier_module,
        "utcnow",
        lambda: datetime(2026, 3, 2, 11, 0, tzinfo=timezone.utc),
    )

    message = await notifier._telegram_status_message()
    plain = _plain_text(message)

    assert "-$12.30" in plain
    assert "$20.00" in plain
    assert "$8.50" in plain
    assert "+$11.50" in plain
