import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services import fill_monitor as fill_monitor_module
from services import live_execution_service as live_execution_service_module
from services import wallet_ws_monitor as wallet_ws_monitor_module


@pytest.mark.asyncio
async def test_register_ws_monitor_starts_wallet_ws_monitor(monkeypatch):
    monitor = fill_monitor_module.FillMonitor()
    added_wallets: list[str] = []
    callbacks: list[object] = []
    start_mock = AsyncMock(return_value=None)

    wallet_monitor = SimpleNamespace(
        add_wallet=lambda address: added_wallets.append(address),
        add_callback=lambda callback: callbacks.append(callback),
        start=start_mock,
    )
    live_service = SimpleNamespace(
        is_ready=lambda: True,
        get_execution_wallet_address=lambda: "0xabc123",
    )

    monkeypatch.setattr(wallet_ws_monitor_module, "wallet_ws_monitor", wallet_monitor)
    monkeypatch.setattr(live_execution_service_module, "live_execution_service", live_service)

    await monitor._register_ws_monitor()

    assert added_wallets == ["0xabc123"]
    assert callbacks == [monitor._on_ws_fill]
    assert start_mock.await_count == 1
    assert monitor._ws_registered is True


@pytest.mark.asyncio
async def test_check_fills_emits_only_when_filled_size_advances(monkeypatch):
    monitor = fill_monitor_module.FillMonitor()
    persist_mock = AsyncMock(return_value=None)
    callback_mock = AsyncMock(return_value=None)
    monitor._persist_fill = persist_mock
    monitor.add_callback(callback_mock)

    first = SimpleNamespace(
        id="order-1",
        token_id="token-1",
        side=SimpleNamespace(value="buy"),
        average_fill_price=0.89,
        price=0.89,
        filled_size=1.72,
        size=10.0,
    )
    second = SimpleNamespace(
        id="order-1",
        token_id="token-1",
        side=SimpleNamespace(value="buy"),
        average_fill_price=0.89,
        price=0.89,
        filled_size=1.72,
        size=10.0,
    )
    third = SimpleNamespace(
        id="order-1",
        token_id="token-1",
        side=SimpleNamespace(value="buy"),
        average_fill_price=0.89,
        price=0.89,
        filled_size=2.05,
        size=10.0,
    )
    live_service = SimpleNamespace(
        is_ready=lambda: True,
        get_open_orders=AsyncMock(side_effect=[[first], [second], [third]]),
    )
    monkeypatch.setattr(live_execution_service_module, "live_execution_service", live_service)

    await monitor._check_fills()
    await monitor._check_fills()
    await monitor._check_fills()

    assert persist_mock.await_count == 2
    assert callback_mock.await_count == 2
    assert monitor._reported_fill_size_by_order["order-1"] == pytest.approx(2.05)
