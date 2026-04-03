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
