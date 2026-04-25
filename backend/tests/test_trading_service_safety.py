import sys
import types
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from config import settings
import services.live_execution_service as live_execution_module
from services.live_execution_service import Order, OrderSide, OrderStatus, LiveExecutionService


class _SequencedClient:
    def __init__(self, outcomes: list[bool]):
        self._outcomes = list(outcomes)
        self._counter = 0
        self.posted_order_types: list[str] = []
        self.created_market_orders: list[dict[str, object]] = []
        self.created_limit_orders: list[dict[str, object]] = []

    def create_order(self, order_args):
        self.created_limit_orders.append(
            {
                "price": order_args.price,
                "size": order_args.size,
                "side": order_args.side,
                "token_id": order_args.token_id,
            }
        )
        return {"order_args": order_args}

    def create_market_order(self, order_args):
        self.created_market_orders.append(
            {
                "token_id": order_args.token_id,
                "amount": order_args.amount,
                "side": order_args.side,
                "price": order_args.price,
                "order_type": order_args.order_type,
            }
        )
        return {"order_args": order_args}

    def post_order(self, signed_order, order_type, post_only=False):
        self.posted_order_types.append(str(order_type))
        self._counter += 1
        success = self._outcomes.pop(0) if self._outcomes else True
        if success:
            return {"success": True, "orderID": f"oid-{self._counter}"}
        return {"success": False, "errorMsg": "simulated failure"}


class _ProviderSnapshotClient:
    def __init__(self):
        self.cancel_calls: list[str] = []

    def get_orders(self):
        return [
            {
                "id": "clob-1",
                "status": "partially_filled",
                "size": "100",
                "size_matched": "25",
                "avg_price": "0.44",
                "price": "0.45",
            }
        ]

    def get_order(self, clob_order_id: str):
        return {
            "id": clob_order_id,
            "status": "filled",
            "size": "30",
            "size_matched": "30",
            "avg_price": "0.52",
        }

    def cancel(self, clob_order_id: str):
        self.cancel_calls.append(clob_order_id)
        return {"canceled": [clob_order_id]}


class _ReinitSnapshotClient:
    def __init__(self):
        self.get_orders_calls = 0

    def get_orders(self):
        self.get_orders_calls += 1
        if self.get_orders_calls == 1:
            raise RuntimeError("session expired")
        return [
            {
                "id": "clob-reinit",
                "status": "filled",
                "size": "12",
                "size_matched": "12",
                "avg_price": "0.33",
            }
        ]

    def get_order(self, clob_order_id: str):
        return {"id": clob_order_id, "status": "filled", "size": "12", "size_matched": "12", "avg_price": "0.33"}


class _NoOpenOrdersClient:
    def get_orders(self):
        return []


class _FailingOpenOrdersClient:
    def get_orders(self):
        raise TimeoutError("simulated provider timeout")


class _CachedSnapshotClient:
    def __init__(self):
        self.get_orders_calls = 0
        self.get_order_calls: list[str] = []

    def get_orders(self):
        self.get_orders_calls += 1
        return [
            {
                "id": "clob-open",
                "status": "open",
                "size": "10",
                "size_matched": "0",
                "price": "0.41",
            }
        ]

    def get_order(self, clob_order_id: str):
        self.get_order_calls.append(clob_order_id)
        raise AssertionError("fresh open-order snapshot cache should avoid single-order provider lookups")


def _install_fake_clob_modules(monkeypatch) -> None:
    py_clob_client = types.ModuleType("py_clob_client")
    clob_types = types.ModuleType("py_clob_client.clob_types")
    order_builder = types.ModuleType("py_clob_client.order_builder")
    constants = types.ModuleType("py_clob_client.order_builder.constants")

    class OrderArgs:
        def __init__(self, price, size, side, token_id):
            self.price = price
            self.size = size
            self.side = side
            self.token_id = token_id

    class MarketOrderArgs:
        def __init__(self, token_id, amount, side, price=0, fee_rate_bps=0, nonce=0, taker="0x0", order_type="FOK"):
            self.token_id = token_id
            self.amount = amount
            self.side = side
            self.price = price
            self.fee_rate_bps = fee_rate_bps
            self.nonce = nonce
            self.taker = taker
            self.order_type = order_type

    clob_types.OrderArgs = OrderArgs
    clob_types.MarketOrderArgs = MarketOrderArgs
    constants.BUY = "BUY"
    constants.SELL = "SELL"

    monkeypatch.setitem(sys.modules, "py_clob_client", py_clob_client)
    monkeypatch.setitem(sys.modules, "py_clob_client.clob_types", clob_types)
    monkeypatch.setitem(sys.modules, "py_clob_client.order_builder", order_builder)
    monkeypatch.setitem(
        sys.modules,
        "py_clob_client.order_builder.constants",
        constants,
    )


def _configure_limits(monkeypatch) -> None:
    monkeypatch.setattr(settings, "MIN_ORDER_SIZE_USD", 1.0)
    monkeypatch.setattr(settings, "MAX_TRADE_SIZE_USD", 10_000.0)
    monkeypatch.setattr(settings, "MAX_DAILY_TRADE_VOLUME", 10_000.0)
    monkeypatch.setattr(settings, "MAX_PER_MARKET_USD", 10_000.0)


@pytest.mark.asyncio
async def test_failed_order_rolls_back_reserved_volume(monkeypatch):
    _configure_limits(monkeypatch)
    _install_fake_clob_modules(monkeypatch)

    refresh_mock = AsyncMock(return_value=False)
    monkeypatch.setattr(live_execution_module.global_pause_state, "refresh_from_db", refresh_mock)
    monkeypatch.setattr(live_execution_module, "pre_trade_vpn_check", AsyncMock(return_value=(True, "")))

    service = LiveExecutionService()
    service._initialized = True
    service._client = _SequencedClient([False, True])

    failed = await service.place_order(
        token_id="token-a",
        side=OrderSide.BUY,
        price=0.5,
        size=8.0,
    )
    assert failed.status == OrderStatus.FAILED
    assert service.get_stats().daily_volume == 0.0

    succeeded = await service.place_order(
        token_id="token-a",
        side=OrderSide.BUY,
        price=0.5,
        size=8.0,
    )
    assert succeeded.status == OrderStatus.OPEN
    assert service.get_stats().daily_volume == pytest.approx(4.0)
    refresh_mock.assert_any_call(force=True)


@pytest.mark.asyncio
async def test_sell_order_reduces_market_exposure(monkeypatch):
    _configure_limits(monkeypatch)
    _install_fake_clob_modules(monkeypatch)

    monkeypatch.setattr(
        live_execution_module.global_pause_state,
        "refresh_from_db",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(live_execution_module, "pre_trade_vpn_check", AsyncMock(return_value=(True, "")))

    service = LiveExecutionService()
    service._initialized = True
    service._client = _SequencedClient([True, True])

    buy = await service.place_order(
        token_id="token-b",
        side=OrderSide.BUY,
        price=0.5,
        size=10.0,
    )
    assert buy.status == OrderStatus.OPEN
    assert float(service._market_positions["token-b"]) == pytest.approx(5.0)

    sell = await service.place_order(
        token_id="token-b",
        side=OrderSide.SELL,
        price=0.5,
        size=10.0,
    )
    assert sell.status == OrderStatus.OPEN
    assert "token-b" not in service._market_positions


@pytest.mark.asyncio
async def test_ioc_orders_submit_as_provider_fak(monkeypatch):
    _configure_limits(monkeypatch)
    _install_fake_clob_modules(monkeypatch)

    monkeypatch.setattr(
        live_execution_module.global_pause_state,
        "refresh_from_db",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(live_execution_module, "pre_trade_vpn_check", AsyncMock(return_value=(True, "")))

    service = LiveExecutionService()
    service._initialized = True
    client = _SequencedClient([True])
    service._client = client

    order = await service.place_order(
        token_id="token-ioc",
        side=OrderSide.BUY,
        price=0.5,
        size=10.0,
        order_type=live_execution_module.OrderType.IOC,
    )

    assert order.status == OrderStatus.OPEN
    assert order.order_type == live_execution_module.OrderType.IOC
    assert client.posted_order_types == ["FAK"]
    assert client.created_limit_orders == []
    assert len(client.created_market_orders) == 1
    assert client.created_market_orders[0]["amount"] == pytest.approx(5.0)


def test_order_cache_is_bounded():
    service = LiveExecutionService()
    service._max_order_history = 3

    open_order = Order(
        id="open-1",
        token_id="tok",
        side=OrderSide.BUY,
        price=0.5,
        size=1.0,
        status=OrderStatus.OPEN,
        created_at=datetime(2026, 1, 1, 0, 0, 0),
        updated_at=datetime(2026, 1, 1, 0, 0, 0),
    )
    service._remember_order(open_order)

    for i in range(4):
        failed = Order(
            id=f"failed-{i}",
            token_id="tok",
            side=OrderSide.BUY,
            price=0.5,
            size=1.0,
            status=OrderStatus.FAILED,
            created_at=datetime(2026, 1, 1, 0, 0, i + 1),
            updated_at=datetime(2026, 1, 1, 0, 0, i + 1),
        )
        service._remember_order(failed)

    assert len(service._orders) == 3
    assert "open-1" in service._orders


@pytest.mark.asyncio
async def test_cancel_order_accepts_provider_clob_id(monkeypatch):
    _configure_limits(monkeypatch)
    service = LiveExecutionService()
    service._initialized = True
    client = _ProviderSnapshotClient()
    service._client = client

    cancelled = await service.cancel_order("clob-provider-1")

    assert cancelled is True
    assert client.cancel_calls == ["clob-provider-1"]


@pytest.mark.asyncio
async def test_cancel_order_releases_unfilled_reservation(monkeypatch):
    _configure_limits(monkeypatch)
    service = LiveExecutionService()
    service._initialized = True
    client = _ProviderSnapshotClient()
    service._client = client

    release_mock = AsyncMock()
    monkeypatch.setattr(service, "_release_reservation", release_mock)

    order = Order(
        id="local-open-1",
        token_id="token-btc-up",
        side=OrderSide.BUY,
        price=0.5,
        size=10.0,
        status=OrderStatus.OPEN,
        filled_size=0.0,
        clob_order_id="clob-provider-1",
    )
    service._remember_order(order)

    cancelled = await service.cancel_order("local-open-1")

    assert cancelled is True
    assert release_mock.await_count == 1
    kwargs = release_mock.await_args.kwargs
    assert float(kwargs["size_usd"]) == pytest.approx(5.0)
    assert kwargs["side"] == OrderSide.BUY
    assert kwargs["token_id"] == "token-btc-up"


@pytest.mark.asyncio
async def test_get_order_snapshots_parses_provider_fill_values(monkeypatch):
    _configure_limits(monkeypatch)
    service = LiveExecutionService()
    service._initialized = True
    service._client = _ProviderSnapshotClient()

    snapshots = await service.get_order_snapshots_by_clob_ids(["clob-1", "clob-2"])

    assert set(snapshots.keys()) == {"clob-1", "clob-2"}
    assert snapshots["clob-1"]["normalized_status"] == "partially_filled"
    assert snapshots["clob-1"]["filled_size"] == pytest.approx(25.0)
    assert snapshots["clob-1"]["average_fill_price"] == pytest.approx(0.44)
    assert snapshots["clob-1"]["filled_notional_usd"] == pytest.approx(11.0)
    assert snapshots["clob-2"]["normalized_status"] == "filled"
    assert snapshots["clob-2"]["filled_notional_usd"] == pytest.approx(15.6)


@pytest.mark.asyncio
async def test_get_order_snapshots_maps_invalid_status_to_failed(monkeypatch):
    _configure_limits(monkeypatch)

    class _InvalidStatusClient:
        def get_orders(self):
            return []

        def get_order(self, clob_order_id: str):
            return {
                "id": clob_order_id,
                "status": "INVALID",
                "size": "17.77",
                "size_matched": "0",
                "price": "0.24",
            }

    service = LiveExecutionService()
    service._initialized = True
    service._client = _InvalidStatusClient()
    service._remember_order(
        Order(
            id="order-invalid",
            token_id="tok-invalid",
            side=OrderSide.BUY,
            price=0.24,
            size=17.77,
            status=OrderStatus.OPEN,
            clob_order_id="clob-invalid",
            created_at=datetime(2026, 1, 1, 0, 0, 0),
            updated_at=datetime(2026, 1, 1, 0, 0, 0),
        )
    )

    snapshots = await service.get_order_snapshots_by_clob_ids(["clob-invalid"])

    assert snapshots["clob-invalid"]["normalized_status"] == "failed"
    assert service.get_order("order-invalid") is not None
    assert service.get_order("order-invalid").status == OrderStatus.FAILED


@pytest.mark.asyncio
async def test_get_order_snapshots_retries_after_reinitializing_client(monkeypatch):
    _configure_limits(monkeypatch)
    service = LiveExecutionService()
    service._initialized = True
    service._client = _ReinitSnapshotClient()
    ensure_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(service, "ensure_initialized", ensure_mock)

    snapshots = await service.get_order_snapshots_by_clob_ids(["clob-reinit"])

    assert snapshots["clob-reinit"]["normalized_status"] == "filled"
    assert snapshots["clob-reinit"]["filled_size"] == pytest.approx(12.0)
    assert ensure_mock.await_count == 1


@pytest.mark.asyncio
async def test_get_order_snapshots_reuses_recent_open_order_snapshot_cache(monkeypatch):
    _configure_limits(monkeypatch)
    service = LiveExecutionService()
    service._initialized = True
    client = _CachedSnapshotClient()
    service._client = client
    service._remember_order(
        Order(
            id="order-open",
            token_id="token-open",
            side=OrderSide.BUY,
            price=0.41,
            size=10.0,
            status=OrderStatus.OPEN,
            clob_order_id="clob-open",
            created_at=datetime(2026, 1, 1, 0, 0, 0),
            updated_at=datetime(2026, 1, 1, 0, 0, 0),
        )
    )
    service._remember_order(
        Order(
            id="order-filled",
            token_id="token-filled",
            side=OrderSide.BUY,
            price=0.52,
            size=12.0,
            status=OrderStatus.OPEN,
            clob_order_id="clob-filled",
            created_at=datetime(2026, 1, 1, 0, 0, 0),
            updated_at=datetime(2026, 1, 1, 0, 0, 0),
        )
    )

    first = await service.get_order_snapshots_by_clob_ids(["clob-open"])
    assert first["clob-open"]["normalized_status"] == "open"

    async def _sync_positions():
        service._positions["token-filled"] = live_execution_module.Position(
            token_id="token-filled",
            market_id="market-1",
            market_question="Market",
            outcome="YES",
            size=12.0,
            average_cost=0.52,
        )

    monkeypatch.setattr(service, "sync_positions", _sync_positions)

    snapshots = await service.get_order_snapshots_by_clob_ids(["clob-open", "clob-filled"])

    assert client.get_orders_calls == 1
    assert client.get_order_calls == []
    assert snapshots["clob-open"]["normalized_status"] == "open"
    assert snapshots["clob-filled"]["normalized_status"] == "filled"
    assert snapshots["clob-filled"]["filled_size"] == pytest.approx(12.0)
    assert snapshots["clob-filled"]["average_fill_price"] == pytest.approx(0.52)


@pytest.mark.asyncio
async def test_get_order_snapshots_respects_open_read_circuit_and_returns_cached_fallback(monkeypatch):
    _configure_limits(monkeypatch)

    class _CircuitClient:
        def get_orders(self):
            raise AssertionError("open-order circuit should suppress provider reads")

    service = LiveExecutionService()
    service._initialized = True
    service._client = _CircuitClient()
    service._remember_order(
        Order(
            id="order-cached",
            token_id="token-cached",
            side=OrderSide.BUY,
            price=0.29,
            size=8.0,
            status=OrderStatus.PARTIALLY_FILLED,
            filled_size=3.0,
            average_fill_price=0.29,
            clob_order_id="clob-cached",
            created_at=datetime(2026, 1, 1, 0, 0, 0),
            updated_at=datetime(2026, 1, 1, 0, 0, 0),
        )
    )
    service._clob_read_circuit_open_until = live_execution_module._time.monotonic() + 30.0

    snapshots = await service.get_order_snapshots_by_clob_ids(["clob-cached"])

    assert snapshots["clob-cached"]["normalized_status"] == "partially_filled"
    assert snapshots["clob-cached"]["filled_size"] == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_sync_provider_open_orders_closes_absent_local_active_orders(monkeypatch):
    _configure_limits(monkeypatch)

    service = LiveExecutionService()
    service._initialized = True
    service._client = _NoOpenOrdersClient()

    open_order = Order(
        id="local-open-order",
        token_id="token-basic-no",
        side=OrderSide.BUY,
        price=0.705,
        size=6.15,
        order_type=live_execution_module.OrderType.IOC,
        status=OrderStatus.OPEN,
        filled_size=0.0,
        clob_order_id="clob-basic-no",
        created_at=datetime(2026, 4, 4, 2, 16, 5),
        updated_at=datetime(2026, 4, 4, 2, 16, 5),
    )
    service._remember_order(open_order)

    open_orders = await service.get_open_orders()

    assert open_orders == []
    assert service.get_order("local-open-order") is not None
    assert service.get_order("local-open-order").status == OrderStatus.CANCELLED


@pytest.mark.asyncio
async def test_sync_provider_open_orders_preserves_cached_active_orders_on_provider_failure(monkeypatch):
    _configure_limits(monkeypatch)

    service = LiveExecutionService()
    service._initialized = True
    service._client = _FailingOpenOrdersClient()

    open_order = Order(
        id="local-open-provider-down",
        token_id="token-provider-down",
        side=OrderSide.BUY,
        price=0.705,
        size=6.15,
        order_type=live_execution_module.OrderType.GTC,
        status=OrderStatus.OPEN,
        filled_size=0.0,
        clob_order_id="clob-provider-down",
        created_at=datetime(2026, 4, 4, 2, 16, 5),
        updated_at=datetime(2026, 4, 4, 2, 16, 5),
    )
    service._remember_order(open_order)

    open_orders = await service.get_open_orders()

    assert [order.id for order in open_orders] == ["local-open-provider-down"]
    assert service.get_order("local-open-provider-down").status == OrderStatus.OPEN


@pytest.mark.asyncio
async def test_sync_provider_open_orders_preserves_cached_active_orders_when_circuit_open(monkeypatch):
    _configure_limits(monkeypatch)

    service = LiveExecutionService()
    service._initialized = True
    service._client = _NoOpenOrdersClient()
    service._clob_read_circuit_open_until = live_execution_module._time.monotonic() + 30.0

    open_order = Order(
        id="local-open-circuit",
        token_id="token-circuit",
        side=OrderSide.BUY,
        price=0.5,
        size=10.0,
        order_type=live_execution_module.OrderType.GTC,
        status=OrderStatus.OPEN,
        filled_size=0.0,
        clob_order_id="clob-circuit",
        created_at=datetime(2026, 4, 4, 2, 16, 5),
        updated_at=datetime(2026, 4, 4, 2, 16, 5),
    )
    service._remember_order(open_order)

    open_orders = await service.get_open_orders()

    assert [order.id for order in open_orders] == ["local-open-circuit"]
    assert service.get_order("local-open-circuit").status == OrderStatus.OPEN


@pytest.mark.asyncio
async def test_sync_provider_open_orders_closes_stale_immediate_orders(monkeypatch):
    _configure_limits(monkeypatch)

    service = LiveExecutionService()
    service._initialized = True
    stale_created_at = live_execution_module.utcnow() - timedelta(seconds=90)

    stale_order = Order(
        id="local-stale-ioc",
        token_id="token-basic-yes",
        side=OrderSide.BUY,
        price=0.215,
        size=20.0,
        order_type=live_execution_module.OrderType.IOC,
        status=OrderStatus.OPEN,
        filled_size=0.0,
        clob_order_id="clob-stale-ioc",
        created_at=stale_created_at,
        updated_at=stale_created_at,
    )
    service._remember_order(stale_order)

    class _StaleOpenOrderClient:
        def get_orders(self):
            return {
                "data": [
                    {
                        "id": "clob-stale-ioc",
                        "asset_id": "token-basic-yes",
                        "side": "BUY",
                        "price": "0.215",
                        "original_size": "20",
                        "size_matched": "0",
                        "status": "open",
                        "order_type": "FAK",
                        "created_at": stale_created_at.isoformat(),
                    }
                ]
            }

    service._client = _StaleOpenOrderClient()

    open_orders = await service.get_open_orders()

    assert open_orders == []
    assert service.get_order("local-stale-ioc") is not None
    assert service.get_order("local-stale-ioc").status == OrderStatus.CANCELLED
