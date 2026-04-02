import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from api import routes_ml
from services import market_runtime
from services import ml_recorder as ml_recorder_module


class _SessionContext:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self) -> None:
        self.snapshots = []

    def add_all(self, snapshots) -> None:
        self.snapshots.extend(snapshots)

    async def commit(self) -> None:
        return None


class _ScalarResult:
    def __init__(self, value) -> None:
        self._value = value

    def scalar_one_or_none(self):
        return self._value

    def scalar(self):
        return self._value


class _RunningJobSession:
    async def execute(self, query):
        del query
        return _ScalarResult("job-running")


class _FakeReferenceRuntime:
    def __init__(self, prices: dict[str, float]) -> None:
        self._prices = {asset.upper(): price for asset, price in prices.items()}

    def get_oracle_price(self, asset: str):
        normalized = str(asset or "").upper()
        price = self._prices.get(normalized)
        if price is None:
            return None
        return {
            "asset": normalized,
            "price": price,
            "updated_at_ms": 1_700_000_000_000,
            "age_seconds": 0.05,
            "source": "chainlink",
        }

    def get_oracle_prices_by_source(self, asset: str):
        oracle = self.get_oracle_price(asset)
        if oracle is None:
            return {}
        return {"chainlink": oracle}

    def get_oracle_history(self, asset: str, *, points: int = 80):
        del points
        oracle = self.get_oracle_price(asset)
        if oracle is None:
            return []
        return [{"t": oracle["updated_at_ms"], "p": oracle["price"]}]

    def get_status(self):
        return {}


@pytest.mark.asyncio
async def test_ml_recorder_normalizes_crypto_runtime_timeframes(monkeypatch):
    session = _FakeSession()
    recorder = ml_recorder_module.MLRecorder()
    monkeypatch.setattr(ml_recorder_module, "AsyncSessionLocal", lambda: _SessionContext(session))
    monkeypatch.setattr(
        recorder,
        "_get_config",
        AsyncMock(
            return_value={
                "is_recording": True,
                "interval_seconds": 1,
                "retention_days": 90,
                "assets": ["btc"],
                "timeframes": ["15m"],
                "schedule_enabled": False,
            }
        ),
    )

    written = await recorder.maybe_record(
        [
            {
                "asset": "BTC",
                "timeframe": "15min",
                "up_price": 0.58,
                "down_price": 0.42,
                "spread": 0.02,
                "combined": 1.0,
                "liquidity": 12000.0,
                "volume": 4500.0,
                "volume_24h": 18000.0,
                "oracle_price": 70250.0,
                "price_to_beat": 70000.0,
                "seconds_left": 420,
                "is_live": True,
            }
        ]
    )

    assert written == 1
    assert len(session.snapshots) == 1
    assert session.snapshots[0].asset == "btc"
    assert session.snapshots[0].timeframe == "15m"


@pytest.mark.asyncio
async def test_prune_route_uses_explicit_days_override(monkeypatch):
    prune_mock = AsyncMock(return_value=7)
    monkeypatch.setattr(routes_ml.ml_recorder, "prune_old_snapshots", prune_mock)

    result = await routes_ml.prune_data(routes_ml.PruneRequest(older_than_days=12, confirm=True))

    assert result == {"action": "pruned", "deleted": 7, "older_than_days": 12}
    prune_mock.assert_awaited_once_with(older_than_days=12)


@pytest.mark.asyncio
async def test_start_training_rejects_invalid_timeframe(monkeypatch):
    monkeypatch.setattr(routes_ml, "is_training_backend_available", lambda model_type: (True, None))

    with pytest.raises(HTTPException) as exc_info:
        await routes_ml.start_training(routes_ml.TrainRequest(timeframes=["2m"]))

    assert exc_info.value.status_code == 400
    assert "Unsupported ML timeframes" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_start_training_blocks_concurrent_job(monkeypatch):
    monkeypatch.setattr(routes_ml, "is_training_backend_available", lambda model_type: (True, None))
    monkeypatch.setattr(routes_ml, "AsyncSessionLocal", lambda: _SessionContext(_RunningJobSession()))

    with pytest.raises(HTTPException) as exc_info:
        await routes_ml.start_training(routes_ml.TrainRequest())

    assert exc_info.value.status_code == 409
    assert "already queued or running" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_market_runtime_records_and_annotates_ml_on_full_refresh(monkeypatch):
    fake_reference = _FakeReferenceRuntime({"BTC": 70000.0})
    monkeypatch.setattr(market_runtime, "get_reference_runtime", lambda: fake_reference)

    runtime = market_runtime.MarketRuntime()
    runtime._reference_runtime = fake_reference
    runtime._feed_manager = SimpleNamespace(_started=False)

    async def _noop_sync_subscriptions():
        return None

    async def _capture_publish(payload, *, trigger):
        published.append((payload, trigger))

    async def _noop_dispatch(payload, *, trigger, full_source_sweep):
        del payload, trigger, full_source_sweep
        return None

    class _FakeCryptoService:
        def get_live_markets(self, force_refresh):
            del force_refresh
            return ["ignored"]

    published: list[tuple[list[dict], str]] = []
    recorded_payloads: list[list[dict]] = []

    async def _fake_record(markets):
        recorded_payloads.append([dict(row) for row in markets])
        return len(markets)

    async def _fake_annotate(markets):
        for market in markets:
            market["ml_prediction"] = {"probability_yes": 0.63}

    monkeypatch.setattr(runtime, "_sync_crypto_subscriptions", _noop_sync_subscriptions)
    monkeypatch.setattr(runtime, "_publish_crypto_snapshot", _capture_publish)
    monkeypatch.setattr(runtime, "_queue_opportunity_dispatch", _noop_dispatch)
    monkeypatch.setattr(
        runtime,
        "_build_crypto_market_payload",
        lambda markets: [
            {
                "id": "btc-15m",
                "slug": "btc-15m",
                "asset": "BTC",
                "timeframe": "15min",
                "up_price": 0.57,
                "down_price": 0.43,
                "spread": 0.02,
                "combined": 1.0,
                "liquidity": 15000.0,
                "volume": 3200.0,
                "volume_24h": 25000.0,
                "seconds_left": 420,
                "oracle_price": 70000.0,
                "price_to_beat": 69800.0,
                "clob_token_ids": ["btc-up", "btc-down"],
                "history_tail": [
                    {"timestamp": "2026-04-01T12:00:00Z", "mid": 0.56, "bid": 0.55, "ask": 0.57},
                    {"timestamp": "2026-04-01T11:59:50Z", "mid": 0.54, "bid": 0.53, "ask": 0.55},
                ],
            }
        ],
    )
    monkeypatch.setattr(market_runtime, "get_crypto_service", lambda: _FakeCryptoService())
    monkeypatch.setattr(market_runtime.ml_recorder, "maybe_record", _fake_record)
    monkeypatch.setattr(market_runtime.ml_recorder, "prune_old_snapshots", AsyncMock(return_value=0))
    monkeypatch.setattr(market_runtime.crypto_ml_runtime, "annotate_markets", _fake_annotate)
    runtime._last_ml_prune_mono = time.monotonic()

    await runtime._refresh_crypto_markets(trigger="startup", full_source_sweep=True)

    assert len(recorded_payloads) == 1
    assert recorded_payloads[0][0]["ml_prediction"]["probability_yes"] == 0.63
    assert runtime._crypto_markets[0]["ml_prediction"]["probability_yes"] == 0.63
    assert len(published) == 1
    assert published[0][0][0]["ml_prediction"]["probability_yes"] == 0.63
