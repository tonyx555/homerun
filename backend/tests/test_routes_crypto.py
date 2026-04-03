import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from api import routes_crypto


@pytest.mark.asyncio
async def test_get_crypto_markets_returns_runtime_payload(monkeypatch):
    fake_stats = {
        "markets": [
            {"id": "btc-15m", "asset": "BTC", "oracle_price": 67890.12},
            {"id": "eth-1h", "asset": "ETH", "oracle_price": 3456.78},
        ]
    }
    monkeypatch.setattr(routes_crypto, "_read_crypto_stats", AsyncMock(return_value=fake_stats))

    out = await routes_crypto.get_crypto_markets()

    assert out == [
        {"id": "btc-15m", "asset": "BTC", "oracle_price": 67890.12},
        {"id": "eth-1h", "asset": "ETH", "oracle_price": 3456.78},
    ]


@pytest.mark.asyncio
async def test_get_oracle_prices_returns_runtime_projection(monkeypatch):
    fake_stats = {
        "oracle_prices": {
            "BTC": {"price": 67890.12, "updated_at_ms": 1234, "age_seconds": 0.2},
            "ETH": {"price": 3456.78, "updated_at_ms": 5678, "age_seconds": 0.4},
        }
    }
    monkeypatch.setattr(routes_crypto, "_read_crypto_stats", AsyncMock(return_value=fake_stats))

    out = await routes_crypto.get_oracle_prices()

    assert out == {
        "BTC": {"price": 67890.12, "updated_at_ms": 1234, "age_seconds": 0.2},
        "ETH": {"price": 3456.78, "updated_at_ms": 5678, "age_seconds": 0.4},
    }


@pytest.mark.asyncio
async def test_get_filter_diagnostics_returns_rich_worker_snapshot(monkeypatch):
    fake_stats = {
        "filter_diagnostics": {
            "primary_strategy_key": "alpha",
            "signals_emitted": 3,
            "strategies": {
                "alpha": {"signals_emitted": 2},
                "beta": {"signals_emitted": 1},
            },
            "dispatch_summary": {
                "strategies_loaded": 2,
                "opportunities_by_strategy": {"alpha": 2, "beta": 1},
            },
        }
    }
    monkeypatch.setattr(routes_crypto, "_read_crypto_stats", AsyncMock(return_value=fake_stats))

    out = await routes_crypto.get_filter_diagnostics()

    assert out["primary_strategy_key"] == "alpha"
    assert out["signals_emitted"] == 3
    assert out["dispatch_summary"]["opportunities_by_strategy"] == {"alpha": 2, "beta": 1}
