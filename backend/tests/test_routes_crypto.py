import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from api import routes_crypto


@pytest.mark.asyncio
async def test_get_crypto_markets_returns_runtime_payload(monkeypatch):
    runtime = SimpleNamespace(
        get_crypto_markets=lambda: [
            {"id": "btc-15m", "asset": "BTC", "oracle_price": 67890.12},
            {"id": "eth-1h", "asset": "ETH", "oracle_price": 3456.78},
        ]
    )
    monkeypatch.setattr(routes_crypto, "get_market_runtime", lambda: runtime)

    out = await routes_crypto.get_crypto_markets()

    assert out == [
        {"id": "btc-15m", "asset": "BTC", "oracle_price": 67890.12},
        {"id": "eth-1h", "asset": "ETH", "oracle_price": 3456.78},
    ]


@pytest.mark.asyncio
async def test_get_oracle_prices_returns_runtime_projection(monkeypatch):
    runtime = SimpleNamespace(
        get_oracle_prices=lambda: {
            "BTC": {"price": 67890.12, "updated_at_ms": 1234, "age_seconds": 0.2},
            "ETH": {"price": 3456.78, "updated_at_ms": 5678, "age_seconds": 0.4},
        }
    )
    monkeypatch.setattr(routes_crypto, "get_market_runtime", lambda: runtime)

    out = await routes_crypto.get_oracle_prices()

    assert out == {
        "BTC": {"price": 67890.12, "updated_at_ms": 1234, "age_seconds": 0.2},
        "ETH": {"price": 3456.78, "updated_at_ms": 5678, "age_seconds": 0.4},
    }
