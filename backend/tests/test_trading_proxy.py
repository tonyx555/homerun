import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import services.trading_proxy as trading_proxy


@pytest.mark.asyncio
async def test_pre_trade_vpn_check_caches_failure_result(monkeypatch):
    cfg = trading_proxy.ProxyConfig(
        enabled=True,
        proxy_url="http://user:pass@proxy.example:8080",
        verify_ssl=True,
        timeout=5.0,
        require_vpn=True,
    )
    load_mock = AsyncMock(return_value=cfg)
    verify_mock = AsyncMock(
        return_value={
            "proxy_reachable": False,
            "proxy_ip_error": "Invalid username/password",
            "vpn_active": False,
        }
    )

    monkeypatch.setattr(trading_proxy, "_load_config_from_db", load_mock)
    monkeypatch.setattr(trading_proxy, "verify_vpn_active", verify_mock)
    monkeypatch.setattr(trading_proxy, "_pre_trade_vpn_signature", None)
    monkeypatch.setattr(trading_proxy, "_pre_trade_vpn_cache_result", None)
    monkeypatch.setattr(trading_proxy, "_pre_trade_vpn_cache_until", 0.0)

    first = await trading_proxy.pre_trade_vpn_check()
    second = await trading_proxy.pre_trade_vpn_check()

    assert first == (False, "Trading proxy unreachable: Invalid username/password")
    assert second == first
    assert verify_mock.await_count == 1


@pytest.mark.asyncio
async def test_pre_trade_vpn_check_skips_verification_when_proxy_disabled(monkeypatch):
    cfg = trading_proxy.ProxyConfig(
        enabled=False,
        proxy_url="http://user:pass@proxy.example:8080",
        verify_ssl=True,
        timeout=5.0,
        require_vpn=True,
    )
    load_mock = AsyncMock(return_value=cfg)
    verify_mock = AsyncMock()

    monkeypatch.setattr(trading_proxy, "_load_config_from_db", load_mock)
    monkeypatch.setattr(trading_proxy, "verify_vpn_active", verify_mock)

    allowed, reason = await trading_proxy.pre_trade_vpn_check()

    assert allowed is True
    assert reason == "Proxy not enabled, direct trading allowed"
    assert verify_mock.await_count == 0
