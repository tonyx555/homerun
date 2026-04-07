"""Unit tests for cross-platform wallet intelligence tracking."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import services.scanner as scanner_module  # noqa: E402
from services.wallet_intelligence import CrossPlatformTracker  # noqa: E402
import services.wallet_intelligence as wallet_intelligence_module  # noqa: E402


class TestCrossPlatformTracker:
    @pytest.mark.asyncio
    async def test_get_polymarket_markets_prefers_scanner_cache(self, monkeypatch):
        tracker = CrossPlatformTracker()
        polymarket_market = SimpleNamespace(platform="polymarket", id="pm1")
        kalshi_market = SimpleNamespace(platform="kalshi", id="k1")
        original_cached_markets = list(getattr(scanner_module.scanner, "_cached_markets", []) or [])
        monkeypatch.setattr(scanner_module.scanner, "_cached_markets", [polymarket_market, kalshi_market])

        async def _unexpected_fetch(active: bool = True):
            raise AssertionError("network fetch should not be used when scanner cache is populated")

        monkeypatch.setattr(wallet_intelligence_module.polymarket_client, "get_all_markets", _unexpected_fetch)

        try:
            markets = await tracker._get_polymarket_markets()
        finally:
            monkeypatch.setattr(scanner_module.scanner, "_cached_markets", original_cached_markets)

        assert markets == [polymarket_market]

    @pytest.mark.asyncio
    async def test_scan_cross_platform_uses_matcher_strategy_flow(self, monkeypatch):
        tracker = CrossPlatformTracker()
        pm_market = SimpleNamespace(
            id="pm1",
            question="Will it rain tomorrow?",
            platform="polymarket",
            active=True,
            closed=False,
            outcome_prices=[0.61, 0.39],
            yes_price=0.61,
            no_price=0.39,
        )
        km_market = SimpleNamespace(
            id="k1",
            question="Will it rain tomorrow?",
            yes_price=0.64,
            no_price=0.36,
        )

        class _FakeKalshiCache:
            def get_markets(self):
                return [km_market]

        class _FakeMatcher:
            def __init__(self):
                self._kalshi_cache = _FakeKalshiCache()

            def _refresh_kalshi_tokens(self, kalshi_markets):
                assert kalshi_markets == [km_market]
                return {"k1": {"rain", "tomorrow"}}

            def _find_best_match(self, pm_market_arg, pm_tokens, kalshi_token_index, multiway_events):
                assert pm_market_arg is pm_market
                assert "rain" in pm_tokens
                assert kalshi_token_index == {"k1": {"rain", "tomorrow"}}
                assert multiway_events == set()
                return km_market, 0.92

        tracker._matcher = _FakeMatcher()

        async def _fake_get_polymarket_markets():
            return [pm_market]

        async def _fake_get_active_wallets_for_markets(market_ids):
            assert market_ids == ["pm1"]
            return {"0xabc": {"pm1"}}

        captured: list[tuple[str, list[dict], bool]] = []

        async def _fake_upsert_cross_platform_entity(*, polymarket_address: str, matching_markets: list[dict], cross_platform_arb: bool = False):
            captured.append((polymarket_address, matching_markets, cross_platform_arb))

        monkeypatch.setattr(tracker, "_get_polymarket_markets", _fake_get_polymarket_markets)
        monkeypatch.setattr(tracker, "_get_active_wallets_for_markets", _fake_get_active_wallets_for_markets)
        monkeypatch.setattr(tracker, "_upsert_cross_platform_entity", _fake_upsert_cross_platform_entity)

        await tracker.scan_cross_platform()

        assert len(captured) == 1
        address, matching_markets, arb_flag = captured[0]
        assert address == "0xabc"
        assert arb_flag is False
        assert matching_markets == [
            {
                "polymarket_id": "pm1",
                "kalshi_id": "k1",
                "question": "Will it rain tomorrow?",
                "price_diff": pytest.approx(0.03),
                "potential_arb": False,
            }
        ]
