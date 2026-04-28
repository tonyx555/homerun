"""Tests for StrategySDK.time_to_resolution.

Universal across market types — works on any market with a parseable
resolution timestamp (Market ORM, market dict, ISO string, datetime,
or raw epoch-millis int). Returns None for open-ended / unparseable.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.strategy_sdk import StrategySDK


# Anchor: 2030-01-01 00:00:00 UTC = 1_893_456_000_000 ms
ANCHOR_MS = 1_893_456_000_000


# ── Raw int / float input ──────────────────────────────────────


def test_raw_epoch_millis_int():
    secs = StrategySDK.time_to_resolution(ANCHOR_MS, now_ms=ANCHOR_MS - 60_000)
    assert secs == pytest.approx(60.0, rel=1e-9)


def test_raw_epoch_millis_float():
    secs = StrategySDK.time_to_resolution(float(ANCHOR_MS), now_ms=ANCHOR_MS - 1_000)
    assert secs == pytest.approx(1.0, rel=1e-9)


def test_returns_negative_when_past_resolution():
    secs = StrategySDK.time_to_resolution(ANCHOR_MS, now_ms=ANCHOR_MS + 5_000)
    assert secs == pytest.approx(-5.0, rel=1e-9)


def test_returns_zero_at_exact_resolution():
    secs = StrategySDK.time_to_resolution(ANCHOR_MS, now_ms=ANCHOR_MS)
    assert secs == 0.0


# ── datetime input ─────────────────────────────────────────────


def test_datetime_input():
    end = datetime(2030, 1, 1, 0, 1, 0, tzinfo=timezone.utc)  # 60s after anchor
    secs = StrategySDK.time_to_resolution(end, now_ms=ANCHOR_MS)
    assert secs == pytest.approx(60.0, rel=1e-9)


# ── Pydantic-shaped market object ──────────────────────────────


def test_market_object_with_end_date_datetime():
    class FakeMarket:
        end_date = datetime(2030, 1, 1, 0, 0, 30, tzinfo=timezone.utc)

    secs = StrategySDK.time_to_resolution(FakeMarket(), now_ms=ANCHOR_MS)
    assert secs == pytest.approx(30.0, rel=1e-9)


def test_market_object_with_end_date_none():
    class OpenEndedMarket:
        end_date = None

    assert StrategySDK.time_to_resolution(OpenEndedMarket()) is None


def test_market_object_without_end_date_attr():
    class NoEnd:
        pass

    assert StrategySDK.time_to_resolution(NoEnd()) is None


# ── Dict-shaped market ────────────────────────────────────────


def test_dict_with_end_date_iso_string():
    market = {"end_date": "2030-01-01T00:00:30+00:00"}
    secs = StrategySDK.time_to_resolution(market, now_ms=ANCHOR_MS)
    assert secs == pytest.approx(30.0, rel=1e-9)


def test_dict_with_end_date_z_suffix():
    market = {"end_date": "2030-01-01T00:00:00Z"}
    secs = StrategySDK.time_to_resolution(market, now_ms=ANCHOR_MS - 5_000)
    assert secs == pytest.approx(5.0, rel=1e-9)


def test_dict_with_endDate_camelcase_key():
    market = {"endDate": "2030-01-01T00:00:00Z"}
    secs = StrategySDK.time_to_resolution(market, now_ms=ANCHOR_MS - 1_000)
    assert secs == pytest.approx(1.0, rel=1e-9)


def test_dict_with_end_ts_ms_key():
    market = {"end_ts_ms": ANCHOR_MS}
    secs = StrategySDK.time_to_resolution(market, now_ms=ANCHOR_MS - 500)
    assert secs == pytest.approx(0.5, rel=1e-9)


def test_dict_without_recognized_key_returns_none():
    market = {"resolution_at": "2030-01-01T00:00:00Z"}
    assert StrategySDK.time_to_resolution(market) is None


def test_dict_with_null_end_date_returns_none():
    market = {"end_date": None}
    assert StrategySDK.time_to_resolution(market) is None


# ── ISO string input directly ─────────────────────────────────


def test_iso_string_input():
    secs = StrategySDK.time_to_resolution(
        "2030-01-01T00:00:30Z", now_ms=ANCHOR_MS
    )
    assert secs == pytest.approx(30.0, rel=1e-9)


def test_empty_string_returns_none():
    assert StrategySDK.time_to_resolution("") is None
    assert StrategySDK.time_to_resolution("   ") is None


def test_unparseable_string_returns_none():
    assert StrategySDK.time_to_resolution("not-a-date") is None


# ── None / invalid sentinel ───────────────────────────────────


def test_none_returns_none():
    assert StrategySDK.time_to_resolution(None) is None


def test_bool_returns_none():
    """Booleans subclass int but should not be treated as epoch ms."""
    assert StrategySDK.time_to_resolution(True) is None
    assert StrategySDK.time_to_resolution(False) is None


# ── Default now_ms ────────────────────────────────────────────


def test_default_now_uses_wall_clock(monkeypatch):
    import services.strategy_sdk as sdk_mod

    monkeypatch.setattr(sdk_mod.time, "time", lambda: ANCHOR_MS / 1000.0 - 10.0)
    secs = StrategySDK.time_to_resolution(ANCHOR_MS)
    assert secs == pytest.approx(10.0, abs=1e-3)


# ── Composability with other helpers ──────────────────────────


def test_works_for_any_market_type_via_dict():
    """Election, sports, weather — anything with end_date is handled."""
    election = {"slug": "trump-vs-harris-2028", "end_date": "2030-01-01T00:00:00Z"}
    sports = {"slug": "lakers-celtics-game7", "end_date": "2030-01-01T00:00:00Z"}
    weather = {"slug": "nyc-temp-above-90f", "end_date": "2030-01-01T00:00:00Z"}
    for m in (election, sports, weather):
        secs = StrategySDK.time_to_resolution(m, now_ms=ANCHOR_MS - 60_000)
        assert secs == pytest.approx(60.0, rel=1e-9), f"failed for {m['slug']}"
