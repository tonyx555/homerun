from __future__ import annotations

from services.strategy_helpers.crypto_strategy_utils import pick_oracle_source


_NOW_MS = 1_777_220_000_000.0


def _market(**ages_ms: float) -> dict:
    base = {
        "binance_direct": 78057.00,
        "chainlink": 78050.08,
        "binance": 78045.13,
    }
    return {
        "oracle_prices_by_source": {
            source: {
                "source": source,
                "price": base[source],
                "updated_at_ms": _NOW_MS - age_ms,
            }
            for source, age_ms in ages_ms.items()
        }
    }


def test_picks_freshest_when_no_preference():
    market = _market(binance_direct=300, chainlink=6200, binance=5300)
    pick = pick_oracle_source(market, now_ms=_NOW_MS)
    assert pick is not None
    assert pick["source"] == "binance_direct"
    assert pick["age_ms"] == 300.0


def test_prefer_returns_preferred_when_present():
    market = _market(binance_direct=300, chainlink=6200, binance=5300)
    pick = pick_oracle_source(market, prefer="chainlink", now_ms=_NOW_MS)
    assert pick is not None
    assert pick["source"] == "chainlink"


def test_prefer_falls_back_when_capped_out():
    market = _market(binance_direct=300, chainlink=6200, binance=5300)
    pick = pick_oracle_source(
        market, prefer="chainlink", max_age_ms=1000, now_ms=_NOW_MS
    )
    assert pick is not None
    # chainlink is older than the cap so the freshest in-cap wins.
    assert pick["source"] == "binance_direct"


def test_max_age_filters_out_all_stale():
    market = _market(chainlink=6200, binance=5300)
    pick = pick_oracle_source(market, max_age_ms=500, now_ms=_NOW_MS)
    assert pick is None


def test_zero_or_missing_price_is_skipped():
    market = {
        "oracle_prices_by_source": {
            "binance_direct": {"price": 0.0, "updated_at_ms": _NOW_MS - 100},
            "chainlink": {"price": 78050.08, "updated_at_ms": _NOW_MS - 5000},
        }
    }
    pick = pick_oracle_source(market, now_ms=_NOW_MS)
    assert pick is not None
    assert pick["source"] == "chainlink"


def test_age_seconds_field_is_accepted():
    market = {
        "oracle_prices_by_source": {
            "binance_direct": {"price": 78057.0, "age_seconds": 0.250, "source": "binance_direct"},
            "chainlink": {"price": 78050.0, "age_seconds": 6.2, "source": "chainlink"},
        }
    }
    pick = pick_oracle_source(market, now_ms=_NOW_MS)
    assert pick is not None
    assert pick["source"] == "binance_direct"
    assert pick["age_ms"] == 250.0


def test_empty_market_returns_none():
    assert pick_oracle_source({}, now_ms=_NOW_MS) is None
    assert pick_oracle_source({"oracle_prices_by_source": {}}, now_ms=_NOW_MS) is None


def test_tiebreak_prefers_binance_direct_over_binance():
    # Both at the same age — binance_direct wins on rank.
    market = _market(binance_direct=1000, binance=1000)
    pick = pick_oracle_source(market, now_ms=_NOW_MS)
    assert pick is not None
    assert pick["source"] == "binance_direct"
