"""Re-exports of services.strategies.crypto_strategy_utils plus oracle-status helpers.

All public symbols from the strategies module are available here so that
importing code never needs to distinguish between the two locations.
"""
from __future__ import annotations

import time as _time
from typing import Any

# Re-export everything from the canonical location so importers of
# services.strategy_helpers.crypto_strategy_utils get the same symbols.
from services.strategies.crypto_strategy_utils import (  # noqa: F401
    build_binary_crypto_market,
    build_binary_market_from_row,
    bounded_sigmoid,
    default_max_market_data_age_ms,
    default_max_oracle_age_ms,
    default_min_seconds_left_for_entry,
    fee_aware_min_edge_pct,
    history_cancel_peak,
    market_ml_probability_yes,
    normalize_ratio,
    normalize_signed_ratio,
    normalize_timeframe,
    parse_datetime_utc,
    pick_oracle_source,
    seconds_left_from_row,
    spread_pct_from_row,
    taker_fee_pct,
    timeframe_seconds,
)
from utils.kelly import polymarket_taker_fee_pct  # noqa: F401
from utils.converters import safe_float as _safe_float


# ---------------------------------------------------------------------------
# first_present — return the first non-None positional argument
# ---------------------------------------------------------------------------

def first_present(*args: Any) -> Any:
    for arg in args:
        if arg is not None:
            return arg
    return None


# ---------------------------------------------------------------------------
# Oracle availability helpers
# ---------------------------------------------------------------------------

def resolve_oracle_availability(
    *,
    price: float | None,
    price_to_beat: float | None,
    age_ms: float | None,
    updated_at_ms: int | None,
) -> dict[str, Any]:
    """Return a dict describing whether an oracle point is usable."""
    has_price = price is not None and float(price) > 0.0
    has_timestamp = updated_at_ms is not None or age_ms is not None
    available = has_price
    fresh_available = has_price and age_ms is not None
    directional_available = available

    return {
        "available": available,
        "has_price": has_price,
        "has_timestamp": has_timestamp,
        "fresh_available": fresh_available,
        "directional_available": directional_available,
        "availability_reasons": [],
        "freshness_state": "fresh" if fresh_available else "unknown",
        "freshness_reasons": [],
        "directional_state": "available" if directional_available else "unavailable",
        "directional_reasons": [],
    }


def extract_oracle_status(
    *,
    live_market: dict[str, Any],
    payload: dict[str, Any],
    now_ms: float,
) -> dict[str, Any]:
    """Extract a normalised oracle-status dict from a live_market + signal payload.

    Merges per-source oracle data from both dicts, picks the best source via
    the same ranking used by ``pick_oracle_source``, and returns a dict
    compatible with the fields consumed by BtcEthHighFreqStrategy.evaluate().
    """
    # Prefer payload over live_market for primary fields.
    oracle_price = _safe_float(
        first_present(payload.get("oracle_price"), live_market.get("oracle_price")), None
    )
    oracle_source_raw = first_present(
        payload.get("oracle_source"), live_market.get("oracle_source")
    )
    oracle_source = str(oracle_source_raw or "").strip().lower() or None
    oracle_updated_at_ms = _safe_float(
        first_present(payload.get("oracle_updated_at_ms"), live_market.get("oracle_updated_at_ms")), None
    )
    oracle_age_seconds = _safe_float(
        first_present(payload.get("oracle_age_seconds"), live_market.get("oracle_age_seconds")), None
    )
    price_to_beat = _safe_float(
        first_present(payload.get("price_to_beat"), live_market.get("price_to_beat")), None
    )

    # Compute age_ms for the primary source.
    age_ms: float | None = None
    if oracle_age_seconds is not None and oracle_age_seconds >= 0:
        age_ms = float(oracle_age_seconds) * 1000.0
    elif oracle_updated_at_ms is not None and oracle_updated_at_ms > 0:
        age_ms = max(0.0, float(now_ms) - float(oracle_updated_at_ms))

    # Build per-source dict from oracle_prices_by_source in either dict.
    by_source_raw = first_present(
        payload.get("oracle_prices_by_source"), live_market.get("oracle_prices_by_source")
    )
    by_source: dict[str, Any] = {}
    if isinstance(by_source_raw, dict):
        for raw_key, raw_point in by_source_raw.items():
            if not isinstance(raw_point, dict):
                continue
            s = str(raw_key).strip().lower()
            if not s:
                continue
            pt_updated = _safe_float(raw_point.get("updated_at_ms"), None)
            pt_age: float | None = _safe_float(raw_point.get("age_ms"), None)
            if pt_age is None and pt_updated is not None and pt_updated > 0:
                pt_age = max(0.0, float(now_ms) - float(pt_updated))
            by_source[s] = {
                "source": s,
                "price": _safe_float(raw_point.get("price"), 0.0),
                "updated_at_ms": pt_updated,
                "age_ms": pt_age,
            }

    # If we have a primary source but no by_source entry for it, synthesise one.
    if oracle_source and oracle_source not in by_source and oracle_price is not None:
        by_source[oracle_source] = {
            "source": oracle_source,
            "price": oracle_price,
            "updated_at_ms": oracle_updated_at_ms,
            "age_ms": age_ms,
        }

    # When the primary oracle_price is missing but we have by_source data, pick
    # the best available source to fill in the primary fields.
    if (oracle_price is None or oracle_source is None) and by_source:
        synthetic = {
            "oracle_prices_by_source": {
                s: {**p, "age_ms": p.get("age_ms") or 0.0}
                for s, p in by_source.items()
            }
        }
        best = pick_oracle_source(synthetic, now_ms=float(now_ms))
        if best:
            if oracle_price is None:
                oracle_price = _safe_float(best.get("price"), None)
            if oracle_source is None:
                oracle_source = str(best.get("source") or "").strip().lower() or None
            if age_ms is None:
                age_ms = _safe_float(best.get("age_ms"), None)
            if oracle_updated_at_ms is None:
                oracle_updated_at_ms = _safe_float(best.get("updated_at_ms"), None)

    has_price = oracle_price is not None and float(oracle_price) > 0.0
    has_timestamp = oracle_updated_at_ms is not None or age_ms is not None
    available = has_price
    fresh_available = has_price and age_ms is not None
    directional_available = available

    return {
        "price": oracle_price,
        "price_to_beat": price_to_beat,
        "source": oracle_source,
        "by_source": by_source,
        "age_ms": age_ms,
        "updated_at_ms": (int(oracle_updated_at_ms) if oracle_updated_at_ms is not None else None),
        "has_price": has_price,
        "has_timestamp": has_timestamp,
        "available": available,
        "fresh_available": fresh_available,
        "directional_available": directional_available,
        "availability_reasons": [],
        "freshness_state": "fresh" if fresh_available else "unknown",
        "freshness_reasons": [],
        "directional_state": "available" if directional_available else "unavailable",
        "directional_reasons": [],
    }
