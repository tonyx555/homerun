from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from models import Market
from utils.converters import clamp, safe_float


def parse_datetime_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts <= 0:
            return None
        if ts > 1_000_000_000_000:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None
    text = str(value or "").strip()
    if not text:
        return None
    try:
        numeric = float(text)
    except Exception:
        numeric = None
    if numeric is not None and numeric > 0:
        return parse_datetime_utc(numeric)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_binary_market_from_row(row: dict[str, Any]) -> Market | None:
    return build_binary_crypto_market(row)


def build_binary_crypto_market(row: dict[str, Any]) -> Market | None:
    market_id = str(row.get("condition_id") or row.get("id") or row.get("slug") or "").strip()
    if not market_id:
        return None

    up_price = safe_float(row.get("up_price"), None)
    down_price = safe_float(row.get("down_price"), None)
    if up_price is None or down_price is None:
        return None

    end_date = parse_datetime_utc(row.get("end_time"))
    token_ids = [
        str(token).strip()
        for token in list(row.get("clob_token_ids") or [])
        if str(token).strip() and len(str(token).strip()) > 20
    ]

    return Market(
        id=market_id,
        condition_id=market_id,
        question=str(row.get("question") or row.get("slug") or market_id),
        slug=str(row.get("slug") or market_id),
        outcome_prices=[float(up_price), float(down_price)],
        liquidity=max(0.0, float(safe_float(row.get("liquidity"), 0.0) or 0.0)),
        end_date=end_date,
        platform="polymarket",
        clob_token_ids=token_ids,
    )


def seconds_left_from_row(row: dict[str, Any], *, fallback_seconds: float | None = None) -> float | None:
    seconds_left = safe_float(row.get("seconds_left"), None)
    if seconds_left is not None and seconds_left >= 0.0:
        return float(seconds_left)
    end_time = parse_datetime_utc(row.get("end_time"))
    if end_time is None:
        return fallback_seconds
    return max(0.0, (end_time - datetime.now(timezone.utc)).total_seconds())


def spread_pct_from_row(row: dict[str, Any]) -> float:
    spread = safe_float(row.get("spread"), None)
    if spread is None or spread < 0.0:
        best_bid = safe_float(row.get("best_bid"), None)
        best_ask = safe_float(row.get("best_ask"), None)
        if best_bid is not None and best_ask is not None and best_ask >= best_bid:
            spread = best_ask - best_bid
    if spread is None:
        spread = 0.0
    return clamp(float(spread), 0.0, 1.0)


def taker_fee_pct(entry_price: float) -> float:
    price = clamp(float(entry_price), 0.0001, 0.9999)
    return 0.25 * ((price * (1.0 - price)) ** 2)


def bounded_sigmoid(z: float) -> float:
    bounded = clamp(float(z), -60.0, 60.0)
    return 1.0 / (1.0 + math.exp(-bounded))


def normalize_ratio(value: Any) -> float | None:
    parsed = safe_float(value, None)
    if parsed is None:
        return None
    ratio = float(parsed)
    if ratio > 1.0 and ratio <= 100.0:
        ratio /= 100.0
    return max(0.0, min(1.0, ratio))


def normalize_signed_ratio(value: Any) -> float | None:
    parsed = safe_float(value, None)
    if parsed is None:
        return None
    ratio = float(parsed)
    if abs(ratio) > 1.0 and abs(ratio) <= 100.0:
        ratio /= 100.0
    return max(-1.0, min(1.0, ratio))


def normalize_timeframe(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"5m", "5min", "5-minute", "5minutes", "five", "fivemin", "fiveminute"}:
        return "5min"
    if text in {"15m", "15min", "15-minute", "15minutes", "fifteen", "fifteenmin"}:
        return "15min"
    if text in {"1h", "1hr", "1hour", "60m"}:
        return "1hr"
    if text in {"4h", "4hr", "4hour", "240m"}:
        return "4hr"
    return text


def timeframe_seconds(value: Any) -> int:
    timeframe = normalize_timeframe(value)
    if timeframe == "5min":
        return 300
    if timeframe == "15min":
        return 900
    if timeframe == "1hr":
        return 3600
    if timeframe == "4hr":
        return 14400
    return 300


def history_cancel_peak(history_tail: Any) -> float | None:
    if not isinstance(history_tail, list):
        return None
    peak: float | None = None
    for row in history_tail:
        if not isinstance(row, dict):
            continue
        candidate = normalize_ratio(
            row.get("cancel_rate_30s")
            if row.get("cancel_rate_30s") is not None
            else row.get("maker_cancel_rate_30s")
        )
        if candidate is None:
            continue
        peak = candidate if peak is None else max(peak, candidate)
    return peak


def market_ml_probability_yes(row: dict[str, Any]) -> float | None:
    if not isinstance(row, dict):
        return None

    machine_learning = row.get("machine_learning")
    if not isinstance(machine_learning, dict):
        return None

    prediction = machine_learning.get("prediction")
    if isinstance(prediction, dict):
        probability_yes = safe_float(prediction.get("probability_yes"), None)
        if probability_yes is not None:
            return clamp(float(probability_yes), 0.03, 0.97)

    probability_yes = safe_float(machine_learning.get("probability_yes"), None)
    if probability_yes is not None:
        return clamp(float(probability_yes), 0.03, 0.97)
    return None
