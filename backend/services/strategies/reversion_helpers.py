from __future__ import annotations

from typing import Any

from utils.converters import safe_float
from utils.signal_helpers import live_move


def market_move_pct(live_market: dict[str, Any] | None, payload: dict[str, Any], horizon: str) -> float | None:
    live_value = live_move(live_market or {}, horizon)
    if live_value is not None:
        return live_value
    return safe_float(
        payload.get(f"{horizon}_percent"),
        default=safe_float(payload.get(f"{horizon}_pct"), default=safe_float(payload.get(horizon))),
    )


def direction_opposes_impulse(direction: str, move_5m_pct: float | None, min_abs_move_5m: float) -> bool:
    if move_5m_pct is None:
        return False
    if direction == "buy_yes":
        return move_5m_pct <= -min_abs_move_5m
    if direction == "buy_no":
        return move_5m_pct >= min_abs_move_5m
    return False


def reversion_shape_ok(
    move_5m_pct: float | None,
    move_30m_pct: float | None,
    move_2h_pct: float | None,
    *,
    require_shape: bool,
    max_abs_move_2h: float,
) -> bool:
    if not require_shape:
        return True
    if move_5m_pct is None or move_30m_pct is None:
        return False
    if abs(move_5m_pct) < abs(move_30m_pct) * 0.55:
        return False
    if move_2h_pct is not None and abs(move_2h_pct) > max_abs_move_2h:
        return False
    return True
