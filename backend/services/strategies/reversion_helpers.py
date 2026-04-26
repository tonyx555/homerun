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
    """Check whether the 5-minute impulse looks like a spike that's
    likely to revert (rather than continuation of a longer-term trend).

    The 30m/2h checks are *contradiction* checks — they fail the shape
    only when present-and-disagreeing with the spike hypothesis.  A
    missing 30m or 2h reading means we have no contradictory evidence
    (oracle history not yet 30/120 minutes deep, or the asset's motion
    summary isn't yet populated for those horizons) so we let the
    signal through.  The earlier behaviour — bailing out the moment
    ``move_30m_pct is None`` — silenced this strategy whenever the
    oracle hadn't yet accumulated 30 minutes of history (e.g. for the
    first half-hour after worker restart, or any asset whose motion
    summary hadn't filled).
    """
    if not require_shape:
        return True
    if move_5m_pct is None:
        return False
    if move_30m_pct is not None:
        # 5m impulse must dominate the 30m trend — otherwise it's
        # continuation, not a spike to revert.
        if abs(move_5m_pct) < abs(move_30m_pct) * 0.55:
            return False
    if move_2h_pct is not None and abs(move_2h_pct) > max_abs_move_2h:
        return False
    return True
