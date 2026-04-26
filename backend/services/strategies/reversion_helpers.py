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


def direction_aligns_impulse(direction: str, move_5m_pct: float | None, min_abs_move_5m: float) -> bool:
    """Inverse of direction_opposes_impulse — used by momentum/breakout strategies."""
    if move_5m_pct is None:
        return False
    if direction == "buy_yes":
        return move_5m_pct >= min_abs_move_5m
    if direction == "buy_no":
        return move_5m_pct <= -min_abs_move_5m
    return False


def breakout_shape_ok(
    move_5m_pct: float | None,
    move_30m_pct: float | None,
    move_2h_pct: float | None,
    *,
    require_shape: bool,
    max_abs_move_2h: float,
    min_5m_share_of_30m: float = 0.45,
) -> bool:
    """Check whether a 5-minute impulse looks like a fresh breakout aligned
    with the recent trend, rather than a reversal spike or a fully-extended
    trend you'd be chasing.

    Three conditions when the relevant horizon is available:
      * 5m and 30m agree in direction
      * 5m carries at least ``min_5m_share_of_30m`` of the 30m move
        (fresh impulse, not slow grind continuation)
      * |2h move| <= ``max_abs_move_2h`` percent
        (caps overextended trends where most of the run is already behind us)

    Missing 30m / 2h horizons are treated as "no contradiction" — same
    permissive policy as ``reversion_shape_ok``.
    """
    if not require_shape:
        return True
    if move_5m_pct is None:
        return False
    if move_30m_pct is not None and move_30m_pct != 0.0:
        if (move_5m_pct > 0) != (move_30m_pct > 0):
            return False
        if abs(move_5m_pct) < abs(move_30m_pct) * min_5m_share_of_30m:
            return False
    if move_2h_pct is not None and abs(move_2h_pct) > max_abs_move_2h:
        return False
    return True


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
