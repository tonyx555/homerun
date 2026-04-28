"""Idempotent milestone tracker for fixed-cycle recurring markets.

NOTE: this helper is only meaningful for markets that recur on a known
cadence (e.g. Polymarket's 5-minute / 15-minute / 1-hour / 4-hour
crypto over-under markets). It is NOT applicable to one-shot markets
like elections or sports — those should use
:meth:`services.strategy_sdk.StrategySDK.time_to_resolution` and reason
about their own lifecycle directly.

A ``CycleTracker`` is parameterized by the cycle length and the
milestones (in seconds since cycle start) you want to fire on. It tracks
which milestones have already been fired *for the current cycle* so a
strategy reacting on every WS event won't emit duplicate signals at the
same milestone.

Cycles are identified by the market's resolution timestamp. When the
market resolves and a new one is created with a later ``end_date``, the
tracker auto-resets its fired-milestones set on the next ``crossed()``
call. One ``CycleTracker`` instance per market — strategies tracking N
markets keep N trackers, typically in a ``dict[market_id, CycleTracker]``.

Usage::

    tracker = CycleTracker(
        cycle_seconds=300.0,
        milestones_s=(0.0, 150.0),   # cycle open + midcycle
    )

    # Inside on_event handler — end_ts_ms is the market's resolution
    # timestamp in epoch millis:
    crossed = tracker.crossed(end_ts_ms=market_end_ms)
    if 0.0 in crossed:
        ...  # cycle just opened
    if 150.0 in crossed:
        ...  # midcycle mark just hit
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class CycleTracker:
    """Tracks which milestones have fired for the current cycle.

    Milestones are expressed in seconds-since-cycle-start. The cycle's
    start is derived from ``end_ts_ms - cycle_seconds * 1000`` on every
    ``crossed()`` / ``phase()`` call, so the tracker doesn't need to be
    notified of cycle rollovers separately — passing a different
    ``end_ts_ms`` resets the fired set automatically.
    """

    cycle_seconds: float
    milestones_s: tuple[float, ...]
    _fired: set[float] = field(default_factory=set, init=False, repr=False)
    _current_cid: Optional[int] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cycle_seconds = float(self.cycle_seconds)
        if self.cycle_seconds <= 0.0:
            raise ValueError("cycle_seconds must be > 0")
        cleaned: list[float] = []
        for m in self.milestones_s:
            ms = float(m)
            if not (0.0 <= ms <= self.cycle_seconds):
                raise ValueError(
                    f"milestone {ms!r} outside [0, {self.cycle_seconds}]"
                )
            cleaned.append(ms)
        self.milestones_s = tuple(sorted(cleaned))

    def crossed(
        self,
        end_ts_ms: int,
        now_ms: Optional[int] = None,
    ) -> list[float]:
        """Return milestones that became true since the previous call.

        Idempotent: a given milestone fires at most once per cycle.
        Cycles are identified by ``end_ts_ms``; passing a new value
        resets the fired set automatically (so the strategy doesn't
        need to call :meth:`reset` on cycle rollover).

        Args:
            end_ts_ms: The cycle's resolution timestamp (epoch millis).
            now_ms: Wall-clock millis. Defaults to ``time.time() * 1000``.

        Returns:
            Sorted list of newly-crossed milestones (seconds-since-cycle-start).
            Empty list when nothing new has crossed (or the cycle hasn't
            started yet).
        """
        cid = int(end_ts_ms)
        if self._current_cid != cid:
            self._current_cid = cid
            self._fired = set()

        now = int(now_ms) if now_ms is not None else int(time.time() * 1000)
        cycle_start_ms = cid - int(self.cycle_seconds * 1000)
        elapsed_s = (now - cycle_start_ms) / 1000.0
        if elapsed_s < 0.0:
            return []

        out: list[float] = []
        for m in self.milestones_s:
            if m in self._fired:
                continue
            if elapsed_s >= m:
                self._fired.add(m)
                out.append(m)
        return out

    def phase(
        self,
        end_ts_ms: int,
        now_ms: Optional[int] = None,
    ) -> dict:
        """Return where in the cycle we are.

        Returns:
            ``{"elapsed_s": float, "remaining_s": float, "fraction": float}``.
            ``elapsed_s`` is clamped to >= 0 (returns 0 when the cycle
            hasn't started). ``remaining_s`` is clamped to >= 0.
            ``fraction`` is clamped to ``[0.0, 1.0]``.
        """
        now = int(now_ms) if now_ms is not None else int(time.time() * 1000)
        cycle_start_ms = int(end_ts_ms) - int(self.cycle_seconds * 1000)
        elapsed_s = max(0.0, (now - cycle_start_ms) / 1000.0)
        remaining_s = max(0.0, (int(end_ts_ms) - now) / 1000.0)
        fraction = min(1.0, max(0.0, elapsed_s / self.cycle_seconds))
        return {
            "elapsed_s": elapsed_s,
            "remaining_s": remaining_s,
            "fraction": fraction,
        }

    def reset(self) -> None:
        """Clear all fired-milestone state. Useful in tests."""
        self._fired.clear()
        self._current_cid = None
