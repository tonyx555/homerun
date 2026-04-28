from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

DEFAULT_WINDOW_SECONDS = 300.0


@dataclass
class PriceSnapshot:
    """Single (yes, no) price observation with a monotonic timestamp."""

    yes_price: float
    no_price: float
    ts: float  # time.monotonic() seconds


@dataclass
class MarketPriceHistory:
    """Window-evicting deque of PriceSnapshot entries.

    Entries older than ``window_seconds`` are evicted on each ``record()``
    call so memory stays bounded.

    Usage::

        history = MarketPriceHistory(window_seconds=60)
        history.record(yes_price=0.55, no_price=0.45)
        if history.has_data:
            vol = history.recent_volatility()
    """

    window_seconds: float = DEFAULT_WINDOW_SECONDS
    _snapshots: deque = field(default_factory=deque, init=False, repr=False)

    def record(self, yes_price: float, no_price: float) -> None:
        now = time.monotonic()
        self._snapshots.append(PriceSnapshot(yes_price=float(yes_price), no_price=float(no_price), ts=now))
        cutoff = now - self.window_seconds
        while self._snapshots and self._snapshots[0].ts < cutoff:
            self._snapshots.popleft()

    @property
    def has_data(self) -> bool:
        return len(self._snapshots) >= 2

    @property
    def snapshots(self) -> list[PriceSnapshot]:
        return list(self._snapshots)

    def max_drop_yes(self) -> float:
        if not self._snapshots:
            return 0.0
        prices = [s.yes_price for s in self._snapshots]
        return max(0.0, max(prices) - prices[-1])

    def max_drop_no(self) -> float:
        if not self._snapshots:
            return 0.0
        prices = [s.no_price for s in self._snapshots]
        return max(0.0, max(prices) - prices[-1])

    def recent_volatility(self, *, side: str = "yes") -> float:
        if len(self._snapshots) < 2:
            return 0.0
        prices = [s.yes_price if side == "yes" else s.no_price for s in self._snapshots]
        return max(prices) - min(prices)
