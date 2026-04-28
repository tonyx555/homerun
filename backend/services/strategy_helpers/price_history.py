from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PriceSnapshot:
    """Single (yes, no) price observation with a wall-clock timestamp."""

    yes: float
    no: float
    ts: float  # epoch seconds


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

    window_seconds: float
    _snapshots: deque = field(default_factory=deque, init=False, repr=False)

    def record(self, yes_price: float, no_price: float) -> None:
        now = time.monotonic()
        self._snapshots.append(PriceSnapshot(yes=float(yes_price), no=float(no_price), ts=now))
        cutoff = now - self.window_seconds
        while self._snapshots and self._snapshots[0].ts < cutoff:
            self._snapshots.popleft()

    @property
    def has_data(self) -> bool:
        return bool(self._snapshots)

    @property
    def snapshots(self) -> list[PriceSnapshot]:
        return list(self._snapshots)

    def recent_volatility(self, *, side: str = "yes") -> Optional[float]:
        """Return std-dev of the chosen price side over the window, or None if < 2 entries."""
        if len(self._snapshots) < 2:
            return None
        prices = [s.yes if side == "yes" else s.no for s in self._snapshots]
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / len(prices)
        return variance**0.5
