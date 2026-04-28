"""Stream-agnostic rolling price window for strategy use.

Replaces the prior :class:`MarketPriceHistory` (which hardcoded yes/no
sides) with a single-stream window that makes no outcome assumptions.
Use one ``PriceWindow`` per price stream you care about — typically
``dict[token_id, PriceWindow]`` for a market's outcomes (so a 3+ outcome
market gets one window per token), or a single ``PriceWindow`` for an
external feed like a crypto spot price or Chainlink oracle.

Exposed via :class:`services.strategy_sdk.StrategySDK` as
``StrategySDK.PriceWindow`` so any strategy can instantiate one without
a direct import.

Typical use::

    from services.strategy_sdk import StrategySDK

    windows: dict[str, StrategySDK.PriceWindow] = {}

    def on_tick(token_id, price, ts_ms):
        w = windows.setdefault(token_id, StrategySDK.PriceWindow(window_seconds=60))
        w.record(price, ts_ms)
        if w.has_data and w.realized_volatility_bps_per_sec() > 50:
            ...

The window is rolling — observations older than ``window_seconds`` are
evicted on every ``record()`` call. Methods that need at least two
observations return None or 0.0 when the window is empty/sparse.
"""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


DEFAULT_WINDOW_SECONDS = 300.0


@dataclass
class PriceWindow:
    """Rolling per-stream price observations.

    ``samples`` holds ``(ts_ms, price)`` tuples in append order. Old
    entries are evicted when ``record()`` runs.
    """

    window_seconds: float = DEFAULT_WINDOW_SECONDS
    samples: deque[tuple[int, float]] = field(default_factory=deque)

    def record(self, price: float, ts_ms: Optional[int] = None) -> None:
        """Append a price observation and evict any older than the window.

        ``ts_ms`` defaults to wall-clock now (``time.time() * 1000``) so
        callers with a server-supplied timestamp can override.
        """
        ts = int(ts_ms) if ts_ms is not None else int(time.time() * 1000)
        self.samples.append((ts, float(price)))
        self._evict(ts)

    def _evict(self, now_ms: int) -> None:
        cutoff = now_ms - int(self.window_seconds * 1000)
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()

    def latest(self) -> Optional[tuple[int, float]]:
        """Return the most recent ``(ts_ms, price)``, or None when empty."""
        if not self.samples:
            return None
        return self.samples[-1]

    def at_or_before(self, ts_ms: int) -> Optional[tuple[int, float]]:
        """Return the most recent observation with ``timestamp <= ts_ms``.

        Returns None when no observation in the window is at-or-before
        the requested time. Walks samples in reverse order so amortized
        cost is O(1) for recent timestamps.
        """
        for sample_ts, sample_price in reversed(self.samples):
            if sample_ts <= ts_ms:
                return (sample_ts, sample_price)
        return None

    def log_return(self, seconds_ago: float) -> Optional[float]:
        """Return ``log(latest / prior)`` over a ``seconds_ago`` window.

        Picks the prior observation via :meth:`at_or_before`. Returns
        None when the window is empty, ``seconds_ago`` is non-positive,
        or there's no observation old enough to anchor the return.
        """
        if not self.samples or seconds_ago <= 0:
            return None
        latest_ts, latest_price = self.samples[-1]
        prior = self.at_or_before(latest_ts - int(seconds_ago * 1000))
        if prior is None:
            return None
        prior_price = prior[1]
        if prior_price <= 0 or latest_price <= 0:
            return None
        return math.log(latest_price / prior_price)

    def stddev(self) -> float:
        """Return the standard deviation of recent prices.

        Returns 0.0 when the window has fewer than 2 samples.
        """
        if len(self.samples) < 2:
            return 0.0
        prices = [p for _, p in self.samples]
        mean = sum(prices) / len(prices)
        var = sum((p - mean) ** 2 for p in prices) / len(prices)
        return math.sqrt(var)

    def realized_volatility_bps_per_sec(self, sample_period_s: float = 1.0) -> float:
        """Realized volatility, normalized to bps per ``sample_period_s``.

        For each adjacent pair of samples, computes the log return scaled
        to a standard ``sample_period_s`` window (so heterogeneous tick
        spacing produces comparable returns), then returns the std-dev
        of those scaled returns in basis points (1 bp = 0.0001).

        Returns 0.0 when the window has fewer than 2 samples or
        ``sample_period_s`` is non-positive.
        """
        if len(self.samples) < 2 or sample_period_s <= 0:
            return 0.0
        scaled_returns: list[float] = []
        prev_ts, prev_p = self.samples[0]
        for ts, p in list(self.samples)[1:]:
            if prev_p > 0 and p > 0:
                dt_s = max(0.001, (ts - prev_ts) / 1000.0)
                r = math.log(p / prev_p)
                scaled = r * math.sqrt(sample_period_s / dt_s)
                scaled_returns.append(scaled)
            prev_ts, prev_p = ts, p
        if not scaled_returns:
            return 0.0
        mean = sum(scaled_returns) / len(scaled_returns)
        var = sum((r - mean) ** 2 for r in scaled_returns) / len(scaled_returns)
        return math.sqrt(var) * 10_000.0

    def distance_bps(self, reference_price: float) -> Optional[float]:
        """Distance from the latest price to ``reference_price`` in bps.

        Positive when latest is above the reference, negative below.
        Returns None when the window is empty or ``reference_price`` is
        non-positive.
        """
        if not self.samples or reference_price <= 0:
            return None
        latest_price = self.samples[-1][1]
        return ((latest_price - reference_price) / reference_price) * 10_000.0

    @property
    def has_data(self) -> bool:
        """True when the window has at least 2 samples (statistics meaningful)."""
        return len(self.samples) >= 2

    def __len__(self) -> int:
        return len(self.samples)
