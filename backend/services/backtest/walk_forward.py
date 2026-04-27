"""Walk-forward validation harness.

Two split modes:

* **Rolling** — fixed-size in-sample window slides forward, OOS window
  follows. Useful when the regime is locally stationary.
* **Anchored** — in-sample starts at t0 and grows; OOS window follows
  the latest in-sample edge. Useful when older data is still informative.

Each fold reports its own ``BacktestMetrics`` (point estimate + bootstrap
CI). The harness returns the OOS metric distribution so callers can
report cross-fold variance — a stronger guard against overfit than a
single train/test split.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from services.backtest.metrics import BacktestMetrics, MetricCI


@dataclass
class WalkForwardConfig:
    mode: str = "rolling"  # "rolling" | "anchored"
    n_folds: int = 5
    train_ratio: float = 0.7  # only used for anchored mode
    embargo_seconds: float = 0.0  # gap between train and test windows


@dataclass
class WalkForwardWindow:
    """One walk-forward fold's in-sample / out-of-sample window."""

    fold_index: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime


@dataclass
class WalkForwardResult:
    config: WalkForwardConfig
    windows: list[WalkForwardWindow] = field(default_factory=list)
    train_metrics: list[BacktestMetrics] = field(default_factory=list)
    test_metrics: list[BacktestMetrics] = field(default_factory=list)

    def aggregate_test_metric(self, attr: str) -> MetricCI:
        """Aggregate a test-fold metric across folds.

        Returns mean of point estimates with min/max as the band — a
        simpler-than-bootstrap summary of cross-fold variance. Use this
        for ``sharpe``, ``sortino``, ``calmar``, ``hit_rate``, etc. that
        are themselves ``MetricCI`` on each fold.
        """
        values: list[float] = []
        for m in self.test_metrics:
            v = getattr(m, attr, None)
            if isinstance(v, MetricCI):
                values.append(v.value)
            elif isinstance(v, (int, float)):
                values.append(float(v))
        if not values:
            return MetricCI(0.0)
        mean = sum(values) / len(values)
        return MetricCI(mean, min(values), max(values))


def walk_forward_split(
    *,
    start: datetime,
    end: datetime,
    config: Optional[WalkForwardConfig] = None,
) -> list[WalkForwardWindow]:
    """Compute the in-sample/out-of-sample windows for a backtest period.

    Both modes guarantee:
      * No look-ahead: train_end < test_start
      * Optional embargo: test_start = train_end + embargo
      * Test windows are roughly equal-sized
      * Folds are returned in chronological order
    """
    cfg = config or WalkForwardConfig()
    if end <= start:
        raise ValueError(f"invalid range: {start} >= {end}")
    n = max(1, int(cfg.n_folds))
    embargo = timedelta(seconds=max(0.0, float(cfg.embargo_seconds)))

    if cfg.mode == "anchored":
        return _anchored_windows(start, end, n, cfg.train_ratio, embargo)
    return _rolling_windows(start, end, n, embargo)


def _rolling_windows(
    start: datetime,
    end: datetime,
    n_folds: int,
    embargo: timedelta,
) -> list[WalkForwardWindow]:
    """Equal-width rolling windows. Each fold is (train, test) of equal
    size, sliding forward by the test window each fold.
    """
    total = end - start
    fold_w = total / (n_folds + 1)  # +1 so first fold has equal train+test
    windows: list[WalkForwardWindow] = []
    for i in range(n_folds):
        train_start = start + fold_w * i
        train_end = train_start + fold_w
        test_start = train_end + embargo
        test_end = test_start + fold_w
        if test_end > end:
            test_end = end
        if test_start >= test_end:
            continue
        windows.append(
            WalkForwardWindow(
                fold_index=i,
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
        )
    return windows


def _anchored_windows(
    start: datetime,
    end: datetime,
    n_folds: int,
    train_ratio: float,
    embargo: timedelta,
) -> list[WalkForwardWindow]:
    """Anchored windows: train always starts at ``start`` and grows; the
    test window follows the most recent train edge.
    """
    total = end - start
    train_ratio = max(0.1, min(0.95, float(train_ratio)))
    # First fold: train = train_ratio * total, test starts after embargo
    # Each subsequent fold extends train_end by (test_w_increment) and
    # tests on the next slice.
    initial_train = total * train_ratio
    remaining = total - initial_train - embargo
    if remaining.total_seconds() <= 0:
        return []
    test_w = remaining / n_folds
    windows: list[WalkForwardWindow] = []
    for i in range(n_folds):
        train_start = start
        train_end = start + initial_train + test_w * i
        test_start = train_end + embargo
        test_end = test_start + test_w
        if test_end > end:
            test_end = end
        if test_start >= test_end:
            continue
        windows.append(
            WalkForwardWindow(
                fold_index=i,
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
        )
    return windows


__all__ = [
    "WalkForwardConfig",
    "WalkForwardWindow",
    "WalkForwardResult",
    "walk_forward_split",
]
