from __future__ import annotations

import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.simulation.historical_data_provider import HistoricalDataProvider


def test_points_to_candles_aggregates_intrabar_range():
    base_ts = 1_700_000_000_000
    points = [
        {"t": base_ts, "p": 0.40, "v": 10.0},
        {"t": base_ts + 10_000, "p": 0.55, "v": 15.0},
        {"t": base_ts + 20_000, "p": 0.35, "v": 12.0},
        {"t": base_ts + 61_000, "p": 0.50, "v": 6.0},
    ]
    candles = HistoricalDataProvider._points_to_candles(points, timeframe_seconds=60)

    assert len(candles) == 2
    assert candles[0]["t"] == (base_ts // 60_000) * 60_000
    assert candles[0]["open"] == 0.40
    assert candles[0]["high"] == 0.55
    assert candles[0]["low"] == 0.35
    assert candles[0]["close"] == 0.35
    assert candles[0]["volume"] == 37.0
    assert candles[1]["t"] == ((base_ts + 61_000) // 60_000) * 60_000
    assert candles[1]["open"] == 0.50
    assert candles[1]["high"] == 0.50
    assert candles[1]["low"] == 0.50
    assert candles[1]["close"] == 0.50
