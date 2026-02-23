from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.param_optimizer import ParameterOptimizer


@pytest.mark.asyncio
async def test_run_backtest_returns_run_manifest_fields(monkeypatch):
    optimizer = ParameterOptimizer()
    now = datetime.now(timezone.utc).isoformat()

    async def _load_opportunity_history():
        return [
            {
                "id": "opp-1",
                "stable_id": "stable-1",
                "strategy_type": "basic",
                "total_cost": 100.0,
                "expected_roi": 8.0,
                "risk_score": 0.2,
                "was_profitable": True,
                "actual_roi": 0.08,
                "detected_at": now,
                "positions_data": [{"liquidity": 10_000.0}],
            },
            {
                "id": "opp-2",
                "stable_id": "stable-2",
                "strategy_type": "basic",
                "total_cost": 100.0,
                "expected_roi": 6.0,
                "risk_score": 0.4,
                "was_profitable": False,
                "actual_roi": -0.05,
                "detected_at": now,
                "positions_data": [{"liquidity": 12_000.0}],
            },
        ]

    monkeypatch.setattr(optimizer, "_load_opportunity_history", _load_opportunity_history)

    result = await optimizer.run_backtest(
        params={
            "min_roi_percent": 1.0,
            "max_risk_score": 0.9,
            "min_profit_threshold": 0.01,
            "run_seed": "seed-1234",
        }
    )

    manifest = result.get("run_manifest")
    assert isinstance(manifest, dict)
    assert manifest["run_seed"] == "seed-1234"
    assert len(manifest["dataset_hash"]) == 64
    assert len(manifest["config_hash"]) == 64
    assert len(manifest["code_sha"]) == 64
