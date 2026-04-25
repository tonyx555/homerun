import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from workers.trader_reconciliation_worker import _clamped_live_lifecycle_params


def test_reconciliation_lifecycle_params_include_runtime_live_risk_clamps():
    params = _clamped_live_lifecycle_params(
        {
            "risk_limits": {
                "max_trade_notional_usd": 20.0,
                "max_position_notional_usd": 20.0,
            },
            "source_configs": [
                {
                    "strategy_params": {
                        "take_profit_pct": 8.0,
                    },
                },
            ],
        },
        {
            "global_runtime": {
                "live_risk_clamps_explicit": True,
                "live_risk_clamps": {
                    "max_trade_notional_usd_cap": 10.0,
                    "max_per_market_exposure_usd_cap": 10.0,
                },
            },
        },
    )

    assert params["take_profit_pct"] == 8.0
    assert params["max_trade_notional_usd"] == 10.0
    assert params["max_position_notional_usd"] == 10.0
    assert params["max_per_market_exposure_usd"] == 10.0
