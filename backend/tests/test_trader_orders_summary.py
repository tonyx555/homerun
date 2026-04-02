import sys
from pathlib import Path
from types import SimpleNamespace

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.trader_orchestrator_state import _grouped_trade_counts


OPEN_STATUSES = ("submitted", "executed", "open")
RESOLVED_STATUSES = ("resolved", "resolved_win", "resolved_loss", "closed_win", "closed_loss", "win", "loss")
FAILED_STATUSES = ("failed", "rejected", "error", "cancelled")


def _signal(signal_id: str, leg_count: int) -> SimpleNamespace:
    return SimpleNamespace(
        id=signal_id,
        payload_json={
            "execution_plan": {
                "legs": [
                    {"leg_id": f"leg_{index + 1}"}
                    for index in range(leg_count)
                ]
            }
        },
    )


def _order(
    *,
    order_id: str,
    trader_id: str,
    signal_id: str | None,
    status: str,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=order_id,
        trader_id=trader_id,
        signal_id=signal_id,
        status=status,
        payload_json={},
    )


def test_grouped_trade_counts_collapse_multi_leg_bundles_and_flag_partial_opens():
    rows = [
        _order(order_id="o1", trader_id="t1", signal_id="bundle_partial", status="executed"),
        _order(order_id="o2", trader_id="t1", signal_id="bundle_partial", status="open"),
        _order(order_id="o3", trader_id="t1", signal_id="bundle_filled", status="executed"),
        _order(order_id="o4", trader_id="t1", signal_id="bundle_filled", status="executed"),
        _order(order_id="o5", trader_id="t1", signal_id="single_open", status="open"),
        _order(order_id="o6", trader_id="t1", signal_id="single_resolved", status="resolved_win"),
    ]
    signals = {
        "bundle_partial": _signal("bundle_partial", 2),
        "bundle_filled": _signal("bundle_filled", 2),
        "single_open": _signal("single_open", 1),
        "single_resolved": _signal("single_resolved", 1),
    }

    totals, by_trader = _grouped_trade_counts(
        rows,
        signals,
        open_statuses=OPEN_STATUSES,
        resolved_statuses=RESOLVED_STATUSES,
        failed_statuses=FAILED_STATUSES,
    )

    assert totals == {
        "total_trades": 4,
        "open_trades": 3,
        "resolved_trades": 1,
        "failed_trades": 0,
        "partial_open_bundles": 1,
    }
    assert by_trader["t1"] == {
        "trade_count": 4,
        "open_trades": 3,
        "resolved_trades": 1,
        "failed_trades": 0,
        "partial_open_bundles": 1,
    }


def test_grouped_trade_counts_keep_single_orders_separate_without_bundle_signal():
    rows = [
        _order(order_id="a", trader_id="t1", signal_id=None, status="open"),
        _order(order_id="b", trader_id="t1", signal_id=None, status="failed"),
        _order(order_id="c", trader_id="t2", signal_id="bundle_resolved", status="resolved_win"),
        _order(order_id="d", trader_id="t2", signal_id="bundle_resolved", status="resolved_loss"),
    ]
    signals = {
        "bundle_resolved": _signal("bundle_resolved", 2),
    }

    totals, by_trader = _grouped_trade_counts(
        rows,
        signals,
        open_statuses=OPEN_STATUSES,
        resolved_statuses=RESOLVED_STATUSES,
        failed_statuses=FAILED_STATUSES,
    )

    assert totals == {
        "total_trades": 3,
        "open_trades": 1,
        "resolved_trades": 1,
        "failed_trades": 1,
        "partial_open_bundles": 0,
    }
    assert by_trader["t1"] == {
        "trade_count": 2,
        "open_trades": 1,
        "resolved_trades": 0,
        "failed_trades": 1,
        "partial_open_bundles": 0,
    }
    assert by_trader["t2"] == {
        "trade_count": 1,
        "open_trades": 0,
        "resolved_trades": 1,
        "failed_trades": 0,
        "partial_open_bundles": 0,
    }
