"""Unit tests for the polymarket_trade_verifier helpers.

The verifier is the single authoritative writer of realized P&L. These
tests pin down the small pure-function helpers it relies on so any
future refactor that drops the invariant fails the suite loudly.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import TraderOrder
from services.polymarket_trade_verifier import _reconcile_status_with_pnl


def _row(status: str) -> TraderOrder:
    # Build an unattached TraderOrder; we only exercise the in-memory
    # status mutation, no DB session needed.
    order = TraderOrder()
    order.id = "test-order"
    order.status = status
    return order


def test_reconcile_flips_resolved_win_to_loss_when_pnl_negative():
    # The Athletics/Rangers case from production: status said win,
    # verifier matched on-chain trade and pnl came back -8.77.
    order = _row("resolved_win")
    _reconcile_status_with_pnl(order, -8.77)
    assert order.status == "resolved_loss"


def test_reconcile_flips_closed_loss_to_win_when_pnl_positive():
    order = _row("closed_loss")
    _reconcile_status_with_pnl(order, 2.50)
    assert order.status == "closed_win"


def test_reconcile_keeps_status_when_pnl_sign_already_matches():
    for status, pnl in [
        ("closed_win", 1.0),
        ("resolved_win", 5.0),
        ("closed_loss", -1.0),
        ("resolved_loss", -10.0),
    ]:
        order = _row(status)
        _reconcile_status_with_pnl(order, pnl)
        assert order.status == status


def test_reconcile_zero_pnl_leaves_status_alone():
    # Exact-zero PnL is rare but possible (full hedge, scratch trade).
    # The helper must not flip; the original label was the orchestrator's
    # best guess and there's no signal to overrule it from here.
    for status in ("closed_win", "resolved_win", "closed_loss", "resolved_loss"):
        order = _row(status)
        _reconcile_status_with_pnl(order, 0.0)
        assert order.status == status


def test_reconcile_ignores_non_terminal_statuses():
    # Statuses outside the terminal win/loss canon are not the verifier's
    # to flip — those rows are still in flight (e.g. "executed",
    # "open"). Touching them would cause status churn.
    for status in ("executed", "open", "cancelled", "failed", "pending", "placing"):
        order = _row(status)
        _reconcile_status_with_pnl(order, -5.0)
        assert order.status == status
        _reconcile_status_with_pnl(order, 5.0)
        assert order.status == status
