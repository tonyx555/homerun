from __future__ import annotations

import sys
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.trader_orchestrator import position_lifecycle


class _ExitDecision:
    def __init__(self, action: str, payload: dict | None = None) -> None:
        self.action = action
        self.payload = dict(payload or {})


def test_rapid_close_trigger_detects_stop_loss_text():
    assert position_lifecycle._is_rapid_close_trigger("strategy:Stop loss hit (-6.8% <= -5%)") is True


def test_rapid_close_trigger_detects_executable_notional_guard_text():
    assert (
        position_lifecycle._is_rapid_close_trigger(
            "strategy:Executable-notional guard (headroom emergency) (headroom=0.34x <= 1.35x)"
        )
        is True
    )


def test_pending_exit_fill_threshold_relaxes_for_stop_loss():
    stop_loss_threshold = position_lifecycle._pending_exit_fill_threshold(
        {
            "close_trigger": "strategy:Stop loss hit (-9.6% <= -5%)",
            "exit_size": 60.0,
            "retry_count": 0,
        }
    )
    neutral_threshold = position_lifecycle._pending_exit_fill_threshold(
        {
            "close_trigger": "strategy:Take profit hit (9.1% >= 8%)",
            "exit_size": 60.0,
            "retry_count": 0,
        }
    )

    assert stop_loss_threshold == 0.92
    assert neutral_threshold > stop_loss_threshold


def test_strategy_hold_blocks_default_exit_when_flagged():
    decision = _ExitDecision("hold", {"skip_default_exit": True})
    assert position_lifecycle._strategy_hold_blocks_default_exit(decision) is True


def test_strategy_hold_does_not_block_without_flag():
    decision = _ExitDecision("hold", {})
    assert position_lifecycle._strategy_hold_blocks_default_exit(decision) is False


def test_strategy_close_never_blocks_default_exit():
    decision = _ExitDecision("close", {"skip_default_exit": True})
    assert position_lifecycle._strategy_hold_blocks_default_exit(decision) is False


def test_failed_exit_retry_delay_expands_for_vpn_proxy_errors():
    delay = position_lifecycle._failed_exit_retry_delay_seconds(
        "VPN check failed: Trading proxy unreachable: Invalid username/password"
    )
    assert delay == 90


def test_pending_exit_verified_terminal_fill_requires_positive_fill_evidence():
    assert position_lifecycle._pending_exit_has_verified_terminal_fill(
        {
            "provider_status": "filled",
            "filled_size": 3.0,
        }
    ) is True
    assert position_lifecycle._pending_exit_has_verified_terminal_fill(
        {
            "provider_status": "filled",
            "filled_size": 0.0,
            "average_fill_price": 0.0,
        }
    ) is False


def test_pending_exit_verified_terminal_fill_rejects_cancelled_status_without_trade():
    assert position_lifecycle._pending_exit_has_verified_terminal_fill(
        {
            "provider_status": "cancelled",
            "filled_size": 4.0,
            "average_fill_price": 0.41,
        }
    ) is False


def test_aggressive_exit_price_decrement_progression():
    """Walk-down progression: original on first attempt, gradually
    larger steps once the venue keeps rejecting, capped at 10 ¢."""
    f = position_lifecycle._aggressive_exit_price_decrement
    assert f(0) == pytest.approx(0.0)
    assert f(1) == pytest.approx(0.01)
    assert f(2) == pytest.approx(0.02)
    assert f(3) == pytest.approx(0.05)
    assert f(4) == pytest.approx(0.05)
    assert f(5) == pytest.approx(0.06)
    assert f(6) == pytest.approx(0.07)
    assert f(8) == pytest.approx(0.09)
    # Cap at 0.10 — never give away more than 10 ¢ on a single retry.
    assert f(9) == pytest.approx(0.10)
    assert f(20) == pytest.approx(0.10)
    assert f(100) == pytest.approx(0.10)


def test_aggressive_exit_price_decrement_no_walk_for_zero_or_negative():
    """The first attempt at the configured close_price gets the
    original price — we only walk down on confirmed rejections."""
    assert position_lifecycle._aggressive_exit_price_decrement(0) == 0.0
    assert position_lifecycle._aggressive_exit_price_decrement(-1) == 0.0
    assert position_lifecycle._aggressive_exit_price_decrement(-100) == 0.0


def test_take_profit_floor_blocks_walk_down():
    """A take-profit exit's floor must trump the walk-down — the limit
    price is set from the gain target, not from market liquidity, so
    walking it down just gives away realized P&L.  Verify
    ``_take_profit_price_floor`` returns a non-None value when
    ``kind == 'take_profit_limit'``, which is the gate the retry path
    uses to decide whether to apply the decrement."""
    floor = position_lifecycle._take_profit_price_floor(
        {
            "kind": "take_profit_limit",
            "close_price": 0.85,
            "entry_fill_price": 0.80,
            "current_mid_price": 0.85,
            "target_take_profit_pct": 5.0,
        }
    )
    assert floor is not None
    assert floor >= 0.80

    # Non-TP exits return None — the retry path then walks the price.
    assert (
        position_lifecycle._take_profit_price_floor(
            {
                "kind": "stop_loss",
                "close_price": 0.40,
                "entry_fill_price": 0.50,
            }
        )
        is None
    )
