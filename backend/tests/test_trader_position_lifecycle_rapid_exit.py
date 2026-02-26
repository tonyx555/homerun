from __future__ import annotations

import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.trader_orchestrator import position_lifecycle


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
