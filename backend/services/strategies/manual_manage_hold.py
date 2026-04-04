from __future__ import annotations

from typing import Any

from services.strategies.base import BaseStrategy, ExitDecision, StrategyDecision
from utils.converters import safe_float


class ManualManageHoldStrategy(BaseStrategy):
    name = "Manual Manage Hold"
    description = "Manage manually adopted positions with downside protection while otherwise holding."
    source_key = "manual"
    allow_new_entries = False
    default_config = {
        "min_hold_minutes": 2.0,
        "hard_stop_loss_pct": 18.0,
        "breakeven_arm_profit_pct": 8.0,
        "breakeven_buffer_pct": 0.0,
        "near_resolution_window_seconds": 300.0,
        "near_resolution_stop_loss_pct": 3.0,
    }

    def evaluate(self, signal, context):
        return StrategyDecision(
            decision="blocked",
            reason="Manual manage strategy blocks new entries",
            checks=[],
        )

    @staticmethod
    def _cfg_float(config: dict[str, Any], key: str, default: float) -> float:
        parsed = safe_float(config.get(key), default)
        return float(default if parsed is None else parsed)

    def _effective_config(self, position: Any) -> dict[str, Any]:
        merged = dict(getattr(self, "config", {}) or {})
        raw_position_config = getattr(position, "config", None)
        if isinstance(raw_position_config, dict):
            merged.update(raw_position_config)
        return merged

    @staticmethod
    def _ensure_strategy_context(position: Any) -> dict[str, Any]:
        strategy_context = getattr(position, "strategy_context", None)
        if isinstance(strategy_context, dict):
            return strategy_context
        strategy_context = {}
        position.strategy_context = strategy_context
        return strategy_context

    @staticmethod
    def _hold(reason: str) -> ExitDecision:
        return ExitDecision(action="hold", reason=reason, payload={"skip_default_exit": True})

    def should_exit(self, position: Any, market_state: dict) -> ExitDecision:
        if market_state.get("is_resolved"):
            return self.default_exit_check(position, market_state)

        current_price = safe_float(market_state.get("current_price"))
        if current_price is None or current_price < 0.0:
            return self._hold("Manual manage: waiting for current price")

        entry_price = safe_float(getattr(position, "entry_price", None))
        if entry_price is None or entry_price <= 0.0:
            return self._hold("Manual manage: waiting for entry price")

        config = self._effective_config(position)
        strategy_context = self._ensure_strategy_context(position)

        age_minutes = float(max(0.0, safe_float(getattr(position, "age_minutes", 0.0), 0.0) or 0.0))
        pnl_pct = float(((current_price - entry_price) / entry_price) * 100.0)

        observed_high = safe_float(getattr(position, "highest_price", None))
        peak_price = max(
            entry_price,
            float(current_price),
            float(observed_high) if observed_high is not None and observed_high > 0.0 else entry_price,
        )
        peak_gain_pct = ((peak_price - entry_price) / entry_price * 100.0) if entry_price > 0.0 else 0.0

        min_hold_minutes = max(0.0, self._cfg_float(config, "min_hold_minutes", 2.0))
        min_hold_passed = age_minutes >= min_hold_minutes

        hard_stop_loss_pct = max(0.0, self._cfg_float(config, "hard_stop_loss_pct", 18.0))
        if min_hold_passed and hard_stop_loss_pct > 0.0 and pnl_pct <= -hard_stop_loss_pct:
            return ExitDecision(
                action="close",
                reason=f"Manual hard stop loss ({pnl_pct:.2f}% <= -{hard_stop_loss_pct:.2f}%)",
                close_price=float(current_price),
            )

        breakeven_arm_profit_pct = max(0.0, self._cfg_float(config, "breakeven_arm_profit_pct", 2.2))
        if peak_gain_pct >= breakeven_arm_profit_pct:
            strategy_context["_manual_breakeven_armed"] = True
        breakeven_armed = bool(strategy_context.get("_manual_breakeven_armed", False))

        breakeven_buffer_pct = self._cfg_float(config, "breakeven_buffer_pct", 0.0)
        if breakeven_armed and min_hold_passed and pnl_pct <= breakeven_buffer_pct:
            return ExitDecision(
                action="close",
                reason=f"Manual breakeven protect ({pnl_pct:.2f}% <= {breakeven_buffer_pct:.2f}%)",
                close_price=float(current_price),
            )

        seconds_left = self._seconds_left_for_position(position, market_state)
        near_resolution_window_seconds = max(0.0, self._cfg_float(config, "near_resolution_window_seconds", 300.0))
        near_resolution_stop_loss_pct = max(0.0, self._cfg_float(config, "near_resolution_stop_loss_pct", 3.0))
        if seconds_left is not None and near_resolution_window_seconds > 0.0 and seconds_left <= near_resolution_window_seconds:
            if pnl_pct <= -near_resolution_stop_loss_pct:
                return ExitDecision(
                    action="close",
                    reason=(
                        f"Manual near-resolution risk exit ({pnl_pct:.2f}% <= "
                        f"-{near_resolution_stop_loss_pct:.2f}%)"
                    ),
                    close_price=float(current_price),
                )
            return self._hold(
                f"Manual near-resolution hold ({seconds_left:.0f}s left, pnl {pnl_pct:.2f}%)"
            )

        return self._hold(
            f"Manual manage hold (pnl {pnl_pct:.2f}%, peak {peak_gain_pct:.2f}%)"
        )
