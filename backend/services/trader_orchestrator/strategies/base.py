"""Backward-compatibility shim.

All strategy base classes now live in services.strategies.base.
This module re-exports them so existing trader strategy code
that imports from here continues to work.
"""

from __future__ import annotations

from services.strategies.base import (
    BaseStrategy as BaseTraderStrategy,
    DecisionCheck,
    ExitDecision,
    StrategyDecision,
)


# Legacy protocol — kept for isinstance checks in old code
class TraderStrategy:
    key: str = "base"

    def evaluate(self, signal, context): ...


__all__ = [
    "BaseTraderStrategy",
    "DecisionCheck",
    "ExitDecision",
    "StrategyDecision",
    "TraderStrategy",
]
