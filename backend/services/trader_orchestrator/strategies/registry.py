"""Strategy registry — thin wrapper around the unified strategy_loader.

All strategies live in the DB and are loaded by the unified loader.
This module provides convenience accessors for callers that need a
strategy instance by slug.
"""

from __future__ import annotations

from typing import Any

from services.strategy_loader import strategy_loader


def list_strategy_keys() -> list[str]:
    return sorted(slug for slug in strategy_loader._loaded)


def get_strategy(strategy_key: str) -> Any | None:
    """Return the loaded BaseStrategy instance for *strategy_key*, or None."""
    key = str(strategy_key or "").strip().lower()
    if not key:
        return None
    return strategy_loader.get_instance(key)
