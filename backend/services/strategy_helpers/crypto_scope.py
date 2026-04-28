"""Pure timeframe-normalization utilities for crypto strategies.

Each crypto strategy owns its own ``default_config`` inline — there is
no shared defaults dict here. This module is intentionally just two
small utility functions used during evaluate() to canonicalize
timeframe strings and look up timeframe-suffixed config overrides like
``take_profit_pct_5m``.
"""
from __future__ import annotations

from typing import Any


def _normalize_timeframe(timeframe: Any) -> str:
    """Canonicalize a timeframe string to ``5m`` / ``15m`` / ``1h`` / ``4h``."""
    tf = str(timeframe or "").strip().lower()
    if tf in {"5m", "5min", "5"}:
        return "5m"
    if tf in {"15m", "15min", "15"}:
        return "15m"
    if tf in {"1h", "1hr", "60m", "60min"}:
        return "1h"
    if tf in {"4h", "4hr", "240m", "240min"}:
        return "4h"
    return tf


def _timeframe_override(config: Any, base_key: str, timeframe: str | None) -> Any:
    """Return ``config[f"{base_key}_{timeframe}"]`` when present, else None.

    Pure lookup — no defaults, no merging. Each strategy looks up its own
    config + its own default_config separately.
    """
    if timeframe and isinstance(config, dict):
        tf_key = f"{base_key}_{timeframe}"
        if tf_key in config:
            return config[tf_key]
    return None
