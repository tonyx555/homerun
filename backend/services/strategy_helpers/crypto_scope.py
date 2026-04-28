"""Crypto strategy scope defaults, config schema, and defaults-merge helper.

Provides the canonical default values + UI schema that all crypto strategies
share. Imported from any non-strategy module (SDK, opportunity catalog,
trader orchestrator) so consumers never need to reach into a specific
strategy file.
"""
from __future__ import annotations

from typing import Any


# Inlined to avoid pulling in services.strategies.* at module-load time
# (that would trigger services.strategies.__init__.py and circular-import
# back to strategy_sdk via news_edge). Mirrors the canonical
# ``services.strategies.crypto_strategy_utils.normalize_timeframe``.
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
    """Return the timeframe-specific override for *base_key* in *config*, or None."""
    if timeframe and isinstance(config, dict):
        tf_key = f"{base_key}_{timeframe}"
        if tf_key in config:
            return config[tf_key]
    return None


def _crypto_hf_default_param_value(key: str, timeframe: Any) -> Any:
    """Return the default value for ``key`` from CRYPTO_HF_SCOPE_DEFAULTS,
    preferring a timeframe-specific entry (e.g. ``key_5m``) when present.

    Used by the crypto strategies as a fallback lookup when the live
    user-edited config doesn't supply a value for ``key``. The defaults
    source is always :data:`CRYPTO_HF_SCOPE_DEFAULTS` — strategies pass
    only ``(key, timeframe)``.
    """
    tf = _normalize_timeframe(timeframe)
    override = _timeframe_override(CRYPTO_HF_SCOPE_DEFAULTS, key, tf)
    if override is not None:
        return override
    return CRYPTO_HF_SCOPE_DEFAULTS.get(key)


# ---------------------------------------------------------------------------
# HF-scope defaults (used as fallback when the live config omits a key)
# ---------------------------------------------------------------------------

CRYPTO_HF_SCOPE_DEFAULTS: dict[str, Any] = {
    # Directional-entry gates
    "opening_directional_buy_yes_enabled": True,
    "opening_directional_buy_no_enabled": True,
    # Exit controls
    "rapid_take_profit_pct": 10.0,
    "take_profit_pct": 8.0,
    "stop_loss_pct": 4.0,
    "stop_loss_policy": "always",
    "stop_loss_activation_seconds": 90.0,
    "trailing_stop_pct": 3.0,
    "trailing_stop_activation_profit_pct": 4.0,
    "min_hold_minutes": 1.0,
    "max_hold_minutes": 60.0,
    "rapid_exit_window_minutes": 2.0,
    "rapid_exit_min_increase_pct": 0.0,
    "rapid_exit_breakeven_buffer_pct": 0.0,
    # Underwater rebound
    "underwater_rebound_exit_enabled": True,
    "underwater_dwell_minutes": 2.5,
    "underwater_recovery_ratio_min": 0.35,
    "underwater_rebound_pct_min": 1.2,
    "underwater_exit_fade_pct": 0.45,
    "underwater_timeout_minutes": 10.0,
    "underwater_timeout_loss_pct": 8.0,
    # Force-flatten
    "force_flatten_seconds_left": 120.0,
    "force_flatten_seconds_left_5m": 30.0,
    "force_flatten_seconds_left_15m": 75.0,
    "force_flatten_seconds_left_1h": 240.0,
    "force_flatten_seconds_left_4h": 600.0,
    "force_flatten_max_profit_pct": 1.0,
    "force_flatten_headroom_floor": 1.15,
    "force_flatten_min_loss_pct": 0.0,
    # Resolution risk
    "resolution_risk_flatten_enabled": True,
    "resolution_risk_seconds_left": 180.0,
    "resolution_risk_max_profit_pct": 6.0,
    "resolution_risk_min_loss_pct": 2.0,
    "resolution_risk_min_headroom_ratio": 0.9,
    "resolution_risk_disable_when_take_profit_armed": True,
    # Circuit breaker
    "max_consecutive_losses_before_pause": 3,
}

# Generic (non-HF) crypto scope defaults — a subset used by the SDK and the
# opportunity catalogue for non-HF crypto strategies.
CRYPTO_SCOPE_DEFAULTS: dict[str, Any] = {
    "opening_directional_buy_yes_enabled": True,
    "opening_directional_buy_no_enabled": True,
    "take_profit_pct": 8.0,
    "stop_loss_pct": 4.0,
    "trailing_stop_pct": 3.0,
    "force_flatten_seconds_left": 120.0,
}


# ---------------------------------------------------------------------------
# Config schema (param_fields list used by strategy config UIs)
# ---------------------------------------------------------------------------

def crypto_scope_config_schema() -> dict[str, Any]:
    """Return the param-fields schema for crypto strategy config UIs."""
    return {
        "param_fields": [
            {"key": "take_profit_pct", "label": "Take profit %", "type": "float", "default": 8.0},
            {"key": "stop_loss_pct", "label": "Stop loss %", "type": "float", "default": 4.0},
            {"key": "trailing_stop_pct", "label": "Trailing stop %", "type": "float", "default": 3.0},
            {
                "key": "force_flatten_seconds_left",
                "label": "Force-flatten seconds left",
                "type": "float",
                "default": 120.0,
            },
            {
                "key": "opening_directional_buy_yes_enabled",
                "label": "Allow directional BUY YES",
                "type": "bool",
                "default": True,
            },
            {
                "key": "opening_directional_buy_no_enabled",
                "label": "Allow directional BUY NO",
                "type": "bool",
                "default": True,
            },
        ]
    }


# ---------------------------------------------------------------------------
# Defaults merge
# ---------------------------------------------------------------------------

def merge_crypto_defaults(config: Any) -> dict[str, Any]:
    """Overlay a crypto strategy's user-supplied config onto the canonical defaults.

    Returns ``CRYPTO_HF_SCOPE_DEFAULTS`` with caller-supplied keys taking
    precedence. Idempotent. Strategies call this in their ``configure``
    path so any key the user hasn't set falls back to the shipped default.
    """
    base: dict[str, Any] = dict(CRYPTO_HF_SCOPE_DEFAULTS)
    overlay: dict[str, Any] = dict(config) if isinstance(config, dict) else {}
    base.update(overlay)
    return base
