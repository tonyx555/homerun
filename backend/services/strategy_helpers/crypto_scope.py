"""Crypto strategy scope defaults, schema, and config-normalisation helpers.

Extracted from services.strategies.btc_eth_highfreq so that non-HF modules
(strategy_sdk, opportunity_strategy_catalog, …) can import them without
pulling in the full HF strategy file.
"""
from __future__ import annotations

from typing import Any

from services.strategies.crypto_strategy_utils import normalize_timeframe


# ---------------------------------------------------------------------------
# Underscore-prefixed aliases used by btc_eth_highfreq internals
# ---------------------------------------------------------------------------

def _normalize_timeframe(timeframe: Any) -> str:
    return normalize_timeframe(timeframe)


def _timeframe_override(config: Any, base_key: str, timeframe: str | None) -> Any:
    """Return the timeframe-specific override for *base_key* in *config*, or None."""
    if timeframe and isinstance(config, dict):
        tf_key = f"{base_key}_{timeframe}"
        if tf_key in config:
            return config[tf_key]
    return None


def _crypto_hf_default_param_value(config: Any, key: str, timeframe: Any) -> Any:
    """Return ``config[key]``, preferring a timeframe-specific key if present."""
    tf = _normalize_timeframe(timeframe)
    override = _timeframe_override(config, key, tf)
    if override is not None:
        return override
    cfg = config if isinstance(config, dict) else {}
    return cfg.get(key)


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
# Legacy-config normalisation
# ---------------------------------------------------------------------------

def normalize_crypto_legacy_config(config: Any) -> dict[str, Any]:
    """Apply legacy field migrations to a generic crypto strategy config.

    Idempotent — safe to call repeatedly on the same dict.
    Returns a (shallow-copied) normalised dict.
    """
    cfg: dict[str, Any] = dict(config) if isinstance(config, dict) else {}
    return cfg


def normalize_crypto_highfreq_legacy_config(config: Any) -> dict[str, Any]:
    """Apply legacy field migrations to a BTC/ETH HF strategy config.

    Merges CRYPTO_HF_SCOPE_DEFAULTS as base, then overlays the provided
    config so that live config always wins.  Idempotent.
    """
    base: dict[str, Any] = dict(CRYPTO_HF_SCOPE_DEFAULTS)
    overlay: dict[str, Any] = dict(config) if isinstance(config, dict) else {}
    base.update(overlay)
    return base
