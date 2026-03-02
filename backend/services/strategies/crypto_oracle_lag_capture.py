"""Oracle-lag capture strategy for short-horizon crypto markets."""

from __future__ import annotations

import logging
from typing import Any

from services.data_events import DataEvent
from services.quality_filter import QualityFilterOverrides
from services.strategies.base import BaseStrategy, DecisionCheck, ExitDecision, StrategyDecision
from services.trader_orchestrator.strategies.sizing import compute_position_size
from utils.converters import safe_float, to_confidence, to_float
from utils.signal_helpers import selected_probability, signal_payload

logger = logging.getLogger(__name__)


class CryptoOracleLagCaptureStrategy(BaseStrategy):
    strategy_type = "crypto_oracle_lag_capture"
    name = "Crypto Oracle Lag Capture"
    description = "Captures temporary lag between oracle prints and binary market prices."
    mispricing_type = "within_market"
    source_key = "crypto"
    worker_affinity = "crypto"
    market_categories = ["crypto"]
    subscriptions = ["crypto_update"]
    accepted_signal_strategy_types = ["btc_eth_highfreq"]
    requires_live_market_context = True

    quality_filter_overrides = QualityFilterOverrides(
        min_roi=0.9,
        max_resolution_months=0.1,
    )

    default_config = {
        "min_edge_percent": 1.8,
        "min_confidence": 0.46,
        "min_oracle_diff_pct": 0.12,
        "max_oracle_age_seconds": 10.0,
        "max_entry_price": 0.92,
        "base_size_usd": 18.0,
        "max_size_usd": 160.0,
        "sizing_policy": "kelly",
        "kelly_fractional_scale": 0.45,
        "take_profit_pct": 9.0,
        "stop_loss_pct": 4.5,
        "max_hold_minutes": 14.0,
    }

    async def on_event(self, event: DataEvent) -> list:
        return []

    def evaluate(self, signal: Any, context: dict[str, Any]) -> StrategyDecision:
        params = context.get("params") or {}
        payload = signal_payload(signal)
        live_market = context.get("live_market")
        if not isinstance(live_market, dict):
            live_market = payload.get("live_market")
        if not isinstance(live_market, dict):
            live_market = {}

        min_edge = max(0.0, to_float(params.get("min_edge_percent", 1.8), 1.8))
        min_conf = to_confidence(params.get("min_confidence", 0.46), 0.46)
        min_oracle_diff_pct = max(0.01, to_float(params.get("min_oracle_diff_pct", 0.12), 0.12))
        max_oracle_age_seconds = max(0.1, to_float(params.get("max_oracle_age_seconds", 10.0), 10.0))
        max_entry_price = min(0.999, max(0.01, to_float(params.get("max_entry_price", 0.92), 0.92)))
        base_size = max(1.0, to_float(params.get("base_size_usd", 18.0), 18.0))
        max_size = max(base_size, to_float(params.get("max_size_usd", 160.0), 160.0))
        sizing_policy = str(params.get("sizing_policy", "kelly") or "kelly")
        kelly_fractional_scale = to_float(params.get("kelly_fractional_scale", 0.45), 0.45)

        source = str(getattr(signal, "source", "") or "").strip().lower()
        signal_type = str(getattr(signal, "signal_type", "") or "").strip().lower()
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        edge = max(0.0, to_float(getattr(signal, "edge_percent", 0.0), 0.0))
        confidence = to_confidence(getattr(signal, "confidence", 0.0), 0.0)
        liquidity = max(
            0.0,
            to_float(
                live_market.get("liquidity_usd"),
                to_float(payload.get("liquidity_usd"), to_float(getattr(signal, "liquidity", 0.0), 0.0)),
            ),
        )

        oracle_diff_pct = safe_float(
            payload.get("oracle_diff_pct"),
            safe_float(live_market.get("oracle_diff_pct"), None),
        )
        oracle_age_seconds = safe_float(
            live_market.get("oracle_age_seconds"),
            safe_float(payload.get("oracle_age_seconds"), None),
        )
        entry_price = to_float(getattr(signal, "entry_price", 0.0), 0.0)
        if entry_price <= 0.0:
            entry_price = to_float(
                live_market.get("live_selected_price"),
                to_float(payload.get("live_selected_price"), to_float(payload.get("selected_price"), 0.0)),
            )

        source_ok = source == "crypto"
        origin_ok = bool(payload.get("strategy_origin") == "crypto_worker") or signal_type.startswith("crypto_worker")
        edge_ok = edge >= min_edge
        confidence_ok = confidence >= min_conf
        oracle_diff_ok = oracle_diff_pct is not None and abs(oracle_diff_pct) >= min_oracle_diff_pct
        oracle_fresh_ok = oracle_age_seconds is not None and oracle_age_seconds <= max_oracle_age_seconds
        entry_price_ok = entry_price > 0.0 and entry_price <= max_entry_price

        if oracle_diff_pct is None:
            direction_ok = False
        elif oracle_diff_pct > 0:
            direction_ok = direction == "buy_yes"
        elif oracle_diff_pct < 0:
            direction_ok = direction == "buy_no"
        else:
            direction_ok = False

        checks = [
            DecisionCheck("source", "Crypto source", source_ok, detail="Requires source=crypto."),
            DecisionCheck("origin", "Crypto worker origin", origin_ok, detail="Requires crypto-worker signal."),
            DecisionCheck("edge", "Edge threshold", edge_ok, score=edge, detail=f"min={min_edge:.2f}%"),
            DecisionCheck(
                "confidence",
                "Confidence threshold",
                confidence_ok,
                score=confidence,
                detail=f"min={min_conf:.2f}",
            ),
            DecisionCheck(
                "oracle_diff",
                "Oracle/market diff",
                oracle_diff_ok,
                score=oracle_diff_pct,
                detail=f"min_abs={min_oracle_diff_pct:.3f}%",
            ),
            DecisionCheck(
                "oracle_freshness",
                "Oracle freshness",
                oracle_fresh_ok,
                score=oracle_age_seconds,
                detail=f"max_age={max_oracle_age_seconds:.1f}s",
            ),
            DecisionCheck(
                "direction_alignment",
                "Direction aligned to oracle",
                direction_ok,
                score=oracle_diff_pct,
                detail=f"direction={direction or 'unknown'}",
            ),
            DecisionCheck(
                "entry_price",
                "Entry price cap",
                entry_price_ok,
                score=entry_price,
                detail=f"max={max_entry_price:.3f}",
            ),
        ]

        score = (
            (edge * 0.55)
            + (confidence * 34.0)
            + (min(1.0, abs(oracle_diff_pct or 0.0) / 0.5) * 10.0)
            + (min(1.0, liquidity / 20_000.0) * 4.0)
        )

        if not all(check.passed for check in checks):
            return StrategyDecision(
                decision="skipped",
                reason="Oracle-lag capture filters not met",
                score=score,
                checks=checks,
                payload={
                    "edge": edge,
                    "confidence": confidence,
                    "oracle_diff_pct": oracle_diff_pct,
                    "oracle_age_seconds": oracle_age_seconds,
                    "entry_price": entry_price,
                },
            )

        probability = selected_probability(signal, payload, direction)
        sizing = compute_position_size(
            base_size_usd=base_size,
            max_size_usd=max_size,
            edge_percent=edge,
            confidence=confidence,
            sizing_policy=sizing_policy,
            probability=probability,
            entry_price=entry_price if entry_price > 0 else None,
            kelly_fractional_scale=kelly_fractional_scale,
            liquidity_usd=liquidity,
            liquidity_cap_fraction=0.07,
            min_size_usd=1.0,
        )
        size_usd = float(sizing.get("size_usd") or base_size)

        return StrategyDecision(
            decision="selected",
            reason="Oracle-lag capture selected",
            score=score,
            size_usd=size_usd,
            checks=checks,
            payload={
                "edge": edge,
                "confidence": confidence,
                "oracle_diff_pct": oracle_diff_pct,
                "oracle_age_seconds": oracle_age_seconds,
                "entry_price": entry_price,
                "sizing": sizing,
            },
        )

    def should_exit(self, position: Any, market_state: dict) -> ExitDecision:
        config = dict(getattr(position, "config", None) or {})
        strategy_config = dict(getattr(self, "config", None) or {})
        config.setdefault("take_profit_pct", float(strategy_config.get("take_profit_pct", 9.0)))
        config.setdefault("stop_loss_pct", float(strategy_config.get("stop_loss_pct", 4.5)))
        config.setdefault("max_hold_minutes", float(strategy_config.get("max_hold_minutes", 14.0)))
        position.config = config
        return self.default_exit_check(position, market_state)

    def on_blocked(self, signal, reason: str, context: dict) -> None:
        logger.info("%s blocked: %s market=%s", self.name, reason, getattr(signal, "market_id", "?"))

