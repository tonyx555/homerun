"""Entropy-weighted maker strategy for crypto binary markets."""

from __future__ import annotations

import logging
import math
from typing import Any

from services.data_events import DataEvent
from services.quality_filter import QualityFilterOverrides
from services.strategies.base import BaseStrategy, DecisionCheck, ExitDecision, StrategyDecision
from services.trader_orchestrator.strategies.sizing import compute_position_size
from utils.converters import safe_float, to_confidence, to_float
from utils.signal_helpers import selected_probability, signal_payload

logger = logging.getLogger(__name__)


def _probability_entropy(prob_yes: float) -> float:
    p = min(0.999999, max(0.000001, float(prob_yes)))
    q = 1.0 - p
    entropy = -(p * math.log(p, 2.0)) - (q * math.log(q, 2.0))
    return max(0.0, min(1.0, entropy))


def _normalize_ratio(value: Any) -> float | None:
    parsed = safe_float(value, None)
    if parsed is None:
        return None
    ratio = float(parsed)
    if ratio > 1.0 and ratio <= 100.0:
        ratio /= 100.0
    return max(0.0, min(1.0, ratio))


class CryptoEntropyMakerStrategy(BaseStrategy):
    strategy_type = "crypto_entropy_maker"
    name = "Crypto Entropy Maker"
    description = "Scales maker entries by binary entropy and live tape quality."
    mispricing_type = "within_market"
    source_key = "crypto"
    market_categories = ["crypto"]
    subscriptions = ["crypto_update"]
    accepted_signal_strategy_types = ["btc_eth_highfreq"]

    quality_filter_overrides = QualityFilterOverrides(
        min_roi=0.6,
        max_resolution_months=0.1,
    )

    default_config = {
        "min_edge_percent": 1.0,
        "min_confidence": 0.40,
        "min_entropy": 0.82,
        "min_spread_pct": 0.006,
        "max_spread_pct": 0.065,
        "max_cancel_rate_30s": 0.75,
        "min_liquidity_usd": 1000.0,
        "base_size_usd": 16.0,
        "max_size_usd": 130.0,
        "sizing_policy": "adaptive",
        "take_profit_pct": 6.5,
        "stop_loss_pct": 4.0,
        "max_hold_minutes": 16.0,
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

        min_edge = max(0.0, to_float(params.get("min_edge_percent", 1.0), 1.0))
        min_conf = to_confidence(params.get("min_confidence", 0.40), 0.40)
        min_entropy = max(0.0, min(1.0, to_float(params.get("min_entropy", 0.82), 0.82)))
        min_spread_pct = max(0.0, min(1.0, to_float(params.get("min_spread_pct", 0.006), 0.006)))
        max_spread_pct = max(min_spread_pct, min(1.0, to_float(params.get("max_spread_pct", 0.065), 0.065)))
        max_cancel_rate_30s = max(0.0, min(1.0, to_float(params.get("max_cancel_rate_30s", 0.75), 0.75)))
        min_liquidity_usd = max(0.0, to_float(params.get("min_liquidity_usd", 1000.0), 1000.0))

        base_size = max(1.0, to_float(params.get("base_size_usd", 16.0), 16.0))
        max_size = max(base_size, to_float(params.get("max_size_usd", 130.0), 130.0))
        sizing_policy = str(params.get("sizing_policy", "adaptive") or "adaptive")

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

        yes_price = safe_float(
            live_market.get("yes_price"),
            safe_float(live_market.get("live_yes_price"), safe_float(payload.get("yes_price"), safe_float(payload.get("up_price"), None))),
        )
        no_price = safe_float(
            live_market.get("no_price"),
            safe_float(live_market.get("live_no_price"), safe_float(payload.get("no_price"), safe_float(payload.get("down_price"), None))),
        )
        spread_pct = safe_float(live_market.get("spread"), safe_float(payload.get("spread"), None))
        cancel_rate_30s = _normalize_ratio(
            live_market.get("cancel_rate_30s")
            if live_market.get("cancel_rate_30s") is not None
            else payload.get("cancel_rate_30s")
        )

        if yes_price is not None and no_price is not None:
            total = yes_price + no_price
            prob_yes = yes_price / total if total > 0 else 0.5
            entropy = _probability_entropy(prob_yes)
        else:
            entropy = None

        source_ok = source == "crypto"
        origin_ok = bool(payload.get("strategy_origin") == "crypto_worker") or signal_type.startswith("crypto_worker")
        edge_ok = edge >= min_edge
        confidence_ok = confidence >= min_conf
        entropy_ok = entropy is not None and entropy >= min_entropy
        spread_ok = spread_pct is not None and min_spread_pct <= spread_pct <= max_spread_pct
        cancel_ok = cancel_rate_30s is None or cancel_rate_30s <= max_cancel_rate_30s
        liquidity_ok = liquidity >= min_liquidity_usd

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
                "entropy",
                "Binary entropy floor",
                entropy_ok,
                score=entropy,
                detail=f"min={min_entropy:.2f}",
            ),
            DecisionCheck(
                "spread_window",
                "Spread window",
                spread_ok,
                score=spread_pct,
                detail=f"range=[{min_spread_pct:.3f}, {max_spread_pct:.3f}]",
            ),
            DecisionCheck(
                "cancel_rate",
                "Cancel-rate cap",
                cancel_ok,
                score=cancel_rate_30s,
                detail=f"max={max_cancel_rate_30s:.2f}",
            ),
            DecisionCheck(
                "liquidity",
                "Liquidity floor",
                liquidity_ok,
                score=liquidity,
                detail=f"min={min_liquidity_usd:.0f}",
            ),
        ]

        score = (
            (edge * 0.45)
            + (confidence * 30.0)
            + (min(1.0, entropy or 0.0) * 10.0)
            + (min(1.0, liquidity / 20_000.0) * 4.0)
        )

        if not all(check.passed for check in checks):
            return StrategyDecision(
                decision="skipped",
                reason="Entropy-maker filters not met",
                score=score,
                checks=checks,
                payload={
                    "edge": edge,
                    "confidence": confidence,
                    "entropy": entropy,
                    "spread_pct": spread_pct,
                    "cancel_rate_30s": cancel_rate_30s,
                },
            )

        probability = selected_probability(signal, payload, direction)
        entry_price = to_float(getattr(signal, "entry_price", 0.0), 0.0)
        if entry_price <= 0.0:
            entry_price = to_float(live_market.get("live_selected_price"), to_float(payload.get("selected_price"), 0.0))
        entropy_multiplier = 0.55 + (0.80 * float(entropy or 0.0))

        sizing = compute_position_size(
            base_size_usd=base_size * entropy_multiplier,
            max_size_usd=max_size,
            edge_percent=edge,
            confidence=confidence,
            sizing_policy=sizing_policy,
            probability=probability,
            entry_price=entry_price if entry_price > 0 else None,
            liquidity_usd=liquidity,
            liquidity_cap_fraction=0.08,
            min_size_usd=1.0,
        )
        size_usd = float(sizing.get("size_usd") or base_size)

        return StrategyDecision(
            decision="selected",
            reason="Entropy-maker signal selected",
            score=score,
            size_usd=size_usd,
            checks=checks,
            payload={
                "edge": edge,
                "confidence": confidence,
                "entropy": entropy,
                "entropy_multiplier": entropy_multiplier,
                "spread_pct": spread_pct,
                "cancel_rate_30s": cancel_rate_30s,
                "sizing": sizing,
            },
        )

    def should_exit(self, position: Any, market_state: dict) -> ExitDecision:
        config = dict(getattr(position, "config", None) or {})
        strategy_config = dict(getattr(self, "config", None) or {})
        config.setdefault("take_profit_pct", float(strategy_config.get("take_profit_pct", 6.5)))
        config.setdefault("stop_loss_pct", float(strategy_config.get("stop_loss_pct", 4.0)))
        config.setdefault("max_hold_minutes", float(strategy_config.get("max_hold_minutes", 16.0)))
        position.config = config
        return self.default_exit_check(position, market_state)

    def on_blocked(self, signal, reason: str, context: dict) -> None:
        logger.info("%s blocked: %s market=%s", self.name, reason, getattr(signal, "market_id", "?"))

