"""Queue-hazard flip strategy for short-term crypto prediction markets."""

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


def _normalize_signed_ratio(value: Any) -> float | None:
    parsed = safe_float(value, None)
    if parsed is None:
        return None
    ratio = float(parsed)
    if abs(ratio) > 1.0 and abs(ratio) <= 100.0:
        ratio /= 100.0
    if ratio > 1.0:
        ratio = 1.0
    if ratio < -1.0:
        ratio = -1.0
    return ratio


class CryptoQueueHazardFlipStrategy(BaseStrategy):
    strategy_type = "crypto_queue_hazard_flip"
    name = "Crypto Queue Hazard Flip"
    description = "Flips into micro-exhaustion when orderflow is one-sided and queue risk is elevated."
    mispricing_type = "within_market"
    source_key = "crypto"
    worker_affinity = "crypto"
    market_categories = ["crypto"]
    subscriptions = ["crypto_update"]
    accepted_signal_strategy_types = ["btc_eth_highfreq"]
    requires_live_market_context = True

    quality_filter_overrides = QualityFilterOverrides(
        min_roi=0.8,
        max_resolution_months=0.1,
    )

    default_config = {
        "min_edge_percent": 2.0,
        "min_confidence": 0.44,
        "min_flow_imbalance": 0.35,
        "min_recent_move_zscore": 1.40,
        "max_spread_widening_bps": 30.0,
        "max_cancel_rate_30s": 0.78,
        "base_size_usd": 20.0,
        "max_size_usd": 120.0,
        "sizing_policy": "adaptive",
        "take_profit_pct": 7.5,
        "stop_loss_pct": 4.2,
        "max_hold_minutes": 10.0,
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

        min_edge = max(0.0, to_float(params.get("min_edge_percent", 2.0), 2.0))
        min_conf = to_confidence(params.get("min_confidence", 0.44), 0.44)
        min_flow_imbalance = max(0.0, min(1.0, to_float(params.get("min_flow_imbalance", 0.35), 0.35)))
        min_recent_move_zscore = max(0.0, to_float(params.get("min_recent_move_zscore", 1.40), 1.40))
        max_spread_widening_bps = max(0.0, to_float(params.get("max_spread_widening_bps", 30.0), 30.0))
        max_cancel_rate_30s = max(0.0, min(1.0, to_float(params.get("max_cancel_rate_30s", 0.78), 0.78)))

        base_size = max(1.0, to_float(params.get("base_size_usd", 20.0), 20.0))
        max_size = max(base_size, to_float(params.get("max_size_usd", 120.0), 120.0))
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
        recent_move_zscore = safe_float(
            live_market.get("recent_move_zscore"),
            safe_float(payload.get("recent_move_zscore"), None),
        )
        spread_widening_bps = safe_float(
            live_market.get("spread_widening_bps"),
            safe_float(payload.get("spread_widening_bps"), None),
        )
        cancel_rate_30s = _normalize_signed_ratio(
            live_market.get("cancel_rate_30s")
            if live_market.get("cancel_rate_30s") is not None
            else payload.get("cancel_rate_30s")
        )
        if cancel_rate_30s is not None:
            cancel_rate_30s = abs(cancel_rate_30s)

        flow_imbalance = _normalize_signed_ratio(
            live_market.get("flow_imbalance")
            if live_market.get("flow_imbalance") is not None
            else payload.get("flow_imbalance")
        )
        if flow_imbalance is None:
            flow_imbalance = _normalize_signed_ratio(
                live_market.get("orderflow_imbalance")
                if live_market.get("orderflow_imbalance") is not None
                else payload.get("orderflow_imbalance")
            )

        source_ok = source == "crypto"
        origin_ok = bool(payload.get("strategy_origin") == "crypto_worker") or signal_type.startswith("crypto_worker")
        edge_ok = edge >= min_edge
        confidence_ok = confidence >= min_conf
        flow_ok = flow_imbalance is not None and abs(flow_imbalance) >= min_flow_imbalance
        move_ok = recent_move_zscore is not None and abs(recent_move_zscore) >= min_recent_move_zscore
        spread_ok = spread_widening_bps is None or spread_widening_bps <= max_spread_widening_bps
        cancel_ok = cancel_rate_30s is None or cancel_rate_30s <= max_cancel_rate_30s

        if flow_imbalance is None:
            flip_alignment_ok = False
        elif direction == "buy_yes":
            flip_alignment_ok = flow_imbalance <= -min_flow_imbalance
        elif direction == "buy_no":
            flip_alignment_ok = flow_imbalance >= min_flow_imbalance
        else:
            flip_alignment_ok = False

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
                "flow_magnitude",
                "Orderflow imbalance magnitude",
                flow_ok,
                score=flow_imbalance,
                detail=f"min=|{min_flow_imbalance:.2f}|",
            ),
            DecisionCheck(
                "flip_alignment",
                "Flip direction aligns with queue hazard",
                flip_alignment_ok,
                score=flow_imbalance,
                detail="Signal must trade against one-sided flow.",
            ),
            DecisionCheck(
                "recent_move",
                "Recent move intensity",
                move_ok,
                score=recent_move_zscore,
                detail=f"min_z={min_recent_move_zscore:.2f}",
            ),
            DecisionCheck(
                "spread_widening",
                "Spread widening cap",
                spread_ok,
                score=spread_widening_bps,
                detail=f"max={max_spread_widening_bps:.1f}bps",
            ),
            DecisionCheck(
                "cancel_rate",
                "Cancel-rate cap",
                cancel_ok,
                score=cancel_rate_30s,
                detail=f"max={max_cancel_rate_30s:.2f}",
            ),
        ]

        score = (
            (edge * 0.55)
            + (confidence * 32.0)
            + (min(1.0, abs(flow_imbalance or 0.0)) * 8.0)
            + (min(1.0, abs(recent_move_zscore or 0.0) / 3.0) * 6.0)
        )

        if not all(check.passed for check in checks):
            return StrategyDecision(
                decision="skipped",
                reason="Queue-hazard flip filters not met",
                score=score,
                checks=checks,
                payload={
                    "edge": edge,
                    "confidence": confidence,
                    "flow_imbalance": flow_imbalance,
                    "recent_move_zscore": recent_move_zscore,
                    "spread_widening_bps": spread_widening_bps,
                    "cancel_rate_30s": cancel_rate_30s,
                },
            )

        probability = selected_probability(signal, payload, direction)
        entry_price = to_float(getattr(signal, "entry_price", 0.0), 0.0)

        sizing = compute_position_size(
            base_size_usd=base_size,
            max_size_usd=max_size,
            edge_percent=edge,
            confidence=confidence,
            sizing_policy=sizing_policy,
            probability=probability,
            entry_price=entry_price if entry_price > 0 else None,
            liquidity_usd=liquidity,
            liquidity_cap_fraction=0.06,
            min_size_usd=1.0,
        )
        size_usd = float(sizing.get("size_usd") or base_size)

        return StrategyDecision(
            decision="selected",
            reason="Queue-hazard flip selected",
            score=score,
            size_usd=size_usd,
            checks=checks,
            payload={
                "edge": edge,
                "confidence": confidence,
                "flow_imbalance": flow_imbalance,
                "recent_move_zscore": recent_move_zscore,
                "spread_widening_bps": spread_widening_bps,
                "cancel_rate_30s": cancel_rate_30s,
                "sizing": sizing,
            },
        )

    def should_exit(self, position: Any, market_state: dict) -> ExitDecision:
        config = dict(getattr(position, "config", None) or {})
        strategy_config = dict(getattr(self, "config", None) or {})
        config.setdefault("take_profit_pct", float(strategy_config.get("take_profit_pct", 7.5)))
        config.setdefault("stop_loss_pct", float(strategy_config.get("stop_loss_pct", 4.2)))
        config.setdefault("max_hold_minutes", float(strategy_config.get("max_hold_minutes", 10.0)))
        position.config = config
        return self.default_exit_check(position, market_state)

    def on_blocked(self, signal, reason: str, context: dict) -> None:
        logger.info("%s blocked: %s market=%s", self.name, reason, getattr(signal, "market_id", "?"))

