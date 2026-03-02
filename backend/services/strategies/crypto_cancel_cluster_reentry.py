"""Cancel-cluster re-entry strategy for crypto prediction markets."""

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


def _normalize_ratio(value: Any) -> float | None:
    parsed = safe_float(value, None)
    if parsed is None:
        return None
    ratio = float(parsed)
    if ratio > 1.0 and ratio <= 100.0:
        ratio /= 100.0
    return max(0.0, min(1.0, ratio))


def _history_cancel_peak(history_tail: Any) -> float | None:
    if not isinstance(history_tail, list):
        return None
    peak: float | None = None
    for row in history_tail:
        if not isinstance(row, dict):
            continue
        candidate = _normalize_ratio(
            row.get("cancel_rate_30s")
            if row.get("cancel_rate_30s") is not None
            else row.get("maker_cancel_rate_30s")
        )
        if candidate is None:
            continue
        peak = candidate if peak is None else max(peak, candidate)
    return peak


class CryptoCancelClusterReentryStrategy(BaseStrategy):
    strategy_type = "crypto_cancel_cluster_reentry"
    name = "Crypto Cancel Cluster Re-Entry"
    description = "Re-enters after cancellation storms subside and spread quality recovers."
    mispricing_type = "within_market"
    source_key = "crypto"
    worker_affinity = "crypto"
    market_categories = ["crypto"]
    subscriptions = ["crypto_update"]
    accepted_signal_strategy_types = ["btc_eth_highfreq"]
    requires_live_market_context = True

    quality_filter_overrides = QualityFilterOverrides(
        min_roi=0.7,
        max_resolution_months=0.1,
    )

    default_config = {
        "min_edge_percent": 1.6,
        "min_confidence": 0.43,
        "min_prior_peak_cancel_rate": 0.80,
        "max_current_cancel_rate": 0.58,
        "min_cancel_drop": 0.18,
        "max_spread_widening_bps": 18.0,
        "min_orderflow_alignment": 0.04,
        "base_size_usd": 18.0,
        "max_size_usd": 140.0,
        "sizing_policy": "adaptive",
        "take_profit_pct": 7.0,
        "stop_loss_pct": 4.0,
        "max_hold_minutes": 11.0,
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

        min_edge = max(0.0, to_float(params.get("min_edge_percent", 1.6), 1.6))
        min_conf = to_confidence(params.get("min_confidence", 0.43), 0.43)
        min_prior_peak_cancel_rate = max(
            0.0,
            min(1.0, to_float(params.get("min_prior_peak_cancel_rate", 0.80), 0.80)),
        )
        max_current_cancel_rate = max(
            0.0,
            min(1.0, to_float(params.get("max_current_cancel_rate", 0.58), 0.58)),
        )
        min_cancel_drop = max(0.0, min(1.0, to_float(params.get("min_cancel_drop", 0.18), 0.18)))
        max_spread_widening_bps = max(0.0, to_float(params.get("max_spread_widening_bps", 18.0), 18.0))
        min_orderflow_alignment = max(0.0, min(1.0, to_float(params.get("min_orderflow_alignment", 0.04), 0.04)))

        base_size = max(1.0, to_float(params.get("base_size_usd", 18.0), 18.0))
        max_size = max(base_size, to_float(params.get("max_size_usd", 140.0), 140.0))
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

        current_cancel_rate = _normalize_ratio(
            live_market.get("cancel_rate_30s")
            if live_market.get("cancel_rate_30s") is not None
            else payload.get("cancel_rate_30s")
        )
        prior_peak_cancel_rate = _normalize_ratio(
            payload.get("cancel_peak_2m")
            if payload.get("cancel_peak_2m") is not None
            else live_market.get("cancel_peak_2m")
        )
        if prior_peak_cancel_rate is None:
            prior_peak_cancel_rate = _history_cancel_peak(
                live_market.get("history_tail")
                if isinstance(live_market.get("history_tail"), list)
                else payload.get("history_tail")
            )
        cancel_drop = None
        if current_cancel_rate is not None and prior_peak_cancel_rate is not None:
            cancel_drop = prior_peak_cancel_rate - current_cancel_rate

        spread_widening_bps = safe_float(
            live_market.get("spread_widening_bps"),
            safe_float(payload.get("spread_widening_bps"), None),
        )

        orderflow_imbalance = safe_float(
            live_market.get("flow_imbalance"),
            safe_float(payload.get("flow_imbalance"), safe_float(payload.get("orderflow_imbalance"), None)),
        )
        if orderflow_imbalance is not None and abs(orderflow_imbalance) > 1.0 and abs(orderflow_imbalance) <= 100.0:
            orderflow_imbalance /= 100.0
        if orderflow_imbalance is not None:
            orderflow_imbalance = max(-1.0, min(1.0, orderflow_imbalance))

        if orderflow_imbalance is None:
            orderflow_alignment_ok = True
        elif direction == "buy_yes":
            orderflow_alignment_ok = orderflow_imbalance >= -min_orderflow_alignment
        elif direction == "buy_no":
            orderflow_alignment_ok = orderflow_imbalance <= min_orderflow_alignment
        else:
            orderflow_alignment_ok = False

        source_ok = source == "crypto"
        origin_ok = bool(payload.get("strategy_origin") == "crypto_worker") or signal_type.startswith("crypto_worker")
        edge_ok = edge >= min_edge
        confidence_ok = confidence >= min_conf
        prior_peak_ok = prior_peak_cancel_rate is not None and prior_peak_cancel_rate >= min_prior_peak_cancel_rate
        current_cancel_ok = current_cancel_rate is not None and current_cancel_rate <= max_current_cancel_rate
        cancel_drop_ok = cancel_drop is not None and cancel_drop >= min_cancel_drop
        spread_ok = spread_widening_bps is None or spread_widening_bps <= max_spread_widening_bps

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
                "prior_cancel_peak",
                "Prior cancellation spike",
                prior_peak_ok,
                score=prior_peak_cancel_rate,
                detail=f"min={min_prior_peak_cancel_rate:.2f}",
            ),
            DecisionCheck(
                "current_cancel_rate",
                "Current cancellation cool-down",
                current_cancel_ok,
                score=current_cancel_rate,
                detail=f"max={max_current_cancel_rate:.2f}",
            ),
            DecisionCheck(
                "cancel_drop",
                "Cancellation drop from peak",
                cancel_drop_ok,
                score=cancel_drop,
                detail=f"min_drop={min_cancel_drop:.2f}",
            ),
            DecisionCheck(
                "spread_widening",
                "Spread widening cap",
                spread_ok,
                score=spread_widening_bps,
                detail=f"max={max_spread_widening_bps:.1f}bps",
            ),
            DecisionCheck(
                "orderflow_alignment",
                "Orderflow alignment",
                orderflow_alignment_ok,
                score=orderflow_imbalance,
                detail=f"min_alignment={min_orderflow_alignment:.2f}",
            ),
        ]

        score = (
            (edge * 0.50)
            + (confidence * 31.0)
            + (min(1.0, max(0.0, cancel_drop or 0.0) / 0.4) * 9.0)
            + (min(1.0, liquidity / 20_000.0) * 4.0)
        )

        if not all(check.passed for check in checks):
            return StrategyDecision(
                decision="skipped",
                reason="Cancel-cluster re-entry filters not met",
                score=score,
                checks=checks,
                payload={
                    "edge": edge,
                    "confidence": confidence,
                    "prior_peak_cancel_rate": prior_peak_cancel_rate,
                    "current_cancel_rate": current_cancel_rate,
                    "cancel_drop": cancel_drop,
                    "spread_widening_bps": spread_widening_bps,
                    "orderflow_imbalance": orderflow_imbalance,
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
            liquidity_cap_fraction=0.07,
            min_size_usd=1.0,
        )
        size_usd = float(sizing.get("size_usd") or base_size)

        return StrategyDecision(
            decision="selected",
            reason="Cancel-cluster re-entry selected",
            score=score,
            size_usd=size_usd,
            checks=checks,
            payload={
                "edge": edge,
                "confidence": confidence,
                "prior_peak_cancel_rate": prior_peak_cancel_rate,
                "current_cancel_rate": current_cancel_rate,
                "cancel_drop": cancel_drop,
                "spread_widening_bps": spread_widening_bps,
                "orderflow_imbalance": orderflow_imbalance,
                "sizing": sizing,
            },
        )

    def should_exit(self, position: Any, market_state: dict) -> ExitDecision:
        config = dict(getattr(position, "config", None) or {})
        strategy_config = dict(getattr(self, "config", None) or {})
        config.setdefault("take_profit_pct", float(strategy_config.get("take_profit_pct", 7.0)))
        config.setdefault("stop_loss_pct", float(strategy_config.get("stop_loss_pct", 4.0)))
        config.setdefault("max_hold_minutes", float(strategy_config.get("max_hold_minutes", 11.0)))
        position.config = config
        return self.default_exit_check(position, market_state)

    def on_blocked(self, signal, reason: str, context: dict) -> None:
        logger.info("%s blocked: %s market=%s", self.name, reason, getattr(signal, "market_id", "?"))

