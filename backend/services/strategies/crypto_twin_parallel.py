"""Twin-leg parallel crypto strategy.

Executes YES and NO legs simultaneously from a single bot using one execution
plan override, so both orders are placed in the same session wave.
"""

from __future__ import annotations

import logging
from typing import Any

from services.data_events import DataEvent
from services.quality_filter import QualityFilterOverrides
from services.strategies.base import BaseStrategy, DecisionCheck, ExitDecision, StrategyDecision
from services.trader_orchestrator.strategies.sizing import compute_position_size
from utils.converters import safe_float, to_confidence, to_float
from utils.signal_helpers import signal_payload

logger = logging.getLogger(__name__)


def _normalize_tif(value: Any, default: str = "GTC") -> str:
    tif = str(value or default).strip().upper()
    if tif in {"GTC", "IOC", "FOK"}:
        return tif
    return default


def _extract_token_id(side: str, live_market: dict[str, Any], payload: dict[str, Any]) -> str:
    side_key = side.lower()
    live_key = f"{side_key}_token_id"
    payload_key = f"{side_key}_token_id"
    token_id = str(live_market.get(live_key) or payload.get(payload_key) or "").strip()
    if token_id:
        return token_id
    token_ids = payload.get("token_ids")
    if not isinstance(token_ids, list):
        token_ids = live_market.get("token_ids")
    if isinstance(token_ids, list) and len(token_ids) >= 2:
        idx = 0 if side_key == "yes" else 1
        token_id = str(token_ids[idx] or "").strip()
        if token_id:
            return token_id
    return ""


def _extract_price(side: str, live_market: dict[str, Any], payload: dict[str, Any]) -> float | None:
    side_key = side.lower()
    direct_live = safe_float(live_market.get(f"{side_key}_price"), None)
    if direct_live is not None and direct_live > 0.0:
        return float(direct_live)
    direct_payload = safe_float(payload.get(f"{side_key}_price"), None)
    if direct_payload is not None and direct_payload > 0.0:
        return float(direct_payload)
    if side_key == "yes":
        fallback = safe_float(live_market.get("live_yes_price"), safe_float(payload.get("up_price"), None))
    else:
        fallback = safe_float(live_market.get("live_no_price"), safe_float(payload.get("down_price"), None))
    if fallback is not None and fallback > 0.0:
        return float(fallback)
    return None


class CryptoTwinParallelStrategy(BaseStrategy):
    strategy_type = "crypto_twin_parallel"
    name = "Crypto Twin Parallel"
    description = "Places YES+NO legs simultaneously via one execution session."
    mispricing_type = "within_market"
    source_key = "crypto"
    market_categories = ["crypto"]
    subscriptions = ["crypto_update"]
    accepted_signal_strategy_types = ["btc_eth_highfreq"]

    quality_filter_overrides = QualityFilterOverrides(
        min_roi=0.5,
        max_resolution_months=0.1,
    )

    default_config = {
        "min_edge_percent": 0.8,
        "min_confidence": 0.40,
        "max_combined_entry_price": 0.985,
        "max_leg_entry_price": 0.88,
        "min_liquidity_usd": 700.0,
        "base_size_usd": 24.0,
        "max_size_usd": 220.0,
        "sizing_policy": "adaptive",
        "yes_notional_weight": 1.0,
        "no_notional_weight": 1.0,
        "maker_price_offset_bps": 0.0,
        "execution_policy": "PARALLEL_MAKER",
        "time_in_force": "GTC",
        "max_unhedged_notional_usd": 2.0,
        "hedge_timeout_seconds": 20,
        "session_timeout_seconds": 300,
        "max_reprice_attempts": 2,
        "pair_lock": True,
        "take_profit_pct": 2.0,
        "stop_loss_pct": 7.0,
        "max_hold_minutes": 18.0,
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

        min_edge = max(0.0, to_float(params.get("min_edge_percent", 0.8), 0.8))
        min_conf = to_confidence(params.get("min_confidence", 0.40), 0.40)
        max_combined_entry_price = min(1.0, max(0.01, to_float(params.get("max_combined_entry_price", 0.985), 0.985)))
        max_leg_entry_price = min(1.0, max(0.01, to_float(params.get("max_leg_entry_price", 0.88), 0.88)))
        min_liquidity_usd = max(0.0, to_float(params.get("min_liquidity_usd", 700.0), 700.0))

        base_size = max(2.0, to_float(params.get("base_size_usd", 24.0), 24.0))
        max_size = max(base_size, to_float(params.get("max_size_usd", 220.0), 220.0))
        sizing_policy = str(params.get("sizing_policy", "adaptive") or "adaptive")
        yes_weight = max(0.0001, to_float(params.get("yes_notional_weight", 1.0), 1.0))
        no_weight = max(0.0001, to_float(params.get("no_notional_weight", 1.0), 1.0))
        maker_price_offset_bps = max(0.0, min(100.0, to_float(params.get("maker_price_offset_bps", 0.0), 0.0)))

        execution_policy = "PARALLEL_MAKER"
        time_in_force = _normalize_tif(params.get("time_in_force"), "GTC")
        max_unhedged_notional_usd = max(0.0, to_float(params.get("max_unhedged_notional_usd", 2.0), 2.0))
        hedge_timeout_seconds = max(1, int(to_float(params.get("hedge_timeout_seconds", 20), 20)))
        session_timeout_seconds = max(1, int(to_float(params.get("session_timeout_seconds", 300), 300)))
        max_reprice_attempts = max(0, int(to_float(params.get("max_reprice_attempts", 2), 2)))
        pair_lock = bool(params.get("pair_lock", True))

        source = str(getattr(signal, "source", "") or "").strip().lower()
        signal_type = str(getattr(signal, "signal_type", "") or "").strip().lower()
        edge = max(0.0, to_float(getattr(signal, "edge_percent", 0.0), 0.0))
        confidence = to_confidence(getattr(signal, "confidence", 0.0), 0.0)
        liquidity = max(
            0.0,
            to_float(
                live_market.get("liquidity_usd"),
                to_float(payload.get("liquidity_usd"), to_float(getattr(signal, "liquidity", 0.0), 0.0)),
            ),
        )

        market_id = str(getattr(signal, "market_id", "") or "").strip()
        market_question = str(getattr(signal, "market_question", "") or "").strip()
        yes_token_id = _extract_token_id("yes", live_market, payload)
        no_token_id = _extract_token_id("no", live_market, payload)
        yes_price = _extract_price("yes", live_market, payload)
        no_price = _extract_price("no", live_market, payload)
        combined_entry = (yes_price + no_price) if yes_price is not None and no_price is not None else None

        source_ok = source == "crypto"
        origin_ok = bool(payload.get("strategy_origin") == "crypto_worker") or signal_type.startswith("crypto_worker")
        edge_ok = edge >= min_edge
        confidence_ok = confidence >= min_conf
        tokens_ok = bool(yes_token_id and no_token_id)
        prices_ok = yes_price is not None and no_price is not None and yes_price > 0.0 and no_price > 0.0
        combined_price_ok = combined_entry is not None and combined_entry <= max_combined_entry_price
        leg_price_ok = (
            yes_price is not None and no_price is not None and yes_price <= max_leg_entry_price and no_price <= max_leg_entry_price
        )
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
            DecisionCheck("tokens", "YES/NO token ids available", tokens_ok, detail="Both token ids must be present."),
            DecisionCheck("prices", "YES/NO prices available", prices_ok, detail="Both leg entry prices required."),
            DecisionCheck(
                "combined_price",
                "Combined entry cap",
                combined_price_ok,
                score=combined_entry,
                detail=f"max={max_combined_entry_price:.3f}",
            ),
            DecisionCheck(
                "leg_price",
                "Per-leg entry cap",
                leg_price_ok,
                score=max(yes_price or 0.0, no_price or 0.0),
                detail=f"max={max_leg_entry_price:.3f}",
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
            (edge * 0.40)
            + (confidence * 30.0)
            + (min(1.0, liquidity / 25_000.0) * 6.0)
            + (2.5 if combined_price_ok else 0.0)
        )

        if not all(check.passed for check in checks):
            return StrategyDecision(
                decision="skipped",
                reason="Twin-parallel filters not met",
                score=score,
                checks=checks,
                payload={
                    "edge": edge,
                    "confidence": confidence,
                    "yes_token_id": yes_token_id,
                    "no_token_id": no_token_id,
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "combined_entry_price": combined_entry,
                },
            )

        sizing = compute_position_size(
            base_size_usd=base_size,
            max_size_usd=max_size,
            edge_percent=edge,
            confidence=confidence,
            sizing_policy=sizing_policy,
            probability=0.5,
            entry_price=combined_entry / 2.0 if combined_entry is not None and combined_entry > 0.0 else None,
            liquidity_usd=liquidity,
            liquidity_cap_fraction=0.10,
            min_size_usd=2.0,
        )
        size_usd = float(sizing.get("size_usd") or base_size)

        if maker_price_offset_bps > 0.0 and yes_price is not None and no_price is not None:
            offset_ratio = maker_price_offset_bps / 10_000.0
            yes_exec_price = max(0.01, min(0.99, yes_price * (1.0 - offset_ratio)))
            no_exec_price = max(0.01, min(0.99, no_price * (1.0 - offset_ratio)))
        else:
            yes_exec_price = yes_price if yes_price is not None else 0.5
            no_exec_price = no_price if no_price is not None else 0.5

        execution_plan_override = {
            "plan_id": f"twin_parallel:{market_id or 'market'}",
            "policy": execution_policy,
            "time_in_force": time_in_force,
            "constraints": {
                "max_unhedged_notional_usd": float(max_unhedged_notional_usd),
                "hedge_timeout_seconds": int(hedge_timeout_seconds),
                "session_timeout_seconds": int(session_timeout_seconds),
                "max_reprice_attempts": int(max_reprice_attempts),
                "pair_lock": bool(pair_lock),
                "leg_fill_tolerance_ratio": 0.02,
            },
            "legs": [
                {
                    "leg_id": "yes_leg",
                    "market_id": market_id,
                    "market_question": market_question,
                    "token_id": yes_token_id,
                    "side": "buy",
                    "outcome": "yes",
                    "limit_price": float(yes_exec_price),
                    "price_policy": "maker_limit",
                    "time_in_force": time_in_force,
                    "notional_weight": float(yes_weight),
                    "min_fill_ratio": 0.0,
                    "metadata": {"twin_leg": "yes"},
                },
                {
                    "leg_id": "no_leg",
                    "market_id": market_id,
                    "market_question": market_question,
                    "token_id": no_token_id,
                    "side": "buy",
                    "outcome": "no",
                    "limit_price": float(no_exec_price),
                    "price_policy": "maker_limit",
                    "time_in_force": time_in_force,
                    "notional_weight": float(no_weight),
                    "min_fill_ratio": 0.0,
                    "metadata": {"twin_leg": "no"},
                },
            ],
            "metadata": {
                "strategy": self.strategy_type,
                "combined_entry_price": combined_entry,
                "maker_price_offset_bps": maker_price_offset_bps,
            },
        }

        return StrategyDecision(
            decision="selected",
            reason="Twin-parallel execution selected",
            score=score,
            size_usd=size_usd,
            checks=checks,
            payload={
                "edge": edge,
                "confidence": confidence,
                "yes_token_id": yes_token_id,
                "no_token_id": no_token_id,
                "yes_price": yes_price,
                "no_price": no_price,
                "combined_entry_price": combined_entry,
                "sizing": sizing,
                "execution_plan_override": execution_plan_override,
            },
        )

    def should_exit(self, position: Any, market_state: dict) -> ExitDecision:
        config = dict(getattr(position, "config", None) or {})
        strategy_config = dict(getattr(self, "config", None) or {})
        config.setdefault("take_profit_pct", float(strategy_config.get("take_profit_pct", 2.0)))
        config.setdefault("stop_loss_pct", float(strategy_config.get("stop_loss_pct", 7.0)))
        config.setdefault("max_hold_minutes", float(strategy_config.get("max_hold_minutes", 18.0)))
        position.config = config
        return self.default_exit_check(position, market_state)

    def on_blocked(self, signal, reason: str, context: dict) -> None:
        logger.info("%s blocked: %s market=%s", self.name, reason, getattr(signal, "market_id", "?"))
