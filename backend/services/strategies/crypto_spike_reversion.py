"""Crypto spike-reversion strategy.

Detects opportunities directly from ``crypto_update`` worker payloads where a
sharp short-horizon price spike has occurred and a mean-reversion entry is
favorable.  Uses 5m/30m/2h price movement context to validate direction
alignment and reversion shape before sizing with Kelly or other configurable
policies.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from models import Market, Opportunity
from services.data_events import DataEvent, EventType
from services.quality_filter import QualityFilterOverrides
from services.strategies.base import BaseStrategy, DecisionCheck, ExitDecision, StrategyDecision, _trader_size_limits
from services.strategies.crypto_strategy_utils import build_binary_crypto_market, parse_datetime_utc
from services.strategies.reversion_helpers import direction_opposes_impulse, market_move_pct, reversion_shape_ok
from services.trader_orchestrator.strategies.sizing import compute_position_size
from utils.converters import clamp, safe_float, to_confidence, to_float
from utils.signal_helpers import selected_probability, signal_payload

logger = logging.getLogger(__name__)


class CryptoSpikeReversionStrategy(BaseStrategy):
    """Spike-reversion detection and execution using live 5m/30m/2h movement context."""

    strategy_type = "crypto_spike_reversion"
    name = "Crypto Spike Reversion"
    description = "Spike-reversion detection using live 5m/30m/2h movement context"
    mispricing_type = "within_market"
    source_key = "crypto"
    market_categories = ["crypto"]
    subscriptions = [EventType.CRYPTO_UPDATE]
    supports_entry_take_profit_exit = True
    default_open_order_timeout_seconds = 20.0

    quality_filter_overrides = QualityFilterOverrides(
        min_roi=1.0,
        max_resolution_months=0.1,
    )

    default_config = {
        "min_edge_percent": 2.8,
        "min_confidence": 0.44,
        "min_abs_move_5m": 1.8,
        "max_abs_move_2h": 14.0,
        "require_reversion_shape": True,
        "min_order_size_usd": 2.0,
        "sizing_policy": "kelly",
        "kelly_fractional_scale": 0.45,
        "take_profit_pct": 8.0,
        "stop_loss_pct": 4.0,
        "max_hold_minutes": 8.0,
        "liquidity_cap_fraction": 0.07,
        "min_liquidity_usd": 2000.0,
        "max_entry_price": 0.92,
        "max_markets_per_event": 24,
    }

    def __init__(self) -> None:
        super().__init__()
        self.min_profit = 0.0

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_market(row: dict[str, Any]) -> Market | None:
        return build_binary_crypto_market(row)

    # ------------------------------------------------------------------
    # Signal scoring — spike-reversion specific
    # ------------------------------------------------------------------

    def _score_market(self, row: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any] | None:
        up_price = safe_float(row.get("up_price"), None)
        down_price = safe_float(row.get("down_price"), None)
        if up_price is None or down_price is None:
            return None

        move_5m = safe_float(row.get("move_5m_percent"), safe_float(row.get("move_5m_pct"), None))
        move_30m = safe_float(row.get("move_30m_percent"), safe_float(row.get("move_30m_pct"), None))
        move_2h = safe_float(row.get("move_2h_percent"), safe_float(row.get("move_2h_pct"), None))

        if move_5m is None:
            return None

        min_abs_move_5m = max(0.2, to_float(cfg.get("min_abs_move_5m", 1.8), 1.8))
        max_abs_move_2h = max(min_abs_move_5m, to_float(cfg.get("max_abs_move_2h", 14.0), 14.0))
        require_reversion_shape = bool(cfg.get("require_reversion_shape", True))

        if abs(move_5m) < min_abs_move_5m:
            return None

        # Reversion shape: short-horizon impulse dominates the 30m trend
        shape_ok = reversion_shape_ok(
            move_5m,
            move_30m,
            move_2h,
            require_shape=require_reversion_shape,
            max_abs_move_2h=max_abs_move_2h,
        )

        if require_reversion_shape and not shape_ok:
            return None

        # Direction OPPOSES the spike: spike up -> buy_no, spike down -> buy_yes
        if move_5m > 0:
            direction = "buy_no"
            outcome = "NO"
            selected_price = float(down_price)
        else:
            direction = "buy_yes"
            outcome = "YES"
            selected_price = float(up_price)

        max_entry_price = clamp(to_float(cfg.get("max_entry_price", 0.92), 0.92), 0.05, 0.99)
        if selected_price <= 0.0 or selected_price >= 1.0 or selected_price > max_entry_price:
            return None

        oracle_price = safe_float(row.get("oracle_price"), None)
        price_to_beat = safe_float(row.get("price_to_beat"), None)

        # Edge: base on spike magnitude; add oracle component if available
        if oracle_price is not None and price_to_beat is not None and price_to_beat > 0:
            diff_pct = abs(((oracle_price - price_to_beat) / price_to_beat) * 100.0)
            edge = abs(move_5m) * 0.6 + diff_pct
        else:
            diff_pct = 0.0
            edge = abs(move_5m) * 0.6

        # Elapsed ratio from end_time
        end_date = parse_datetime_utc(row.get("end_time"))
        elapsed_ratio = 0.5  # default mid-window
        if end_date is not None:
            seconds_left = max(0.0, (end_date - datetime.now(timezone.utc)).total_seconds())
            # Assume 5m (300s) timeframe for elapsed ratio
            elapsed_ratio = clamp(1.0 - (seconds_left / 300.0), 0.0, 1.0)

        confidence = clamp(
            0.50
            + clamp(abs(move_5m) / 12.0, 0, 0.20)
            + (0.10 if shape_ok else 0)
            + clamp(elapsed_ratio * 0.10, 0, 0.10),
            0.44,
            0.90,
        )

        min_confidence = to_confidence(cfg.get("min_confidence", 0.44), 0.44)
        if confidence < min_confidence:
            return None

        liquidity = max(0.0, float(safe_float(row.get("liquidity"), 0.0) or 0.0))
        min_liquidity_usd = max(0.0, to_float(cfg.get("min_liquidity_usd", 2000.0), 2000.0))
        if liquidity < min_liquidity_usd:
            return None

        risk_score = clamp(
            0.60
            - (min(1.0, abs(move_5m) / 8.0) * 0.20)
            - (0.08 if shape_ok else 0)
            + (0.10 if diff_pct < 0.1 else 0),
            0.15,
            0.85,
        )

        score = (
            (edge * 0.55)
            + (confidence * 34.0)
            + (min(1.0, abs(move_5m) / 8.0) * 8.0)
            + (min(1.0, liquidity / 20000.0) * 4.0)
        )

        net_edge_percent = max(0.0, edge - 0.25)  # rough fee/slippage deduction
        target_exit_price = clamp(selected_price + (net_edge_percent / 100.0), selected_price + 0.0001, 0.999)

        return {
            "direction": direction,
            "outcome": outcome,
            "selected_price": selected_price,
            "move_5m": float(move_5m),
            "move_30m": float(move_30m) if move_30m is not None else None,
            "move_2h": float(move_2h) if move_2h is not None else None,
            "oracle_price": float(oracle_price) if oracle_price is not None else None,
            "price_to_beat": float(price_to_beat) if price_to_beat is not None else None,
            "oracle_diff_pct": float(diff_pct),
            "edge": float(edge),
            "net_edge_percent": float(net_edge_percent),
            "confidence": float(confidence),
            "risk_score": float(risk_score),
            "shape_ok": shape_ok,
            "elapsed_ratio": float(elapsed_ratio),
            "liquidity_usd": float(liquidity),
            "target_exit_price": float(target_exit_price),
            "score": float(score),
        }

    # ------------------------------------------------------------------
    # Opportunity builder
    # ------------------------------------------------------------------

    def _build_opportunity(self, row: dict[str, Any], signal: dict[str, Any]) -> Opportunity | None:
        typed_market = self._row_market(row)
        if typed_market is None:
            return None

        strategy_cfg = dict(getattr(self, "config", None) or {})
        direction = str(signal["direction"])
        outcome = str(signal["outcome"])
        selected_price = float(signal["selected_price"])
        token_ids = list(typed_market.clob_token_ids or [])
        token_idx = 0 if direction == "buy_yes" else 1
        token_id = token_ids[token_idx] if len(token_ids) > token_idx else None

        reversion_context = {
            "strategy_origin": "crypto_worker",
            "direction": direction,
            "move_5m": signal["move_5m"],
            "move_30m": signal["move_30m"],
            "move_2h": signal["move_2h"],
            "oracle_price": signal["oracle_price"],
            "price_to_beat": signal["price_to_beat"],
            "oracle_diff_pct": signal["oracle_diff_pct"],
            "edge": signal["edge"],
            "net_edge_percent": signal["net_edge_percent"],
            "confidence": signal["confidence"],
            "risk_score": signal["risk_score"],
            "shape_ok": signal["shape_ok"],
            "elapsed_ratio": signal["elapsed_ratio"],
            "liquidity_usd": signal["liquidity_usd"],
            "target_exit_price": signal["target_exit_price"],
            "market_data_age_ms": safe_float(row.get("market_data_age_ms"), None),
            "fetched_at": row.get("fetched_at"),
            "start_time": row.get("start_time"),
            "end_time": row.get("end_time"),
            "regime": row.get("regime"),
            "regime_params": row.get("regime_params"),
        }

        positions = [
            {
                "action": "BUY",
                "outcome": outcome,
                "price": selected_price,
                "token_id": token_id,
                "price_policy": "taker_limit",
                "time_in_force": "IOC",
                "_reversion": reversion_context,
            }
        ]

        opp = self.create_opportunity(
            title=f"Spike Reversion: {outcome} @ {selected_price:.3f} (5m={signal['move_5m']:+.2f}%)",
            description=(
                f"Mean-reversion after {signal['move_5m']:+.2f}% 5m spike, "
                f"edge {signal['edge']:.2f}%, conf {signal['confidence']:.2f}"
            ),
            total_cost=selected_price,
            expected_payout=signal["target_exit_price"],
            markets=[typed_market],
            positions=positions,
            is_guaranteed=False,
            skip_fee_model=True,
            custom_roi_percent=signal["net_edge_percent"],
            custom_risk_score=signal["risk_score"],
            confidence=signal["confidence"],
            min_liquidity_hard=max(100.0, to_float(strategy_cfg.get("min_liquidity_usd", 2000.0), 2000.0)),
            min_position_size=max(1.0, to_float(strategy_cfg.get("min_order_size_usd", 2.0), 2.0)),
        )
        if opp is None:
            return None

        opp.risk_factors = [
            f"Spike reversion (5m={signal['move_5m']:+.2f}%)",
            f"Shape {'valid' if signal['shape_ok'] else 'weak'} (30m={signal['move_30m']}, 2h={signal['move_2h']})",
            f"Edge={signal['edge']:.2f}%, net={signal['net_edge_percent']:.2f}%",
        ]
        opp.strategy_context = {
            "source_key": "crypto",
            "strategy_slug": self.strategy_type,
            **reversion_context,
        }
        return opp

    def _rejection_reason(self, row: dict[str, Any], cfg: dict[str, Any]) -> str:
        up_price = safe_float(row.get("up_price"), None)
        down_price = safe_float(row.get("down_price"), None)
        if up_price is None or down_price is None:
            return "missing_prices"

        move_5m = safe_float(row.get("move_5m_percent"), safe_float(row.get("move_5m_pct"), None))
        move_30m = safe_float(row.get("move_30m_percent"), safe_float(row.get("move_30m_pct"), None))
        move_2h = safe_float(row.get("move_2h_percent"), safe_float(row.get("move_2h_pct"), None))
        if move_5m is None:
            return "missing_move_5m"

        min_abs_move_5m = max(0.2, to_float(cfg.get("min_abs_move_5m", 1.8), 1.8))
        if abs(move_5m) < min_abs_move_5m:
            return "move_below_threshold"

        max_abs_move_2h = max(min_abs_move_5m, to_float(cfg.get("max_abs_move_2h", 14.0), 14.0))
        require_reversion_shape = bool(cfg.get("require_reversion_shape", True))
        shape_ok = reversion_shape_ok(
            move_5m,
            move_30m,
            move_2h,
            require_shape=require_reversion_shape,
            max_abs_move_2h=max_abs_move_2h,
        )
        if require_reversion_shape and not shape_ok:
            return "shape_invalid"

        selected_price = float(down_price if move_5m > 0 else up_price)
        max_entry_price = clamp(to_float(cfg.get("max_entry_price", 0.92), 0.92), 0.05, 0.99)
        if selected_price <= 0.0 or selected_price >= 1.0:
            return "invalid_entry_price"
        if selected_price > max_entry_price:
            return "entry_price_too_high"

        oracle_price = safe_float(row.get("oracle_price"), None)
        price_to_beat = safe_float(row.get("price_to_beat"), None)
        if oracle_price is not None and price_to_beat is not None and price_to_beat > 0:
            diff_pct = abs(((oracle_price - price_to_beat) / price_to_beat) * 100.0)
            edge = abs(move_5m) * 0.6 + diff_pct
        else:
            diff_pct = 0.0
            edge = abs(move_5m) * 0.6

        end_date = parse_datetime_utc(row.get("end_time"))
        elapsed_ratio = 0.5
        if end_date is not None:
            seconds_left = max(0.0, (end_date - datetime.now(timezone.utc)).total_seconds())
            elapsed_ratio = clamp(1.0 - (seconds_left / 300.0), 0.0, 1.0)
        confidence = clamp(
            0.50
            + clamp(abs(move_5m) / 12.0, 0, 0.20)
            + (0.10 if shape_ok else 0)
            + clamp(elapsed_ratio * 0.10, 0, 0.10),
            0.44,
            0.90,
        )
        min_confidence = to_confidence(cfg.get("min_confidence", 0.44), 0.44)
        if confidence < min_confidence:
            return "confidence_too_low"

        liquidity = max(0.0, float(safe_float(row.get("liquidity"), 0.0) or 0.0))
        min_liquidity_usd = max(0.0, to_float(cfg.get("min_liquidity_usd", 2000.0), 2000.0))
        if liquidity < min_liquidity_usd:
            return "low_liquidity"

        min_edge_percent = max(0.0, to_float(cfg.get("min_edge_percent", 2.8), 2.8))
        if edge < min_edge_percent:
            return "edge_too_small"
        return "filtered"

    # ------------------------------------------------------------------
    # Detection from raw crypto rows
    # ------------------------------------------------------------------

    def _detect_from_rows(self, rows: list[dict[str, Any]]) -> list[Opportunity]:
        cfg = dict(self.default_config)
        cfg.update(getattr(self, "config", {}) or {})

        candidates: list[tuple[float, Opportunity]] = []
        rejection_counts: dict[str, int] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            signal = self._score_market(row, cfg)
            if signal is None:
                reason = self._rejection_reason(row, cfg)
                rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
                continue
            opp = self._build_opportunity(row, signal)
            if opp is None:
                rejection_counts["invalid_opportunity"] = rejection_counts.get("invalid_opportunity", 0) + 1
                continue
            candidates.append((float(signal["score"]), opp))

        top_rejections = sorted(rejection_counts.items(), key=lambda item: item[1], reverse=True)[:4]
        message_parts = [f"Scanned {len(rows)} markets, {len(candidates)} signals"]
        if top_rejections:
            message_parts.append(
                "rejected: " + ", ".join(f"{count} {reason}" for reason, count in top_rejections)
            )
        self._filter_diagnostics = {
            "strategy_key": self.strategy_type,
            "scanned_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "markets_scanned": len(rows),
            "signals_emitted": len(candidates),
            "rejections": rejection_counts,
            "message": " \u2014 ".join(message_parts),
            "summary": {f"rejected_{reason}": count for reason, count in rejection_counts.items()},
        }

        if not candidates:
            return []

        candidates.sort(key=lambda item: item[0], reverse=True)
        max_markets_per_event = max(1, int(to_float(cfg.get("max_markets_per_event", 24), 24.0)))

        out: list[Opportunity] = []
        seen_markets: set[str] = set()
        for _, opp in candidates:
            market_id = str((opp.markets or [{}])[0].get("id") or "")
            if market_id in seen_markets:
                continue
            seen_markets.add(market_id)
            out.append(opp)
            if len(out) >= max_markets_per_event:
                break
        return out

    # ------------------------------------------------------------------
    # Event handler — now generates its own signals
    # ------------------------------------------------------------------

    async def on_event(self, event: DataEvent) -> list[Opportunity]:
        if event.event_type != EventType.CRYPTO_UPDATE:
            return []
        rows = event.payload.get("markets") if isinstance(event.payload, dict) else None
        if not isinstance(rows, list) or not rows:
            return []
        return self._detect_from_rows(rows)

    # ------------------------------------------------------------------
    # Evaluate (unchanged)
    # ------------------------------------------------------------------

    def evaluate(self, signal: Any, context: dict[str, Any]) -> StrategyDecision:
        params = context.get("params") or {}
        payload = signal_payload(signal)
        live_market = context.get("live_market")
        if not isinstance(live_market, dict):
            live_market = payload.get("live_market")
        if not isinstance(live_market, dict):
            live_market = {}

        min_edge = to_float(params.get("min_edge_percent", 2.8), 2.8)
        min_conf = to_confidence(params.get("min_confidence", 0.44), 0.44)
        min_abs_move_5m = max(0.2, to_float(params.get("min_abs_move_5m", 1.8), 1.8))
        max_abs_move_2h = max(min_abs_move_5m, to_float(params.get("max_abs_move_2h", 14.0), 14.0))
        require_reversion_shape = bool(params.get("require_reversion_shape", True))

        base_size, max_size = _trader_size_limits(context)
        sizing_policy = str(params.get("sizing_policy", "kelly") or "kelly")
        kelly_fractional_scale = to_float(params.get("kelly_fractional_scale", 0.45), 0.45)

        source = str(getattr(signal, "source", "") or "").strip().lower()
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        signal_type = str(getattr(signal, "signal_type", "") or "").strip().lower()

        edge = max(0.0, to_float(getattr(signal, "edge_percent", 0.0), 0.0))
        confidence = to_confidence(getattr(signal, "confidence", 0.0), 0.0)
        liquidity = max(0.0, to_float(getattr(signal, "liquidity", 0.0), 0.0))

        move_5m = market_move_pct(live_market, payload, "move_5m")
        move_30m = market_move_pct(live_market, payload, "move_30m")
        move_2h = market_move_pct(live_market, payload, "move_2h")

        # Direction alignment: signal direction must oppose the spike
        direction_alignment = direction_opposes_impulse(direction, move_5m, min_abs_move_5m)

        # Reversion shape: short-horizon impulse dominates the 30m trend
        shape_ok = reversion_shape_ok(
            move_5m,
            move_30m,
            move_2h,
            require_shape=require_reversion_shape,
            max_abs_move_2h=max_abs_move_2h,
        )

        origin_ok = bool(payload.get("strategy_origin") == "crypto_worker") or signal_type.startswith("crypto_worker")

        checks = [
            DecisionCheck("source", "Crypto source", source == "crypto", detail="Requires source=crypto."),
            DecisionCheck(
                "origin", "Crypto worker origin", origin_ok, detail="Requires worker-generated crypto signal."
            ),
            DecisionCheck("edge", "Edge threshold", edge >= min_edge, score=edge, detail=f"min={min_edge:.2f}"),
            DecisionCheck(
                "confidence",
                "Confidence threshold",
                confidence >= min_conf,
                score=confidence,
                detail=f"min={min_conf:.2f}",
            ),
            DecisionCheck(
                "direction_alignment",
                "Direction aligns with spike",
                direction_alignment,
                score=move_5m,
                detail=f"|move_5m| >= {min_abs_move_5m:.2f}% and opposes impulse",
            ),
            DecisionCheck(
                "reversion_shape",
                "Reversion shape",
                shape_ok,
                score=move_30m,
                detail=f"2h cap <= {max_abs_move_2h:.2f}%",
            ),
        ]

        score = (
            (edge * 0.55)
            + (confidence * 34.0)
            + (min(1.0, abs(move_5m or 0.0) / 8.0) * 8.0)
            + (min(1.0, liquidity / 20000.0) * 4.0)
        )

        if not all(check.passed for check in checks):
            return StrategyDecision(
                decision="skipped",
                reason="Crypto spike-reversion filters not met",
                score=score,
                checks=checks,
                payload={
                    "edge": edge,
                    "confidence": confidence,
                    "move_5m": move_5m,
                    "move_30m": move_30m,
                    "move_2h": move_2h,
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
            kelly_fractional_scale=kelly_fractional_scale,
            liquidity_usd=liquidity,
            liquidity_cap_fraction=0.07,
        )

        return StrategyDecision(
            decision="selected",
            reason="Crypto spike-reversion signal selected",
            score=score,
            size_usd=float(sizing["size_usd"]),
            checks=checks,
            payload={
                "edge": edge,
                "confidence": confidence,
                "move_5m": move_5m,
                "move_30m": move_30m,
                "move_2h": move_2h,
                "sizing": sizing,
            },
        )

    # ------------------------------------------------------------------
    # Exit
    # ------------------------------------------------------------------

    def should_exit(self, position: Any, market_state: dict) -> ExitDecision:
        """Tight exit for spike reversion -- short hold with explicit TP default."""
        config = getattr(position, "config", None) or {}
        config = dict(config)
        configured_tp = (getattr(self, "config", None) or {}).get("take_profit_pct", 8.0)
        try:
            default_tp = float(configured_tp)
        except (TypeError, ValueError):
            default_tp = 8.0
        config.setdefault("take_profit_pct", default_tp)
        position.config = config
        return self.default_exit_check(position, market_state)

    # ------------------------------------------------------------------
    # Platform gate hooks
    # ------------------------------------------------------------------

    def on_blocked(self, signal, reason: str, context: dict) -> None:
        logger.info("%s: signal blocked — %s (market=%s)", self.name, reason, getattr(signal, "market_id", "?"))

    def on_size_capped(self, original_size: float, capped_size: float, reason: str) -> None:
        logger.info("%s: size capped $%.0f → $%.0f — %s", self.name, original_size, capped_size, reason)
