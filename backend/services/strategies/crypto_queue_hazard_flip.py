"""Queue-hazard flip strategy for short-term crypto prediction markets.

Detects opportunities directly from ``crypto_update`` worker payloads when
orderflow is one-sided and queue exhaustion is likely, then trades AGAINST
the dominant flow direction. Does not depend on other strategies for signal
generation.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from models import Market, Opportunity
from services.data_events import DataEvent, EventType
from services.quality_filter import QualityFilterOverrides
from services.strategies.base import BaseStrategy, DecisionCheck, ExitDecision, StrategyDecision
from services.trader_orchestrator.strategies.sizing import compute_position_size
from utils.converters import clamp, safe_float, to_confidence, to_float
from utils.signal_helpers import selected_probability, signal_payload

logger = logging.getLogger(__name__)


def _parse_datetime_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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
    market_categories = ["crypto"]
    subscriptions = [EventType.CRYPTO_UPDATE]

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
        "max_markets_per_event": 24,
    }

    @staticmethod
    def _row_market(row: dict[str, Any]) -> Market | None:
        market_id = str(row.get("condition_id") or row.get("id") or "").strip()
        if not market_id:
            return None

        up_price = safe_float(row.get("up_price"), None)
        down_price = safe_float(row.get("down_price"), None)
        if up_price is None or down_price is None:
            return None

        end_date = _parse_datetime_utc(row.get("end_time"))
        token_ids = [
            str(token).strip()
            for token in list(row.get("clob_token_ids") or [])
            if str(token).strip() and len(str(token).strip()) > 20
        ]

        return Market(
            id=market_id,
            condition_id=market_id,
            question=str(row.get("question") or row.get("slug") or market_id),
            slug=str(row.get("slug") or market_id),
            outcome_prices=[float(up_price), float(down_price)],
            liquidity=max(0.0, float(safe_float(row.get("liquidity"), 0.0) or 0.0)),
            end_date=end_date,
            platform="polymarket",
            clob_token_ids=token_ids,
        )

    def _score_market(self, row: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any] | None:
        up_price = safe_float(row.get("up_price"), None)
        down_price = safe_float(row.get("down_price"), None)
        if up_price is None or down_price is None:
            return None
        if not (0.0 < up_price < 1.0 and 0.0 < down_price < 1.0):
            return None

        flow_imbalance = _normalize_signed_ratio(row.get("flow_imbalance") or row.get("orderflow_imbalance"))
        if flow_imbalance is None:
            return None

        min_flow_imbalance = max(0.0, min(1.0, to_float(cfg.get("min_flow_imbalance", 0.35), 0.35)))
        if abs(flow_imbalance) < min_flow_imbalance:
            return None

        recent_move_zscore = safe_float(row.get("recent_move_zscore"), None)
        if recent_move_zscore is None:
            return None

        min_recent_move_zscore = max(0.0, to_float(cfg.get("min_recent_move_zscore", 1.40), 1.40))
        if abs(recent_move_zscore) < min_recent_move_zscore:
            return None

        max_spread_widening_bps = max(0.0, to_float(cfg.get("max_spread_widening_bps", 30.0), 30.0))
        spread_widening_bps = safe_float(row.get("spread_widening_bps"), None)
        if spread_widening_bps is not None and spread_widening_bps > max_spread_widening_bps:
            return None

        max_cancel_rate_30s = max(0.0, min(1.0, to_float(cfg.get("max_cancel_rate_30s", 0.78), 0.78)))
        cancel_rate_30s = _normalize_signed_ratio(row.get("cancel_rate_30s") or row.get("maker_cancel_rate_30s"))
        if cancel_rate_30s is not None:
            cancel_rate_30s = abs(cancel_rate_30s)
        if cancel_rate_30s is not None and cancel_rate_30s > max_cancel_rate_30s:
            return None

        # Direction: AGAINST the flow. Buying pressure (flow > 0) -> buy_no,
        # selling pressure (flow < 0) -> buy_yes.
        if flow_imbalance > 0:
            direction = "buy_no"
            outcome = "NO"
            entry_price = float(down_price)
        else:
            direction = "buy_yes"
            outcome = "YES"
            entry_price = float(up_price)

        # Oracle / price-to-beat for additional edge confirmation
        oracle_price = safe_float(row.get("oracle_price"), None)
        price_to_beat = safe_float(row.get("price_to_beat"), None)
        diff_pct: float | None = None
        oracle_aligned = False
        if oracle_price is not None and price_to_beat is not None and price_to_beat > 0:
            diff_pct = ((oracle_price - price_to_beat) / price_to_beat) * 100.0
            # Oracle says buy_yes when diff_pct > 0, buy_no when diff_pct < 0.
            # Aligned means oracle agrees with the anti-flow direction.
            if direction == "buy_yes" and diff_pct > 0:
                oracle_aligned = True
            elif direction == "buy_no" and diff_pct < 0:
                oracle_aligned = True

        # Edge heuristic based on microstructure signal strength
        edge = abs(flow_imbalance) * 4.0 + abs(recent_move_zscore) * 1.5
        if oracle_aligned and diff_pct is not None:
            edge += abs(diff_pct) * 0.5

        min_edge_percent = max(0.0, to_float(cfg.get("min_edge_percent", 2.0), 2.0))
        if edge < min_edge_percent:
            return None

        confidence = clamp(
            0.48
            + clamp(abs(flow_imbalance) * 0.20, 0, 0.20)
            + clamp(abs(recent_move_zscore) / 5.0 * 0.15, 0, 0.15)
            + (0.05 if oracle_aligned else 0),
            0.44,
            0.90,
        )

        liquidity_usd = max(0.0, float(safe_float(row.get("liquidity"), 0.0) or 0.0))

        score = (
            (edge * 0.55)
            + (confidence * 32.0)
            + (min(1.0, abs(flow_imbalance)) * 8.0)
            + (min(1.0, abs(recent_move_zscore) / 3.0) * 6.0)
        )

        return {
            "direction": direction,
            "outcome": outcome,
            "entry_price": entry_price,
            "edge": float(edge),
            "confidence": float(confidence),
            "flow_imbalance": float(flow_imbalance),
            "recent_move_zscore": float(recent_move_zscore),
            "spread_widening_bps": float(spread_widening_bps) if spread_widening_bps is not None else None,
            "cancel_rate_30s": float(cancel_rate_30s) if cancel_rate_30s is not None else None,
            "oracle_price": float(oracle_price) if oracle_price is not None else None,
            "price_to_beat": float(price_to_beat) if price_to_beat is not None else None,
            "oracle_diff_pct": float(diff_pct) if diff_pct is not None else None,
            "oracle_aligned": oracle_aligned,
            "liquidity_usd": float(liquidity_usd),
            "score": float(score),
        }

    def _build_opportunity(self, row: dict[str, Any], signal: dict[str, Any]) -> Opportunity | None:
        typed_market = self._row_market(row)
        if typed_market is None:
            return None

        strategy_cfg = dict(getattr(self, "config", None) or {})
        direction = str(signal["direction"])
        outcome = str(signal["outcome"])
        entry_price = float(signal["entry_price"])
        token_ids = list(typed_market.clob_token_ids or [])
        token_idx = 0 if direction == "buy_yes" else 1
        token_id = token_ids[token_idx] if len(token_ids) > token_idx else None

        flip_context = {
            "strategy_origin": "crypto_worker",
            "direction": direction,
            "edge": signal["edge"],
            "confidence": signal["confidence"],
            "flow_imbalance": signal["flow_imbalance"],
            "recent_move_zscore": signal["recent_move_zscore"],
            "spread_widening_bps": signal["spread_widening_bps"],
            "cancel_rate_30s": signal["cancel_rate_30s"],
            "oracle_price": signal["oracle_price"],
            "price_to_beat": signal["price_to_beat"],
            "oracle_diff_pct": signal["oracle_diff_pct"],
            "oracle_aligned": signal["oracle_aligned"],
            "liquidity_usd": signal["liquidity_usd"],
            "market_data_age_ms": safe_float(row.get("market_data_age_ms"), None),
            "fetched_at": row.get("fetched_at"),
            "end_time": row.get("end_time"),
        }

        risk_score = clamp(0.55 - (signal["confidence"] - 0.50) * 0.4, 0.15, 0.85)

        positions = [
            {
                "action": "BUY",
                "outcome": outcome,
                "price": entry_price,
                "token_id": token_id,
                "price_policy": "taker_limit",
                "time_in_force": "IOC",
                "_flip": flip_context,
            }
        ]

        opp = self.create_opportunity(
            title=f"Queue Hazard Flip: {outcome} @ {entry_price:.3f}",
            description=(
                f"flow_imb={signal['flow_imbalance']:+.3f}, "
                f"move_z={signal['recent_move_zscore']:+.2f}, "
                f"edge={signal['edge']:.2f}%"
            ),
            total_cost=entry_price,
            expected_payout=entry_price + (signal["edge"] / 100.0),
            markets=[typed_market],
            positions=positions,
            is_guaranteed=False,
            skip_fee_model=True,
            custom_roi_percent=signal["edge"],
            custom_risk_score=risk_score,
            confidence=signal["confidence"],
            min_position_size=max(1.0, to_float(strategy_cfg.get("base_size_usd", 20.0), 20.0)),
        )
        if opp is None:
            return None

        opp.risk_factors = [
            f"Anti-flow entry (flow_imbalance={signal['flow_imbalance']:+.3f})",
            f"Recent move z-score={signal['recent_move_zscore']:+.2f}",
            f"Edge={signal['edge']:.2f}%",
        ]
        opp.strategy_context = {
            "source_key": "crypto",
            "strategy_slug": self.strategy_type,
            **flip_context,
        }
        return opp

    def _detect_from_rows(self, rows: list[dict[str, Any]]) -> list[Opportunity]:
        cfg = dict(self.default_config)
        cfg.update(getattr(self, "config", {}) or {})

        candidates: list[tuple[float, Opportunity]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            signal = self._score_market(row, cfg)
            if signal is None:
                continue
            opp = self._build_opportunity(row, signal)
            if opp is None:
                continue
            candidates.append((float(signal["score"]), opp))

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

    async def on_event(self, event: DataEvent) -> list[Opportunity]:
        if event.event_type != EventType.CRYPTO_UPDATE:
            return []
        rows = event.payload.get("markets") if isinstance(event.payload, dict) else None
        if not isinstance(rows, list) or not rows:
            return []
        return self._detect_from_rows(rows)

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
