"""Entropy-weighted maker strategy for crypto binary markets.

Detects opportunities directly from ``crypto_update`` worker payloads by
scoring binary entropy combined with spread quality and cancellation rate.
Does not depend on other crypto strategies for signal generation.
"""

from __future__ import annotations

import logging
import math
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
    subscriptions = [EventType.CRYPTO_UPDATE]
    supports_entry_take_profit_exit = True
    default_open_order_timeout_seconds = 20.0

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
        "max_entry_price": 0.92,
        "max_markets_per_event": 24,
        "base_size_usd": 16.0,
        "max_size_usd": 130.0,
        "min_order_size_usd": 2.0,
        "sizing_policy": "adaptive",
        "take_profit_pct": 6.5,
        "stop_loss_pct": 4.0,
        "max_hold_minutes": 16.0,
    }

    def __init__(self) -> None:
        super().__init__()
        self.min_profit = 0.0

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

        # Binary entropy
        total = up_price + down_price
        prob_yes = up_price / total if total > 0 else 0.5
        entropy = _probability_entropy(prob_yes)

        min_entropy = max(0.0, min(1.0, to_float(cfg.get("min_entropy", 0.82), 0.82)))
        if entropy < min_entropy:
            return None

        # Spread
        spread = safe_float(row.get("spread"), None)
        min_spread_pct = max(0.0, min(1.0, to_float(cfg.get("min_spread_pct", 0.006), 0.006)))
        max_spread_pct = max(min_spread_pct, min(1.0, to_float(cfg.get("max_spread_pct", 0.065), 0.065)))
        if spread is None or not (min_spread_pct <= spread <= max_spread_pct):
            return None

        # Cancel rate
        cancel_rate_30s = _normalize_ratio(row.get("cancel_rate_30s") or row.get("maker_cancel_rate_30s"))
        max_cancel_rate_30s = max(0.0, min(1.0, to_float(cfg.get("max_cancel_rate_30s", 0.75), 0.75)))
        if cancel_rate_30s is not None and cancel_rate_30s > max_cancel_rate_30s:
            return None

        # Liquidity
        liquidity = max(0.0, float(safe_float(row.get("liquidity"), 0.0) or 0.0))
        min_liquidity_usd = max(0.0, to_float(cfg.get("min_liquidity_usd", 1000.0), 1000.0))
        if liquidity < min_liquidity_usd:
            return None

        # Direction from oracle or price skew
        oracle_price = safe_float(row.get("oracle_price"), None)
        price_to_beat = safe_float(row.get("price_to_beat"), None)

        if oracle_price is not None and price_to_beat is not None and price_to_beat > 0:
            diff_pct = ((oracle_price - price_to_beat) / price_to_beat) * 100.0
            if diff_pct > 0.0:
                direction = "buy_yes"
                outcome = "YES"
            else:
                direction = "buy_no"
                outcome = "NO"
        else:
            # Determine direction from price skew
            if up_price < 0.5:
                direction = "buy_yes"
                outcome = "YES"
            else:
                direction = "buy_no"
                outcome = "NO"
            diff_pct = abs(0.5 - up_price) * 100.0

        # Entry price
        if direction == "buy_yes":
            entry_price = float(up_price)
        else:
            entry_price = float(down_price)

        max_entry_price = clamp(to_float(cfg.get("max_entry_price", 0.92), 0.92), 0.05, 0.99)
        if entry_price <= 0.0 or entry_price >= 1.0 or entry_price > max_entry_price:
            return None

        # Entropy-weighted edge
        entropy_multiplier = 0.55 + (0.80 * entropy)
        edge = abs(diff_pct) * entropy_multiplier

        min_edge_percent = max(0.0, to_float(cfg.get("min_edge_percent", 1.0), 1.0))
        if edge < min_edge_percent:
            return None

        # Confidence
        confidence = clamp(
            0.50
            + clamp(entropy - 0.5, 0, 0.20)
            + clamp(abs(diff_pct or 0) / 10.0, 0, 0.15)
            + clamp((1 - (spread or 0) / 0.065) * 0.08, 0, 0.08),
            0.40,
            0.92,
        )

        min_confidence = to_confidence(cfg.get("min_confidence", 0.40), 0.40)
        if confidence < min_confidence:
            return None

        # Score
        score = (
            (edge * 0.45)
            + (confidence * 30.0)
            + (min(1.0, entropy) * 10.0)
            + (min(1.0, liquidity / 20_000.0) * 4.0)
        )

        return {
            "direction": direction,
            "outcome": outcome,
            "entry_price": entry_price,
            "up_price": float(up_price),
            "down_price": float(down_price),
            "oracle_price": float(oracle_price) if oracle_price is not None else None,
            "price_to_beat": float(price_to_beat) if price_to_beat is not None else None,
            "diff_pct": float(diff_pct),
            "entropy": float(entropy),
            "entropy_multiplier": float(entropy_multiplier),
            "edge": float(edge),
            "confidence": float(confidence),
            "spread": float(spread),
            "cancel_rate_30s": float(cancel_rate_30s) if cancel_rate_30s is not None else None,
            "liquidity": float(liquidity),
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

        entropy_context = {
            "strategy_origin": "crypto_worker",
            "direction": direction,
            "entry_price": entry_price,
            "up_price": signal["up_price"],
            "down_price": signal["down_price"],
            "oracle_price": signal["oracle_price"],
            "price_to_beat": signal["price_to_beat"],
            "diff_pct": signal["diff_pct"],
            "entropy": signal["entropy"],
            "entropy_multiplier": signal["entropy_multiplier"],
            "edge": signal["edge"],
            "confidence": signal["confidence"],
            "spread": signal["spread"],
            "cancel_rate_30s": signal["cancel_rate_30s"],
            "liquidity": signal["liquidity"],
            "market_data_age_ms": safe_float(row.get("market_data_age_ms"), None),
            "fetched_at": row.get("fetched_at"),
            "end_time": row.get("end_time"),
        }

        positions = [
            {
                "action": "BUY",
                "outcome": outcome,
                "price": entry_price,
                "token_id": token_id,
                "price_policy": "maker_limit",
                "time_in_force": "GTC",
                "_entropy": entropy_context,
            }
        ]

        opp = self.create_opportunity(
            title=f"Entropy Maker: {outcome} @ {entry_price:.3f} (H={signal['entropy']:.3f})",
            description=(
                f"entropy {signal['entropy']:.3f}, edge {signal['edge']:.2f}%, "
                f"spread {signal['spread']:.4f}"
            ),
            total_cost=entry_price,
            expected_payout=entry_price + (signal["edge"] / 100.0),
            markets=[typed_market],
            positions=positions,
            is_guaranteed=False,
            skip_fee_model=True,
            custom_roi_percent=signal["edge"],
            custom_risk_score=clamp(0.55 - (signal["entropy"] * 0.15), 0.15, 0.85),
            confidence=signal["confidence"],
            min_liquidity_hard=max(100.0, to_float(strategy_cfg.get("min_liquidity_usd", 1000.0), 1000.0)),
            min_position_size=max(1.0, to_float(strategy_cfg.get("min_order_size_usd", 2.0), 2.0)),
        )
        if opp is None:
            return None

        opp.risk_factors = [
            f"Binary entropy={signal['entropy']:.3f}",
            f"Edge={signal['edge']:.2f}% (entropy-weighted)",
            f"Spread={signal['spread']:.4f}",
        ]
        opp.strategy_context = {
            "source_key": "crypto",
            "strategy_slug": self.strategy_type,
            **entropy_context,
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

    def detect(self, events: list, markets: list, prices: dict[str, dict]) -> list[Opportunity]:
        return []

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
