"""Tail-end carry opportunity filter.

Portable pattern adapted from public tail-end Polymarket bots: filter for
high-probability outcomes close to resolution, then only keep entries with
liquidity/spread quality and non-trivial expected repricing room.
"""

from __future__ import annotations

from datetime import timezone
from typing import Optional

from config import settings
from models import ArbitrageOpportunity, Event, Market
from models.opportunity import MispricingType
from services.strategies.base import BaseStrategy, make_aware, utcnow


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class TailEndCarryStrategy(BaseStrategy):
    """Find near-resolution high-probability outcomes with executable carry."""

    strategy_type = "tail_end_carry"
    name = "Tail-End Carry"
    description = "Near-expiry high-probability carry with liquidity and spread gates"

    default_config = {
        "min_probability": 0.88,
        "max_probability": 0.98,
        "min_days_to_resolution": 0.15,
        "max_days_to_resolution": 10.0,
        "min_liquidity": 5000.0,
        "max_spread": 0.03,
        "min_repricing_buffer": 0.015,
        "repricing_weight": 0.45,
        "max_opportunities": 35,
    }

    def __init__(self) -> None:
        super().__init__()
        self.config = dict(self.default_config)

    @staticmethod
    def _book_value(payload: Optional[dict], key: str) -> Optional[float]:
        if not isinstance(payload, dict):
            return None
        val = payload.get(key)
        if isinstance(val, (int, float)):
            return float(val)
        return None

    def _extract_side_book(
        self,
        market: Market,
        prices: dict[str, dict],
        outcome: str,
    ) -> tuple[float, Optional[float], Optional[float], Optional[str]]:
        yes = _safe_float(getattr(market, "yes_price", 0.0))
        no = _safe_float(getattr(market, "no_price", 0.0))
        tokens = list(getattr(market, "clob_token_ids", []) or [])

        if outcome == "YES":
            token_id = tokens[0] if len(tokens) > 0 else None
            payload = prices.get(token_id) if token_id else None
            mid = self._book_value(payload, "mid")
            if mid is None:
                mid = self._book_value(payload, "price")
            price = mid if mid is not None else yes
            bid = self._book_value(payload, "bid") or self._book_value(payload, "best_bid")
            ask = self._book_value(payload, "ask") or self._book_value(payload, "best_ask")
            return price, bid, ask, token_id

        token_id = tokens[1] if len(tokens) > 1 else None
        payload = prices.get(token_id) if token_id else None
        mid = self._book_value(payload, "mid")
        if mid is None:
            mid = self._book_value(payload, "price")
        price = mid if mid is not None else no
        bid = self._book_value(payload, "bid") or self._book_value(payload, "best_bid")
        ask = self._book_value(payload, "ask") or self._book_value(payload, "best_ask")
        return price, bid, ask, token_id

    def detect(
        self,
        events: list[Event],
        markets: list[Market],
        prices: dict[str, dict],
    ) -> list[ArbitrageOpportunity]:
        cfg = dict(self.default_config)
        cfg.update(getattr(self, "config", {}) or {})

        min_probability = _clamp(_safe_float(cfg.get("min_probability"), 0.88), 0.5, 0.99)
        max_probability = _clamp(_safe_float(cfg.get("max_probability"), 0.98), min_probability + 0.01, 0.995)
        min_days = max(0.0, _safe_float(cfg.get("min_days_to_resolution"), 0.15))
        max_days = max(min_days + 0.01, _safe_float(cfg.get("max_days_to_resolution"), 10.0))
        min_liquidity = max(100.0, _safe_float(cfg.get("min_liquidity"), 5000.0))
        max_spread = _clamp(_safe_float(cfg.get("max_spread"), 0.03), 0.005, 0.20)
        min_repricing_buffer = _clamp(_safe_float(cfg.get("min_repricing_buffer"), 0.015), 0.005, 0.10)
        repricing_weight = _clamp(_safe_float(cfg.get("repricing_weight"), 0.45), 0.10, 0.90)
        max_opportunities = max(1, int(_safe_float(cfg.get("max_opportunities"), 35)))

        event_by_market: dict[str, Event] = {}
        for event in events:
            for event_market in event.markets:
                event_by_market[event_market.id] = event

        now = utcnow().astimezone(timezone.utc)
        candidates: list[tuple[float, ArbitrageOpportunity]] = []

        for market in markets:
            if market.closed or not market.active:
                continue
            if _safe_float(getattr(market, "liquidity", 0.0)) < min_liquidity:
                continue
            if len(list(getattr(market, "clob_token_ids", []) or [])) < 2 and len(list(getattr(market, "outcome_prices", []) or [])) < 2:
                continue

            end_date = make_aware(getattr(market, "end_date", None))
            if end_date is None:
                continue

            days_to_resolution = (end_date - now).total_seconds() / 86400.0
            if days_to_resolution < min_days or days_to_resolution > max_days:
                continue

            for outcome in ("YES", "NO"):
                price, bid, ask, token_id = self._extract_side_book(market, prices, outcome)
                if not (min_probability <= price <= max_probability):
                    continue

                spread = 0.0
                if isinstance(bid, float) and isinstance(ask, float) and bid > 0.0 and ask > 0.0:
                    spread = max(0.0, ask - bid)
                    if spread > max_spread:
                        continue

                target_move = max(min_repricing_buffer, (1.0 - price) * repricing_weight)
                target_price = min(0.999, price + target_move)
                if target_price <= (price + 1e-6):
                    continue

                positions = [
                    {
                        "action": "BUY",
                        "outcome": outcome,
                        "price": price,
                        "token_id": token_id,
                        "entry_style": "tail_carry",
                        "_tail_end": {
                            "days_to_resolution": days_to_resolution,
                            "spread": spread,
                            "target_price": target_price,
                            "probability": price,
                        },
                    }
                ]

                opp = self.create_opportunity(
                    title=f"Tail Carry: {outcome} {price:.1%} into resolution",
                    description=(
                        f"{outcome} at {price:.3f} with {days_to_resolution:.1f} days to resolution; "
                        f"target repricing to {target_price:.3f}."
                    ),
                    total_cost=price,
                    expected_payout=target_price,
                    markets=[market],
                    positions=positions,
                    event=event_by_market.get(market.id),
                    is_guaranteed=False,
                    min_liquidity_hard=min_liquidity,
                    min_position_size=max(settings.MIN_POSITION_SIZE, 5.0),
                )
                if not opp:
                    continue

                time_score = 1.0 - (days_to_resolution / max_days)
                black_swan_penalty = _clamp((price - 0.90) * 0.40, 0.0, 0.12)
                spread_penalty = min(0.14, spread * 2.8)
                risk = 0.56 - (time_score * 0.10) + black_swan_penalty + spread_penalty
                opp.risk_score = _clamp(risk, 0.35, 0.82)
                opp.risk_factors = [
                    f"Near-expiry carry ({days_to_resolution:.1f} days)",
                    f"Selected probability {price:.1%}",
                    "Non-zero tail-risk remains until resolution",
                ]
                opp.mispricing_type = MispricingType.WITHIN_MARKET

                strength = (target_price - price) * (1.0 - opp.risk_score)
                candidates.append((strength, opp))

        if not candidates:
            return []

        candidates.sort(key=lambda item: item[0], reverse=True)
        out: list[ArbitrageOpportunity] = []
        seen: set[tuple[str, str]] = set()
        for _, opp in candidates:
            market_id = str((opp.markets or [{}])[0].get("id") or "")
            outcome = str((opp.positions_to_take or [{}])[0].get("outcome") or "")
            key = (market_id, outcome)
            if key in seen:
                continue
            seen.add(key)
            out.append(opp)
            if len(out) >= max_opportunities:
                break
        return out
