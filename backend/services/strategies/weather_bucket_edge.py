"""
Weather Bucket Edge Strategy

Per-bucket edge: compare model probability directly to market price for each
temperature bucket independently. This is the simplest and most direct
approach -- use the deterministic forecast data with a user-tunable sigmoid
sharpness to compute the probability a temperature falls in a given bucket,
then compare that probability to the current market price.

The key difference from WeatherEdgeStrategy is a tighter sigmoid scale
(1.5 vs 2.0) for sharper bucket discrimination, giving better resolution
when buckets are narrow.

Pipeline:
  1. Weather workflow fetches forecasts from multiple sources
  2. Consensus engine computes agreement, spread, and dislocation
  3. Enriched intents are built with raw forecast data
  4. This strategy computes model probability per bucket and compares
     directly to market price
"""

from __future__ import annotations

import logging
from typing import Optional

from config import settings
from models import ArbitrageOpportunity, Event, Market
from models.opportunity import MispricingType
from services.strategies.base import BaseStrategy
from services.weather.signal_engine import (
    compute_confidence,
    compute_model_agreement,
    temp_range_probability,
)

logger = logging.getLogger(__name__)


class WeatherBucketEdgeStrategy(BaseStrategy):
    """
    Weather Bucket Edge Strategy

    Per-bucket edge: compare model probability directly to market price for
    each temperature bucket. Uses a tighter sigmoid scale (1.5) than the
    default weather edge strategy (2.0) for sharper bucket discrimination.
    """

    strategy_type = "weather_bucket_edge"
    name = "Weather Bucket Edge"
    description = "Per-bucket edge: compare model probability directly to market price for each temperature bucket"

    DEFAULT_CONFIG = {
        "min_edge_percent": 6.0,
        "probability_scale_c": 1.5,  # tighter sigmoid than default 2.0 for sharper bucket discrimination
        "min_confidence": 0.50,
        "min_model_agreement": 0.55,
        "min_source_count": 2,
        "max_source_spread_c": 5.0,
        "max_entry_price": 0.85,
        "risk_base_score": 0.30,
    }

    def __init__(self):
        super().__init__()
        self._config = dict(self.DEFAULT_CONFIG)

    def configure(self, config: dict) -> None:
        """Apply user config overrides from the DB config column."""
        if config:
            for key in self.DEFAULT_CONFIG:
                if key in config:
                    self._config[key] = config[key]

    def detect(
        self,
        events: list[Event],
        markets: list[Market],
        prices: dict[str, dict],
    ) -> list[ArbitrageOpportunity]:
        """Sync detect - returns empty.

        WeatherBucketEdgeStrategy is fed by the weather workflow pipeline,
        not the main scanner loop. Weather intents are converted to
        opportunities by detect_from_intents().
        """
        return []

    def detect_from_intents(
        self,
        intents: list[dict],
        markets: list[Market],
        events: list[Event],
    ) -> list[ArbitrageOpportunity]:
        """Convert weather trade intents into ArbitrageOpportunity objects.

        Accepts enriched intents with raw forecast data. Computes model
        probability per bucket and compares directly to market price.
        """
        if not intents:
            return []

        cfg = self._config
        opportunities: list[ArbitrageOpportunity] = []
        market_map = {m.id: m for m in markets}
        event_map: dict[str, Event] = {}
        for event in events:
            for m in event.markets:
                event_map[m.id] = event

        for intent in intents:
            try:
                opp = self._evaluate_intent(intent, market_map, event_map, cfg)
                if opp:
                    opportunities.append(opp)
            except Exception as e:
                logger.debug("Bucket Edge: skipped intent: %s", e)

        if opportunities:
            logger.info(
                "Bucket Edge: %d opportunities from %d intents",
                len(opportunities),
                len(intents),
            )
        return opportunities

    def _evaluate_intent(
        self,
        intent: dict,
        market_map: dict[str, Market],
        event_map: dict[str, Event],
        cfg: dict,
    ) -> Optional[ArbitrageOpportunity]:
        """Evaluate a single weather trade intent against config thresholds.

        Computes direction by comparing model probability to market price
        for each bucket independently.
        """
        # Extract forecast metadata
        source_count = int(intent.get("source_count", 0))
        source_spread_c = float(intent.get("source_spread_c") or intent.get("source_spread_c", 0) or 0)
        agreement = float(intent.get("model_agreement", 0))
        yes_price = float(intent.get("yes_price", 0.5))
        no_price = float(intent.get("no_price", 0.5))
        consensus_value_c = intent.get("consensus_value_c")
        bucket_low_c = intent.get("bucket_low_c")
        bucket_high_c = intent.get("bucket_high_c")

        # Quality gates (non-directional filters)
        if agreement < cfg["min_model_agreement"]:
            return None
        if source_count < cfg["min_source_count"]:
            return None
        if source_spread_c > cfg["max_source_spread_c"]:
            return None

        # Compute model probability for this bucket
        scale_c = float(cfg.get("probability_scale_c", 1.5))
        if bucket_low_c is not None and bucket_high_c is not None and consensus_value_c is not None:
            low = float(bucket_low_c)
            high = float(bucket_high_c)
            model_prob = temp_range_probability(
                float(consensus_value_c), low, high, scale_c
            )
        else:
            # Fallback: use pre-computed consensus probability
            model_prob = float(intent.get("consensus_probability", 0.5) or 0.5)

        # Direction: compare model probability directly to market price
        if model_prob > yes_price:
            direction = "buy_yes"
            entry_price = yes_price
            target_price = model_prob
            edge = (model_prob - yes_price) * 100.0
        else:
            direction = "buy_no"
            entry_price = no_price
            target_price = 1.0 - model_prob
            edge = ((1.0 - model_prob) - no_price) * 100.0

        # Apply edge and entry price filters
        if edge < cfg["min_edge_percent"]:
            return None
        if entry_price > cfg["max_entry_price"]:
            return None

        # Confidence filter
        confidence = compute_confidence(agreement, model_prob, source_count, source_spread_c)
        if confidence < cfg["min_confidence"]:
            return None

        # Resolve market and event
        market_id = intent.get("market_id")
        market = market_map.get(market_id) if market_id else None
        if not market:
            return None

        event = event_map.get(market_id)
        city = intent.get("location", "Unknown")
        question = market.question or ""

        # Position sizing
        side = "YES" if direction == "buy_yes" else "NO"
        token_id = None
        if market.clob_token_ids:
            idx = 0 if direction == "buy_yes" else (1 if len(market.clob_token_ids) > 1 else 0)
            token_id = market.clob_token_ids[idx]

        expected_payout = target_price
        total_cost = entry_price
        gross_profit = expected_payout - total_cost
        fee_amount = expected_payout * self.fee
        net_profit = gross_profit - fee_amount
        roi = (net_profit / total_cost) * 100 if total_cost > 0 else 0

        if roi < cfg["min_edge_percent"] / 2:
            return None

        min_liquidity = market.liquidity
        max_position = min(min_liquidity * 0.05, 400.0)

        if max_position < settings.MIN_POSITION_SIZE:
            return None

        # Risk scoring
        risk_score = float(cfg["risk_base_score"])
        risk_factors = [
            "Weather-driven directional bet (per-bucket model vs market)",
            f"Model agreement: {agreement:.0%}",
            f"Source spread: {source_spread_c:.1f}C across {source_count} sources",
        ]
        if confidence < 0.65:
            risk_score += 0.15
            risk_factors.append("Moderate confidence")
        if source_spread_c > 3.0:
            risk_score += 0.1
            risk_factors.append("High source disagreement")
        risk_score = min(risk_score, 1.0)

        # Build bucket center for description
        bucket_center = 0.0
        if bucket_low_c is not None and bucket_high_c is not None:
            bucket_center = (float(bucket_low_c) + float(bucket_high_c)) / 2.0

        consensus_temp = intent.get("consensus_value_c")
        market_temp = intent.get("market_implied_temp_c")

        positions = [
            {
                "action": "BUY",
                "outcome": side,
                "price": entry_price,
                "token_id": token_id,
                "_weather_bucket_edge": {
                    "city": city,
                    "consensus_temp_c": consensus_temp,
                    "market_implied_temp_c": market_temp,
                    "model_agreement": agreement,
                    "source_count": source_count,
                    "source_spread_c": source_spread_c,
                    "edge_percent": edge,
                    "confidence": confidence,
                    "direction": direction,
                    "model_probability": model_prob,
                    "bucket_low_c": bucket_low_c,
                    "bucket_high_c": bucket_high_c,
                    "probability_scale_c": scale_c,
                },
            }
        ]

        market_dict = {
            "id": market.id,
            "slug": market.slug,
            "question": question,
            "yes_price": market.yes_price,
            "no_price": market.no_price,
            "liquidity": market.liquidity,
        }

        return ArbitrageOpportunity(
            strategy=self.strategy_type,
            title=f"Bucket Edge: {city} - {question[:40]}",
            description=(
                f"Model {model_prob:.0%} vs market {yes_price:.0%} "
                f"for {bucket_center:.0f}C bucket "
                f"(edge {edge:.1f}%, scale {scale_c})"
            ),
            total_cost=total_cost,
            expected_payout=expected_payout,
            gross_profit=gross_profit,
            fee=fee_amount,
            net_profit=net_profit,
            roi_percent=roi,
            is_guaranteed=False,
            roi_type="directional_payout",
            risk_score=risk_score,
            risk_factors=risk_factors,
            markets=[market_dict],
            event_id=event.id if event else None,
            event_slug=event.slug if event else None,
            event_title=event.title if event else None,
            category=event.category if event else None,
            min_liquidity=min_liquidity,
            max_position_size=max_position,
            resolution_date=market.end_date,
            mispricing_type=MispricingType.NEWS_INFORMATION,
            positions_to_take=positions,
        )
