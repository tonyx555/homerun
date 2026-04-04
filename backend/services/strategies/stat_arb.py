"""
Strategy: Statistical Arbitrage / Information Edge

Compares Polymarket prices against external probability signals
to find markets where the crowd is wrong.

External signals used:
1. Implied probability from price (YES price = market's probability)
2. Multi-market consensus: average probability across related markets
3. Category base rates: historical resolution rates by category
4. Price momentum: trending markets tend to continue trending
5. Anchoring detection: markets stuck near round numbers (50%, 25%, 75%)

The key insight from the $2.2M bot: ensemble multiple weak signals
into a composite "fair probability" and trade the deviation.

NOT risk-free. This is informed speculation with statistical edge.
"""

from __future__ import annotations

import re
import statistics
import time
from datetime import datetime, timezone
from typing import Any, Optional

import logging

from models import Market, Event, Opportunity
from config import settings
from .base import BaseStrategy, DecisionCheck, ExitDecision, ScoringWeights, SizingConfig, make_aware, utcnow
from services.quality_filter import QualityFilterOverrides
from utils.kelly import kelly_fraction

logger = logging.getLogger(__name__)


# Round numbers where human anchoring bias is common
ANCHOR_PRICES = [0.10, 0.25, 0.33, 0.50, 0.67, 0.75, 0.90]

# Anchoring detection tolerance: if price is within this of a round number,
# it is considered anchored
ANCHOR_TOLERANCE = 0.02

# Category base rates: historical resolution rates by category.
# These are hard-coded initial estimates of how often YES resolves in
# markets of each type.
CATEGORY_BASE_RATES: dict[str, float] = {
    # Crypto price targets: most ambitious targets don't hit
    "crypto": 0.35,
    "cryptocurrency": 0.35,
    "bitcoin": 0.35,
    "ethereum": 0.35,
    # Political events: roughly balanced but slightly below 50%
    "politics": 0.45,
    "political": 0.45,
    "elections": 0.45,
    "government": 0.45,
    # Sports: balanced by design (bookmakers are efficient)
    "sports": 0.50,
    "nfl": 0.50,
    "nba": 0.50,
    "mlb": 0.50,
    "soccer": 0.50,
    "football": 0.50,
    # Science/tech: most ambitious predictions fail
    "science": 0.25,
    "technology": 0.25,
    "tech": 0.25,
    "ai": 0.30,
    # Entertainment: moderate resolution rate
    "entertainment": 0.40,
    "culture": 0.40,
    "pop culture": 0.40,
    # Finance / economics
    "finance": 0.40,
    "economics": 0.40,
    # Default for unknown categories
    "_default": 0.45,
}

# Signal weights for composite fair probability
SIGNAL_WEIGHTS = {
    "anchoring": 0.13,
    "category_base_rate": 0.18,
    "consensus": 0.20,
    "momentum": 0.12,
    "volume_price": 0.15,
    "favorite_longshot": 0.10,
    "liquidity_imbalance": 0.12,
}

_DEADLINE_PATTERNS = [
    re.compile(r"\bby\s+(\w+\s+\d{1,2},?\s+\d{4}|\w+\s+\d{4})", re.IGNORECASE),
    re.compile(r"\bbefore\s+(\w+\s+\d{1,2},?\s+\d{4}|\w+\s+\d{4})", re.IGNORECASE),
    re.compile(r"\bin\s+(\w+\s+\d{4})", re.IGNORECASE),
    re.compile(r"\bby\s+(?:the\s+)?end\s+of\s+(\d{4})", re.IGNORECASE),
]

_MONTH_MAP = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "sept": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}

_PLAYER_STAT_RE = re.compile(r"[A-Z][a-z]+ [A-Z][a-z]+:\s*\d+\+", re.IGNORECASE)
_DEFAULT_DECAY_RATE = 0.5
_MIN_DEVIATION = 0.05
_MAX_DAYS_TO_DEADLINE = 30
_MIN_DAYS_TO_DEADLINE = 1
_MIN_HISTORY_POINTS = 5
_MIN_ENTRY_PRICE = 0.08
_MAX_ENTRY_PRICE = 0.92
_MIN_EXPECTED_MOVE = 0.02
_SHOCK_LOOKBACK_SECONDS = 6 * 3600
_SHOCK_MIN_POINTS = 3
_SHOCK_MAX_DAYS_TO_DEADLINE = 10.0
_SHOCK_MIN_DAYS_TO_DEADLINE = -0.25
_SHOCK_MIN_ABS_MOVE = 0.18
_SHOCK_MAX_RETRACE = 0.12
_SHOCK_MIN_FAVORED_PRICE = 0.55
_SHOCK_MAX_FAVORED_PRICE = 0.97
_SHOCK_TARGET_CERTAINTY = 0.96
_SHOCK_EXTENSION_FACTOR = 0.45
_SHOCK_MIN_EXPECTED_MOVE = 0.03
_SHOCK_MIN_LIQUIDITY_HARD = 1000.0
_SHOCK_MIN_POSITION_SIZE = 50.0
_MULTILEG_MARKET_PREFIXES = ("KXMVESPORTSMULTIGAMEEXTENDED-",)


class StatArbStrategy(BaseStrategy):
    """
    Statistical mispricing strategy with a temporal deadline subfamily.

    Primary branch: ensemble fair-probability estimation from weak statistical
    signals.

    Secondary branch: deadline-aware certainty-shock / decay-curve detection
    folded in from the removed temporal_decay strategy.
    """

    strategy_type = "stat_arb"
    name = "Statistical Arbitrage"
    description = "Trade statistical mispricings and deadline-driven certainty shocks"
    mispricing_type = "within_market"
    subscriptions = ["market_data_refresh"]

    quality_filter_overrides = QualityFilterOverrides(
        min_roi=2.0,
        max_resolution_months=3.0,
    )

    default_config = {
        "min_edge_percent": 5.0,
        "min_confidence": 0.45,
        "max_risk_score": 0.75,
        "enable_stat_signals": True,
        "enable_certainty_shock": True,
        "enable_decay_curve": True,
        "shock_lookback_seconds": 21600,
        "shock_min_abs_move": 0.18,
        "shock_max_retrace": 0.12,
        "shock_min_favored_price": 0.55,
        "shock_target_certainty": 0.96,
        "max_days_to_deadline": 30.0,
        "min_days_to_deadline": 1.0,
        "exclude_market_keywords": [
            "bitcoin",
            "btc",
            "ethereum",
            "eth",
            "solana",
            "sol",
            "xrp",
            "crypto",
            "up or down",
            "doge",
            "dogecoin",
            "bnb",
            "cardano",
            "ada",
            "polygon",
            "matic",
            "avax",
            "avalanche",
            "chainlink",
            "link",
            "litecoin",
            "ltc",
        ],
        "take_profit_pct": 12.0,
        "stop_loss_pct": 25.0,
        "trailing_stop_pct": 15.0,
    }

    pipeline_defaults = {
        "min_edge_percent": 3.5,
        "min_confidence": 0.45,
        "max_risk_score": 0.75,
    }

    # Composable evaluate pipeline: score = edge*0.58 + conf*32 - risk*8
    scoring_weights = ScoringWeights(
        edge_weight=0.58,
        confidence_weight=32.0,
        risk_penalty=8.0,
    )
    # size = base*(1+edge/100)*(0.75+conf), no risk/market scaling
    sizing_config = SizingConfig(
        base_divisor=100.0,
        confidence_offset=0.75,
        risk_scale_factor=0.0,
        risk_floor=1.0,
        market_scale_factor=0.0,
        market_scale_cap=0,
    )

    def __init__(self):
        super().__init__()
        # Track previous prices for momentum signal across scans
        self._prev_prices: dict[str, float] = {}

    @staticmethod
    def _normalize_excluded_keywords(value: Any) -> list[str]:
        if isinstance(value, str):
            candidates = [token.strip() for token in value.split(",")]
        elif isinstance(value, list):
            candidates = list(value)
        else:
            return []
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in candidates:
            token = str(raw or "").strip().lower()
            if not token or token in seen:
                continue
            seen.add(token)
            normalized.append(token)
        return normalized

    @staticmethod
    def _keyword_in_text(keyword: str, text: str) -> bool:
        if not keyword or not text:
            return False
        if len(keyword) <= 4 and keyword.replace("-", "").replace("_", "").isalnum():
            return re.search(rf"\b{re.escape(keyword)}\b", text) is not None
        return keyword in text

    @classmethod
    def _first_blocked_keyword(cls, text: str, excluded_keywords: list[str]) -> Optional[str]:
        for keyword in excluded_keywords:
            if cls._keyword_in_text(keyword, text):
                return keyword
        return None

    @classmethod
    def _market_text(cls, market: Market) -> str:
        chunks: list[str] = []
        for value in (
            market.id,
            market.question,
            getattr(market, "slug", None),
            getattr(market, "group_item_title", None),
            getattr(market, "event_slug", None),
        ):
            text = str(value or "").strip().lower()
            if text:
                chunks.append(text)
        for tag in list(getattr(market, "tags", []) or []):
            text = str(tag or "").strip().lower()
            if text:
                chunks.append(text)
        return " | ".join(chunks)

    @classmethod
    def _signal_market_text(cls, signal: Any, payload: dict) -> str:
        chunks: list[str] = []
        for value in (
            getattr(signal, "market_question", None),
            payload.get("market_question"),
            payload.get("title"),
            payload.get("description"),
            payload.get("market_id"),
        ):
            text = str(value or "").strip().lower()
            if text:
                chunks.append(text)
        markets = payload.get("markets")
        if isinstance(markets, list):
            for raw_market in markets[:2]:
                if not isinstance(raw_market, dict):
                    continue
                for value in (raw_market.get("id"), raw_market.get("question"), raw_market.get("slug")):
                    text = str(value or "").strip().lower()
                    if text:
                        chunks.append(text)
        return " | ".join(chunks)

    # ------------------------------------------------------------------
    # Signal 1: Anchoring Detection
    # ------------------------------------------------------------------
    def _signal_anchoring(self, yes_price: float) -> float:
        """Detect whether a market price is anchored to a round number.

        Markets at exactly 50%, 25%, 75% etc. are often anchored because
        humans round to convenient numbers.  When a market sits at one of
        these levels for a long time with volume it is probably anchored
        rather than reflecting true probability.

        Returns 1.0 if anchored (price within ANCHOR_TOLERANCE of a round
        number), 0.0 otherwise.
        """
        for anchor in ANCHOR_PRICES:
            if abs(yes_price - anchor) < ANCHOR_TOLERANCE:
                return 1.0
        return 0.0

    # ------------------------------------------------------------------
    # Signal 2: Category Base Rate
    # ------------------------------------------------------------------
    def _signal_category_base_rate(self, yes_price: float, category: Optional[str]) -> float:
        """Compare market price to category base rate.

        If the market price is significantly above the category's
        historical resolution rate the YES side may be overpriced (and
        vice versa).

        Returns a value in [-1, 1]:
          positive => market is under-priced (YES should be higher)
          negative => market is over-priced  (YES should be lower)
        """
        base_rate = CATEGORY_BASE_RATES.get("_default")
        if category:
            cat_lower = category.lower().strip()
            base_rate = CATEGORY_BASE_RATES.get(cat_lower, base_rate)
        deviation = base_rate - yes_price  # type: ignore[operator]
        # Clamp to [-1, 1]
        return max(-1.0, min(1.0, deviation))

    # ------------------------------------------------------------------
    # Signal 3: Multi-Market Consensus (within same event)
    # ------------------------------------------------------------------
    def _signal_consensus(
        self,
        market: Market,
        event_markets: list[Market],
        prices: dict[str, dict],
    ) -> Optional[float]:
        """Detect price outliers within an event using Median Absolute
        Deviation (MAD).

        For markets in the same event, check consistency.  If event has 5
        markets and 4 imply X should be ~60% but market 5 prices X at 40%,
        market 5 is the outlier.

        Returns a value in [-1, 1] representing deviation from consensus,
        or None if consensus cannot be determined (fewer than 3 sibling
        markets).
        """
        if len(event_markets) < 3:
            return None

        # Gather YES prices for all sibling binary markets
        sibling_prices: list[float] = []
        target_price: Optional[float] = None
        for m in event_markets:
            if len(m.outcome_prices) != 2:
                continue
            if m.closed or not m.active:
                continue
            p = self._live_yes_price(m, prices)
            sibling_prices.append(p)
            if m.id == market.id:
                target_price = p

        if target_price is None or len(sibling_prices) < 3:
            return None

        median = statistics.median(sibling_prices)
        mad = statistics.median([abs(p - median) for p in sibling_prices])
        if mad < 0.01:
            # All prices are essentially identical; no outlier signal
            return 0.0

        # Modified Z-score: how many MADs away is the target?
        z = (target_price - median) / mad
        # Positive z means target is above consensus (possibly overpriced)
        # Negative z means target is below consensus (possibly underpriced)
        # Normalize to [-1, 1] by capping at 3 MADs
        signal = max(-1.0, min(1.0, -z / 3.0))
        return signal

    # ------------------------------------------------------------------
    # Signal 4: Momentum
    # ------------------------------------------------------------------
    def _signal_momentum(self, market_id: str, current_price: float) -> float:
        """Track price changes across scans to detect momentum.

        Markets trending up tend to continue (momentum factor).

        Returns a value in roughly [-1, 1].  Positive means upward momentum
        (price has been rising), negative means downward.
        """
        prev = self._prev_prices.get(market_id)
        # Always store the latest price for next scan
        self._prev_prices[market_id] = current_price

        if prev is None or prev == 0:
            return 0.0

        momentum = (current_price - prev) / prev
        # Cap momentum signal
        return max(-1.0, min(1.0, momentum * 10))  # scale up small moves

    # ------------------------------------------------------------------
    # Signal 5: Volume-Price Divergence
    # ------------------------------------------------------------------
    def _signal_volume_price(self, yes_price: float, volume: float, liquidity: float) -> float:
        """Detect volume-price divergence.

        High volume with small price change away from 0.50 = informed
        disagreement, which could signal an upcoming move.
        Low volume with price at an extreme = stale/illiquid (risky, but
        may present an opportunity if the market hasn't adjusted).

        Returns a value in [-1, 1].
          Positive => suggests upward pressure (volume high, price low)
          Negative => suggests downward pressure (volume high, price high)
          Near zero => no clear signal
        """
        if volume <= 0 or liquidity <= 0:
            return 0.0

        # Volume-to-liquidity ratio as a proxy for trading activity
        vol_liq = volume / liquidity if liquidity > 0 else 0.0

        # Price distance from 0.50 (how extreme the price is)
        price_extremity = abs(yes_price - 0.50)

        if vol_liq > 5.0 and price_extremity < 0.15:
            # High volume but price near 50%: strong disagreement
            # Slight positive bias -- active markets tend to be efficient
            return 0.0

        if vol_liq > 5.0 and price_extremity > 0.25:
            # High volume and extreme price: strong conviction
            # The crowd is probably right when volume is high
            return 0.0

        if vol_liq < 1.0 and price_extremity > 0.30:
            # Low volume, extreme price: might be stale / illiquid
            # Signal that price should revert toward 0.50
            direction = -1.0 if yes_price > 0.50 else 1.0
            return direction * 0.5

        if vol_liq < 0.5 and price_extremity > 0.40:
            # Very low volume, very extreme price: likely stale
            direction = -1.0 if yes_price > 0.50 else 1.0
            return direction * 0.8

        return 0.0

    def _signal_favorite_longshot(self, yes_price: float) -> float:
        """Capture favorite-longshot skew as one weak component.

        Longshots (very low YES prices) are frequently overbid by retail.
        Favorites are frequently underbid. Positive values imply YES should be
        priced higher; negative values imply YES should be priced lower.
        """
        if yes_price <= 0.25:
            return -min(1.0, (0.25 - yes_price) / 0.25) * 0.8
        if yes_price >= 0.75:
            return min(1.0, (yes_price - 0.75) / 0.25) * 0.6
        return 0.0

    def _signal_liquidity_imbalance(self, market: Market, prices: dict[str, dict]) -> float:
        """Use bid/ask depth imbalance as an additional directional input."""
        token_ids = list(getattr(market, "clob_token_ids", []) or [])
        if len(token_ids) < 2:
            return 0.0

        yes_payload = prices.get(token_ids[0]) if token_ids[0] in prices else None
        no_payload = prices.get(token_ids[1]) if token_ids[1] in prices else None

        def _depth(payload: dict | None, *keys: str) -> float:
            if not isinstance(payload, dict):
                return 0.0
            for key in keys:
                value = payload.get(key)
                if isinstance(value, (int, float)) and value > 0:
                    return float(value)
            return 0.0

        yes_bid_depth = _depth(yes_payload, "bid_depth", "best_bid_size", "bid_size")
        yes_ask_depth = _depth(yes_payload, "ask_depth", "best_ask_size", "ask_size")
        no_bid_depth = _depth(no_payload, "bid_depth", "best_bid_size", "bid_size")
        no_ask_depth = _depth(no_payload, "ask_depth", "best_ask_size", "ask_size")

        total_yes = yes_bid_depth + yes_ask_depth
        total_no = no_bid_depth + no_ask_depth
        if total_yes <= 0 and total_no <= 0:
            return 0.0

        yes_pressure = 0.0
        if total_yes > 0:
            yes_pressure = (yes_bid_depth - yes_ask_depth) / total_yes
        no_pressure = 0.0
        if total_no > 0:
            no_pressure = (no_bid_depth - no_ask_depth) / total_no

        # If NO book has stronger bid pressure, YES should be lower.
        imbalance = (yes_pressure - no_pressure) / 2.0
        return max(-1.0, min(1.0, imbalance))

    # ------------------------------------------------------------------
    # Temporal awareness
    # ------------------------------------------------------------------
    _YEAR_PATTERN = re.compile(r"\b(20[0-9]{2})\b")

    def _is_likely_already_resolved(self, market: Market) -> bool:
        """Detect if a market's subject matter has likely already occurred.

        Markets about past events (e.g., "U.S. tariff revenue in 2025"
        scanned in February 2026) should not be traded on statistical
        signals, because the market price reflects actual known outcomes
        while our base-rate model uses stale historical averages.

        Also rejects markets whose resolution date has already passed.
        """
        now = utcnow()

        # Check 1: Resolution date already passed (should be filtered
        # upstream but double-check)
        if market.end_date:
            end_aware = make_aware(market.end_date)
            if end_aware < now:
                return True

        # Check 2: Question references a specific past year.
        # E.g., "How much revenue will the U.S. raise from tariffs in 2025?"
        # scanned in 2026 means the 2025 data is already known.
        question = market.question or ""
        current_year = now.year
        for match in self._YEAR_PATTERN.finditer(question):
            referenced_year = int(match.group(1))
            if referenced_year < current_year:
                return True

        return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _live_yes_price(self, market: Market, prices: dict[str, dict]) -> float:
        """Return the best available YES price (live > static)."""
        yes_price = market.yes_price
        if market.clob_token_ids and len(market.clob_token_ids) > 0:
            yes_token = market.clob_token_ids[0]
            if yes_token in prices:
                yes_price = prices[yes_token].get("mid", yes_price)
        return yes_price

    def _live_no_price(self, market: Market, prices: dict[str, dict]) -> float:
        """Return the best available NO price (live > static)."""
        no_price = market.no_price
        if market.clob_token_ids and len(market.clob_token_ids) > 1:
            no_token = market.clob_token_ids[1]
            if no_token in prices:
                no_price = prices[no_token].get("mid", no_price)
        return no_price

    def _extract_deadline(self, market: Market) -> Optional[datetime]:
        if market.end_date:
            return make_aware(market.end_date)

        for pattern in _DEADLINE_PATTERNS:
            match = pattern.search(market.question or "")
            if not match:
                continue
            parsed = self._parse_date_string(match.group(1).strip().rstrip(","))
            if parsed is not None:
                return parsed
        return None

    def _parse_date_string(self, date_str: str) -> Optional[datetime]:
        parts = date_str.lower().split()
        if len(parts) == 1:
            try:
                year = int(parts[0])
            except ValueError:
                return None
            return datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

        if len(parts) >= 2:
            month = _MONTH_MAP.get(parts[0])
            if month is None:
                return None
            if len(parts) == 2:
                try:
                    year = int(parts[1])
                except ValueError:
                    return None
                import calendar

                _, day = calendar.monthrange(year, month)
                return datetime(year, month, day, 23, 59, 59, tzinfo=timezone.utc)
            try:
                day = int(parts[1].rstrip(","))
                year = int(parts[2])
            except (IndexError, ValueError):
                return None
            return datetime(year, month, day, 23, 59, 59, tzinfo=timezone.utc)
        return None

    def _is_sports_parlay(self, market: Market) -> bool:
        question = str(market.question or "").lower()
        if question.count("yes ") + question.count("no ") >= 2:
            return True
        if question.count(",") >= 2:
            return True
        if _PLAYER_STAT_RE.search(market.question or ""):
            return True
        sport_keywords = [
            "nba",
            "nfl",
            "mlb",
            "nhl",
            "ufc",
            "mma",
            "boxing",
            "ncaa",
            "march madness",
            "college basketball",
            "ncaab",
            "college",
            "premier league",
            "la liga",
            "bundesliga",
            "serie a",
            "champions league",
            "mls",
            "ligue 1",
            "epl",
            "moneyline",
            "parlay",
            "spread",
            "halftime",
            "quarter",
            "inning",
            "period",
            "overtime",
            "points",
            "rebounds",
            "assists",
            "touchdowns",
            "yards",
            "goals scored",
        ]
        return any(keyword in question for keyword in sport_keywords)

    def _create_certainty_shock_opportunity(
        self,
        *,
        market: Market,
        yes_price: float,
        no_price: float,
        now: datetime,
        scan_time: float,
        lookback_seconds: int,
        min_abs_move: float,
        max_retrace: float,
        min_favored_price: float,
        target_certainty: float,
    ) -> Optional[Opportunity]:
        price_history = self.state.setdefault("price_history", {})
        history = price_history.get(market.id, [])
        min_points = int(_SHOCK_MIN_POINTS)
        if len(history) < min_points:
            return None

        deadline = self._extract_deadline(market)
        if deadline is None:
            return None

        days_remaining = (deadline - now).total_seconds() / 86400.0
        if days_remaining > _SHOCK_MAX_DAYS_TO_DEADLINE or days_remaining < _SHOCK_MIN_DAYS_TO_DEADLINE:
            return None

        cutoff = scan_time - max(1, lookback_seconds)
        window_prices = [price for ts, price in history if ts >= cutoff]
        if len(window_prices) < min_points:
            window_prices = [price for _, price in history[-min_points:]]
        if len(window_prices) < min_points:
            return None

        peak = max(window_prices)
        trough = min(window_prices)
        up_move = yes_price - trough
        down_move = peak - yes_price
        if up_move < min_abs_move and down_move < min_abs_move:
            return None

        yes_token = market.clob_token_ids[0] if market.clob_token_ids else None
        no_token = market.clob_token_ids[1] if len(market.clob_token_ids) > 1 else None

        if up_move >= down_move:
            outcome = "YES"
            entry_price = yes_price
            token_id = yes_token
            move = up_move
            retrace = max(peak - yes_price, 0.0)
            shock_desc = "YES repricing upward"
        else:
            outcome = "NO"
            entry_price = no_price
            token_id = no_token
            move = down_move
            retrace = max(yes_price - trough, 0.0)
            shock_desc = "YES repricing downward (NO upward)"

        if retrace > max_retrace:
            return None
        if entry_price < min_favored_price or entry_price > _SHOCK_MAX_FAVORED_PRICE:
            return None

        target_exit_price = max(target_certainty, entry_price + move * _SHOCK_EXTENSION_FACTOR)
        target_exit_price = max(0.01, min(0.995, target_exit_price))
        expected_move = target_exit_price - entry_price
        if expected_move < _SHOCK_MIN_EXPECTED_MOVE:
            return None

        positions = [
            {
                "action": "BUY",
                "outcome": outcome,
                "market": market.question[:50],
                "price": entry_price,
                "token_id": token_id,
                "rationale": (
                    f"{shock_desc}; lookback move {move:.3f}, retrace {retrace:.3f}, target ${target_exit_price:.3f}"
                ),
            }
        ]
        opportunity = self.create_opportunity(
            title=f"Certainty Shock: {market.question[:50]}...",
            description=(
                f"Rapid repricing detected near deadline ({days_remaining:.2f}d). "
                f"{shock_desc}: peak=${peak:.3f}, trough=${trough:.3f}, current YES=${yes_price:.3f}. "
                f"Buy {outcome} @ ${entry_price:.3f}, target repricing ${target_exit_price:.3f}."
            ),
            total_cost=entry_price,
            expected_payout=target_exit_price,
            markets=[market],
            positions=positions,
            is_guaranteed=False,
            min_liquidity_hard=_SHOCK_MIN_LIQUIDITY_HARD,
            min_position_size=_SHOCK_MIN_POSITION_SIZE,
        )
        if opportunity is None or opportunity.roi_percent > 120.0:
            return None

        risk_score = 0.62 - min(move * 0.35, 0.20)
        if days_remaining <= 1.0:
            risk_score -= 0.05
        opportunity.risk_score = max(0.35, min(risk_score, 0.75))
        opportunity.risk_factors.insert(
            0,
            "DIRECTIONAL BET — certainty shock can reverse before final settlement.",
        )
        opportunity.risk_factors.append(f"Certainty shock: {shock_desc}, move={move:.1%}, retrace={retrace:.1%}")
        opportunity.risk_factors.append(f"Near expiry window: {days_remaining:.2f} days to deadline")
        opportunity.risk_factors.append(f"Target repricing edge: +${expected_move:.3f} per share")
        opportunity.strategy_context["sub_strategy"] = "certainty_shock"
        return opportunity

    def _create_decay_opportunity(
        self,
        *,
        market: Market,
        actual_price: float,
        expected_price: float,
        deviation: float,
        days_remaining: float,
        deadline: datetime,
        prices: dict[str, dict],
    ) -> Optional[Opportunity]:
        no_price = self._live_no_price(market, prices)
        if deviation > 0:
            outcome = "NO"
            entry_price = no_price
            token_id = market.clob_token_ids[1] if len(market.clob_token_ids) > 1 else None
            direction_desc = "overpriced"
        else:
            outcome = "YES"
            entry_price = actual_price
            token_id = market.clob_token_ids[0] if market.clob_token_ids else None
            direction_desc = "underpriced"

        if entry_price < _MIN_ENTRY_PRICE or entry_price > _MAX_ENTRY_PRICE:
            return None

        target_exit_price = max(0.01, min(0.99, 1.0 - expected_price if deviation > 0 else expected_price))
        expected_move = target_exit_price - entry_price
        if expected_move < _MIN_EXPECTED_MOVE:
            return None

        positions = [
            {
                "action": "BUY",
                "outcome": outcome,
                "market": market.question[:50],
                "price": entry_price,
                "token_id": token_id,
                "rationale": (
                    f"Expected decay price: ${expected_price:.3f}, actual: ${actual_price:.3f} "
                    f"({direction_desc} by {abs(deviation):.3f})"
                ),
            }
        ]
        opportunity = self.create_opportunity(
            title=f"Temporal Decay: {market.question[:50]}...",
            description=(
                f"Deadline market {direction_desc} vs expected decay curve. "
                f"YES actual: ${actual_price:.3f}, expected: ${expected_price:.3f} "
                f"(deviation: {abs(deviation):.3f}). {days_remaining:.1f} days to deadline. "
                f"Buy {outcome} at ${entry_price:.3f}, target re-price ${target_exit_price:.3f}."
            ),
            total_cost=entry_price,
            expected_payout=target_exit_price,
            markets=[market],
            positions=positions,
            is_guaranteed=False,
            min_liquidity_hard=1500.0,
            min_position_size=50.0,
        )
        if opportunity is None or opportunity.roi_percent > 120.0:
            return None

        deviation_adjustment = min(abs(deviation) * 1.5, 0.10)
        opportunity.risk_score = max(0.55, 0.65 - deviation_adjustment)
        opportunity.risk_factors.insert(
            0,
            "DIRECTIONAL BET — decay deviation may reflect new information instead of mispricing.",
        )
        opportunity.risk_factors.append(f"Statistical edge: decay deviation {abs(deviation):.1%}")
        opportunity.risk_factors.append(f"Deadline in {days_remaining:.0f} days ({deadline.strftime('%Y-%m-%d')})")
        opportunity.risk_factors.append(f"Expected repricing target: +${expected_move:.3f} per share")
        if days_remaining < 7:
            opportunity.risk_factors.append("Near-deadline: steep decay but higher event uncertainty")
        opportunity.strategy_context["sub_strategy"] = "decay_curve"
        return opportunity

    def _get_category(self, market: Market, events: list[Event]) -> Optional[str]:
        """Look up the category for a market via its parent event."""
        for event in events:
            for em in event.markets:
                if em.id == market.id:
                    return event.category
        return None

    def _get_event_for_market(self, market: Market, events: list[Event]) -> Optional[Event]:
        """Find the parent event for a given market."""
        for event in events:
            for em in event.markets:
                if em.id == market.id:
                    return event
        return None

    # ------------------------------------------------------------------
    # Composite Fair Probability
    # ------------------------------------------------------------------
    def _compute_fair_probability(
        self,
        market: Market,
        yes_price: float,
        events: list[Event],
        event_markets: list[Market],
        prices: dict[str, dict],
        category: Optional[str],
    ) -> tuple[float, dict[str, float]]:
        """Ensemble all signals into a composite fair probability estimate.

        Returns (fair_probability, signal_breakdown).
        """
        signals: dict[str, float] = {}

        # Signal 1: Anchoring
        anchor = self._signal_anchoring(yes_price)
        signals["anchoring"] = anchor

        # Signal 2: Category base rate
        cat_signal = self._signal_category_base_rate(yes_price, category)
        signals["category_base_rate"] = cat_signal

        # Signal 3: Consensus
        consensus = self._signal_consensus(market, event_markets, prices)
        if consensus is None:
            consensus = 0.0
        signals["consensus"] = consensus

        # Signal 4: Momentum
        momentum = self._signal_momentum(market.id, yes_price)
        signals["momentum"] = momentum

        # Signal 5: Volume-price divergence
        vol_price = self._signal_volume_price(yes_price, market.volume, market.liquidity)
        signals["volume_price"] = vol_price

        # Signal 6: Favorite/longshot skew
        flb_signal = self._signal_favorite_longshot(yes_price)
        signals["favorite_longshot"] = flb_signal

        # Signal 7: Liquidity imbalance (depth/pressure)
        liquidity_imbalance = self._signal_liquidity_imbalance(market, prices)
        signals["liquidity_imbalance"] = liquidity_imbalance

        # Weighted composite adjustment
        # Each signal contributes a directional shift from the market price.
        # Positive signal => fair prob higher than market (buy YES),
        # Negative signal => fair prob lower than market (buy NO).
        adjustment = 0.0
        total_weight = 0.0
        for sig_name, sig_value in signals.items():
            w = SIGNAL_WEIGHTS.get(sig_name, 0.0)
            adjustment += w * sig_value
            total_weight += w

        if total_weight > 0:
            adjustment /= total_weight
            # Scale the adjustment: full signal moves price by up to 0.15
            adjustment *= 0.15

        fair_prob = yes_price + adjustment
        # Clamp to [0.01, 0.99]
        fair_prob = max(0.01, min(0.99, fair_prob))

        return fair_prob, signals

    # ------------------------------------------------------------------
    # detect()
    # ------------------------------------------------------------------
    def detect(
        self,
        events: list[Event],
        markets: list[Market],
        prices: dict[str, dict],
    ) -> list[Opportunity]:
        """Detect statistical arbitrage opportunities.

        For each binary market, calculate all signals, compute composite
        fair probability, and if the edge exceeds the configured minimum
        create an opportunity.
        """
        config = dict(self.default_config)
        config.update(getattr(self, "config", {}) or {})
        min_edge = max(0.0, float(config.get("min_edge_percent", 5.0) or 5.0) / 100.0)
        enable_stat_signals = bool(config.get("enable_stat_signals", True))
        enable_certainty_shock = bool(config.get("enable_certainty_shock", True))
        enable_decay_curve = bool(config.get("enable_decay_curve", True))
        excluded_keywords = self._normalize_excluded_keywords(config.get("exclude_market_keywords"))
        shock_lookback_seconds = max(1, int(float(config.get("shock_lookback_seconds", _SHOCK_LOOKBACK_SECONDS) or _SHOCK_LOOKBACK_SECONDS)))
        shock_min_abs_move = max(0.0, float(config.get("shock_min_abs_move", _SHOCK_MIN_ABS_MOVE) or _SHOCK_MIN_ABS_MOVE))
        shock_max_retrace = max(0.0, float(config.get("shock_max_retrace", _SHOCK_MAX_RETRACE) or _SHOCK_MAX_RETRACE))
        shock_min_favored_price = max(0.0, float(config.get("shock_min_favored_price", _SHOCK_MIN_FAVORED_PRICE) or _SHOCK_MIN_FAVORED_PRICE))
        shock_target_certainty = max(0.01, min(0.995, float(config.get("shock_target_certainty", _SHOCK_TARGET_CERTAINTY) or _SHOCK_TARGET_CERTAINTY)))
        max_days_to_deadline = max(0.0, float(config.get("max_days_to_deadline", _MAX_DAYS_TO_DEADLINE) or _MAX_DAYS_TO_DEADLINE))
        min_days_to_deadline = max(0.0, float(config.get("min_days_to_deadline", _MIN_DAYS_TO_DEADLINE) or _MIN_DAYS_TO_DEADLINE))
        opportunities: list[Opportunity] = []
        price_history = self.state.setdefault("price_history", {})
        market_baselines = self.state.setdefault("market_baselines", {})
        now = utcnow()
        scan_time = time.time()

        # Pre-build market-id -> event mapping for fast lookup
        market_to_event: dict[str, Event] = {}
        for event in events:
            for em in event.markets:
                market_to_event[em.id] = event

        for market in markets:
            # Only binary markets
            if len(market.outcome_prices) != 2:
                continue

            # Skip inactive or closed
            if market.closed or not market.active:
                continue

            if excluded_keywords:
                market_text = self._market_text(market)
                if self._first_blocked_keyword(market_text, excluded_keywords) is not None:
                    continue

            market_keys = [
                str(getattr(market, "id", "") or "").upper(),
                str(getattr(market, "condition_id", "") or "").upper(),
            ]
            if any(key.startswith(prefix) for key in market_keys for prefix in _MULTILEG_MARKET_PREFIXES):
                continue
            if self._is_sports_parlay(market):
                continue

            # Skip markets whose subject matter has already occurred.
            # Statistical base-rate signals are meaningless when the market
            # price reflects actual known outcomes (e.g., "2025 revenue"
            # scanned in 2026).
            if self._is_likely_already_resolved(market):
                continue

            # Get live prices
            yes_price = self._live_yes_price(market, prices)
            no_price = self._live_no_price(market, prices)

            if enable_certainty_shock or enable_decay_curve:
                if market.id not in price_history:
                    price_history[market.id] = []
                price_history[market.id].append((scan_time, yes_price))
                if len(price_history[market.id]) > 100:
                    price_history[market.id] = price_history[market.id][-100:]

            if enable_certainty_shock:
                certainty_opportunity = self._create_certainty_shock_opportunity(
                    market=market,
                    yes_price=yes_price,
                    no_price=no_price,
                    now=now,
                    scan_time=scan_time,
                    lookback_seconds=shock_lookback_seconds,
                    min_abs_move=shock_min_abs_move,
                    max_retrace=shock_max_retrace,
                    min_favored_price=shock_min_favored_price,
                    target_certainty=shock_target_certainty,
                )
                if certainty_opportunity is not None:
                    opportunities.append(certainty_opportunity)
                    continue

            if enable_decay_curve:
                deadline = self._extract_deadline(market)
                if deadline is not None:
                    days_remaining = (deadline - now).total_seconds() / 86400.0
                    if min_days_to_deadline <= days_remaining <= max_days_to_deadline:
                        if market.id not in market_baselines:
                            market_baselines[market.id] = (deadline, max(yes_price, 0.10))
                        else:
                            stored_deadline, stored_price = market_baselines[market.id]
                            market_baselines[market.id] = (stored_deadline, max(stored_price, yes_price))

                        history = price_history.get(market.id, [])
                        if len(history) >= _MIN_HISTORY_POINTS:
                            first_seen_dt = datetime.fromtimestamp(history[0][0], tz=timezone.utc)
                            total_days = max((deadline - first_seen_dt).total_seconds() / 86400.0, 1.0)
                            ratio = min(days_remaining / total_days, 1.0)
                            initial_price = market_baselines[market.id][1]
                            expected_price = initial_price * (ratio**_DEFAULT_DECAY_RATE)
                            deviation = yes_price - expected_price
                            if abs(deviation) >= _MIN_DEVIATION:
                                decay_opportunity = self._create_decay_opportunity(
                                    market=market,
                                    actual_price=yes_price,
                                    expected_price=expected_price,
                                    deviation=deviation,
                                    days_remaining=days_remaining,
                                    deadline=deadline,
                                    prices=prices,
                                )
                                if decay_opportunity is not None:
                                    opportunities.append(decay_opportunity)
                                    continue

            # Skip extreme prices (nearly resolved, nothing to trade)
            if yes_price < 0.03 or yes_price > 0.97:
                continue

            if not enable_stat_signals:
                continue

            # Look up parent event and category
            event = market_to_event.get(market.id)
            category = event.category if event else None
            event_markets = event.markets if event else [market]

            # Compute fair probability and signal breakdown
            fair_prob, signal_breakdown = self._compute_fair_probability(
                market=market,
                yes_price=yes_price,
                events=events,
                event_markets=event_markets,
                prices=prices,
                category=category,
            )

            # Edge = fair probability minus market price
            edge = fair_prob - yes_price

            # Only trade if edge exceeds threshold
            if abs(edge) < min_edge:
                continue

            # Determine direction
            if edge > 0:
                # Fair prob > market price => YES is underpriced, buy YES
                outcome = "YES"
                buy_price = yes_price
                token_id = (
                    market.clob_token_ids[0] if market.clob_token_ids and len(market.clob_token_ids) > 0 else None
                )
            else:
                # Fair prob < market price => YES is overpriced, buy NO
                outcome = "NO"
                buy_price = no_price
                token_id = (
                    market.clob_token_ids[1] if market.clob_token_ids and len(market.clob_token_ids) > 1 else None
                )

            # Profit metrics
            # expected_payout is 1.0 if our prediction is correct
            total_cost = buy_price
            expected_payout = 1.0
            edge_pct = abs(edge) * 100.0
            roi = self.fee_adjusted_edge_pct(
                edge_pct,
                buy_price,
                platform=str(getattr(market, "platform", "polymarket") or "polymarket"),
            )
            net_profit = total_cost * (roi / 100.0)

            # Skip if negative expected profit after fees
            if net_profit <= 0:
                continue

            # Liquidity & position sizing (moved up for hard filter checks)
            min_liquidity = market.liquidity
            max_position = min_liquidity * 0.05  # Conservative: 5% of liquidity

            # --- Hard filters (matching create_opportunity gate) ---
            if roi > settings.MAX_PLAUSIBLE_ROI:
                continue
            if market.liquidity < settings.MIN_LIQUIDITY_HARD:
                continue
            if max_position < settings.MIN_POSITION_SIZE:
                continue
            absolute_profit = max_position * (net_profit / total_cost) if total_cost > 0 else 0
            if absolute_profit < settings.MIN_ABSOLUTE_PROFIT:
                continue
            if market.end_date:
                resolution_aware = make_aware(market.end_date)
                days_until = (resolution_aware - utcnow()).days
                if days_until > settings.MAX_RESOLUTION_MONTHS * 30:
                    continue
                annualized_roi = roi * (365.0 / max(days_until, 1))
                if annualized_roi < settings.MIN_ANNUALIZED_ROI:
                    continue

            # Risk assessment: stat arb is uncertain by nature
            # Risk score between 0.40 and 0.60 depending on edge strength
            # Stronger edge = slightly lower risk
            edge_strength = min(abs(edge) / 0.20, 1.0)  # Normalize to [0, 1]
            risk_score = 0.60 - (edge_strength * 0.20)  # Range: 0.40 - 0.60
            risk_score = max(0.40, min(0.60, risk_score))

            risk_factors = [
                "Statistical edge, not guaranteed profit",
                f"Composite edge: {abs(edge):.1%} ({outcome} side)",
                f"Fair probability estimate: {fair_prob:.1%} vs market {yes_price:.1%}",
            ]

            # Add signal-specific risk factors
            if signal_breakdown.get("anchoring", 0) > 0:
                risk_factors.append("Anchoring bias detected at round number")
            if abs(signal_breakdown.get("category_base_rate", 0)) > 0.10:
                risk_factors.append(f"Category base rate deviation: {signal_breakdown['category_base_rate']:+.2f}")
            if abs(signal_breakdown.get("consensus", 0)) > 0.3:
                risk_factors.append("Price is an outlier within its event")
            if abs(signal_breakdown.get("momentum", 0)) > 0.3:
                direction_word = "upward" if signal_breakdown["momentum"] > 0 else "downward"
                risk_factors.append(f"Strong {direction_word} momentum detected")
            if abs(signal_breakdown.get("volume_price", 0)) > 0.3:
                risk_factors.append("Volume-price divergence detected")
            if abs(signal_breakdown.get("favorite_longshot", 0)) > 0.25:
                risk_factors.append("Favorite-longshot skew signal contributed")
            if abs(signal_breakdown.get("liquidity_imbalance", 0)) > 0.25:
                risk_factors.append("Order-book liquidity imbalance contributed")

            # Build position
            positions = [
                {
                    "action": "BUY",
                    "outcome": outcome,
                    "price": buy_price,
                    "token_id": token_id,
                    "edge": round(edge, 4),
                    "fair_probability": round(fair_prob, 4),
                    "signals": {k: round(v, 4) for k, v in signal_breakdown.items()},
                }
            ]

            opp = self.create_opportunity(
                title=f"Stat Arb: {market.question[:60]}",
                description=(
                    f"Buy {outcome} @ ${buy_price:.3f} | "
                    f"Fair prob {fair_prob:.1%} vs market {yes_price:.1%} | "
                    f"Edge {abs(edge):.1%}"
                ),
                total_cost=total_cost,
                expected_payout=expected_payout,
                markets=[market],
                positions=positions,
                event=event,
                is_guaranteed=False,
                custom_roi_percent=roi,
                custom_risk_score=risk_score,
            )
            if opp is not None:
                opp.risk_factors = risk_factors

            opportunities.append(opp)

        # Sort by absolute edge (strongest signals first)
        opportunities.sort(
            key=lambda opportunity: abs(
                float(opportunity.positions_to_take[0].get("edge", 0.0))
                if opportunity.positions_to_take and isinstance(opportunity.positions_to_take[0], dict)
                else float(opportunity.roi_percent or 0.0)
            ),
            reverse=True,
        )

        return opportunities

    def compute_size(
        self, base_size: float, max_size: float, edge: float, confidence: float, risk_score: float, market_count: int
    ) -> float:
        """Kelly-informed sizing for statistical arbitrage."""
        p_estimated = 0.5 + (edge / 200.0)
        p_market = 0.5
        kelly_f = kelly_fraction(p_estimated, p_market, fraction=0.25)
        kelly_sz = base_size * (1.0 + kelly_f * 10.0)
        size = kelly_sz * (0.7 + confidence * 0.6) * max(0.4, 1.0 - risk_score)
        return max(1.0, min(max_size, size))

    def custom_checks(self, signal, context, params, payload):
        source = str(getattr(signal, "source", "") or "").strip().lower()
        excluded_keywords = self._normalize_excluded_keywords(
            params.get("exclude_market_keywords", self.config.get("exclude_market_keywords", ""))
        )
        blocked_keyword = self._first_blocked_keyword(self._signal_market_text(signal, payload), excluded_keywords)
        return [
            DecisionCheck("source", "Signal source", source == "scanner", detail=f"got={source}"),
            DecisionCheck(
                "keyword_exclusion",
                "Market keyword exclusion",
                blocked_keyword is None,
                detail=f"blocked by '{blocked_keyword}'" if blocked_keyword else "no excluded keywords matched",
            ),
        ]

    def should_exit(self, position: Any, market_state: dict) -> ExitDecision:
        """Stat arb: standard TP/SL exit with temporal defaults applied."""
        if market_state.get("is_resolved"):
            return self.default_exit_check(position, market_state)
        config = getattr(position, "config", None) or {}
        config = dict(config)
        for key, fallback in (
            ("take_profit_pct", 12.0),
            ("stop_loss_pct", 25.0),
            ("trailing_stop_pct", 15.0),
        ):
            try:
                default_value = float((getattr(self, "config", None) or {}).get(key, fallback))
            except (TypeError, ValueError):
                default_value = fallback
            config.setdefault(key, default_value)
        position.config = config
        return self.default_exit_check(position, market_state)

    # ------------------------------------------------------------------
    # Platform gate hooks
    # ------------------------------------------------------------------

    def on_blocked(self, signal, reason: str, context: dict) -> None:
        logger.info("%s: signal blocked — %s (market=%s)", self.name, reason, getattr(signal, "market_id", "?"))

    def on_size_capped(self, original_size: float, capped_size: float, reason: str) -> None:
        logger.info("%s: size capped $%.0f → $%.0f — %s", self.name, original_size, capped_size, reason)
