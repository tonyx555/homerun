"""Flash-crash reversion opportunity filter.

Portable pattern adapted from open-source Polymarket flash-crash bots:
track short-horizon price drops and surface rebound entries only when
liquidity/spread constraints make execution realistic.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Optional

from config import settings
from models import ArbitrageOpportunity, Event, Market
from models.opportunity import MispricingType
from services.strategies.base import BaseStrategy


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class FlashCrashReversionStrategy(BaseStrategy):
    """Detect abrupt short-window probability crashes and buy reversion."""

    strategy_type = "flash_crash_reversion"
    name = "Flash Crash Reversion"
    description = "Short-horizon crash detector with liquidity/spread gating"

    default_config = {
        "lookback_seconds": 240.0,
        "stale_history_seconds": 1800.0,
        "drop_threshold": 0.08,
        "min_rebound_fraction": 0.45,
        "min_target_move": 0.015,
        "max_entry_price": 0.82,
        "max_spread": 0.07,
        "min_liquidity": 2500.0,
        "max_opportunities": 40,
    }

    def __init__(self) -> None:
        super().__init__()
        self.config = dict(self.default_config)
        # market_id -> deque[(ts, yes, no, yes_bid, yes_ask, no_bid, no_ask)]
        self._history: dict[str, deque[tuple[float, float, float, Optional[float], Optional[float], Optional[float], Optional[float]]]] = {}

    @staticmethod
    def _extract_book_value(payload: Optional[dict], key: str) -> Optional[float]:
        if not isinstance(payload, dict):
            return None
        val = payload.get(key)
        if isinstance(val, (int, float)):
            return float(val)
        return None

    def _extract_yes_no_snapshot(
        self,
        market: Market,
        prices: dict[str, dict],
    ) -> tuple[float, float, Optional[float], Optional[float], Optional[float], Optional[float]]:
        yes = _safe_float(market.yes_price)
        no = _safe_float(market.no_price)
        yes_bid = None
        yes_ask = None
        no_bid = None
        no_ask = None

        token_ids = list(getattr(market, "clob_token_ids", []) or [])
        if token_ids:
            yes_raw = prices.get(token_ids[0])
            yes_mid = self._extract_book_value(yes_raw, "mid")
            if yes_mid is None:
                yes_mid = self._extract_book_value(yes_raw, "price")
            if yes_mid is not None:
                yes = yes_mid
            yes_bid = self._extract_book_value(yes_raw, "bid") or self._extract_book_value(yes_raw, "best_bid")
            yes_ask = self._extract_book_value(yes_raw, "ask") or self._extract_book_value(yes_raw, "best_ask")

        if len(token_ids) > 1:
            no_raw = prices.get(token_ids[1])
            no_mid = self._extract_book_value(no_raw, "mid")
            if no_mid is None:
                no_mid = self._extract_book_value(no_raw, "price")
            if no_mid is not None:
                no = no_mid
            no_bid = self._extract_book_value(no_raw, "bid") or self._extract_book_value(no_raw, "best_bid")
            no_ask = self._extract_book_value(no_raw, "ask") or self._extract_book_value(no_raw, "best_ask")

        if (yes <= 0.0 or no <= 0.0) and len(getattr(market, "outcome_prices", []) or []) >= 2:
            yes = yes if yes > 0.0 else _safe_float(market.outcome_prices[0])
            no = no if no > 0.0 else _safe_float(market.outcome_prices[1])

        return yes, no, yes_bid, yes_ask, no_bid, no_ask

    def _side_spread(
        self,
        bid: Optional[float],
        ask: Optional[float],
    ) -> float:
        if bid is None or ask is None:
            return 0.0
        if bid <= 0.0 or ask <= 0.0:
            return 0.0
        return max(0.0, ask - bid)

    def detect(
        self,
        events: list[Event],
        markets: list[Market],
        prices: dict[str, dict],
    ) -> list[ArbitrageOpportunity]:
        cfg = dict(self.default_config)
        cfg.update(getattr(self, "config", {}) or {})

        lookback_seconds = max(30.0, _safe_float(cfg.get("lookback_seconds"), 240.0))
        stale_history_seconds = max(lookback_seconds * 2.0, _safe_float(cfg.get("stale_history_seconds"), 1800.0))
        drop_threshold = _clamp(_safe_float(cfg.get("drop_threshold"), 0.08), 0.01, 0.50)
        min_rebound_fraction = _clamp(_safe_float(cfg.get("min_rebound_fraction"), 0.45), 0.10, 0.95)
        min_target_move = _clamp(_safe_float(cfg.get("min_target_move"), 0.015), 0.005, 0.15)
        max_entry_price = _clamp(_safe_float(cfg.get("max_entry_price"), 0.82), 0.2, 0.99)
        max_spread = _clamp(_safe_float(cfg.get("max_spread"), 0.07), 0.005, 0.25)
        min_liquidity = max(100.0, _safe_float(cfg.get("min_liquidity"), 2500.0))
        max_opportunities = max(1, int(_safe_float(cfg.get("max_opportunities"), 40)))

        event_by_market: dict[str, Event] = {}
        for event in events:
            for event_market in event.markets:
                event_by_market[event_market.id] = event

        now = time.time()
        candidates: list[tuple[float, ArbitrageOpportunity]] = []

        for market in markets:
            if market.closed or not market.active:
                continue
            if len(list(getattr(market, "outcome_prices", []) or [])) < 2 and len(list(getattr(market, "clob_token_ids", []) or [])) < 2:
                continue
            if _safe_float(getattr(market, "liquidity", 0.0)) < min_liquidity:
                continue

            yes, no, yes_bid, yes_ask, no_bid, no_ask = self._extract_yes_no_snapshot(market, prices)
            if not (0.0 < yes < 1.0 and 0.0 < no < 1.0):
                continue

            row = (now, yes, no, yes_bid, yes_ask, no_bid, no_ask)
            history = self._history.setdefault(market.id, deque(maxlen=180))
            history.append(row)

            # Keep only recent rows for crash detection.
            while history and (now - history[0][0]) > stale_history_seconds:
                history.popleft()

            if len(history) < 2:
                continue

            baseline = None
            cutoff = now - lookback_seconds
            for point in history:
                if point[0] >= cutoff:
                    baseline = point
                    break
            if baseline is None:
                continue

            for outcome, idx, bid, ask in (
                ("YES", 1, yes_bid, yes_ask),
                ("NO", 2, no_bid, no_ask),
            ):
                old_price = _safe_float(baseline[idx])
                current_price = yes if outcome == "YES" else no
                if old_price <= 0.0 or current_price <= 0.0:
                    continue

                drop = old_price - current_price
                if drop < drop_threshold:
                    continue
                if current_price > max_entry_price:
                    continue

                spread = self._side_spread(bid, ask)
                if spread > max_spread:
                    continue

                target_move = max(min_target_move, drop * min_rebound_fraction)
                target_price = min(0.99, current_price + target_move)
                target_price = min(target_price, old_price - 0.001)
                if target_price <= (current_price + 1e-6):
                    continue

                token_ids = list(getattr(market, "clob_token_ids", []) or [])
                token_id = token_ids[0] if outcome == "YES" and len(token_ids) > 0 else None
                if outcome == "NO" and len(token_ids) > 1:
                    token_id = token_ids[1]

                positions = [
                    {
                        "action": "BUY",
                        "outcome": outcome,
                        "price": current_price,
                        "token_id": token_id,
                        "entry_style": "reversion",
                        "_flash_crash": {
                            "old_price": old_price,
                            "new_price": current_price,
                            "drop": drop,
                            "lookback_seconds": lookback_seconds,
                            "target_price": target_price,
                            "spread": spread,
                        },
                    }
                ]

                opp = self.create_opportunity(
                    title=f"Flash Reversion: {outcome} dip in {market.question[:64]}",
                    description=(
                        f"{outcome} dropped {drop:.3f} ({old_price:.3f}->{current_price:.3f}) "
                        f"over {int(lookback_seconds)}s; targeting rebound to {target_price:.3f}."
                    ),
                    total_cost=current_price,
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

                liquidity = _safe_float(getattr(market, "liquidity", 0.0))
                liquidity_penalty = 0.0 if liquidity >= 10000.0 else 0.08
                risk_score = 0.66 - min(0.20, drop * 1.25) + min(0.16, spread * 2.5) + liquidity_penalty
                opp.risk_score = _clamp(risk_score, 0.35, 0.86)
                opp.risk_factors = [
                    f"Short-window crash magnitude {drop:.1%}",
                    f"Targeting partial rebound ({target_move:.1%})",
                    f"Book spread {spread:.2%}",
                ]
                opp.mispricing_type = MispricingType.WITHIN_MARKET
                candidates.append((drop, opp))

        if not candidates:
            return []

        # Keep strongest crashes first; de-duplicate by (market, outcome).
        candidates.sort(key=lambda item: item[0], reverse=True)
        out: list[ArbitrageOpportunity] = []
        seen: set[tuple[str, str]] = set()
        for _, opp in candidates:
            position = (opp.positions_to_take or [{}])[0]
            outcome = str(position.get("outcome") or "")
            market_id = str((opp.markets or [{}])[0].get("id") or "")
            key = (market_id, outcome)
            if key in seen:
                continue
            seen.add(key)
            out.append(opp)
            if len(out) >= max_opportunities:
                break
        return out
