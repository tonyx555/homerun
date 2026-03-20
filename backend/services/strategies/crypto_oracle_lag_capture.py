"""Oracle-lag capture strategy for short-horizon crypto markets.

Detects opportunities directly from ``crypto_update`` worker payloads where
the oracle price has moved but binary market prices have not yet caught up.
Fully independent — does not depend on other strategies for signal generation.
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


def _parse_datetime_utc(value):
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


class CryptoOracleLagCaptureStrategy(BaseStrategy):
    strategy_type = "crypto_oracle_lag_capture"
    name = "Crypto Oracle Lag Capture"
    description = "Captures temporary lag between oracle prints and binary market prices."
    mispricing_type = "within_market"
    source_key = "crypto"
    market_categories = ["crypto"]
    subscriptions = ["crypto_update"]

    quality_filter_overrides = QualityFilterOverrides(
        min_roi=0.9,
        max_resolution_months=0.1,
    )

    default_config = {
        "min_edge_percent": 1.8,
        "min_confidence": 0.46,
        "min_oracle_diff_pct": 0.12,
        "max_oracle_age_seconds": 10.0,
        "max_entry_price": 0.92,
        "base_size_usd": 18.0,
        "max_size_usd": 160.0,
        "sizing_policy": "kelly",
        "kelly_fractional_scale": 0.45,
        "take_profit_pct": 9.0,
        "stop_loss_pct": 4.5,
        "max_hold_minutes": 14.0,
        "max_markets_per_event": 24,
    }

    # ------------------------------------------------------------------ #
    #  Market builder
    # ------------------------------------------------------------------ #

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

    # ------------------------------------------------------------------ #
    #  Scoring / detection
    # ------------------------------------------------------------------ #

    def _score_market(self, row: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any] | None:
        up_price = safe_float(row.get("up_price"), None)
        down_price = safe_float(row.get("down_price"), None)
        oracle_price = safe_float(row.get("oracle_price"), None)
        price_to_beat = safe_float(row.get("price_to_beat"), None)

        if up_price is None or down_price is None or oracle_price is None or price_to_beat is None:
            return None
        if not (0.0 < up_price < 1.0 and 0.0 < down_price < 1.0 and price_to_beat > 0.0):
            return None

        # Oracle freshness gate
        oracle_age_seconds = safe_float(row.get("oracle_age_seconds"), None)
        max_oracle_age_seconds = max(0.1, to_float(cfg.get("max_oracle_age_seconds", 10.0), 10.0))
        if oracle_age_seconds is None or oracle_age_seconds > max_oracle_age_seconds:
            return None

        # Oracle / market divergence
        diff_pct = ((oracle_price - price_to_beat) / price_to_beat) * 100.0
        min_oracle_diff_pct = max(0.01, to_float(cfg.get("min_oracle_diff_pct", 0.12), 0.12))
        if abs(diff_pct) < min_oracle_diff_pct:
            return None

        # Direction
        if diff_pct > 0.0:
            direction = "buy_yes"
            outcome = "YES"
            selected_price = float(up_price)
        else:
            direction = "buy_no"
            outcome = "NO"
            selected_price = float(down_price)

        # Entry price cap
        max_entry_price = min(0.999, max(0.01, to_float(cfg.get("max_entry_price", 0.92), 0.92)))
        if selected_price <= 0.0 or selected_price >= 1.0 or selected_price > max_entry_price:
            return None

        # Time info
        seconds_left = safe_float(row.get("seconds_left"), None)
        if seconds_left is None:
            end_date = _parse_datetime_utc(row.get("end_time"))
            if end_date is not None:
                seconds_left = max(0.0, (end_date - datetime.now(timezone.utc)).total_seconds())

        # Elapsed ratio (use a default window of 300s if we don't know)
        timeframe_seconds = safe_float(row.get("timeframe_seconds"), 300.0) or 300.0
        elapsed_ratio = 0.0
        if seconds_left is not None and timeframe_seconds > 0:
            elapsed_ratio = clamp(1.0 - (seconds_left / float(max(1, timeframe_seconds))), 0.0, 1.0)

        # Liquidity
        liquidity = max(0.0, float(safe_float(row.get("liquidity"), 0.0) or 0.0))

        # Confidence
        edge = abs(diff_pct)
        confidence = clamp(
            0.55
            + clamp(edge / 8.0, 0.0, 0.25)
            + clamp(elapsed_ratio * 0.15, 0.0, 0.15),
            0.55,
            0.92,
        )

        # Risk score
        freshness = clamp(1.0 - (float(oracle_age_seconds) / max_oracle_age_seconds), 0.0, 1.0)
        risk_score = clamp(
            0.55
            - clamp(edge / 6.0, 0.0, 0.20)
            - (freshness * 0.10)
            + ((1.0 - clamp(liquidity / 20_000.0, 0.0, 1.0)) * 0.10),
            0.15,
            0.85,
        )

        # Composite score (mirrors evaluate() scoring)
        score = (
            (edge * 0.55)
            + (confidence * 34.0)
            + (min(1.0, edge / 0.5) * 10.0)
            + (min(1.0, liquidity / 20_000.0) * 4.0)
        )

        return {
            "direction": direction,
            "outcome": outcome,
            "selected_price": selected_price,
            "oracle_price": float(oracle_price),
            "price_to_beat": float(price_to_beat),
            "oracle_diff_pct": float(diff_pct),
            "oracle_age_seconds": float(oracle_age_seconds),
            "edge": float(edge),
            "confidence": float(confidence),
            "risk_score": float(risk_score),
            "liquidity": float(liquidity),
            "seconds_left": float(seconds_left) if seconds_left is not None else None,
            "elapsed_ratio": float(elapsed_ratio),
            "freshness": float(freshness),
            "score": float(score),
        }

    # ------------------------------------------------------------------ #
    #  Opportunity builder
    # ------------------------------------------------------------------ #

    def _build_opportunity(self, row: dict[str, Any], signal: dict[str, Any]) -> Opportunity | None:
        typed_market = self._row_market(row)
        if typed_market is None:
            return None

        direction = str(signal["direction"])
        outcome = str(signal["outcome"])
        selected_price = float(signal["selected_price"])
        edge = float(signal["edge"])
        token_ids = list(typed_market.clob_token_ids or [])
        token_idx = 0 if direction == "buy_yes" else 1
        token_id = token_ids[token_idx] if len(token_ids) > token_idx else None

        target_exit_price = clamp(selected_price + (edge / 100.0), selected_price + 0.0001, 0.999)

        positions = [
            {
                "action": "BUY",
                "outcome": outcome,
                "price": selected_price,
                "token_id": token_id,
            }
        ]

        lag_context = {
            "strategy_origin": "crypto_worker",
            "direction": direction,
            "oracle_price": signal["oracle_price"],
            "price_to_beat": signal["price_to_beat"],
            "oracle_diff_pct": signal["oracle_diff_pct"],
            "oracle_age_seconds": signal["oracle_age_seconds"],
            "edge": signal["edge"],
            "confidence": signal["confidence"],
            "risk_score": signal["risk_score"],
            "liquidity": signal["liquidity"],
            "seconds_left": signal["seconds_left"],
            "elapsed_ratio": signal["elapsed_ratio"],
            "freshness": signal["freshness"],
            "target_exit_price": float(target_exit_price),
            "market_data_age_ms": safe_float(row.get("market_data_age_ms"), None),
            "fetched_at": row.get("fetched_at"),
            "end_time": row.get("end_time"),
        }

        opp = self.create_opportunity(
            title=f"Oracle Lag: {outcome} @ {selected_price:.3f}",
            description=(
                f"Oracle diff {signal['oracle_diff_pct']:+.3f}%, "
                f"age {signal['oracle_age_seconds']:.1f}s, "
                f"edge {signal['edge']:.2f}%"
            ),
            total_cost=selected_price,
            expected_payout=target_exit_price,
            markets=[typed_market],
            positions=positions,
            is_guaranteed=False,
            skip_fee_model=True,
            custom_roi_percent=edge,
            custom_risk_score=signal["risk_score"],
            confidence=signal["confidence"],
        )
        if opp is None:
            return None

        opp.risk_factors = [
            f"Oracle lag entry (diff={signal['oracle_diff_pct']:+.3f}%)",
            f"Oracle age={signal['oracle_age_seconds']:.1f}s",
            f"Edge={signal['edge']:.2f}%",
        ]
        opp.strategy_context = {
            "source_key": "crypto",
            "strategy_slug": self.strategy_type,
            **lag_context,
        }
        return opp

    # ------------------------------------------------------------------ #
    #  Batch detection
    # ------------------------------------------------------------------ #

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

    # ------------------------------------------------------------------ #
    #  Event handler (independent signal generation)
    # ------------------------------------------------------------------ #

    async def on_event(self, event: DataEvent) -> list:
        payload = event.payload if isinstance(event.payload, dict) else {}
        rows = payload.get("markets")
        if not isinstance(rows, list) or not rows:
            return []
        return self._detect_from_rows(rows)

    # ------------------------------------------------------------------ #
    #  Evaluate (unchanged — still used by orchestrator for final gate)
    # ------------------------------------------------------------------ #

    def evaluate(self, signal: Any, context: dict[str, Any]) -> StrategyDecision:
        params = context.get("params") or {}
        payload = signal_payload(signal)
        live_market = context.get("live_market")
        if not isinstance(live_market, dict):
            live_market = payload.get("live_market")
        if not isinstance(live_market, dict):
            live_market = {}

        min_edge = max(0.0, to_float(params.get("min_edge_percent", 1.8), 1.8))
        min_conf = to_confidence(params.get("min_confidence", 0.46), 0.46)
        min_oracle_diff_pct = max(0.01, to_float(params.get("min_oracle_diff_pct", 0.12), 0.12))
        max_oracle_age_seconds = max(0.1, to_float(params.get("max_oracle_age_seconds", 10.0), 10.0))
        max_entry_price = min(0.999, max(0.01, to_float(params.get("max_entry_price", 0.92), 0.92)))
        base_size = max(1.0, to_float(params.get("base_size_usd", 18.0), 18.0))
        max_size = max(base_size, to_float(params.get("max_size_usd", 160.0), 160.0))
        sizing_policy = str(params.get("sizing_policy", "kelly") or "kelly")
        kelly_fractional_scale = to_float(params.get("kelly_fractional_scale", 0.45), 0.45)

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

        oracle_diff_pct = safe_float(
            payload.get("oracle_diff_pct"),
            safe_float(live_market.get("oracle_diff_pct"), None),
        )
        oracle_age_seconds = safe_float(
            live_market.get("oracle_age_seconds"),
            safe_float(payload.get("oracle_age_seconds"), None),
        )
        entry_price = to_float(getattr(signal, "entry_price", 0.0), 0.0)
        if entry_price <= 0.0:
            entry_price = to_float(
                live_market.get("live_selected_price"),
                to_float(payload.get("live_selected_price"), to_float(payload.get("selected_price"), 0.0)),
            )

        source_ok = source == "crypto"
        origin_ok = bool(payload.get("strategy_origin") == "crypto_worker") or signal_type.startswith("crypto_worker")
        edge_ok = edge >= min_edge
        confidence_ok = confidence >= min_conf
        oracle_diff_ok = oracle_diff_pct is not None and abs(oracle_diff_pct) >= min_oracle_diff_pct
        oracle_fresh_ok = oracle_age_seconds is not None and oracle_age_seconds <= max_oracle_age_seconds
        entry_price_ok = entry_price > 0.0 and entry_price <= max_entry_price

        if oracle_diff_pct is None:
            direction_ok = False
        elif oracle_diff_pct > 0:
            direction_ok = direction == "buy_yes"
        elif oracle_diff_pct < 0:
            direction_ok = direction == "buy_no"
        else:
            direction_ok = False

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
                "oracle_diff",
                "Oracle/market diff",
                oracle_diff_ok,
                score=oracle_diff_pct,
                detail=f"min_abs={min_oracle_diff_pct:.3f}%",
            ),
            DecisionCheck(
                "oracle_freshness",
                "Oracle freshness",
                oracle_fresh_ok,
                score=oracle_age_seconds,
                detail=f"max_age={max_oracle_age_seconds:.1f}s",
            ),
            DecisionCheck(
                "direction_alignment",
                "Direction aligned to oracle",
                direction_ok,
                score=oracle_diff_pct,
                detail=f"direction={direction or 'unknown'}",
            ),
            DecisionCheck(
                "entry_price",
                "Entry price cap",
                entry_price_ok,
                score=entry_price,
                detail=f"max={max_entry_price:.3f}",
            ),
        ]

        score = (
            (edge * 0.55)
            + (confidence * 34.0)
            + (min(1.0, abs(oracle_diff_pct or 0.0) / 0.5) * 10.0)
            + (min(1.0, liquidity / 20_000.0) * 4.0)
        )

        if not all(check.passed for check in checks):
            return StrategyDecision(
                decision="skipped",
                reason="Oracle-lag capture filters not met",
                score=score,
                checks=checks,
                payload={
                    "edge": edge,
                    "confidence": confidence,
                    "oracle_diff_pct": oracle_diff_pct,
                    "oracle_age_seconds": oracle_age_seconds,
                    "entry_price": entry_price,
                },
            )

        probability = selected_probability(signal, payload, direction)
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
            min_size_usd=1.0,
        )
        size_usd = float(sizing.get("size_usd") or base_size)

        return StrategyDecision(
            decision="selected",
            reason="Oracle-lag capture selected",
            score=score,
            size_usd=size_usd,
            checks=checks,
            payload={
                "edge": edge,
                "confidence": confidence,
                "oracle_diff_pct": oracle_diff_pct,
                "oracle_age_seconds": oracle_age_seconds,
                "entry_price": entry_price,
                "sizing": sizing,
            },
        )

    def should_exit(self, position: Any, market_state: dict) -> ExitDecision:
        config = dict(getattr(position, "config", None) or {})
        strategy_config = dict(getattr(self, "config", None) or {})
        config.setdefault("take_profit_pct", float(strategy_config.get("take_profit_pct", 9.0)))
        config.setdefault("stop_loss_pct", float(strategy_config.get("stop_loss_pct", 4.5)))
        config.setdefault("max_hold_minutes", float(strategy_config.get("max_hold_minutes", 14.0)))
        position.config = config
        return self.default_exit_check(position, market_state)

    def on_blocked(self, signal, reason: str, context: dict) -> None:
        logger.info("%s blocked: %s market=%s", self.name, reason, getattr(signal, "market_id", "?"))
