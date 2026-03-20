"""Cancel-cluster re-entry strategy for crypto prediction markets.

Detects opportunities directly from ``crypto_update`` worker payloads by
identifying cancel-rate spikes followed by cooldowns.  Does not depend on
other crypto strategies for signal generation.
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
    market_categories = ["crypto"]
    subscriptions = [EventType.CRYPTO_UPDATE]
    supports_entry_take_profit_exit = True
    default_open_order_timeout_seconds = 20.0

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
        "max_markets_per_event": 24,
        "min_order_size_usd": 2.0,
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
        # -- prices -----------------------------------------------------------
        up_price = safe_float(row.get("up_price"), None)
        down_price = safe_float(row.get("down_price"), None)
        if up_price is None or down_price is None:
            return None
        if not (0.0 < up_price < 1.0 and 0.0 < down_price < 1.0):
            return None

        # -- cancel-cluster microstructure ------------------------------------
        cancel_rate_30s = _normalize_ratio(
            row.get("cancel_rate_30s") or row.get("maker_cancel_rate_30s")
        )

        prior_peak = _normalize_ratio(row.get("cancel_peak_2m"))
        if prior_peak is None:
            prior_peak = _history_cancel_peak(row.get("history_tail"))

        if cancel_rate_30s is None or prior_peak is None:
            return None

        min_prior_peak_cancel_rate = max(
            0.0, min(1.0, to_float(cfg.get("min_prior_peak_cancel_rate", 0.80), 0.80))
        )
        if prior_peak < min_prior_peak_cancel_rate:
            return None

        max_current_cancel_rate = max(
            0.0, min(1.0, to_float(cfg.get("max_current_cancel_rate", 0.58), 0.58))
        )
        if cancel_rate_30s > max_current_cancel_rate:
            return None

        cancel_drop = prior_peak - cancel_rate_30s
        min_cancel_drop = max(0.0, min(1.0, to_float(cfg.get("min_cancel_drop", 0.18), 0.18)))
        if cancel_drop < min_cancel_drop:
            return None

        # -- spread widening guard --------------------------------------------
        spread_widening_bps = safe_float(row.get("spread_widening_bps"), None)
        max_spread_widening_bps = max(0.0, to_float(cfg.get("max_spread_widening_bps", 18.0), 18.0))
        if spread_widening_bps is not None and spread_widening_bps > max_spread_widening_bps:
            return None

        # -- direction from oracle or orderflow --------------------------------
        oracle_price = safe_float(row.get("oracle_price"), None)
        price_to_beat = safe_float(row.get("price_to_beat"), None)

        diff_pct: float | None = None
        direction: str | None = None
        outcome: str | None = None

        if oracle_price is not None and price_to_beat is not None and price_to_beat > 0.0:
            diff_pct = ((oracle_price - price_to_beat) / price_to_beat) * 100.0
            if diff_pct > 0.0:
                direction = "buy_yes"
                outcome = "YES"
            else:
                direction = "buy_no"
                outcome = "NO"
        else:
            # Fallback: infer direction from orderflow
            flow_imbalance = safe_float(
                row.get("flow_imbalance") or row.get("orderflow_imbalance"), None
            )
            if flow_imbalance is not None:
                if flow_imbalance > 0.0:
                    direction = "buy_yes"
                    outcome = "YES"
                elif flow_imbalance < 0.0:
                    direction = "buy_no"
                    outcome = "NO"

        if direction is None or outcome is None:
            return None

        # -- orderflow alignment check ----------------------------------------
        min_orderflow_alignment = max(
            0.0, min(1.0, to_float(cfg.get("min_orderflow_alignment", 0.04), 0.04))
        )
        flow_imbalance = safe_float(
            row.get("flow_imbalance") or row.get("orderflow_imbalance"), None
        )
        if flow_imbalance is not None:
            if flow_imbalance > 1.0 and flow_imbalance <= 100.0:
                flow_imbalance /= 100.0
            elif flow_imbalance < -1.0 and flow_imbalance >= -100.0:
                flow_imbalance /= 100.0
            flow_imbalance = max(-1.0, min(1.0, flow_imbalance))
            if direction == "buy_yes" and flow_imbalance < -min_orderflow_alignment:
                return None
            if direction == "buy_no" and flow_imbalance > min_orderflow_alignment:
                return None

        # -- entry price ------------------------------------------------------
        selected_price = float(up_price) if direction == "buy_yes" else float(down_price)
        if selected_price <= 0.0 or selected_price >= 1.0:
            return None

        # -- edge -------------------------------------------------------------
        if diff_pct is not None:
            edge = abs(diff_pct)
        else:
            edge = cancel_drop * 8.0  # heuristic when oracle is unavailable

        min_edge_percent = max(0.0, to_float(cfg.get("min_edge_percent", 1.6), 1.6))
        if edge < min_edge_percent:
            return None

        # -- confidence -------------------------------------------------------
        confidence = clamp(
            0.50
            + clamp(cancel_drop / 0.5, 0.0, 0.20)
            + clamp(abs(diff_pct or 0.0) / 10.0, 0.0, 0.15),
            0.43,
            0.90,
        )

        # -- liquidity & score ------------------------------------------------
        liquidity = max(0.0, float(safe_float(row.get("liquidity"), 0.0) or 0.0))

        score = (
            (edge * 0.50)
            + (confidence * 31.0)
            + (min(1.0, max(0.0, cancel_drop) / 0.4) * 9.0)
            + (min(1.0, liquidity / 20_000.0) * 4.0)
        )

        return {
            "direction": direction,
            "outcome": outcome,
            "selected_price": selected_price,
            "edge": float(edge),
            "confidence": float(confidence),
            "cancel_rate_30s": float(cancel_rate_30s),
            "prior_peak": float(prior_peak),
            "cancel_drop": float(cancel_drop),
            "spread_widening_bps": spread_widening_bps,
            "flow_imbalance": flow_imbalance,
            "oracle_price": oracle_price,
            "price_to_beat": price_to_beat,
            "diff_pct": diff_pct,
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
        selected_price = float(signal["selected_price"])
        token_ids = list(typed_market.clob_token_ids or [])
        token_idx = 0 if direction == "buy_yes" else 1
        token_id = token_ids[token_idx] if len(token_ids) > token_idx else None

        reentry_context = {
            "strategy_origin": "crypto_worker",
            "direction": direction,
            "edge": signal["edge"],
            "confidence": signal["confidence"],
            "cancel_rate_30s": signal["cancel_rate_30s"],
            "prior_peak": signal["prior_peak"],
            "cancel_drop": signal["cancel_drop"],
            "spread_widening_bps": signal["spread_widening_bps"],
            "flow_imbalance": signal["flow_imbalance"],
            "oracle_price": signal["oracle_price"],
            "price_to_beat": signal["price_to_beat"],
            "diff_pct": signal["diff_pct"],
            "liquidity": signal["liquidity"],
            "market_data_age_ms": safe_float(row.get("market_data_age_ms"), None),
            "fetched_at": row.get("fetched_at"),
            "end_time": row.get("end_time"),
        }

        positions = [
            {
                "action": "BUY",
                "outcome": outcome,
                "price": selected_price,
                "token_id": token_id,
                "price_policy": "taker_limit",
                "time_in_force": "IOC",
                "_reentry": reentry_context,
            }
        ]

        risk_score = clamp(0.55 - (signal["cancel_drop"] * 0.3), 0.15, 0.85)

        opp = self.create_opportunity(
            title=f"Cancel Re-Entry: {outcome} @ {selected_price:.3f}",
            description=(
                f"cancel drop {signal['cancel_drop']:.2f} "
                f"(peak {signal['prior_peak']:.2f} -> {signal['cancel_rate_30s']:.2f}), "
                f"edge {signal['edge']:.2f}%"
            ),
            total_cost=selected_price,
            expected_payout=selected_price + (signal["edge"] / 100.0),
            markets=[typed_market],
            positions=positions,
            is_guaranteed=False,
            skip_fee_model=True,
            custom_roi_percent=signal["edge"],
            custom_risk_score=risk_score,
            confidence=signal["confidence"],
            min_liquidity_hard=100.0,
            min_position_size=max(1.0, to_float(strategy_cfg.get("min_order_size_usd", 2.0), 2.0)),
        )
        if opp is None:
            return None

        opp.risk_factors = [
            f"Cancel cluster cooldown (drop={signal['cancel_drop']:.2f})",
            f"Edge={signal['edge']:.2f}%",
        ]
        opp.strategy_context = {
            "source_key": "crypto",
            "strategy_slug": self.strategy_type,
            **reentry_context,
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
