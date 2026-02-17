from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .base import BaseTraderStrategy, DecisionCheck, StrategyDecision
from .sizing import compute_position_size


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_conf(value: Any, default: float = 0.0) -> float:
    parsed = _to_float(value, default)
    if parsed > 1.0:
        parsed = parsed / 100.0
    return max(0.0, min(1.0, parsed))


def _payload(signal: Any) -> dict[str, Any]:
    payload = getattr(signal, "payload_json", None)
    return payload if isinstance(payload, dict) else {}


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def _days_to_resolution(payload: dict[str, Any]) -> float | None:
    resolution = _parse_iso(payload.get("resolution_date"))
    if resolution is None:
        return None
    now = datetime.now(timezone.utc)
    return (resolution - now).total_seconds() / 86400.0


def _selected_probability(signal: Any, payload: dict[str, Any], direction: str) -> float | None:
    # Scanner signals generally encode directional carry/repricing opportunities.
    # For selected side probability we can use entry price directly.
    entry = _to_float(getattr(signal, "entry_price", None), -1.0)
    if 0.0 < entry < 1.0:
        return entry

    positions = payload.get("positions_to_take") if isinstance(payload.get("positions_to_take"), list) else []
    if positions:
        candidate = _to_float((positions[0] or {}).get("price"), -1.0)
        if 0.0 < candidate < 1.0:
            return candidate

    model_prob = _to_float(payload.get("model_probability"), -1.0)
    if 0.0 <= model_prob <= 1.0:
        return model_prob

    edge = _to_float(getattr(signal, "edge_percent", 0.0), 0.0)
    if 0.0 < entry < 1.0:
        implied = entry + (edge / 100.0)
        if 0.0 <= implied <= 1.0:
            return implied

    return None


def _live_move_5m(context: dict[str, Any]) -> float | None:
    summary = context.get("history_summary")
    if not isinstance(summary, dict):
        return None
    move = summary.get("move_5m")
    if not isinstance(move, dict):
        return None
    raw = move.get("percent")
    if raw is None:
        return None
    return _to_float(raw, 0.0)


class OpportunityFlashReversionStrategy(BaseTraderStrategy):
    key = "opportunity_flash_reversion"

    def evaluate(self, signal: Any, context: dict[str, Any]) -> StrategyDecision:
        params = context.get("params") or {}
        payload = _payload(signal)
        live_market = context.get("live_market") or {}

        min_edge = _to_float(params.get("min_edge_percent", 3.0), 3.0)
        min_conf = _to_conf(params.get("min_confidence", 0.40), 0.40)
        max_risk = _to_conf(params.get("max_risk_score", 0.80), 0.80)
        min_liquidity = max(0.0, _to_float(params.get("min_liquidity", 1500.0), 1500.0))
        min_abs_move_5m = max(0.1, _to_float(params.get("min_abs_move_5m", 1.5), 1.5))
        require_alignment = bool(params.get("require_crash_alignment", True))

        base_size = max(1.0, _to_float(params.get("base_size_usd", 16.0), 16.0))
        max_size = max(base_size, _to_float(params.get("max_size_usd", 130.0), 130.0))
        sizing_policy = str(params.get("sizing_policy", "kelly") or "kelly")
        kelly_fractional_scale = _to_float(params.get("kelly_fractional_scale", 0.5), 0.5)

        source = str(getattr(signal, "source", "") or "").strip().lower()
        direction = str(getattr(signal, "direction", "") or "").strip().lower()

        strategy_type = str(payload.get("strategy") or payload.get("strategy_type") or "").strip().lower()
        strategy_ok = strategy_type == "flash_crash_reversion"

        edge = max(0.0, _to_float(getattr(signal, "edge_percent", 0.0), 0.0))
        confidence = _to_conf(getattr(signal, "confidence", 0.0), 0.0)
        liquidity = max(0.0, _to_float(getattr(signal, "liquidity", 0.0), 0.0))
        risk_score = _to_conf(payload.get("risk_score", 0.5), 0.5)

        move_5m_pct = _live_move_5m(live_market)
        alignment_ok = True
        if require_alignment:
            if move_5m_pct is None:
                alignment_ok = False
            elif direction == "buy_yes":
                alignment_ok = move_5m_pct <= -min_abs_move_5m
            elif direction == "buy_no":
                alignment_ok = move_5m_pct >= min_abs_move_5m
            else:
                alignment_ok = False

        checks = [
            DecisionCheck("source", "Scanner source", source == "scanner", detail="Requires source=scanner."),
            DecisionCheck("strategy", "Flash reversion strategy type", strategy_ok, detail="strategy=flash_crash_reversion"),
            DecisionCheck("edge", "Edge threshold", edge >= min_edge, score=edge, detail=f"min={min_edge:.2f}"),
            DecisionCheck("confidence", "Confidence threshold", confidence >= min_conf, score=confidence, detail=f"min={min_conf:.2f}"),
            DecisionCheck("risk", "Risk ceiling", risk_score <= max_risk, score=risk_score, detail=f"max={max_risk:.2f}"),
            DecisionCheck("liquidity", "Liquidity floor", liquidity >= min_liquidity, score=liquidity, detail=f"min={min_liquidity:.0f}"),
            DecisionCheck(
                "alignment_5m",
                "Crash alignment (5m move)",
                alignment_ok,
                score=move_5m_pct,
                detail=f"abs move >= {min_abs_move_5m:.2f}% in signal direction",
            ),
        ]

        score = (edge * 0.65) + (confidence * 30.0) + (min(1.0, liquidity / 10000.0) * 8.0) - (risk_score * 10.0)

        if not all(check.passed for check in checks):
            return StrategyDecision(
                decision="skipped",
                reason="Flash reversion filters not met",
                score=score,
                checks=checks,
                payload={
                    "edge": edge,
                    "confidence": confidence,
                    "risk_score": risk_score,
                    "liquidity": liquidity,
                    "move_5m_pct": move_5m_pct,
                },
            )

        probability = _selected_probability(signal, payload, direction)
        entry_price = _to_float(getattr(signal, "entry_price", None), 0.0)

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
            liquidity_cap_fraction=0.08,
        )

        return StrategyDecision(
            decision="selected",
            reason="Flash reversion signal selected",
            score=score,
            size_usd=float(sizing["size_usd"]),
            checks=checks,
            payload={
                "edge": edge,
                "confidence": confidence,
                "risk_score": risk_score,
                "liquidity": liquidity,
                "move_5m_pct": move_5m_pct,
                "sizing": sizing,
                "strategy_type": strategy_type,
            },
        )


class OpportunityTailCarryStrategy(BaseTraderStrategy):
    key = "opportunity_tail_carry"

    def evaluate(self, signal: Any, context: dict[str, Any]) -> StrategyDecision:
        params = context.get("params") or {}
        payload = _payload(signal)

        min_edge = _to_float(params.get("min_edge_percent", 1.6), 1.6)
        min_conf = _to_conf(params.get("min_confidence", 0.35), 0.35)
        max_risk = _to_conf(params.get("max_risk_score", 0.78), 0.78)
        min_entry = _clamp(_to_float(params.get("min_entry_price", 0.85), 0.85), 0.01, 0.99)
        max_entry = _clamp(_to_float(params.get("max_entry_price", 0.985), 0.985), min_entry, 0.999)
        min_days = max(0.0, _to_float(params.get("min_days_to_resolution", 0.03), 0.03))
        max_days = max(min_days + 0.01, _to_float(params.get("max_days_to_resolution", 7.0), 7.0))

        base_size = max(1.0, _to_float(params.get("base_size_usd", 14.0), 14.0))
        max_size = max(base_size, _to_float(params.get("max_size_usd", 90.0), 90.0))
        sizing_policy = str(params.get("sizing_policy", "adaptive") or "adaptive")

        source = str(getattr(signal, "source", "") or "").strip().lower()
        edge = max(0.0, _to_float(getattr(signal, "edge_percent", 0.0), 0.0))
        confidence = _to_conf(getattr(signal, "confidence", 0.0), 0.0)
        liquidity = max(0.0, _to_float(getattr(signal, "liquidity", 0.0), 0.0))
        risk_score = _to_conf(payload.get("risk_score", 0.5), 0.5)

        strategy_type = str(payload.get("strategy") or payload.get("strategy_type") or "").strip().lower()
        strategy_ok = strategy_type == "tail_end_carry"

        entry_price = _to_float(getattr(signal, "entry_price", 0.0), 0.0)
        if entry_price <= 0.0:
            positions = payload.get("positions_to_take") if isinstance(payload.get("positions_to_take"), list) else []
            if positions:
                entry_price = _to_float((positions[0] or {}).get("price"), 0.0)

        days_to_resolution = _days_to_resolution(payload)
        days_ok = days_to_resolution is not None and min_days <= days_to_resolution <= max_days

        checks = [
            DecisionCheck("source", "Scanner source", source == "scanner", detail="Requires source=scanner."),
            DecisionCheck("strategy", "Tail carry strategy type", strategy_ok, detail="strategy=tail_end_carry"),
            DecisionCheck("edge", "Edge threshold", edge >= min_edge, score=edge, detail=f"min={min_edge:.2f}"),
            DecisionCheck("confidence", "Confidence threshold", confidence >= min_conf, score=confidence, detail=f"min={min_conf:.2f}"),
            DecisionCheck("risk", "Risk ceiling", risk_score <= max_risk, score=risk_score, detail=f"max={max_risk:.2f}"),
            DecisionCheck("entry", "Entry probability band", min_entry <= entry_price <= max_entry, score=entry_price, detail=f"[{min_entry:.3f}, {max_entry:.3f}]"),
            DecisionCheck(
                "resolution_window",
                "Resolution window",
                days_ok,
                score=days_to_resolution,
                detail=f"[{min_days:.2f}, {max_days:.2f}] days",
            ),
        ]

        score = (edge * 0.55) + (confidence * 28.0) + (entry_price * 6.0) - (risk_score * 9.0)
        if days_to_resolution is not None:
            score += max(0.0, (max_days - days_to_resolution) * 0.4)

        if not all(check.passed for check in checks):
            return StrategyDecision(
                decision="skipped",
                reason="Tail carry filters not met",
                score=score,
                checks=checks,
                payload={
                    "edge": edge,
                    "confidence": confidence,
                    "risk_score": risk_score,
                    "entry_price": entry_price,
                    "days_to_resolution": days_to_resolution,
                },
            )

        probability = _selected_probability(signal, payload, str(getattr(signal, "direction", "") or ""))

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
        )

        return StrategyDecision(
            decision="selected",
            reason="Tail carry signal selected",
            score=score,
            size_usd=float(sizing["size_usd"]),
            checks=checks,
            payload={
                "edge": edge,
                "confidence": confidence,
                "risk_score": risk_score,
                "entry_price": entry_price,
                "days_to_resolution": days_to_resolution,
                "sizing": sizing,
                "strategy_type": strategy_type,
            },
        )


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
