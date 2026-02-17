from __future__ import annotations

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
    raw = getattr(signal, "payload_json", None)
    return raw if isinstance(raw, dict) else {}


def _history_move(context: dict[str, Any], key: str) -> float | None:
    summary = context.get("history_summary")
    if not isinstance(summary, dict):
        return None
    move = summary.get(key)
    if not isinstance(move, dict):
        return None
    raw = move.get("percent")
    if raw is None:
        return None
    return _to_float(raw, 0.0)


def _selected_probability(signal: Any, payload: dict[str, Any], direction: str) -> float | None:
    direction = str(direction or "").strip().lower()

    if direction == "buy_yes":
        candidate = _to_float(payload.get("model_prob_yes"), -1.0)
        if 0.0 <= candidate <= 1.0:
            return candidate
    elif direction == "buy_no":
        candidate = _to_float(payload.get("model_prob_no"), -1.0)
        if 0.0 <= candidate <= 1.0:
            return candidate

    model_prob = _to_float(payload.get("model_probability"), -1.0)
    if 0.0 <= model_prob <= 1.0:
        return model_prob

    entry = _to_float(getattr(signal, "entry_price", 0.0), 0.0)
    edge = _to_float(getattr(signal, "edge_percent", 0.0), 0.0)
    if 0.0 < entry < 1.0:
        implied = entry + (edge / 100.0)
        if 0.0 <= implied <= 1.0:
            return implied

    return None


class CryptoSpikeReversionStrategy(BaseTraderStrategy):
    key = "crypto_spike_reversion"

    def evaluate(self, signal: Any, context: dict[str, Any]) -> StrategyDecision:
        params = context.get("params") or {}
        payload = _payload(signal)
        live_market = context.get("live_market") or {}

        min_edge = _to_float(params.get("min_edge_percent", 2.8), 2.8)
        min_conf = _to_conf(params.get("min_confidence", 0.44), 0.44)
        min_abs_move_5m = max(0.2, _to_float(params.get("min_abs_move_5m", 1.8), 1.8))
        max_abs_move_2h = max(min_abs_move_5m, _to_float(params.get("max_abs_move_2h", 14.0), 14.0))
        require_reversion_shape = bool(params.get("require_reversion_shape", True))

        base_size = max(1.0, _to_float(params.get("base_size_usd", 20.0), 20.0))
        max_size = max(base_size, _to_float(params.get("max_size_usd", 120.0), 120.0))
        sizing_policy = str(params.get("sizing_policy", "kelly") or "kelly")
        kelly_fractional_scale = _to_float(params.get("kelly_fractional_scale", 0.45), 0.45)

        source = str(getattr(signal, "source", "") or "").strip().lower()
        direction = str(getattr(signal, "direction", "") or "").strip().lower()
        signal_type = str(getattr(signal, "signal_type", "") or "").strip().lower()

        edge = max(0.0, _to_float(getattr(signal, "edge_percent", 0.0), 0.0))
        confidence = _to_conf(getattr(signal, "confidence", 0.0), 0.0)
        liquidity = max(0.0, _to_float(getattr(signal, "liquidity", 0.0), 0.0))

        move_5m = _history_move(live_market, "move_5m")
        move_30m = _history_move(live_market, "move_30m")
        move_2h = _history_move(live_market, "move_2h")

        direction_alignment = False
        if move_5m is not None:
            if direction == "buy_yes":
                direction_alignment = move_5m <= -min_abs_move_5m
            elif direction == "buy_no":
                direction_alignment = move_5m >= min_abs_move_5m

        shape_ok = True
        if require_reversion_shape:
            shape_ok = False
            if move_5m is not None and move_30m is not None:
                # Prefer fresh spikes where short-horizon impulse dominates the 30m trend.
                shape_ok = abs(move_5m) >= abs(move_30m) * 0.55
            if shape_ok and move_2h is not None:
                shape_ok = abs(move_2h) <= max_abs_move_2h

        origin_ok = bool(payload.get("strategy_origin") == "crypto_worker") or signal_type.startswith("crypto_worker")

        checks = [
            DecisionCheck("source", "Crypto source", source == "crypto", detail="Requires source=crypto."),
            DecisionCheck("origin", "Crypto worker origin", origin_ok, detail="Requires worker-generated crypto signal."),
            DecisionCheck("edge", "Edge threshold", edge >= min_edge, score=edge, detail=f"min={min_edge:.2f}"),
            DecisionCheck("confidence", "Confidence threshold", confidence >= min_conf, score=confidence, detail=f"min={min_conf:.2f}"),
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

        probability = _selected_probability(signal, payload, direction)
        entry_price = _to_float(getattr(signal, "entry_price", 0.0), 0.0)

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
