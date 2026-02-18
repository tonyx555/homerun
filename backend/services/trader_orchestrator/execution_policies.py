from __future__ import annotations

from typing import Any

from utils.converters import safe_float, safe_int


SUPPORTED_EXECUTION_POLICIES = {
    "SINGLE_LEG",
    "PARALLEL_MAKER",
    "SEQUENTIAL_HEDGE",
    "REPRICE_LOOP",
    "TIMEBOX_EXIT",
    "PAIR_LOCK",
}


def normalize_execution_policy(value: Any, *, legs_count: int) -> str:
    policy = str(value or "").strip().upper()
    if not policy:
        return "PARALLEL_MAKER" if legs_count > 1 else "SINGLE_LEG"
    if policy in SUPPORTED_EXECUTION_POLICIES:
        return policy
    return "PARALLEL_MAKER" if legs_count > 1 else "SINGLE_LEG"


def normalize_execution_constraints(raw: Any) -> dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    return {
        "max_unhedged_notional_usd": max(0.0, safe_float(payload.get("max_unhedged_notional_usd"), 0.0)),
        "hedge_timeout_seconds": max(1, safe_int(payload.get("hedge_timeout_seconds"), 20)),
        "session_timeout_seconds": max(1, safe_int(payload.get("session_timeout_seconds"), 300)),
        "max_reprice_attempts": max(0, safe_int(payload.get("max_reprice_attempts"), 3)),
        "pair_lock": bool(payload.get("pair_lock", True)),
        "leg_fill_tolerance_ratio": max(
            0.0,
            min(1.0, safe_float(payload.get("leg_fill_tolerance_ratio"), 0.02)),
        ),
    }


def allocate_leg_notionals(total_notional_usd: float, legs: list[dict[str, Any]]) -> list[float]:
    notional = max(0.0, safe_float(total_notional_usd, 0.0))
    if not legs:
        return []

    weights: list[float] = []
    for leg in legs:
        weight = max(0.0001, safe_float(leg.get("notional_weight"), 1.0))
        weights.append(weight)

    total_weight = sum(weights) if weights else 0.0
    if total_weight <= 0:
        equal = notional / len(legs)
        return [equal for _ in legs]

    notionals = [(weight / total_weight) * notional for weight in weights]
    if notionals:
        drift = notional - sum(notionals)
        notionals[-1] += drift
    return notionals


def execution_waves(policy: str, legs: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    normalized = normalize_execution_policy(policy, legs_count=len(legs))
    if normalized == "SEQUENTIAL_HEDGE":
        return [[leg] for leg in legs]
    return [list(legs)]


def requires_pair_lock(policy: str, constraints: dict[str, Any]) -> bool:
    normalized = normalize_execution_policy(policy, legs_count=2)
    if normalized == "PAIR_LOCK":
        return True
    return bool(constraints.get("pair_lock", False))


def supports_reprice(policy: str) -> bool:
    normalized = normalize_execution_policy(policy, legs_count=2)
    return normalized in {"REPRICE_LOOP", "PARALLEL_MAKER", "SEQUENTIAL_HEDGE"}


def reprice_limit_price(base_price: float | None, side: str, attempt: int) -> float | None:
    if base_price is None:
        return None
    ticks = max(1, int(attempt))
    direction = 1.0 if str(side or "").strip().lower() == "buy" else -1.0
    adjusted = float(base_price) + (0.01 * ticks * direction)
    return max(0.01, min(0.99, round(adjusted, 4)))
