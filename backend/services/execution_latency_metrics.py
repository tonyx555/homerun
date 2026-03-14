from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Any


_STAGE_KEYS = (
    "emit_to_queue_wake_ms",
    "wake_to_context_ready_ms",
    "context_ready_to_decision_ms",
    "decision_to_submit_start_ms",
    "submit_start_to_provider_ack_ms",
    "emit_to_submit_start_ms",
)
_MAX_SAMPLES = 5000
_PER_GROUP_LIMIT = 25


@dataclass
class _LatencySample:
    trader_id: str
    source: str
    strategy_key: str
    payload: dict[str, Any]


def _safe_int(value: Any) -> int | None:
    try:
        parsed = int(float(value))
    except Exception:
        return None
    return parsed if parsed >= 0 else None


def _percentile(values: list[int], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * percentile))))
    return ordered[index]


def _stage_summary(samples: list[_LatencySample]) -> dict[str, Any]:
    summary: dict[str, Any] = {"count": len(samples)}
    for stage_key in _STAGE_KEYS:
        values = [_safe_int(sample.payload.get(stage_key)) for sample in samples]
        cleaned = [value for value in values if value is not None]
        summary[stage_key] = {
            "p50": _percentile(cleaned, 0.50),
            "p95": _percentile(cleaned, 0.95),
            "p99": _percentile(cleaned, 0.99),
        }
    return summary


class ExecutionLatencyMetrics:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._samples: deque[_LatencySample] = deque(maxlen=_MAX_SAMPLES)
        self._sla_target_ms = 200
        self._sla_definition = "signal_emitted_at->submit_started_at"

    async def record(
        self,
        *,
        trader_id: str,
        source: str,
        strategy_key: str,
        payload: dict[str, Any],
    ) -> None:
        async with self._lock:
            self._samples.append(
                _LatencySample(
                    trader_id=str(trader_id or "").strip(),
                    source=str(source or "").strip().lower(),
                    strategy_key=str(strategy_key or "").strip().lower(),
                    payload=dict(payload or {}),
                )
            )

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            samples = list(self._samples)

        by_source: dict[str, list[_LatencySample]] = {}
        by_strategy: dict[str, list[_LatencySample]] = {}
        by_trader: dict[str, list[_LatencySample]] = {}
        for sample in samples:
            if sample.source:
                by_source.setdefault(sample.source, []).append(sample)
            if sample.strategy_key:
                by_strategy.setdefault(sample.strategy_key, []).append(sample)
            if sample.trader_id:
                by_trader.setdefault(sample.trader_id, []).append(sample)

        def _trim(groups: dict[str, list[_LatencySample]]) -> dict[str, Any]:
            ranked = sorted(groups.items(), key=lambda item: len(item[1]), reverse=True)[:_PER_GROUP_LIMIT]
            return {key: _stage_summary(rows) for key, rows in ranked}

        return {
            "internal_sla_definition": self._sla_definition,
            "internal_sla_target_ms": self._sla_target_ms,
            "overall": _stage_summary(samples),
            "by_source": _trim(by_source),
            "by_strategy": _trim(by_strategy),
            "by_trader": _trim(by_trader),
        }


execution_latency_metrics = ExecutionLatencyMetrics()
