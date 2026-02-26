from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from config import settings
from services.redis_streams import redis_streams

_ACTIONABLE_EVENT_TYPES = frozenset(
    {
        "upsert_insert",
        "upsert_update",
        "upsert_reactivated",
        "reverse_entry",
    }
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_signal_ids(raw_ids: Any, *, limit: int = 5000) -> list[str]:
    if not isinstance(raw_ids, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_ids:
        signal_id = str(raw or "").strip()
        if not signal_id or signal_id in seen:
            continue
        seen.add(signal_id)
        out.append(signal_id)
        if len(out) >= max(1, int(limit)):
            break
    return out


def _normalize_source(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_event_type(value: Any) -> str:
    return str(value or "").strip().lower()


def _is_actionable_event(event_type: str, trigger: str) -> bool:
    if event_type in _ACTIONABLE_EVENT_TYPES:
        return True
    return event_type == "" and trigger == "strategy_signal_bridge"


async def publish_trade_signal_batch(
    *,
    event_type: str,
    source: str | None,
    signal_ids: list[str],
    trigger: str,
    reason: str | None = None,
    emitted_at: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> str | None:
    normalized_ids = _normalize_signal_ids(signal_ids)
    if not normalized_ids:
        return None

    normalized_event_type = _normalize_event_type(event_type)
    normalized_trigger = str(trigger or "").strip().lower()
    if not _is_actionable_event(normalized_event_type, normalized_trigger):
        return None

    payload = {
        "event_type": normalized_event_type,
        "source": _normalize_source(source),
        "signal_ids": normalized_ids,
        "signal_count": int(len(normalized_ids)),
        "trigger": normalized_trigger,
        "reason": reason,
        "emitted_at": str(emitted_at or _utc_now_iso()),
    }
    if isinstance(metadata, dict) and metadata:
        payload["metadata"] = metadata

    return await redis_streams.append_json(
        str(settings.TRADE_SIGNAL_STREAM_KEY),
        payload,
        maxlen=max(1000, int(settings.TRADE_SIGNAL_STREAM_MAXLEN)),
    )


async def ensure_trade_signal_group() -> bool:
    return await redis_streams.ensure_consumer_group(
        stream=str(settings.TRADE_SIGNAL_STREAM_KEY),
        group=str(settings.TRADE_SIGNAL_STREAM_GROUP),
        start_id="$",
        create_stream=True,
    )


def _normalize_batch_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    event_type = _normalize_event_type(payload.get("event_type"))
    trigger = str(payload.get("trigger") or "").strip().lower()
    if not _is_actionable_event(event_type, trigger):
        return None

    signal_ids = _normalize_signal_ids(payload.get("signal_ids"))
    if not signal_ids:
        return None

    return {
        "event_type": event_type,
        "source": _normalize_source(payload.get("source")),
        "signal_ids": signal_ids,
        "signal_count": int(len(signal_ids)),
        "trigger": trigger,
        "reason": payload.get("reason"),
        "emitted_at": payload.get("emitted_at"),
        "metadata": payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
    }


async def read_trade_signal_batches(
    *,
    consumer: str,
    block_ms: int | None = None,
    count: int | None = None,
    include_pending: bool = False,
) -> list[tuple[str, dict[str, Any]]]:
    rows = await redis_streams.read_group_raw(
        stream=str(settings.TRADE_SIGNAL_STREAM_KEY),
        group=str(settings.TRADE_SIGNAL_STREAM_GROUP),
        consumer=str(consumer),
        block_ms=(
            max(1, int(block_ms))
            if block_ms is not None
            else max(1, int(settings.TRADE_SIGNAL_STREAM_BLOCK_MS))
        ),
        count=(
            max(1, int(count))
            if count is not None
            else max(1, int(settings.TRADE_SIGNAL_STREAM_READ_COUNT))
        ),
        include_pending=bool(include_pending),
    )
    out: list[tuple[str, dict[str, Any]]] = []
    for entry_id, raw_payload in rows:
        try:
            payload = json.loads(raw_payload)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        normalized = _normalize_batch_payload(payload)
        if normalized is None:
            continue
        out.append((str(entry_id), normalized))
    return out


async def auto_claim_trade_signal_batches(
    *,
    consumer: str,
    min_idle_ms: int | None = None,
    start_id: str = "0-0",
    count: int | None = None,
) -> tuple[str, list[tuple[str, dict[str, Any]]]]:
    next_start_id, rows = await redis_streams.auto_claim_raw(
        stream=str(settings.TRADE_SIGNAL_STREAM_KEY),
        group=str(settings.TRADE_SIGNAL_STREAM_GROUP),
        consumer=str(consumer),
        min_idle_ms=(
            max(1, int(min_idle_ms))
            if min_idle_ms is not None
            else max(1, int(settings.TRADE_SIGNAL_STREAM_CLAIM_IDLE_MS))
        ),
        start_id=str(start_id or "0-0"),
        count=(
            max(1, int(count))
            if count is not None
            else max(1, int(settings.TRADE_SIGNAL_STREAM_CLAIM_READ_COUNT))
        ),
    )
    out: list[tuple[str, dict[str, Any]]] = []
    for entry_id, raw_payload in rows:
        try:
            payload = json.loads(raw_payload)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        normalized = _normalize_batch_payload(payload)
        if normalized is None:
            continue
        out.append((str(entry_id), normalized))
    return str(next_start_id or start_id), out


async def ack_trade_signal_batches(entry_ids: list[str]) -> int:
    return await redis_streams.ack(
        stream=str(settings.TRADE_SIGNAL_STREAM_KEY),
        group=str(settings.TRADE_SIGNAL_STREAM_GROUP),
        entry_ids=entry_ids,
    )
