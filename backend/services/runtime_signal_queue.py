from __future__ import annotations

import asyncio
import copy
import uuid
from datetime import datetime, timezone
from typing import Any

_ACTIONABLE_EVENT_TYPES = frozenset(
    {
        "upsert_insert",
        "upsert_update",
        "upsert_reactivated",
        "reverse_entry",
    }
)
_LANE_GENERAL = "general"
_LANE_CRYPTO = "crypto"
_LANES = (_LANE_GENERAL, _LANE_CRYPTO)
_QUEUE_DRAIN_LIMIT = 256
_queues: dict[str, asyncio.Queue[dict[str, Any]]] = {
    lane: asyncio.Queue()
    for lane in _LANES
}


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


def _default_lane_for_source(source: str) -> str:
    if _normalize_source(source) == _LANE_CRYPTO:
        return _LANE_CRYPTO
    return _LANE_GENERAL


def _coalesce_batches(batches: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not batches:
        return None
    source_signal_ids: dict[str, list[str]] = {}
    source_seen_ids: dict[str, set[str]] = {}
    source_signal_snapshots: dict[str, dict[str, dict[str, Any]]] = {}
    batch_ids: list[str] = []
    batch_event_types: list[str] = []
    for payload in batches:
        batch_id = str(payload.get("batch_id") or "").strip()
        if batch_id:
            batch_ids.append(batch_id)
        batch_event_type = str(payload.get("batch_event_type") or "").strip().lower()
        if batch_event_type:
            batch_event_types.append(batch_event_type)
        source_key = _normalize_source(payload.get("source")) or "__all__"
        source_signal_ids.setdefault(source_key, [])
        source_seen_ids.setdefault(source_key, set())
        source_signal_snapshots.setdefault(source_key, {})
        raw_snapshots = payload.get("signal_snapshots")
        snapshot_map = raw_snapshots if isinstance(raw_snapshots, dict) else {}
        for raw_signal_id in payload.get("signal_ids") or []:
            signal_id = str(raw_signal_id or "").strip()
            if not signal_id or signal_id in source_seen_ids[source_key]:
                continue
            source_seen_ids[source_key].add(signal_id)
            source_signal_ids[source_key].append(signal_id)
            snapshot = snapshot_map.get(signal_id)
            if isinstance(snapshot, dict):
                source_signal_snapshots[source_key][signal_id] = dict(snapshot)
    non_global_sources = sorted(source for source in source_signal_ids.keys() if source != "__all__")
    trigger_source = non_global_sources[0] if len(non_global_sources) == 1 and "__all__" not in source_signal_ids else ""
    return {
        "event_type": "runtime_signal_batch",
        "source": trigger_source,
        "source_signal_ids": source_signal_ids,
        "source_signal_snapshots": source_signal_snapshots,
        "batch_ids": batch_ids,
        "batch_event_types": batch_event_types,
    }


async def publish_signal_batch(
    *,
    event_type: str,
    source: str | None,
    signal_ids: list[str],
    trigger: str,
    reason: str | None = None,
    emitted_at: str | None = None,
    metadata: dict[str, Any] | None = None,
    signal_snapshots: dict[str, dict[str, Any]] | None = None,
) -> str | None:
    normalized_ids = _normalize_signal_ids(signal_ids)
    if not normalized_ids:
        return None

    normalized_event_type = _normalize_event_type(event_type)
    normalized_trigger = str(trigger or "").strip().lower()
    if not _is_actionable_event(normalized_event_type, normalized_trigger):
        return None

    payload: dict[str, Any] = {
        "batch_id": uuid.uuid4().hex,
        "batch_event_type": normalized_event_type,
        "source": _normalize_source(source),
        "signal_ids": normalized_ids,
        "signal_count": int(len(normalized_ids)),
        "trigger": normalized_trigger,
        "reason": reason,
        "emitted_at": str(emitted_at or _utc_now_iso()),
    }
    if isinstance(metadata, dict) and metadata:
        payload["metadata"] = metadata
    if isinstance(signal_snapshots, dict) and signal_snapshots:
        payload["signal_snapshots"] = signal_snapshots

    token_ids = _extract_token_ids(signal_snapshots)
    if token_ids:
        _schedule_token_warmup(token_ids)

    target_lane = _default_lane_for_source(str(source or ""))
    payload["lane"] = target_lane
    for lane in (target_lane,):
        await _queues[lane].put(copy.deepcopy(payload))
    return str(payload["batch_id"])


async def wait_for_signal_batch(
    *,
    lane: str,
    timeout_seconds: float,
) -> dict[str, Any] | None:
    lane_key = str(lane or _LANE_GENERAL).strip().lower()
    queue = _queues.get(lane_key)
    if queue is None:
        queue = _queues[_LANE_GENERAL]
    timeout = max(0.05, float(timeout_seconds))
    try:
        first = await asyncio.wait_for(queue.get(), timeout=timeout)
    except asyncio.TimeoutError:
        return None
    batches = [first]
    while len(batches) < _QUEUE_DRAIN_LIMIT:
        try:
            batches.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            break
    return _coalesce_batches(batches)


def _extract_token_ids(
    signal_snapshots: dict[str, dict[str, Any]] | None,
) -> set[str]:
    if not isinstance(signal_snapshots, dict):
        return set()
    token_ids: set[str] = set()
    for snapshot in signal_snapshots.values():
        if not isinstance(snapshot, dict):
            continue
        positions = snapshot.get("positions_to_take")
        if isinstance(positions, list):
            for pos in positions:
                if isinstance(pos, dict):
                    tid = pos.get("token_id")
                    if isinstance(tid, str) and tid:
                        token_ids.add(tid)
        markets = snapshot.get("markets")
        if isinstance(markets, list):
            for mkt in markets:
                if isinstance(mkt, dict):
                    for key in ("yes_token_id", "no_token_id"):
                        tid = mkt.get(key)
                        if isinstance(tid, str) and tid:
                            token_ids.add(tid)
        ctx = snapshot.get("strategy_context_json")
        if isinstance(ctx, dict):
            tid = ctx.get("token_id")
            if isinstance(tid, str) and tid:
                token_ids.add(tid)
            tids = ctx.get("token_ids")
            if isinstance(tids, list):
                for tid in tids:
                    if isinstance(tid, str) and tid:
                        token_ids.add(tid)
    return token_ids


def _schedule_token_warmup(token_ids: set[str]) -> None:
    async def _warmup() -> None:
        try:
            from services.ws_feeds import get_feed_manager

            fm = get_feed_manager()
            await fm.polymarket_feed.subscribe(sorted(token_ids))
        except Exception:
            pass

    try:
        task = asyncio.create_task(_warmup())
        task.add_done_callback(lambda _t: None)
    except RuntimeError:
        pass


def get_queue_depth(*, lane: str | None = None) -> int | dict[str, int]:
    if lane is None:
        return {name: queue.qsize() for name, queue in _queues.items()}
    lane_key = str(lane or _LANE_GENERAL).strip().lower()
    queue = _queues.get(lane_key)
    return int(queue.qsize()) if queue is not None else 0
