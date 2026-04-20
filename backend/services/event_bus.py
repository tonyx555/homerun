"""WebSocket broadcast bus for single-process runtime delivery."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, List

from utils.logger import get_logger

logger = get_logger("event_bus")

EventCallback = Callable[[str, Dict[str, Any]], Awaitable[None]]


class EventBus:
    def __init__(self) -> None:
        self._subscribers: Dict[str, List[EventCallback]] = {}
        self._start_lock = asyncio.Lock()
        self._running = False
        self._dispatch_tasks: set[asyncio.Task] = set()

    def subscribe(self, event_type: str, callback: EventCallback) -> None:
        # Deduplicate: adding the same callback twice would fire it twice per
        # publish AND retain two closures over whatever state the callback
        # captures.  When a worker is restarted without a clean shutdown (e.g.
        # the host lag-watchdog re-spawns the task), the old subscription can
        # remain registered; without this guard a 24h uptime with a dozen
        # restarts holds a dozen orphan runtime instances alive through
        # closure references.  This is the primary leak vector for the
        # fast_trader_runtime 9GB RSS growth.
        existing = self._subscribers.setdefault(event_type, [])
        for cb in existing:
            # Bound methods compare equal when they share __self__ and __func__
            # even though the bound-method objects themselves are freshly
            # created each lookup (so `is` would miss).  Using == catches
            # duplicate subscriptions from the same runtime instance.
            if cb == callback:
                logger.debug("Skipping duplicate subscribe for event_type=%s", event_type)
                return
        existing.append(callback)
        logger.debug("Subscribed to event_type=%s (count=%d)", event_type, len(existing))

    def unsubscribe(self, event_type: str, callback: EventCallback) -> None:
        callbacks = self._subscribers.get(event_type)
        if not callbacks:
            return
        # Match by equality (bound-method aware) so stale subscriptions from
        # a previous runtime instance can be removed even though the caller
        # passes a freshly-bound method object.
        self._subscribers[event_type] = [existing for existing in callbacks if existing != callback]
        if not self._subscribers[event_type]:
            self._subscribers.pop(event_type, None)
        logger.debug("Unsubscribed from event_type=%s", event_type)

    async def start(self) -> None:
        if self._running:
            return
        async with self._start_lock:
            if self._running:
                return
            self._running = True

    async def stop(self) -> None:
        self._running = False

    async def publish(self, event_type: str, data: Dict[str, Any]) -> None:
        self._dispatch_local(event_type, data)

    def _dispatch_local(self, event_type: str, data: Dict[str, Any]) -> None:
        callbacks: List[EventCallback] = []
        callbacks.extend(self._subscribers.get(event_type, []))
        callbacks.extend(self._subscribers.get("*", []))
        if not callbacks:
            return
        for cb in callbacks:
            try:
                task = asyncio.create_task(_safe_invoke(cb, event_type, data))
                self._dispatch_tasks.add(task)
                task.add_done_callback(self._dispatch_tasks.discard)
            except Exception:
                logger.debug(
                    "Failed to schedule event callback",
                    event_type=event_type,
                )


async def _safe_invoke(
    cb: EventCallback,
    event_type: str,
    data: Dict[str, Any],
) -> None:
    try:
        await cb(event_type, data)
    except Exception as exc:
        logger.debug(
            "Event subscriber raised",
            event_type=event_type,
            exc_info=exc,
        )


event_bus = EventBus()
