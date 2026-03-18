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
        self._subscribers.setdefault(event_type, []).append(callback)
        logger.debug("Subscribed to event_type=%s", event_type)

    def unsubscribe(self, event_type: str, callback: EventCallback) -> None:
        callbacks = self._subscribers.get(event_type)
        if not callbacks:
            return
        self._subscribers[event_type] = [existing for existing in callbacks if existing is not callback]
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
