"""WebSocket broadcast bus — pushes real-time updates to connected frontend clients.

**Purpose**: Workers publish events (scanner snapshot, signal updates, worker
status changes) when they produce new data. The ``SnapshotBroadcaster``
subscribes here and forwards deltas to all connected WebSocket clients so
the frontend receives live updates without polling.

**Not for strategy routing.** Strategies subscribe to ``DataEvent`` objects
via ``event_dispatcher`` (``services.event_dispatcher``), not this bus.
The two systems serve different purposes:

- ``event_bus`` → frontend WebSocket broadcast (UI live updates)
- ``event_dispatcher`` → strategy DataEvent routing (trading logic)

Thread-safe: ``publish()`` can be called from any asyncio task in the same
event loop.  Subscribers are invoked concurrently via ``asyncio.create_task``
so a slow callback never blocks the publisher.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, List

from utils.logger import get_logger

logger = get_logger("event_bus")

# Type alias for subscriber callbacks.
EventCallback = Callable[[str, Dict[str, Any]], Awaitable[None]]


class EventBus:
    """Minimalist async pub/sub for in-process event routing."""

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[EventCallback]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def subscribe(self, event_type: str, callback: EventCallback) -> None:
        """Register *callback* for *event_type*.

        Use ``"*"`` as event_type to receive every event (wildcard).
        """
        self._subscribers.setdefault(event_type, []).append(callback)
        logger.debug("Subscribed to event_type=%s", event_type)

    def unsubscribe(self, event_type: str, callback: EventCallback) -> None:
        """Remove a previously registered callback."""
        callbacks = self._subscribers.get(event_type)
        if callbacks is None:
            return
        try:
            callbacks.remove(callback)
        except ValueError:
            pass

    async def publish(self, event_type: str, data: Dict[str, Any]) -> None:
        """Publish an event to all matching subscribers.

        Matching includes exact event_type subscribers **and** wildcard
        (``"*"``) subscribers.  Each callback is fired as a task so a slow
        or failing subscriber does not block the publisher.
        """
        callbacks: List[EventCallback] = []
        callbacks.extend(self._subscribers.get(event_type, []))
        callbacks.extend(self._subscribers.get("*", []))

        if not callbacks:
            return

        for cb in callbacks:
            try:
                # Fire-and-forget: wrap in a task so the publisher is never
                # blocked by a slow consumer.
                asyncio.create_task(_safe_invoke(cb, event_type, data))
            except Exception:
                # If we cannot even create the task (e.g. no running loop),
                # log and move on -- never crash the worker.
                logger.debug(
                    "Failed to schedule event callback",
                    event_type=event_type,
                )


async def _safe_invoke(
    cb: EventCallback,
    event_type: str,
    data: Dict[str, Any],
) -> None:
    """Invoke a subscriber callback, swallowing any exception."""
    try:
        await cb(event_type, data)
    except Exception as exc:
        logger.debug(
            "Event subscriber raised: %s",
            exc,
            event_type=event_type,
        )


# ------------------------------------------------------------------
# Module-level singleton -- import and use directly.
# ------------------------------------------------------------------
event_bus = EventBus()
