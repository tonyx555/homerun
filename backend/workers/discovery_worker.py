"""Discovery worker: runs wallet discovery loop and writes DB status snapshot.

Run from backend dir:
  python -m workers.discovery_worker
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from sqlalchemy.exc import DBAPIError

from config import settings
from models.database import AsyncSessionLocal
from services.discovery_shared_state import (
    clear_discovery_run_request,
    read_discovery_control,
    write_discovery_snapshot,
)
from services.pause_state import global_pause_state
from services.wallet_discovery import wallet_discovery
from services.worker_state import write_worker_snapshot
from utils.logger import get_logger
from utils.retry import is_retryable_db_error, db_retry_delay

logger = get_logger("discovery_worker")
PRIORITY_BACKLOG_INTERVAL_MINUTES = 10
_DISCOVERY_DB_RETRY_ATTEMPTS = 3
_ACTIVE_DISCOVERY_HEARTBEAT_SECONDS = 60.0


async def _active_discovery_heartbeat(
    stop_event: asyncio.Event,
    *,
    interval_minutes: int,
    enabled: bool,
    priority_backlog_mode: bool,
    priority_backlog_count: int,
) -> None:
    while True:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=_ACTIVE_DISCOVERY_HEARTBEAT_SECONDS)
            return
        except asyncio.TimeoutError:
            pass
        try:
            async with AsyncSessionLocal() as session:
                await write_worker_snapshot(
                    session,
                    "discovery",
                    running=True,
                    enabled=enabled,
                    current_activity="Scanning trader wallets...",
                    interval_seconds=interval_minutes * 60,
                    last_run_at=None,
                    last_error=None,
                    stats={
                        "wallets_discovered_last_run": wallet_discovery._wallets_discovered_last_run,
                        "wallets_analyzed_last_run": wallet_discovery._wallets_analyzed_last_run,
                        "priority_backlog_mode": priority_backlog_mode,
                        "priority_backlog_count": priority_backlog_count,
                    },
                )
        except Exception:
            pass


async def _run_loop() -> None:
    logger.info("Discovery worker started")

    # Ensure an initial snapshot exists for UI status.
    try:
        async with AsyncSessionLocal() as session:
            await write_discovery_snapshot(
                session,
                {
                    "running": True,
                    "enabled": True,
                    "run_interval_minutes": settings.DISCOVERY_RUN_INTERVAL_MINUTES,
                    "last_run_at": None,
                    "current_activity": "Discovery worker started; first run pending.",
                    "wallets_discovered_last_run": 0,
                    "wallets_analyzed_last_run": 0,
                },
            )
            await write_worker_snapshot(
                session,
                "discovery",
                running=True,
                enabled=True,
                current_activity="Discovery worker started; first run pending.",
                interval_seconds=settings.DISCOVERY_RUN_INTERVAL_MINUTES * 60,
                last_run_at=None,
                last_error=None,
                stats={
                    "wallets_discovered_last_run": 0,
                    "wallets_analyzed_last_run": 0,
                    "priority_backlog_mode": True,
                    "priority_backlog_count": 0,
                },
            )
    except Exception:
        pass

    next_scheduled_run_at: datetime | None = None

    while True:
        async with AsyncSessionLocal() as session:
            control = await read_discovery_control(session)

        interval_minutes = int(
            max(
                5,
                min(
                    1440,
                    control.get("run_interval_minutes") or settings.DISCOVERY_RUN_INTERVAL_MINUTES,
                ),
            )
        )
        priority_backlog_mode = bool(control.get("priority_backlog_mode", True))
        priority_backlog_count = 0
        if priority_backlog_mode:
            try:
                priority_backlog_count = int(await wallet_discovery.get_priority_backlog_count())
            except Exception:
                priority_backlog_count = 0
        if priority_backlog_mode and priority_backlog_count > 0:
            interval_minutes = min(interval_minutes, PRIORITY_BACKLOG_INTERVAL_MINUTES)
        paused = bool(control.get("is_paused", False))
        requested = control.get("requested_run_at") is not None
        enabled = bool(control.get("is_enabled", True))
        now = datetime.now(timezone.utc)

        should_run_scheduled = (
            enabled
            and not paused
            and not global_pause_state.is_paused
            and (next_scheduled_run_at is None or now >= next_scheduled_run_at)
        )
        should_run = requested or should_run_scheduled

        if not should_run:
            try:
                async with AsyncSessionLocal() as session:
                    await write_worker_snapshot(
                        session,
                        "discovery",
                        running=True,
                        enabled=enabled and not paused and not global_pause_state.is_paused,
                        current_activity=(
                            "Paused"
                            if paused or global_pause_state.is_paused
                            else "Idle - waiting for next discovery cycle."
                        ),
                        interval_seconds=interval_minutes * 60,
                        last_run_at=None,
                        last_error=None,
                        stats={
                            "wallets_discovered_last_run": wallet_discovery._wallets_discovered_last_run,
                            "wallets_analyzed_last_run": wallet_discovery._wallets_analyzed_last_run,
                            "priority_backlog_mode": priority_backlog_mode,
                            "priority_backlog_count": priority_backlog_count,
                        },
                    )
            except Exception:
                pass
            await asyncio.sleep(10)
            continue

        try:
            async with AsyncSessionLocal() as session:
                await write_discovery_snapshot(
                    session,
                    {
                        "running": True,
                        "enabled": not paused,
                        "run_interval_minutes": interval_minutes,
                        "current_activity": "Scanning trader wallets...",
                    },
                )
                await write_worker_snapshot(
                    session,
                    "discovery",
                    running=True,
                    enabled=not paused,
                    current_activity="Scanning trader wallets...",
                    interval_seconds=interval_minutes * 60,
                    last_run_at=None,
                    last_error=None,
                    stats={
                        "wallets_discovered_last_run": wallet_discovery._wallets_discovered_last_run,
                        "wallets_analyzed_last_run": wallet_discovery._wallets_analyzed_last_run,
                    },
                )

            discovery_exc: Exception | None = None
            for _db_attempt in range(_DISCOVERY_DB_RETRY_ATTEMPTS):
                heartbeat_stop = asyncio.Event()
                heartbeat_task = asyncio.create_task(
                    _active_discovery_heartbeat(
                        heartbeat_stop,
                        interval_minutes=interval_minutes,
                        enabled=not paused,
                        priority_backlog_mode=priority_backlog_mode,
                        priority_backlog_count=priority_backlog_count,
                    ),
                    name="discovery-active-heartbeat",
                )
                try:
                    await wallet_discovery.run_discovery()
                    discovery_exc = None
                    break
                except DBAPIError as db_exc:
                    if not is_retryable_db_error(db_exc) or _db_attempt >= _DISCOVERY_DB_RETRY_ATTEMPTS - 1:
                        raise
                    delay = db_retry_delay(_db_attempt)
                    logger.warning(
                        "Discovery cycle hit retryable DB error; retrying",
                        attempt=_db_attempt + 1,
                        delay=round(delay, 3),
                        error=str(db_exc),
                    )
                    discovery_exc = db_exc
                    await asyncio.sleep(delay)
                finally:
                    heartbeat_stop.set()
                    try:
                        await heartbeat_task
                    except Exception:
                        pass
            if discovery_exc is not None:
                raise discovery_exc

            async with AsyncSessionLocal() as session:
                await clear_discovery_run_request(session)
                await write_discovery_snapshot(
                    session,
                    {
                        "running": True,
                        "enabled": not paused,
                        "run_interval_minutes": interval_minutes,
                        "last_run_at": wallet_discovery._last_run_at.isoformat()
                        if wallet_discovery._last_run_at
                        else None,
                        "current_activity": "Idle - waiting for next discovery cycle.",
                        "wallets_discovered_last_run": wallet_discovery._wallets_discovered_last_run,
                        "wallets_analyzed_last_run": wallet_discovery._wallets_analyzed_last_run,
                    },
                )
                await write_worker_snapshot(
                    session,
                    "discovery",
                    running=True,
                    enabled=not paused,
                    current_activity="Idle - waiting for next discovery cycle.",
                    interval_seconds=interval_minutes * 60,
                    last_run_at=wallet_discovery._last_run_at.replace(tzinfo=None)
                    if wallet_discovery._last_run_at
                    else None,
                    last_error=None,
                    stats={
                        "wallets_discovered_last_run": wallet_discovery._wallets_discovered_last_run,
                        "wallets_analyzed_last_run": wallet_discovery._wallets_analyzed_last_run,
                        "priority_backlog_mode": priority_backlog_mode,
                        "priority_backlog_count": priority_backlog_count,
                    },
                )

            next_scheduled_run_at = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(
                minutes=interval_minutes
            )
            logger.info(
                "Discovery cycle complete: wallets_discovered=%s wallets_analyzed=%s next_run_at=%s",
                wallet_discovery._wallets_discovered_last_run,
                wallet_discovery._wallets_analyzed_last_run,
                next_scheduled_run_at.isoformat(),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Discovery cycle failed: %s", exc)
            try:
                async with AsyncSessionLocal() as session:
                    await clear_discovery_run_request(session)
                    await write_discovery_snapshot(
                        session,
                        {
                            "running": True,
                            "enabled": not paused,
                            "run_interval_minutes": interval_minutes,
                            "last_run_at": datetime.now(timezone.utc).isoformat(),
                            "current_activity": f"Last discovery run error: {exc}",
                            "wallets_discovered_last_run": wallet_discovery._wallets_discovered_last_run,
                            "wallets_analyzed_last_run": wallet_discovery._wallets_analyzed_last_run,
                        },
                    )
                    await write_worker_snapshot(
                        session,
                        "discovery",
                        running=True,
                        enabled=not paused,
                        current_activity=f"Last discovery run error: {exc}",
                        interval_seconds=interval_minutes * 60,
                        last_run_at=datetime.now(timezone.utc).replace(tzinfo=None),
                        last_error=str(exc),
                        stats={
                            "wallets_discovered_last_run": wallet_discovery._wallets_discovered_last_run,
                            "wallets_analyzed_last_run": wallet_discovery._wallets_analyzed_last_run,
                            "priority_backlog_mode": priority_backlog_mode,
                            "priority_backlog_count": priority_backlog_count,
                        },
                    )
            except Exception as snapshot_exc:
                logger.warning("Discovery error snapshot write also failed: %s", snapshot_exc)
            next_scheduled_run_at = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(
                minutes=interval_minutes
            )

        await asyncio.sleep(10)


async def start_loop() -> None:
    """Run the discovery worker loop (called from API process lifespan)."""
    try:
        await _run_loop()
    except asyncio.CancelledError:
        logger.info("Discovery worker shutting down")
