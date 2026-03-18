from __future__ import annotations

from fastapi import APIRouter

from models.database import AsyncSessionLocal
from services.worker_state import read_worker_snapshot

router = APIRouter()


async def _read_crypto_stats() -> dict:
    async with AsyncSessionLocal() as session:
        snapshot = await read_worker_snapshot(session, "crypto")
    stats = snapshot.get("stats") if isinstance(snapshot.get("stats"), dict) else {}
    return dict(stats)


@router.get("/crypto/markets")
async def get_crypto_markets(viewer_active: bool = False) -> list[dict]:
    del viewer_active
    stats = await _read_crypto_stats()
    return list(stats.get("markets") or [])


@router.get("/crypto/oracle-prices")
async def get_oracle_prices() -> dict[str, dict]:
    stats = await _read_crypto_stats()
    return dict(stats.get("oracle_prices") or {})


@router.get("/crypto/filter-diagnostics")
async def get_filter_diagnostics() -> dict:
    """Return crypto filter diagnostics from the worker process snapshot.

    Previously this read from a module-level variable which was only
    populated in the worker process, making it always return {} from
    the web server.  Now reads from the persisted worker snapshot.
    """
    stats = await _read_crypto_stats()
    return dict(stats.get("filter_diagnostics") or {})


@router.get("/crypto/pipeline-debug")
async def get_pipeline_debug() -> dict:
    """Debug endpoint showing dispatch telemetry and strategy subscriptions.

    Reads from the worker snapshot (persisted to DB) so it reflects the
    worker process state, not the web server process.
    """
    stats = await _read_crypto_stats()
    dispatch = stats.get("dispatch") or {}

    # Also show web-server-local dispatcher state for comparison
    from services.event_dispatcher import event_dispatcher

    web_subs: dict[str, list[str]] = {}
    for event_type, handlers in event_dispatcher._handlers.items():
        if handlers:
            web_subs[event_type] = [h[0] for h in handlers]

    return {
        "worker_dispatch": dispatch,
        "worker_filter_diagnostics_keys": sorted(
            (stats.get("filter_diagnostics") or {}).keys()
        ),
        "web_server_event_dispatcher_running": event_dispatcher._running,
        "web_server_subscriptions": web_subs,
    }
