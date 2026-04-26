from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import and_, select

from models.database import AsyncSessionLocal
from services.wallet_ws_monitor import WalletMonitorEvent
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


@router.get("/crypto/markets/{condition_id}/wallet-trades")
async def get_market_wallet_trades(
    condition_id: str,
    since_seconds: int = Query(default=900, ge=10, le=14_400),
) -> dict:
    """Return tracked-wallet trades for a specific crypto market's tokens.

    Used by the per-cycle synchronized chart to plot trade markers on the
    same canvas as oracle / UP-mid / DOWN-mid lines. Looks up the market
    from the worker snapshot (avoiding a join against gamma) to find its
    UP/DOWN ``clob_token_ids``, then fetches the most recent trades from
    ``wallet_monitor_events`` filtered to those tokens within the lookback
    window.
    """
    stats = await _read_crypto_stats()
    markets = stats.get("markets") or []
    market = next(
        (
            m
            for m in markets
            if isinstance(m, dict) and m.get("condition_id") == condition_id
        ),
        None,
    )
    if market is None:
        raise HTTPException(status_code=404, detail="market not found in worker snapshot")

    token_ids = list(market.get("clob_token_ids") or [])
    if len(token_ids) < 2:
        return {"trades": [], "up_token_id": None, "down_token_id": None}

    up_token_id, down_token_id = token_ids[0], token_ids[1]
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=since_seconds)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(WalletMonitorEvent)
            .where(
                and_(
                    WalletMonitorEvent.token_id.in_(token_ids),
                    WalletMonitorEvent.detected_at >= cutoff,
                )
            )
            .order_by(WalletMonitorEvent.detected_at.asc())
            .limit(500)
        )
        rows = result.scalars().all()

    trades = [
        {
            "id": row.id,
            "wallet": row.wallet_address,
            "token_id": row.token_id,
            "outcome": "UP" if row.token_id == up_token_id else "DOWN",
            "side": row.side,
            "price": float(row.price) if row.price is not None else None,
            "size": float(row.size) if row.size is not None else None,
            "tx_hash": row.tx_hash,
            "block_number": row.block_number,
            "detected_at_ms": int(row.detected_at.timestamp() * 1000) if row.detected_at else None,
        }
        for row in rows
    ]

    return {
        "condition_id": condition_id,
        "up_token_id": up_token_id,
        "down_token_id": down_token_id,
        "trades": trades,
    }


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
    filter_diagnostics = dict(stats.get("filter_diagnostics") or {})

    # Also show web-server-local dispatcher state for comparison
    from services.event_dispatcher import event_dispatcher

    web_subs: dict[str, list[str]] = {}
    for event_type, handlers in event_dispatcher._handlers.items():
        if handlers:
            web_subs[event_type] = [h[0] for h in handlers]

    return {
        "worker_dispatch": dispatch,
        "worker_filter_diagnostics": filter_diagnostics,
        "worker_filter_diagnostics_keys": sorted(
            filter_diagnostics.keys()
        ),
        "web_server_event_dispatcher_running": event_dispatcher._running,
        "web_server_subscriptions": web_subs,
    }
