"""Dedicated live reconciliation worker for trader order/position state."""

from __future__ import annotations

import asyncio
import json as _json
import time
from typing import Any

import asyncpg

from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm.exc import StaleDataError

from models.database import AsyncSessionLocal, init_database, recover_pool
from services.event_bus import event_bus
from services.opportunity_strategy_catalog import ensure_all_strategies_seeded
from services.strategy_runtime import refresh_strategy_runtime_if_needed
from services.live_execution_service import live_execution_service
from services.trader_orchestrator.position_lifecycle import (
    reconcile_live_positions,
    register_open_orders as register_exit_orders,
    unregister_token as unregister_exit_token,
    get_registered_token_ids as get_exit_registered_tokens,
)
from services.trader_orchestrator_state import (
    _dedupe_live_authority_rows,
    create_trader_event,
    get_open_order_count_for_trader,
    list_traders,
    read_orchestrator_control,
    recover_missing_live_trader_orders,
    reconcile_live_provider_orders,
    sync_trader_position_inventory,
    OPEN_ORDER_STATUSES,
)
from services.wallet_ws_monitor import wallet_ws_monitor
from services.worker_state import (
    _is_retryable_db_error,
    clear_worker_run_request,
    read_worker_control,
    write_worker_snapshot,
)
from utils.converters import safe_float
from utils.logger import get_logger
from utils.utcnow import utcnow

logger = get_logger("trader_reconciliation_worker")

WORKER_NAME = "trader_reconciliation"
DEFAULT_INTERVAL_SECONDS = 1
_IDLE_SLEEP_SECONDS = 1
_POSITION_MARK_SYNC_INTERVAL_SECONDS = 10.0
_last_position_mark_sync_at = 0.0
_MAX_CONSECUTIVE_DB_FAILURES = 3
_CONTROL_REFRESH_SECONDS = 5.0
_ACTIVE_POSITION_TICK_SECONDS = 60.0
_EVENT_QUEUE_MAXSIZE = 4096
_previous_active_order_ids: set[str] = set()
_pg_notify_conn: asyncpg.Connection | None = None
_WALLET_MONITOR_REFRESH_SECONDS = 15.0
_TRADER_RECONCILE_ATTEMPTS = 3
_TRADER_RECONCILE_RETRY_BASE_DELAY_SECONDS = 0.05
_TRADER_RECONCILE_RETRY_MAX_DELAY_SECONDS = 0.3
_TRADER_RECONCILE_TIMEOUT_SECONDS = 30.0
_RECONCILIATION_CYCLE_TIMEOUT_SECONDS = 120.0
_MAX_TRIGGER_DRAIN_PER_CYCLE = 128
_TIMEOUT_CANCEL_GRACE_SECONDS = 5.0
_STARTUP_INTER_TRADER_SLEEP_SECONDS = 0.0
_DEFAULT_INTER_TRADER_SLEEP_SECONDS = 0.1
_SCHEDULED_CYCLE_COOLDOWN_SECONDS = 1.0
_EVENT_CYCLE_COOLDOWN_SECONDS = 0.5
_POSITION_TICK_CYCLE_COOLDOWN_SECONDS = 0.25
_RECONCILE_TRIGGER_EVENTS = frozenset(
    {
        "trader_order",
        "execution_order",
        "execution_session",
        "execution_session_event",
        "wallet_trade",
        "provider_status",
    }
)
_abandoned_timed_tasks: set[asyncio.Task] = set()
_inflight_timed_tasks: dict[str, asyncio.Task] = {}


class _TimedTaskStillRunningError(RuntimeError):
    pass


def _inter_trader_sleep_seconds(*, reason: str) -> float:
    normalized_reason = str(reason or "").strip().lower()
    if normalized_reason == "startup":
        return _STARTUP_INTER_TRADER_SLEEP_SECONDS
    return _DEFAULT_INTER_TRADER_SLEEP_SECONDS


def _post_cycle_cooldown_seconds(*, reason: str, provider_pass: bool) -> float:
    normalized_reason = str(reason or "").strip().lower()
    if normalized_reason == "position_tick" or not provider_pass:
        return _POSITION_TICK_CYCLE_COOLDOWN_SECONDS
    if normalized_reason.startswith("event:"):
        return _EVENT_CYCLE_COOLDOWN_SECONDS
    return _SCHEDULED_CYCLE_COOLDOWN_SECONDS


def _reconcile_retry_delay_seconds(attempt: int) -> float:
    return min(
        _TRADER_RECONCILE_RETRY_BASE_DELAY_SECONDS * (2**attempt),
        _TRADER_RECONCILE_RETRY_MAX_DELAY_SECONDS,
    )


def _discard_abandoned_task(task: asyncio.Task) -> None:
    _abandoned_timed_tasks.discard(task)


def _clear_inflight_timed_task(label: str, task: asyncio.Task) -> None:
    if _inflight_timed_tasks.get(label) is task:
        _inflight_timed_tasks.pop(label, None)


async def _graceful_timeout(coro, *, timeout: float, label: str):
    existing = _inflight_timed_tasks.get(label)
    if existing is not None and not existing.done():
        close = getattr(coro, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
        raise _TimedTaskStillRunningError(label)

    task = asyncio.create_task(coro, name=f"trader-reconciliation-{label}")
    _inflight_timed_tasks[label] = task
    task.add_done_callback(lambda done_task, step_label=label: _clear_inflight_timed_task(step_label, done_task))

    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if done:
            return task.result()

        _abandoned_timed_tasks.add(task)
        task.add_done_callback(_discard_abandoned_task)
        raise asyncio.TimeoutError()
    except asyncio.CancelledError:
        if not task.done():
            task.cancel()
            try:
                await asyncio.shield(asyncio.wait({task}, timeout=_TIMEOUT_CANCEL_GRACE_SECONDS))
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            if not task.done():
                _abandoned_timed_tasks.add(task)
                task.add_done_callback(_discard_abandoned_task)
        raise


def _default_strategy_params(trader: dict[str, Any]) -> dict[str, Any]:
    source_configs = trader.get("source_configs")
    if not isinstance(source_configs, list):
        return {}
    for row in source_configs:
        if not isinstance(row, dict):
            continue
        params = row.get("strategy_params")
        if isinstance(params, dict):
            return dict(params)
    return {}


def _clamped_live_lifecycle_params(trader: dict[str, Any], control_settings: dict[str, Any]) -> dict[str, Any]:
    params = _default_strategy_params(trader)
    risk_limits = trader.get("risk_limits")
    if isinstance(risk_limits, dict):
        params.update(dict(risk_limits))

    global_runtime = control_settings.get("global_runtime")
    global_runtime = global_runtime if isinstance(global_runtime, dict) else {}
    live_risk_clamps = global_runtime.get("live_risk_clamps")
    live_risk_clamps = live_risk_clamps if isinstance(live_risk_clamps, dict) else {}

    trade_cap = safe_float(live_risk_clamps.get("max_trade_notional_usd_cap"))
    if trade_cap is not None and trade_cap > 0.0:
        configured = safe_float(params.get("max_trade_notional_usd"))
        params["max_trade_notional_usd"] = min(configured, trade_cap) if configured and configured > 0.0 else trade_cap

    market_cap = safe_float(live_risk_clamps.get("max_per_market_exposure_usd_cap"))
    if market_cap is not None and market_cap > 0.0:
        configured_market = safe_float(params.get("max_per_market_exposure_usd"))
        params["max_per_market_exposure_usd"] = (
            min(configured_market, market_cap) if configured_market and configured_market > 0.0 else market_cap
        )
        configured_position = safe_float(params.get("max_position_notional_usd"))
        params["max_position_notional_usd"] = (
            min(configured_position, market_cap) if configured_position and configured_position > 0.0 else market_cap
        )

    return params


def _empty_cycle_summary() -> dict[str, Any]:
    return {
        "traders_seen": 0,
        "traders_processed": 0,
        "provider_unavailable_traders": 0,
        "provider_active_seen": 0,
        "provider_status_changes": 0,
        "provider_updates": 0,
        "provider_notional_updates": 0,
        "provider_price_updates": 0,
        "positions_would_close": 0,
        "positions_closed": 0,
        "positions_held": 0,
        "positions_skipped": 0,
        "inventory_open_positions": 0,
        "inventory_updates": 0,
        "inventory_inserts": 0,
        "inventory_closures": 0,
        "failures": 0,
    }


def _wallet_monitor_snapshot_stats() -> dict[str, Any]:
    status = wallet_ws_monitor.get_status()
    raw_stats = status.get("stats")
    stats = dict(raw_stats) if isinstance(raw_stats, dict) else {}
    return {
        "wallet_monitor_running": bool(status.get("running")),
        "wallet_monitor_ws_connected": bool(status.get("ws_connected")),
        "wallet_monitor_fallback_polling": bool(status.get("fallback_polling")),
        "wallet_monitor_tracked_wallets": int(status.get("tracked_wallets") or 0),
        "wallet_monitor_last_event_detected_at": str(stats.get("last_event_detected_at") or ""),
        "wallet_monitor_last_block_seen_at": str(stats.get("last_block_seen_at") or ""),
        "wallet_monitor_last_block_processed_at": str(stats.get("last_block_processed_at") or ""),
        "wallet_monitor_last_fallback_poll_at": str(stats.get("last_fallback_poll_at") or ""),
        "wallet_monitor_events_detected_total": int(stats.get("events_detected") or 0),
        "wallet_monitor_blocks_processed_total": int(stats.get("blocks_processed") or 0),
        "wallet_monitor_errors_total": int(stats.get("errors") or 0),
    }


async def _sync_live_wallet_monitor_source(current_wallet: str) -> str:
    execution_wallet = ""
    try:
        if await live_execution_service.ensure_initialized():
            execution_wallet = str(live_execution_service.get_execution_wallet_address() or "").strip().lower()
    except Exception as exc:
        logger.warning("Failed to initialize trading service for wallet WS monitor sync", exc_info=exc)

    if execution_wallet != current_wallet:
        wallet_ws_monitor.set_wallets_for_source(WORKER_NAME, [execution_wallet] if execution_wallet else [])
        current_wallet = execution_wallet

    if execution_wallet:
        try:
            await wallet_ws_monitor.start()
        except Exception as exc:
            logger.warning("Failed to start wallet WS monitor for reconciliation worker", exc_info=exc)

    return current_wallet


async def _reconcile_live_state_for_trader(
    trader: dict[str, Any],
    *,
    provider_pass: bool,
    control_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    trader_id = str(trader.get("id") or "").strip()
    if not trader_id:
        return {
            "provider": {"active_seen": 0, "status_changes": 0, "updated_orders": 0},
            "lifecycle": {"would_close": 0, "closed": 0},
            "inventory": {"open_positions": 0, "updates": 0, "inserts": 0, "closures": 0},
        }

    provider_result: dict[str, Any] = {
        "provider_ready": True,
        "active_seen": 0,
        "status_changes": 0,
        "updated_orders": 0,
        "notional_updates": 0,
        "price_updates": 0,
    }

    # Provider reconcile + open order count are independent — run in parallel.
    async def _provider_pass() -> dict[str, Any]:
        async with AsyncSessionLocal() as session:
            return await reconcile_live_provider_orders(
                session,
                trader_id=trader_id,
                commit=True,
                broadcast=True,
            )

    async def _open_order_count() -> int:
        async with AsyncSessionLocal() as session:
            return await get_open_order_count_for_trader(session, trader_id, mode="live", statuses=OPEN_ORDER_STATUSES)

    if provider_pass:
        provider_result, active_open_orders = await asyncio.gather(
            _provider_pass(), _open_order_count()
        )
    else:
        active_open_orders = await _open_order_count()

    active_seen = int(provider_result.get("active_seen", 0) or 0)
    lifecycle_result: dict[str, Any] = {"would_close": 0, "closed": 0}
    trader_params = _clamped_live_lifecycle_params(trader, control_settings or {})
    if (not provider_pass) or active_seen > 0 or active_open_orders > 0:
        async with AsyncSessionLocal() as session:
            lifecycle_result = await reconcile_live_positions(
                session,
                trader_id=trader_id,
                trader_params=trader_params,
                dry_run=False,
                reason="reconciliation_worker",
            )
    async with AsyncSessionLocal() as session:
        inventory_result = await sync_trader_position_inventory(
            session,
            trader_id=trader_id,
            mode="live",
            commit=True,
        )

    return {
        "provider": provider_result,
        "lifecycle": lifecycle_result,
        "inventory": inventory_result,
    }


async def _get_pg_notify_conn() -> asyncpg.Connection | None:
    """Return (or create) a raw asyncpg connection for NOTIFY.

    Re-uses a module-level connection so we don't open a new one per cycle.
    """
    global _pg_notify_conn
    if _pg_notify_conn is not None:
        try:
            if not _pg_notify_conn.is_closed():
                return _pg_notify_conn
        except Exception:
            pass
        _pg_notify_conn = None

    try:
        from config import settings as _settings
        dsn = str(_settings.DATABASE_URL).replace("+asyncpg", "")
        _pg_notify_conn = await asyncpg.connect(dsn=dsn)
        return _pg_notify_conn
    except Exception as exc:
        logger.debug("Failed to create asyncpg NOTIFY connection: %s", exc)
        return None


async def _pg_notify_position_changes(
    new_active: set[str],
    closed_ids: set[str],
    opened_ids: set[str],
    order_token_map: dict[str, str],
) -> None:
    """Fire PG NOTIFY for each detected position change (fill or closure)."""
    conn = await _get_pg_notify_conn()
    if conn is None:
        return

    payloads: list[str] = []
    for oid in opened_ids:
        payloads.append(_json.dumps({
            "action": "fill",
            "order_id": oid,
            "token_id": order_token_map.get(oid, ""),
        }))
    for oid in closed_ids:
        payloads.append(_json.dumps({
            "action": "close",
            "order_id": oid,
            "token_id": order_token_map.get(oid, ""),
        }))

    for payload in payloads:
        try:
            await conn.execute("SELECT pg_notify('position_change', $1)", payload)
        except Exception as exc:
            logger.debug("pg_notify failed: %s", exc)
            # Reset connection on failure so it reconnects next cycle
            global _pg_notify_conn
            try:
                await conn.close()
            except Exception:
                pass
            _pg_notify_conn = None
            break


async def _sync_position_marks_and_exit_registry() -> None:
    """Refresh PositionMarkState and exit evaluation registry with current open orders.

    Called periodically after reconciliation cycles to keep the event-driven
    infrastructure aware of which positions exist and which tokens to monitor.
    """
    global _last_position_mark_sync_at
    now = time.monotonic()
    if (now - _last_position_mark_sync_at) < _POSITION_MARK_SYNC_INTERVAL_SECONDS:
        return
    _last_position_mark_sync_at = now

    try:
        from models.database import TraderOrder
        from sqlalchemy import select

        from services.position_mark_state import get_position_mark_state
        from services.ws_feeds import get_feed_manager

        pms = get_position_mark_state()
        feed_manager = get_feed_manager()

        async with AsyncSessionLocal() as session:
            rows = list(
                (
                    await session.execute(
                        select(TraderOrder).where(
                            TraderOrder.mode == "live",
                            TraderOrder.status.in_(tuple(OPEN_ORDER_STATUSES)),
                        )
                    )
                )
                .scalars()
                .all()
            )
        rows = _dedupe_live_authority_rows(rows)

        # Build sets for tracking
        active_order_ids: set[str] = set()
        token_ids_to_subscribe: set[str] = set()
        exit_registry_by_token: dict[str, list[dict[str, Any]]] = {}

        for row in rows:
            order_id = str(row.id)
            active_order_ids.add(order_id)
            payload = dict(row.payload_json or {})
            live_market = payload.get("live_market") if isinstance(payload.get("live_market"), dict) else {}
            position_state = payload.get("position_state") if isinstance(payload.get("position_state"), dict) else {}

            # Resolve token_id
            token_id = str(
                payload.get("selected_token_id")
                or payload.get("token_id")
                or live_market.get("selected_token_id")
                or ""
            ).strip()
            if not token_id:
                continue

            market_id = str(row.market_id or "").strip()
            direction = str(row.direction or "yes").strip().lower()
            entry_price = float(row.effective_price or row.entry_price or 0)
            # Try payload fill data when row-level price is missing
            if entry_price <= 0:
                _recon = payload.get("provider_reconciliation")
                if isinstance(_recon, dict):
                    entry_price = float(_recon.get("average_fill_price") or 0)
                if entry_price <= 0:
                    _snap = _recon.get("snapshot") if isinstance(_recon, dict) else None
                    if isinstance(_snap, dict):
                        entry_price = float(_snap.get("limit_price") or _snap.get("average_fill_price") or 0)
            notional = float(row.notional_usd or 0)
            edge_pct = float(row.edge_percent or 0)

            if notional <= 0:
                continue

            token_ids_to_subscribe.add(token_id)

            # Register with PositionMarkState
            pms.register_position(
                order_id=order_id,
                market_id=market_id,
                token_id=token_id,
                direction=direction,
                entry_price=entry_price,
                notional=notional,
                edge_percent=edge_pct,
            )

            # Build exit evaluation registry entry
            if token_id not in exit_registry_by_token:
                exit_registry_by_token[token_id] = []

            strategy_exit_config = (
                payload.get("strategy_exit_config")
                if isinstance(payload.get("strategy_exit_config"), dict)
                else {}
            )
            pending_exit = payload.get("pending_live_exit") if isinstance(payload.get("pending_live_exit"), dict) else {}

            exit_registry_by_token[token_id].append({
                "order_id": order_id,
                "trader_id": str(row.trader_id or ""),
                "entry_price": entry_price,
                "has_pending_exit": bool(pending_exit.get("status") in ("submitted", "working")),
                "take_profit_pct": float(strategy_exit_config.get("take_profit_pct") or 0) or None,
                "stop_loss_pct": float(strategy_exit_config.get("stop_loss_pct") or 0) or None,
                "trailing_stop_pct": float(strategy_exit_config.get("trailing_stop_pct") or 0) or None,
                "min_hold_minutes": float(strategy_exit_config.get("min_hold_minutes") or 0),
                "highest_price": float(position_state.get("highest_price") or 0) or None,
                "age_anchor": str(
                    (row.executed_at or row.updated_at or row.created_at).isoformat()
                    if (row.executed_at or row.updated_at or row.created_at)
                    else ""
                ),
            })

        # Unregister closed positions from PositionMarkState
        current_marks = pms.get_marks()
        for existing_oid in list(current_marks.keys()):
            if existing_oid not in active_order_ids:
                pms.unregister_position(existing_oid)

        # Update exit evaluation registry
        all_registered_tokens = set(get_exit_registered_tokens())
        for token_id, orders in exit_registry_by_token.items():
            register_exit_orders(token_id, orders)
        for old_token in all_registered_tokens - set(exit_registry_by_token.keys()):
            unregister_exit_token(old_token)

        # Subscribe tokens to WS feed for price updates
        if token_ids_to_subscribe and getattr(feed_manager, "_started", False):
            try:
                await feed_manager.polymarket_feed.subscribe(sorted(token_ids_to_subscribe))
            except Exception:
                pass

        # Detect position changes and fire PG NOTIFY for the API process
        global _previous_active_order_ids
        opened_ids = active_order_ids - _previous_active_order_ids
        closed_ids = _previous_active_order_ids - active_order_ids
        if opened_ids or closed_ids:
            # Build order→token map for the notification payload
            order_token_map: dict[str, str] = {}
            for row in rows:
                oid = str(row.id)
                payload = dict(row.payload_json or {})
                lm = payload.get("live_market") if isinstance(payload.get("live_market"), dict) else {}
                tid = str(
                    payload.get("selected_token_id")
                    or payload.get("token_id")
                    or lm.get("selected_token_id")
                    or ""
                ).strip()
                order_token_map[oid] = tid
            try:
                await _pg_notify_position_changes(
                    new_active=active_order_ids,
                    closed_ids=closed_ids,
                    opened_ids=opened_ids,
                    order_token_map=order_token_map,
                )
            except Exception as exc:
                logger.debug("Position change notify failed: %s", exc)
            _previous_active_order_ids = set(active_order_ids)

        logger.debug(
            "Position mark sync: positions=%d tokens=%d",
            len(active_order_ids),
            len(token_ids_to_subscribe),
        )

    except Exception as exc:
        logger.warning("Position mark sync failed", exc_info=exc)


_recovery_next_attempt_at: float = 0.0
_RECOVERY_BACKOFF_SECONDS = 120.0  # back off 2 minutes on failure
_RECOVERY_SUCCESS_INTERVAL_SECONDS = 300.0
_live_order_cleanup_next_at: float = 0.0


async def _run_reconciliation_cycle(
    *,
    reason: str,
    emit_event: bool,
    provider_pass: bool,
    heartbeat_activity: str = "",
) -> dict[str, Any]:
    global _recovery_next_attempt_at
    summary = _empty_cycle_summary()
    now_mono = time.monotonic()
    if now_mono >= _recovery_next_attempt_at:
        try:
            async def _authority_recovery() -> None:
                async with AsyncSessionLocal() as session:
                    await recover_missing_live_trader_orders(
                        session,
                        trader_ids=None,
                        commit=True,
                        broadcast=True,
                    )

            await _graceful_timeout(
                _authority_recovery(),
                label="authority_recovery",
                timeout=_TRADER_RECONCILE_TIMEOUT_SECONDS,
            )
            _recovery_next_attempt_at = time.monotonic() + _RECOVERY_SUCCESS_INTERVAL_SECONDS
        except _TimedTaskStillRunningError:
            _recovery_next_attempt_at = time.monotonic() + _RECOVERY_BACKOFF_SECONDS
            logger.warning(
                "Live order authority recovery is still finishing from the prior timeout; backing off %.0fs",
                _RECOVERY_BACKOFF_SECONDS,
            )
        except asyncio.TimeoutError:
            _recovery_next_attempt_at = time.monotonic() + _RECOVERY_BACKOFF_SECONDS
            logger.warning(
                "Live order authority recovery timed out after %.0fs; backing off %.0fs",
                _TRADER_RECONCILE_TIMEOUT_SECONDS,
                _RECOVERY_BACKOFF_SECONDS,
            )
        except Exception as exc:
            _recovery_next_attempt_at = time.monotonic() + _RECOVERY_BACKOFF_SECONDS
            logger.warning(
                "Live order authority recovery failed; backing off %.0fs",
                _RECOVERY_BACKOFF_SECONDS,
                exc_info=exc,
            )
    # Periodic cleanup of stale live_trading_orders (once per hour).
    # These accumulate from CLOB order syncs and the recovery function
    # loads ALL of them — keeping the table small prevents memory spikes.
    global _live_order_cleanup_next_at
    now_mono2 = time.monotonic()
    if now_mono2 >= _live_order_cleanup_next_at:
        _live_order_cleanup_next_at = now_mono2 + 3600.0  # 1 hour
        try:
            async with AsyncSessionLocal() as cleanup_session:
                from sqlalchemy import text as sa_text
                result = await cleanup_session.execute(
                    sa_text("""
                        DELETE FROM live_trading_orders
                        WHERE created_at < now() - interval '7 days'
                        AND lower(coalesce(status, '')) NOT IN ('open', 'pending', 'partially_filled')
                    """)
                )
                deleted = result.rowcount
                await cleanup_session.commit()
                if deleted > 0:
                    logger.info("Cleaned up %d stale live_trading_orders (>7 days old)", deleted)
        except Exception as exc:
            logger.warning("Failed to clean up stale live_trading_orders", exc_info=exc)

    async with AsyncSessionLocal() as session:
        traders = await list_traders(session)
        orchestrator_control = await read_orchestrator_control(session)
    control_settings = dict(orchestrator_control.get("settings") or {})

    summary["traders_seen"] = len(traders)

    async def _reconcile_one(trader: dict[str, Any]) -> dict[str, Any] | None:
        trader_id = str(trader.get("id") or "").strip()
        if not trader_id:
            return None
        for attempt in range(_TRADER_RECONCILE_ATTEMPTS):
            try:
                return await _graceful_timeout(
                    _reconcile_live_state_for_trader(
                        trader,
                        provider_pass=provider_pass,
                        control_settings=control_settings,
                    ),
                    label=f"reconcile:{trader_id}",
                    timeout=_TRADER_RECONCILE_TIMEOUT_SECONDS,
                )
            except _TimedTaskStillRunningError:
                logger.warning(
                    "Live reconciliation skipped because prior timed-out cleanup is still finishing for trader=%s reason=%s",
                    trader_id,
                    reason,
                )
                summary["failures"] = int(summary["failures"]) + 1
                break
            except asyncio.TimeoutError:
                logger.warning(
                    "Live reconciliation timed out for trader=%s reason=%s attempt=%d/%d timeout=%.1fs",
                    trader_id,
                    reason,
                    attempt + 1,
                    _TRADER_RECONCILE_ATTEMPTS,
                    _TRADER_RECONCILE_TIMEOUT_SECONDS,
                )
                summary["failures"] = int(summary["failures"]) + 1
                break
            except StaleDataError as exc:
                if attempt < _TRADER_RECONCILE_ATTEMPTS - 1:
                    logger.warning(
                        "Live reconciliation stale-row conflict for trader=%s reason=%s attempt=%d/%d; retrying",
                        trader_id,
                        reason,
                        attempt + 1,
                        _TRADER_RECONCILE_ATTEMPTS,
                        exc_info=exc,
                    )
                    await asyncio.sleep(_reconcile_retry_delay_seconds(attempt))
                    continue
                summary["failures"] = int(summary["failures"]) + 1
                logger.warning(
                    "Live reconciliation failed for trader=%s reason=%s error_type=%s retryable_db=%s",
                    trader_id,
                    reason,
                    type(exc).__name__,
                    False,
                    exc_info=exc,
                )
            except Exception as exc:
                retryable_db = _is_retryable_db_error(exc)
                if retryable_db and attempt < _TRADER_RECONCILE_ATTEMPTS - 1:
                    logger.warning(
                        "Live reconciliation retrying trader=%s reason=%s attempt=%d/%d due retryable DB error (%s)",
                        trader_id,
                        reason,
                        attempt + 1,
                        _TRADER_RECONCILE_ATTEMPTS,
                        type(exc).__name__,
                        exc_info=exc,
                    )
                    await asyncio.sleep(_reconcile_retry_delay_seconds(attempt))
                    continue
                summary["failures"] = int(summary["failures"]) + 1
                logger.warning(
                    "Live reconciliation failed for trader=%s reason=%s error_type=%s retryable_db=%s",
                    trader_id,
                    reason,
                    type(exc).__name__,
                    retryable_db,
                    exc_info=exc,
                )
            break
        return None

    # Run traders sequentially (concurrency=1) so we can update heartbeat between each.
    # Sleep between traders to release DB pool pressure — the reconciliation holds
    # a connection for 7-15s of Polymarket API I/O per trader, which starves the
    # trader orchestrator of connections for signal processing.
    inter_trader_sleep_seconds = _inter_trader_sleep_seconds(reason=reason)
    results: list[dict[str, Any] | None | BaseException] = []
    for idx, trader in enumerate(traders):
        try:
            result = await _reconcile_one(trader)
        except Exception as exc:
            result = exc
        results.append(result)
        if inter_trader_sleep_seconds > 0.0 and idx < len(traders) - 1:
            await asyncio.sleep(inter_trader_sleep_seconds)
        # Update heartbeat between traders to prevent stale detection
        if heartbeat_activity and idx < len(traders) - 1:
            try:
                async with AsyncSessionLocal() as hb_session:
                    processed = int(summary["traders_processed"]) + (1 if result and not isinstance(result, BaseException) else 0)
                    await write_worker_snapshot(
                        hb_session,
                        WORKER_NAME,
                        running=True,
                        enabled=True,
                        current_activity=(
                            f"{heartbeat_activity} {processed}/"
                            f"{len(traders)} traders"
                        ),
                        interval_seconds=DEFAULT_INTERVAL_SECONDS,
                        last_run_at=utcnow(),
                        stats={
                            "cycle_reason": reason,
                            "provider_pass": bool(provider_pass),
                            "traders_seen": len(traders),
                            "traders_processed": processed,
                            "provider_updates": int(summary["provider_updates"]),
                            "positions_closed": int(summary["positions_closed"]),
                            "inventory_open_positions": int(summary["inventory_open_positions"]),
                            **_wallet_monitor_snapshot_stats(),
                        },
                    )
            except Exception:
                pass
    for result in results:
        if isinstance(result, BaseException) or result is None:
            continue
        provider = result.get("provider") or {}
        lifecycle = result.get("lifecycle") or {}
        inventory = result.get("inventory") or {}
        summary["traders_processed"] = int(summary["traders_processed"]) + 1
        if not bool(provider.get("provider_ready", True)):
            summary["provider_unavailable_traders"] = int(summary["provider_unavailable_traders"]) + 1
        summary["provider_active_seen"] = int(summary["provider_active_seen"]) + int(
            provider.get("active_seen", 0) or 0
        )
        summary["provider_status_changes"] = int(summary["provider_status_changes"]) + int(
            provider.get("status_changes", 0) or 0
        )
        summary["provider_updates"] = int(summary["provider_updates"]) + int(provider.get("updated_orders", 0) or 0)
        summary["provider_notional_updates"] = int(summary["provider_notional_updates"]) + int(
            provider.get("notional_updates", 0) or 0
        )
        summary["provider_price_updates"] = int(summary["provider_price_updates"]) + int(
            provider.get("price_updates", 0) or 0
        )
        summary["positions_would_close"] = int(summary["positions_would_close"]) + int(
            lifecycle.get("would_close", 0) or 0
        )
        summary["positions_closed"] = int(summary["positions_closed"]) + int(lifecycle.get("closed", 0) or 0)
        summary["positions_held"] = int(summary["positions_held"]) + int(lifecycle.get("held", 0) or 0)
        summary["positions_skipped"] = int(summary["positions_skipped"]) + int(lifecycle.get("skipped", 0) or 0)
        summary["inventory_open_positions"] = int(summary["inventory_open_positions"]) + int(
            inventory.get("open_positions", 0) or 0
        )
        summary["inventory_updates"] = int(summary["inventory_updates"]) + int(inventory.get("updates", 0) or 0)
        summary["inventory_inserts"] = int(summary["inventory_inserts"]) + int(inventory.get("inserts", 0) or 0)
        summary["inventory_closures"] = int(summary["inventory_closures"]) + int(inventory.get("closures", 0) or 0)

    if emit_event:
        async with AsyncSessionLocal() as session:
            await create_trader_event(
                session,
                trader_id=None,
                event_type="live_reconciliation_cycle",
                source="worker",
                message=(
                    f"Live reconciliation {reason}: "
                    f"processed={int(summary['traders_processed'])}/{int(summary['traders_seen'])}, "
                    f"provider_updates={int(summary['provider_updates'])}, "
                    f"positions_closed={int(summary['positions_closed'])}"
                ),
                payload={
                    "reason": reason,
                    "provider_pass": bool(provider_pass),
                    **summary,
                    **_wallet_monitor_snapshot_stats(),
                },
                commit=True,
            )
    return summary


async def run_worker_loop() -> None:
    logger.info("Starting trader reconciliation worker loop")

    try:
        async with AsyncSessionLocal() as session:
            await ensure_all_strategies_seeded(session)
        await refresh_strategy_runtime_if_needed(source_keys=None, force=True)
    except Exception as exc:
        logger.warning("Reconciliation strategy startup sync failed", exc_info=exc)

    consecutive_db_failures = 0
    wallet_monitor_wallet = ""
    wallet_monitor_refresh_at = 0.0
    wallet_monitor_wallet = await _sync_live_wallet_monitor_source(wallet_monitor_wallet)
    wallet_monitor_refresh_at = time.monotonic() + _WALLET_MONITOR_REFRESH_SECONDS

    startup_summary = _empty_cycle_summary()
    try:
        startup_summary = await _graceful_timeout(
            _run_reconciliation_cycle(
                reason="startup",
                emit_event=True,
                provider_pass=True,
                heartbeat_activity="Startup reconciling",
            ),
            label="cycle:startup",
            timeout=_RECONCILIATION_CYCLE_TIMEOUT_SECONDS,
        )
    except _TimedTaskStillRunningError:
        logger.warning("Startup reconciliation cycle is still finishing from the prior timeout")
    except asyncio.TimeoutError:
        logger.warning(
            "Startup reconciliation cycle timed out after %.0fs",
            _RECONCILIATION_CYCLE_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        logger.warning("Startup full reconciliation failed", exc_info=exc)

    try:
        async with AsyncSessionLocal() as session:
            await write_worker_snapshot(
                session,
                WORKER_NAME,
                running=True,
                enabled=True,
                current_activity=(
                    f"Startup full sync processed={int(startup_summary['traders_processed'])}/"
                    f"{int(startup_summary['traders_seen'])}"
                ),
                interval_seconds=DEFAULT_INTERVAL_SECONDS,
                last_run_at=utcnow(),
                stats={
                    **startup_summary,
                    **_wallet_monitor_snapshot_stats(),
                    "cycle_reason": "startup",
                    "provider_pass": True,
                },
            )
    except Exception as exc:
        logger.warning("Failed to persist startup reconciliation snapshot", exc_info=exc)

    interval_seconds = DEFAULT_INTERVAL_SECONDS
    is_enabled = True
    is_paused = False
    requested_run = False
    last_open_positions = int(startup_summary.get("inventory_open_positions", 0) or 0)
    last_provider_pass_at = time.monotonic()
    control_refresh_at = 0.0
    trigger_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=_EVENT_QUEUE_MAXSIZE)

    async def _on_runtime_event(event_type: str, data: dict[str, Any]) -> None:
        _ = data
        if event_type not in _RECONCILE_TRIGGER_EVENTS:
            return
        try:
            trigger_queue.put_nowait(event_type)
        except asyncio.QueueFull:
            logger.warning("Reconciliation event queue full; dropping trigger", event_type=event_type)

    await event_bus.start()
    for event_type in sorted(_RECONCILE_TRIGGER_EVENTS):
        event_bus.subscribe(event_type, _on_runtime_event)

    try:
        while True:
            try:
                now_mono = time.monotonic()
                if now_mono >= control_refresh_at:
                    async with AsyncSessionLocal() as session:
                        control = await read_worker_control(
                            session,
                            WORKER_NAME,
                            default_interval=DEFAULT_INTERVAL_SECONDS,
                        )
                        interval_seconds = max(1, int(control.get("interval_seconds") or DEFAULT_INTERVAL_SECONDS))
                        is_enabled = bool(control.get("is_enabled", True))
                        is_paused = bool(control.get("is_paused", False))
                        requested_run = bool(control.get("requested_run_at") is not None)
                        if requested_run:
                            await clear_worker_run_request(session, WORKER_NAME)
                    try:
                        await refresh_strategy_runtime_if_needed(source_keys=None)
                    except Exception as exc:
                        logger.warning("Reconciliation strategy runtime refresh failed", exc_info=exc)

                    if not is_enabled or is_paused:
                        async with AsyncSessionLocal() as snap_session:
                            await write_worker_snapshot(
                                snap_session,
                                WORKER_NAME,
                                running=False,
                                enabled=bool(is_enabled and not is_paused),
                                current_activity="Paused" if is_paused else "Disabled",
                                interval_seconds=interval_seconds,
                                stats={
                                    "requested_run": bool(requested_run),
                                    "open_positions_last_cycle": int(last_open_positions),
                                    **_wallet_monitor_snapshot_stats(),
                                },
                            )

                    control_refresh_at = now_mono + _CONTROL_REFRESH_SECONDS

                if now_mono >= wallet_monitor_refresh_at:
                    wallet_monitor_wallet = await _sync_live_wallet_monitor_source(wallet_monitor_wallet)
                    wallet_monitor_refresh_at = now_mono + _WALLET_MONITOR_REFRESH_SECONDS

                if not is_enabled or is_paused:
                    await asyncio.sleep(max(_IDLE_SLEEP_SECONDS, interval_seconds))
                    continue

                cycle_reason = "scheduled"
                provider_pass = True
                emit_event = False

                if requested_run:
                    cycle_reason = "requested"
                    provider_pass = True
                    emit_event = True
                    requested_run = False
                else:
                    wait_seconds = _ACTIVE_POSITION_TICK_SECONDS if last_open_positions > 0 else float(interval_seconds)
                    try:
                        event_type = await asyncio.wait_for(
                            trigger_queue.get(),
                            timeout=max(0.05, wait_seconds),
                        )
                        drained_event_types = [event_type]
                        while len(drained_event_types) < _MAX_TRIGGER_DRAIN_PER_CYCLE:
                            try:
                                drained_event_types.append(trigger_queue.get_nowait())
                            except asyncio.QueueEmpty:
                                break
                        unique_event_types = list(dict.fromkeys(str(item or "").strip() for item in drained_event_types if item))
                        drained_count = len(drained_event_types) - 1
                        cycle_reason = f"event:{unique_event_types[0]}"
                        if len(unique_event_types) > 1:
                            cycle_reason = f"{cycle_reason}+{len(unique_event_types) - 1}_types"
                        elif drained_count > 0:
                            cycle_reason = f"{cycle_reason}+{drained_count}_queued"
                        provider_pass = True
                    except asyncio.TimeoutError:
                        if last_open_positions > 0:
                            elapsed_provider_seconds = time.monotonic() - last_provider_pass_at
                            if elapsed_provider_seconds >= float(interval_seconds):
                                cycle_reason = "scheduled"
                                provider_pass = True
                            else:
                                cycle_reason = "position_tick"
                                provider_pass = False
                        else:
                            cycle_reason = "scheduled"
                            provider_pass = True

                try:
                    cycle_summary = await _graceful_timeout(
                        _run_reconciliation_cycle(
                            reason=cycle_reason,
                            emit_event=emit_event,
                            provider_pass=provider_pass,
                            heartbeat_activity="Reconciling",
                        ),
                        label="cycle:main",
                        timeout=_RECONCILIATION_CYCLE_TIMEOUT_SECONDS,
                    )
                except _TimedTaskStillRunningError:
                    logger.warning(
                        "Reconciliation cycle skipped because the prior timed-out cycle is still finishing reason=%s",
                        cycle_reason,
                    )
                    cycle_summary = _empty_cycle_summary()
                except asyncio.TimeoutError:
                    logger.warning(
                        "Reconciliation cycle timed out after %.0fs reason=%s",
                        _RECONCILIATION_CYCLE_TIMEOUT_SECONDS,
                        cycle_reason,
                    )
                    cycle_summary = _empty_cycle_summary()
                consecutive_db_failures = 0

                # Mandatory cooldown after each cycle to release DB pool pressure
                # and let the trader orchestrator process signals uncontested.
                cooldown_seconds = _post_cycle_cooldown_seconds(
                    reason=cycle_reason,
                    provider_pass=provider_pass,
                )
                if cooldown_seconds > 0.0:
                    await asyncio.sleep(cooldown_seconds)

                # Sync position marks and exit registry for event-driven updates
                await _sync_position_marks_and_exit_registry()
                last_open_positions = int(cycle_summary.get("inventory_open_positions", 0) or 0)
                if provider_pass:
                    last_provider_pass_at = time.monotonic()

                async with AsyncSessionLocal() as session:
                    await write_worker_snapshot(
                        session,
                        WORKER_NAME,
                        running=True,
                        enabled=True,
                        current_activity=(
                            f"Reconciled {int(cycle_summary['traders_processed'])}/"
                            f"{int(cycle_summary['traders_seen'])} traders, "
                            f"provider_updates={int(cycle_summary['provider_updates'])}, "
                            f"positions_closed={int(cycle_summary['positions_closed'])}, "
                            f"provider_unavailable={int(cycle_summary['provider_unavailable_traders'])}"
                        ),
                        interval_seconds=interval_seconds,
                        last_run_at=utcnow(),
                        stats={
                            **cycle_summary,
                            "cycle_reason": cycle_reason,
                            "provider_pass": bool(provider_pass),
                            **_wallet_monitor_snapshot_stats(),
                        },
                    )
            except DBAPIError as exc:
                if not _is_retryable_db_error(exc):
                    raise
                consecutive_db_failures += 1
                logger.warning(
                    "Trader reconciliation cycle hit transient DB error (failure=%d)",
                    consecutive_db_failures,
                    exc_info=exc,
                )
                if consecutive_db_failures >= _MAX_CONSECUTIVE_DB_FAILURES:
                    await recover_pool()
                    consecutive_db_failures = 0
                    logger.warning("Recovered DB connection pool after trader reconciliation disconnects")
                control_refresh_at = 0.0
                wallet_monitor_refresh_at = 0.0
                await asyncio.sleep(_IDLE_SLEEP_SECONDS)
            except Exception as exc:
                logger.exception("Trader reconciliation worker cycle failed: %s", exc)
                try:
                    async with AsyncSessionLocal() as session:
                        await write_worker_snapshot(
                            session,
                            WORKER_NAME,
                            running=False,
                            enabled=True,
                            current_activity="Worker error",
                            interval_seconds=interval_seconds,
                            last_error=str(exc),
                        )
                except Exception:
                    pass
                control_refresh_at = 0.0
                wallet_monitor_refresh_at = 0.0
                await asyncio.sleep(_IDLE_SLEEP_SECONDS)
    finally:
        for event_type in sorted(_RECONCILE_TRIGGER_EVENTS):
            event_bus.unsubscribe(event_type, _on_runtime_event)


async def start_loop() -> None:
    try:
        await run_worker_loop()
    except asyncio.CancelledError:
        logger.info("Trader reconciliation worker shutting down")


async def main() -> None:
    await init_database()
    try:
        await run_worker_loop()
    except asyncio.CancelledError:
        logger.info("Trader reconciliation worker shutting down")


if __name__ == "__main__":
    asyncio.run(main())
