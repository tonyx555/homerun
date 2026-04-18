"""Fast-tier trader runtime.

A dedicated worker that owns every trader with ``latency_class='fast'``.
Each trader gets its own long-lived asyncio Task with:

* An isolated DB engine (``FastAsyncSessionLocal``) — aggressive 1500ms
  statement_timeout, small dedicated pool, cannot be starved by the
  slow-tier reconciler or session_engine.
* Event-driven wakeup via ``event_bus`` (``trade_signal_batch`` /
  ``trade_signal_emission``) with a 250ms polling fallback so a missed
  wakeup never stalls a trader longer than a quarter second.
* Direct single-leg submission via ``fast_submit.execute_fast_signal``
  — no ``execution_sessions``, no pre-submit placeholder, no 45s ack
  wait, no leg-wave orchestration.

A fast-tier cycle looks like: event arrives → session opens → one quick
SELECT for pending signals → strategy.evaluate → one quick INSERT of
the trader_order row → commit → done.  Target end-to-end: <150ms under
normal CLOB latency.

Fast traders are *opt-in*: the shared orchestrator loop skips any trader
with ``latency_class='fast'`` (see trader_orchestrator_worker), so the
two runtimes never fight over the same trader.  Pre-existing traders
default to ``latency_class='normal'`` and keep the existing path.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

from sqlalchemy.exc import DBAPIError

from models.database import AsyncSessionLocal, FastAsyncSessionLocal
from services.event_bus import event_bus
from services.strategy_loader import strategy_loader
from services.trader_hot_state import get_signal_sequence_cursor as _hot_get_signal_sequence_cursor
from services.trader_orchestrator.fast_submit import (
    advance_fast_trader_cursor,
    execute_fast_signal,
)
from services.trader_orchestrator_state import (
    get_trader_signal_cursor,
    list_fast_traders,
    list_unconsumed_trade_signals,
)
from services.worker_state import _is_retryable_db_error, write_worker_snapshot
from utils.logger import get_logger

logger = get_logger(__name__)

WORKER_NAME = "fast_trader_runtime"

# Budgets for the fast tier.  These are deliberately tiny: if something
# in this path takes longer than a budget, the fast tier is the wrong
# home for it and the trader should be moved to normal/slow.
_CYCLE_HARD_BUDGET_SECONDS = 1.5
_EVALUATE_BUDGET_SECONDS = 0.2
_SUBMIT_BUDGET_SECONDS = 1.0
_POLL_FALLBACK_SECONDS = 0.25
_HEARTBEAT_INTERVAL_SECONDS = 5.0
_TRADER_REFRESH_INTERVAL_SECONDS = 15.0
_MAX_SIGNALS_PER_CYCLE = 4

# Events that should wake up a fast trader.  Anything that adds a new
# signal row or changes signal state belongs here.
_WAKE_EVENTS = (
    "trade_signal_emission",
    "trade_signal_batch",
    "signals_update",
)


def _get_sequence_cursor(trader_id: str) -> Optional[int]:
    """Read the live signal-sequence cursor from hot state (non-blocking)."""
    try:
        value = _hot_get_signal_sequence_cursor(trader_id, "live")
    except Exception:
        return None
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


class _FastTraderTask:
    """One fast-tier trader's event-driven loop."""

    def __init__(self, trader: dict[str, Any], wake: asyncio.Event) -> None:
        self._trader = dict(trader)
        self._wake = wake
        self._stopped = False

    @property
    def trader_id(self) -> str:
        return str(self._trader.get("id") or "")

    def refresh_trader(self, trader: dict[str, Any]) -> None:
        self._trader = dict(trader)

    def stop(self) -> None:
        self._stopped = True
        self._wake.set()

    async def run(self) -> None:
        """Infinite per-trader loop.  Exits only when ``stop()`` is called."""
        trader_id = self.trader_id
        if not trader_id:
            return
        logger.info("Fast trader runtime started", trader_id=trader_id, name=self._trader.get("name"))
        while not self._stopped:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=_POLL_FALLBACK_SECONDS)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            if self._stopped:
                break
            if not self._trader.get("is_enabled", True) or self._trader.get("is_paused", False):
                continue
            try:
                await asyncio.wait_for(self._run_once(), timeout=_CYCLE_HARD_BUDGET_SECONDS)
            except asyncio.TimeoutError:
                logger.warning(
                    "Fast trader cycle exceeded hard budget",
                    trader_id=trader_id,
                    budget_s=_CYCLE_HARD_BUDGET_SECONDS,
                )
            except Exception as exc:
                if _is_retryable_db_error(exc):
                    logger.warning(
                        "Fast trader cycle hit retryable DB error",
                        trader_id=trader_id,
                        exc_info=exc,
                    )
                else:
                    logger.error("Fast trader cycle failed", trader_id=trader_id, exc_info=exc)
        logger.info("Fast trader runtime stopped", trader_id=trader_id)

    def _accepted_sources(self) -> list[str]:
        configs = self._trader.get("source_configs") or []
        sources: list[str] = []
        for cfg in configs:
            if not isinstance(cfg, dict):
                continue
            if not cfg.get("enabled", True):
                continue
            src = str(cfg.get("source_key") or "").strip().lower()
            if src:
                sources.append(src)
        return sources

    def _strategy_params_for_source(self, source_key: str) -> dict[str, Any]:
        configs = self._trader.get("source_configs") or []
        src_key = str(source_key or "").strip().lower()
        for cfg in configs:
            if not isinstance(cfg, dict):
                continue
            if str(cfg.get("source_key") or "").strip().lower() != src_key:
                continue
            params = cfg.get("strategy_params")
            return dict(params) if isinstance(params, dict) else {}
        return {}

    def _strategy_key_for_source(self, source_key: str) -> Optional[str]:
        configs = self._trader.get("source_configs") or []
        src_key = str(source_key or "").strip().lower()
        for cfg in configs:
            if not isinstance(cfg, dict):
                continue
            if str(cfg.get("source_key") or "").strip().lower() != src_key:
                continue
            sk = str(cfg.get("strategy_key") or "").strip().lower()
            return sk or None
        return None

    async def _run_once(self) -> None:
        trader_id = self.trader_id
        accepted_sources = self._accepted_sources()
        if not accepted_sources:
            return

        async with FastAsyncSessionLocal() as session:
            cursor_created_at, cursor_signal_id = await get_trader_signal_cursor(
                session, trader_id=trader_id
            )
            cursor_runtime_sequence = _get_sequence_cursor(trader_id)
            signals = await list_unconsumed_trade_signals(
                session,
                trader_id=trader_id,
                sources=accepted_sources,
                cursor_runtime_sequence=cursor_runtime_sequence,
                cursor_created_at=cursor_created_at,
                cursor_signal_id=cursor_signal_id,
                limit=_MAX_SIGNALS_PER_CYCLE,
            )
            if not signals:
                return

            mode = str(self._trader.get("mode", "shadow")).strip().lower() or "shadow"
            risk_limits = dict(self._trader.get("risk_limits") or {})
            default_size_usd = float(max(0.01, float(risk_limits.get("max_trade_notional_usd") or 1.0)))

            for signal in signals:
                try:
                    await self._process_one(
                        session,
                        signal=signal,
                        mode=mode,
                        default_size_usd=default_size_usd,
                    )
                except Exception as exc:
                    logger.warning(
                        "Fast trader signal processing failed",
                        trader_id=trader_id,
                        signal_id=getattr(signal, "id", None),
                        exc_info=exc,
                    )

            try:
                await session.commit()
            except Exception as exc:
                logger.warning("Fast trader cycle commit failed", trader_id=trader_id, exc_info=exc)

    async def _process_one(
        self,
        session,
        *,
        signal: Any,
        mode: str,
        default_size_usd: float,
    ) -> None:
        trader_id = self.trader_id
        source_key = str(getattr(signal, "source", "") or "").strip().lower()
        strategy_key = self._strategy_key_for_source(source_key) or str(
            getattr(signal, "strategy_type", "") or ""
        ).strip().lower()
        if not strategy_key:
            logger.debug("Fast trader skipped signal: no strategy_key", signal_id=getattr(signal, "id", None))
            return

        strategy = strategy_loader.get_instance(strategy_key)
        if strategy is None:
            logger.debug(
                "Fast trader skipped signal: strategy not loaded",
                strategy_key=strategy_key,
                signal_id=getattr(signal, "id", None),
            )
            return

        strategy_params = self._strategy_params_for_source(source_key)

        # Build a minimal evaluation context.  The fast path intentionally
        # skips live_market_context enrichment and heavy risk snapshots —
        # the strategy's own evaluate() is the gate, nothing else.
        context: dict[str, Any] = {
            "trader": self._trader,
            "params": strategy_params,
            "mode": mode,
            "source_config": {"source_key": source_key, "strategy_key": strategy_key},
            "fast_tier": True,
        }

        try:
            decision = await asyncio.wait_for(
                asyncio.to_thread(strategy.evaluate, signal, context),
                timeout=_EVALUATE_BUDGET_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Fast trader evaluate() exceeded budget",
                trader_id=trader_id,
                signal_id=getattr(signal, "id", None),
                budget_s=_EVALUATE_BUDGET_SECONDS,
            )
            return
        except Exception as exc:
            logger.warning(
                "Fast trader evaluate() raised",
                trader_id=trader_id,
                signal_id=getattr(signal, "id", None),
                exc_info=exc,
            )
            return

        final_decision = str(getattr(decision, "decision", "failed") or "failed").strip().lower()
        reason = str(getattr(decision, "reason", "") or "")
        evaluated_size = float(getattr(decision, "size_usd", None) or default_size_usd)

        if final_decision != "selected":
            await advance_fast_trader_cursor(
                session,
                trader_id=trader_id,
                signal=signal,
                decision_id=None,
                outcome="skipped" if final_decision == "skipped" else "blocked",
                reason=reason or f"fast:{final_decision}",
            )
            return

        try:
            result = await asyncio.wait_for(
                execute_fast_signal(
                    session,
                    trader_id=trader_id,
                    signal=signal,
                    decision_id=None,
                    strategy_key=strategy_key,
                    strategy_version=None,
                    strategy_params=strategy_params,
                    mode=mode,
                    size_usd=evaluated_size,
                    reason=reason or None,
                ),
                timeout=_SUBMIT_BUDGET_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Fast trader submit exceeded budget",
                trader_id=trader_id,
                signal_id=getattr(signal, "id", None),
                budget_s=_SUBMIT_BUDGET_SECONDS,
            )
            return
        except Exception as exc:
            logger.warning(
                "Fast trader submit raised",
                trader_id=trader_id,
                signal_id=getattr(signal, "id", None),
                exc_info=exc,
            )
            return

        outcome_for_cursor = "executed" if result.orders_written > 0 else result.status
        await advance_fast_trader_cursor(
            session,
            trader_id=trader_id,
            signal=signal,
            decision_id=None,
            outcome=outcome_for_cursor,
            reason=result.error_message or reason,
        )


class _FastRuntime:
    """Supervises the per-trader tasks + event bus subscription."""

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task] = {}
        self._wake_events: dict[str, asyncio.Event] = {}
        self._task_objs: dict[str, _FastTraderTask] = {}
        self._refresh_lock = asyncio.Lock()
        self._stopping = False
        self._last_heartbeat: float = 0.0

    async def _on_signal_event(self, event_type: str, data: dict[str, Any]) -> None:
        """Wake every fast trader on any new-signal event."""
        if self._stopping:
            return
        for wake in self._wake_events.values():
            wake.set()

    async def _emit_heartbeat(self) -> None:
        now_mono = time.monotonic()
        if now_mono - self._last_heartbeat < _HEARTBEAT_INTERVAL_SECONDS:
            return
        self._last_heartbeat = now_mono
        try:
            # Heartbeat uses the MAIN pool — the fast-tier pool is reserved
            # for the trading hot path, not worker-snapshot writes.  Keeping
            # it here caused _fast_on_checkin warnings ("held for 7.8s").
            async with AsyncSessionLocal() as session:
                await write_worker_snapshot(
                    session,
                    WORKER_NAME,
                    running=True,
                    enabled=True,
                    current_activity=f"Supervising {len(self._task_objs)} fast trader(s)",
                    interval_seconds=int(_TRADER_REFRESH_INTERVAL_SECONDS),
                    stats={"fast_trader_count": len(self._task_objs)},
                )
        except DBAPIError as exc:
            if not _is_retryable_db_error(exc):
                logger.debug("Fast runtime heartbeat failed", exc_info=exc)
        except Exception as exc:
            logger.debug("Fast runtime heartbeat failed", exc_info=exc)

    async def _refresh_roster(self) -> None:
        """Reconcile the set of running per-trader tasks with the DB roster."""
        async with self._refresh_lock:
            if self._stopping:
                return
            # Roster refresh is a slow-cadence maintenance read; use the
            # main pool so we do not touch the tight fast-tier pool for
            # anything other than the per-trader trading loop.
            async with AsyncSessionLocal() as session:
                traders = await list_fast_traders(session)
            seen_ids: set[str] = set()
            for trader in traders:
                trader_id = str(trader.get("id") or "").strip()
                if not trader_id:
                    continue
                seen_ids.add(trader_id)
                existing = self._task_objs.get(trader_id)
                if existing is not None:
                    existing.refresh_trader(trader)
                    continue
                wake = asyncio.Event()
                wake.set()  # Process pending work on first tick
                runner = _FastTraderTask(trader, wake)
                task = asyncio.create_task(runner.run(), name=f"fast-trader-{trader_id[:12]}")
                self._wake_events[trader_id] = wake
                self._task_objs[trader_id] = runner
                self._tasks[trader_id] = task
            stale_ids = [tid for tid in list(self._tasks.keys()) if tid not in seen_ids]
            for tid in stale_ids:
                runner = self._task_objs.pop(tid, None)
                if runner is not None:
                    runner.stop()
                task = self._tasks.pop(tid, None)
                self._wake_events.pop(tid, None)
                if task is not None:
                    try:
                        await asyncio.wait_for(task, timeout=2.0)
                    except asyncio.TimeoutError:
                        task.cancel()
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("Fast trader stopped with error", trader_id=tid, exc_info=exc)

    async def run(self) -> None:
        logger.info("Fast-tier runtime supervisor started")
        for evt in _WAKE_EVENTS:
            event_bus.subscribe(evt, self._on_signal_event)

        try:
            await self._refresh_roster()
            await self._emit_heartbeat()
            while not self._stopping:
                try:
                    await asyncio.sleep(_TRADER_REFRESH_INTERVAL_SECONDS)
                except asyncio.CancelledError:
                    break
                try:
                    await self._refresh_roster()
                except Exception as exc:
                    logger.warning("Fast-tier runtime roster refresh failed", exc_info=exc)
                await self._emit_heartbeat()
        finally:
            self._stopping = True
            for runner in self._task_objs.values():
                runner.stop()
            for task in self._tasks.values():
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except asyncio.TimeoutError:
                    task.cancel()
                except Exception:  # noqa: BLE001
                    pass
            for evt in _WAKE_EVENTS:
                try:
                    event_bus.unsubscribe(evt, self._on_signal_event)
                except Exception:
                    pass
            logger.info("Fast-tier runtime supervisor stopped")


async def start_loop() -> None:
    """Worker entrypoint.  Exported at module level for the host registry."""
    runtime = _FastRuntime()
    try:
        await runtime.run()
    except asyncio.CancelledError:
        logger.info("Fast-tier runtime cancelled")
        raise
