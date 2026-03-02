"""Telegram notification service focused on trader-orchestrator activity."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
from sqlalchemy import and_, func, or_, select

from config import settings
from models.database import (
    AppSettings,
    AsyncSessionLocal,
    Trader,
    TraderDecision,
    TraderEvent,
    TraderOrder,
    TraderOrchestratorControl,
    TraderOrchestratorSnapshot,
)
from services.pause_state import global_pause_state
from services.trader_orchestrator_state import (
    ORCHESTRATOR_DEFAULT_RUN_INTERVAL_SECONDS,
    arm_live_start,
    consume_live_arm_token,
    create_live_preflight,
    create_trader_event,
    read_orchestrator_control,
    read_orchestrator_snapshot,
    update_orchestrator_control,
    write_orchestrator_snapshot,
)
from utils.logger import get_logger
from utils.secrets import decrypt_secret
from utils.utcnow import utcnow

logger = get_logger("notifier")

TELEGRAM_API_BASE = "https://api.telegram.org"
MAX_MESSAGES_PER_MINUTE = 20
MONITOR_POLL_SECONDS = 5
SETTINGS_REFRESH_SECONDS = 30
DEFAULT_SUMMARY_INTERVAL_MINUTES = 60
COMMAND_POLL_TIMEOUT_SECONDS = 25
COMMAND_LOOP_IDLE_SECONDS = 3
CLOSE_ALERT_MARKER_LIMIT = 4000
CLOSE_ALERT_BATCH_LIMIT = 10
TELEGRAM_UPDATE_DRAIN_LIMIT = 50
REALIZED_ORDER_STATUSES = {"resolved_win", "resolved_loss", "closed_win", "closed_loss", "win", "loss"}
ISSUE_ORDER_STATUSES = {"failed", "rejected", "cancelled", "error"}


def _escape_md(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    special = r"\_*[]()~`>#+=|{}.!-"
    escaped: list[str] = []
    for ch in str(text):
        if ch in special:
            escaped.append(f"\\{ch}")
        else:
            escaped.append(ch)
    return "".join(escaped)


def _bold(text: str) -> str:
    return f"*{text}*"


def _clamp_int(value: object, minimum: int, maximum: int, fallback: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = fallback
    return max(minimum, min(maximum, parsed))


def _to_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_money(value: float) -> str:
    return f"${value:,.2f}"


def _format_signed_money(value: float) -> str:
    amount = float(value or 0.0)
    sign = "+" if amount >= 0 else "-"
    return f"{sign}${abs(amount):,.2f}"


def _truncate_text(value: Any, limit: int) -> str:
    text = str(value or "").replace("\n", " ").replace("\r", " ").strip()
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return f"{text[: limit - 3]}..."


def _compact_join(parts: list[str], *, max_items: int) -> str:
    compact_parts = [str(part).strip() for part in parts if str(part).strip()]
    if not compact_parts:
        return "none"
    selected = compact_parts[:max_items]
    overflow = len(compact_parts) - len(selected)
    if overflow > 0:
        selected.append(f"+{overflow} more")
    return " · ".join(selected)


def _normalize_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    if mode == "paper":
        return "shadow"
    if mode in {"shadow", "live"}:
        return mode
    return "other"


def _mode_label(value: Any) -> str:
    mode = _normalize_mode(value)
    if mode == "other":
        return "OTHER"
    return mode.upper()


def _mode_label_from_orders(orders: list[TraderOrder], fallback: str) -> str:
    counts: dict[str, int] = defaultdict(int)
    for row in orders:
        counts[_mode_label(getattr(row, "mode", None))] += 1
    if not counts:
        return fallback
    if len(counts) == 1:
        return next(iter(counts.keys()))
    parts = [f"{label} {counts[label]}" for label in sorted(counts.keys())]
    return f"MIXED ({', '.join(parts)})"


class TelegramNotifier:
    """Telegram notifier that tracks autotrader-only runtime activity."""

    def __init__(self) -> None:
        self._bot_token: Optional[str] = None
        self._chat_id: Optional[str] = None
        self._notifications_enabled: bool = False

        self._notify_autotrader_orders: bool = False
        self._notify_autotrader_closes: bool = True
        self._notify_autotrader_issues: bool = True
        self._notify_autotrader_timeline: bool = True
        self._summary_interval_minutes: int = DEFAULT_SUMMARY_INTERVAL_MINUTES
        self._summary_per_trader: bool = False

        self._send_timestamps: deque[float] = deque()
        self._message_queue: asyncio.Queue[str] = asyncio.Queue()
        self._queue_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._command_task: Optional[asyncio.Task] = None

        self._http_client: Optional[httpx.AsyncClient] = None

        self._running = False
        self._started = False
        self._delivery_ready = False

        self._autotrader_active: bool = False
        self._last_summary_at: Optional[datetime] = None
        self._last_snapshot_error: Optional[str] = None
        self._last_settings_reload_monotonic: float = 0.0

        self._last_order_cursor: Optional[tuple[datetime, str]] = None
        self._last_order_update_cursor: Optional[tuple[datetime, str]] = None
        self._last_event_cursor: Optional[tuple[datetime, str]] = None
        self._telegram_update_offset: Optional[int] = None
        self._close_alert_markers: dict[str, str] = {}

    async def start(self) -> None:
        """Initialize notifier runtime and start background workers."""
        if self._started:
            logger.warning("Notifier already started")
            return

        await self._load_settings()
        await self._prime_cursors()

        if not self._bot_token or not self._chat_id:
            logger.info("Telegram credentials not configured -- notifier will stay dormant until credentials are set")
        else:
            logger.info("Telegram notifier credentials loaded")

        self._http_client = httpx.AsyncClient(timeout=15.0)
        self._running = True
        self._started = True
        self._last_settings_reload_monotonic = time.monotonic()
        await self._prime_telegram_update_offset()

        self._queue_task = asyncio.create_task(self._queue_worker())
        self._monitor_task = asyncio.create_task(self._autotrader_monitor_loop())
        self._command_task = asyncio.create_task(self._telegram_command_loop())

        logger.info("Telegram notifier started (autotrader mode)")

    def stop(self) -> None:
        """Stop notifier tasks."""
        self._running = False
        for task in (self._queue_task, self._monitor_task, self._command_task):
            if task and not task.done():
                task.cancel()
        logger.info("Telegram notifier stopped")

    async def shutdown(self) -> None:
        """Stop and close network resources."""
        self.stop()
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def _load_settings(self) -> None:
        """Load notifier configuration from DB with environment fallback."""
        row: Optional[AppSettings] = None
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(select(AppSettings).where(AppSettings.id == "default"))
                row = result.scalar_one_or_none()
        except Exception as exc:
            logger.warning(
                "Could not load notifier settings from DB, using config.py",
                exc_info=exc,
            )

        if row is None:
            self._bot_token = settings.TELEGRAM_BOT_TOKEN
            self._chat_id = settings.TELEGRAM_CHAT_ID
            self._notifications_enabled = bool(self._bot_token and self._chat_id)
            self._notify_autotrader_orders = False
            self._notify_autotrader_closes = True
            self._notify_autotrader_issues = True
            self._notify_autotrader_timeline = True
            self._summary_interval_minutes = DEFAULT_SUMMARY_INTERVAL_MINUTES
            self._summary_per_trader = False
            return

        db_bot_token = decrypt_secret(row.telegram_bot_token)
        db_bot_token = str(db_bot_token).strip() if db_bot_token is not None else ""
        db_chat_id = str(getattr(row, "telegram_chat_id", "") or "").strip()
        env_bot_token = str(getattr(settings, "TELEGRAM_BOT_TOKEN", "") or "").strip()
        env_chat_id = str(getattr(settings, "TELEGRAM_CHAT_ID", "") or "").strip()

        # Runtime precedence: DB non-null override > env > default(None).
        self._bot_token = db_bot_token or env_bot_token or None
        self._chat_id = db_chat_id or env_chat_id or None
        if row.notifications_enabled is None:
            self._notifications_enabled = bool(self._bot_token and self._chat_id)
        else:
            self._notifications_enabled = bool(row.notifications_enabled)

        # Legacy bridge: if new toggles are missing for any reason, fall back to old flags.
        self._notify_autotrader_orders = bool(getattr(row, "notify_autotrader_orders", bool(row.notify_on_trade)))
        self._notify_autotrader_closes = bool(getattr(row, "notify_autotrader_closes", True))
        self._notify_autotrader_issues = bool(getattr(row, "notify_autotrader_issues", True))
        self._notify_autotrader_timeline = bool(
            getattr(row, "notify_autotrader_timeline", bool(row.notify_on_opportunity))
        )
        self._summary_interval_minutes = _clamp_int(
            getattr(
                row,
                "notify_autotrader_summary_interval_minutes",
                DEFAULT_SUMMARY_INTERVAL_MINUTES,
            ),
            5,
            1440,
            DEFAULT_SUMMARY_INTERVAL_MINUTES,
        )
        self._summary_per_trader = bool(getattr(row, "notify_autotrader_summary_per_trader", False))

    async def reload_settings(self) -> None:
        await self._load_settings()
        logger.info("Notifier settings reloaded")

    async def _prime_cursors(self) -> None:
        """Avoid replaying old DB records when notifier starts."""
        try:
            async with AsyncSessionLocal() as session:
                latest_order = (
                    await session.execute(
                        select(TraderOrder.created_at, TraderOrder.id)
                        .order_by(TraderOrder.created_at.desc(), TraderOrder.id.desc())
                        .limit(1)
                    )
                ).first()
                if latest_order and latest_order[0] is not None:
                    self._last_order_cursor = (_to_utc(latest_order[0]), str(latest_order[1]))

                latest_order_update = (
                    await session.execute(
                        select(TraderOrder.updated_at, TraderOrder.id)
                        .where(TraderOrder.updated_at.is_not(None))
                        .order_by(TraderOrder.updated_at.desc(), TraderOrder.id.desc())
                        .limit(1)
                    )
                ).first()
                if latest_order_update and latest_order_update[0] is not None:
                    self._last_order_update_cursor = (_to_utc(latest_order_update[0]), str(latest_order_update[1]))

                latest_event = (
                    await session.execute(
                        select(TraderEvent.created_at, TraderEvent.id)
                        .order_by(TraderEvent.created_at.desc(), TraderEvent.id.desc())
                        .limit(1)
                    )
                ).first()
                if latest_event and latest_event[0] is not None:
                    self._last_event_cursor = (_to_utc(latest_event[0]), str(latest_event[1]))

                snapshot = await session.get(TraderOrchestratorSnapshot, "latest")
                if snapshot is not None:
                    self._last_snapshot_error = str(snapshot.last_error).strip() if snapshot.last_error else None

                control = await session.get(TraderOrchestratorControl, "default")
                self._autotrader_active = bool(
                    control is not None and bool(control.is_enabled) and not bool(control.is_paused)
                )
                if self._autotrader_active:
                    self._last_summary_at = utcnow()
                self._close_alert_markers = {}
        except Exception as exc:
            logger.debug("Notifier cursor priming skipped", exc_info=exc)

    async def _autotrader_monitor_loop(self) -> None:
        """Poll trader-orchestrator state and emit Telegram notifications."""
        while self._running:
            try:
                now_monotonic = time.monotonic()
                if now_monotonic - self._last_settings_reload_monotonic >= SETTINGS_REFRESH_SECONDS:
                    await self._load_settings()
                    self._last_settings_reload_monotonic = now_monotonic

                delivery_ready = bool(self._notifications_enabled and self._bot_token and self._chat_id)

                if delivery_ready and not self._delivery_ready:
                    # Avoid replaying stale history when notifications are
                    # enabled after running with delivery disabled.
                    await self._prime_cursors()
                    self._delivery_ready = True

                if not delivery_ready:
                    self._delivery_ready = False
                    await asyncio.sleep(MONITOR_POLL_SECONDS)
                    continue

                async with AsyncSessionLocal() as session:
                    control = await session.get(TraderOrchestratorControl, "default")
                    snapshot = await session.get(TraderOrchestratorSnapshot, "latest")

                    active = bool(control is not None and bool(control.is_enabled) and not bool(control.is_paused))
                    mode = _mode_label(getattr(control, "mode", "shadow"))

                    traders_running = int(getattr(snapshot, "traders_running", 0) if snapshot else 0)
                    traders_total = int(getattr(snapshot, "traders_total", 0) if snapshot else 0)

                    if active and not self._autotrader_active:
                        await self._enqueue(
                            self._format_runtime_state_message(
                                title="Autotrader Active",
                                mode=mode,
                                traders_running=traders_running,
                                traders_total=traders_total,
                            )
                        )
                        self._last_summary_at = utcnow()
                    elif not active and self._autotrader_active:
                        await self._enqueue(
                            self._format_runtime_state_message(
                                title="Autotrader Paused",
                                mode=mode,
                                traders_running=traders_running,
                                traders_total=traders_total,
                            )
                        )
                        self._last_summary_at = None

                    self._autotrader_active = active

                    new_events = await self._load_new_trader_events(session)
                    new_orders = await self._load_new_trader_orders(session)
                    updated_orders = await self._load_updated_trader_orders(session)

                    if active and self._notify_autotrader_issues:
                        await self._send_issue_alerts(
                            session=session,
                            events=new_events,
                            orders=new_orders,
                            snapshot=snapshot,
                        )

                    if active and self._notify_autotrader_orders:
                        await self._send_order_activity_alert(
                            session=session,
                            orders=new_orders,
                            mode=mode,
                        )

                    if active and self._notify_autotrader_closes:
                        await self._send_position_close_alerts(
                            session=session,
                            orders=updated_orders,
                            mode=mode,
                        )

                    if active and self._notify_autotrader_timeline:
                        await self._maybe_send_timeline_summary(
                            session=session,
                            control=control,
                            snapshot=snapshot,
                        )
                    elif not active:
                        self._last_summary_at = None

            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error("Autotrader notifier monitor error", exc_info=exc)

            await asyncio.sleep(MONITOR_POLL_SECONDS)

    async def _load_new_trader_orders(self, session) -> list[TraderOrder]:
        query = select(TraderOrder).order_by(
            TraderOrder.created_at.asc(),
            TraderOrder.id.asc(),
        )

        if self._last_order_cursor is not None:
            cursor_ts, cursor_id = self._last_order_cursor
            query = query.where(
                or_(
                    TraderOrder.created_at > cursor_ts.replace(tzinfo=None),
                    and_(
                        TraderOrder.created_at == cursor_ts.replace(tzinfo=None),
                        TraderOrder.id > cursor_id,
                    ),
                )
            )

        rows = (await session.execute(query.limit(200))).scalars().all()
        if rows:
            last = rows[-1]
            if last.created_at is not None:
                self._last_order_cursor = (_to_utc(last.created_at), str(last.id))
        return rows

    async def _load_updated_trader_orders(self, session) -> list[TraderOrder]:
        query = (
            select(TraderOrder)
            .where(TraderOrder.updated_at.is_not(None))
            .order_by(TraderOrder.updated_at.asc(), TraderOrder.id.asc())
        )

        if self._last_order_update_cursor is not None:
            cursor_ts, cursor_id = self._last_order_update_cursor
            query = query.where(
                or_(
                    TraderOrder.updated_at > cursor_ts.replace(tzinfo=None),
                    and_(
                        TraderOrder.updated_at == cursor_ts.replace(tzinfo=None),
                        TraderOrder.id > cursor_id,
                    ),
                )
            )

        rows = (await session.execute(query.limit(300))).scalars().all()
        if rows:
            last = rows[-1]
            if last.updated_at is not None:
                self._last_order_update_cursor = (_to_utc(last.updated_at), str(last.id))
        return rows

    async def _load_new_trader_events(self, session) -> list[TraderEvent]:
        query = select(TraderEvent).order_by(
            TraderEvent.created_at.asc(),
            TraderEvent.id.asc(),
        )

        if self._last_event_cursor is not None:
            cursor_ts, cursor_id = self._last_event_cursor
            query = query.where(
                or_(
                    TraderEvent.created_at > cursor_ts.replace(tzinfo=None),
                    and_(
                        TraderEvent.created_at == cursor_ts.replace(tzinfo=None),
                        TraderEvent.id > cursor_id,
                    ),
                )
            )

        rows = (await session.execute(query.limit(200))).scalars().all()
        if rows:
            last = rows[-1]
            if last.created_at is not None:
                self._last_event_cursor = (_to_utc(last.created_at), str(last.id))
        return rows

    @staticmethod
    def _is_issue_event(event: TraderEvent) -> bool:
        severity = str(event.severity or "").strip().lower()
        if severity in {"warn", "warning", "error", "critical"}:
            return True

        event_type = str(event.event_type or "").strip().lower()
        payload = event.payload_json or {}

        if event_type == "kill_switch":
            return bool(payload.get("enabled", True))

        if event_type == "live_preflight":
            return str(payload.get("status") or "").lower() == "failed"

        return False

    @staticmethod
    def _is_issue_order(order: TraderOrder) -> bool:
        status = str(order.status or "").strip().lower()
        if status in ISSUE_ORDER_STATUSES:
            return True
        if order.error_message:
            return True
        return False

    @staticmethod
    def _is_realized_order(order: TraderOrder) -> bool:
        return str(order.status or "").strip().lower() in REALIZED_ORDER_STATUSES

    async def _load_trader_name_map(
        self,
        session,
        trader_ids: set[str],
    ) -> dict[str, str]:
        if not trader_ids:
            return {}
        rows = (await session.execute(select(Trader.id, Trader.name).where(Trader.id.in_(tuple(trader_ids))))).all()
        return {str(row[0]): str(row[1]) for row in rows}

    async def _send_issue_alerts(
        self,
        *,
        session,
        events: list[TraderEvent],
        orders: list[TraderOrder],
        snapshot: Optional[TraderOrchestratorSnapshot],
    ) -> None:
        trader_ids = {str(item.trader_id) for item in list(events) + list(orders) if getattr(item, "trader_id", None)}
        trader_names = await self._load_trader_name_map(session, trader_ids)

        for event in events:
            if not self._is_issue_event(event):
                continue
            await self._enqueue(self._format_issue_event_message(event=event, trader_names=trader_names))

        for order in orders:
            if not self._is_issue_order(order):
                continue
            await self._enqueue(self._format_issue_order_message(order=order, trader_names=trader_names))

        snapshot_error = None
        if snapshot is not None and snapshot.last_error:
            snapshot_error = str(snapshot.last_error).strip()

        if snapshot_error != self._last_snapshot_error:
            if snapshot_error:
                await self._enqueue(self._format_worker_issue_message(snapshot_error))
            self._last_snapshot_error = snapshot_error

    async def _send_order_activity_alert(
        self,
        *,
        session,
        orders: list[TraderOrder],
        mode: str,
    ) -> None:
        regular_orders = [row for row in orders if not self._is_issue_order(row)]
        if not regular_orders:
            return
        mode_label = _mode_label_from_orders(regular_orders, _mode_label(mode))

        status_counts: dict[str, int] = defaultdict(int)
        status_notional: dict[str, float] = defaultdict(float)
        notional = 0.0
        trader_counts: dict[str, int] = defaultdict(int)
        trader_notional: dict[str, float] = defaultdict(float)
        trader_ids: set[str] = set()

        for row in regular_orders:
            status = str(row.status or "unknown").lower()
            order_notional = float(row.notional_usd or 0.0)

            status_counts[status] += 1
            status_notional[status] += order_notional
            notional += order_notional
            if row.trader_id:
                trader_id = str(row.trader_id)
                trader_ids.add(trader_id)
                trader_counts[trader_id] += 1
                trader_notional[trader_id] += order_notional

        trader_names = await self._load_trader_name_map(session, trader_ids)
        status_parts = [
            f"{status.upper()} {status_counts[status]}/{_format_money(status_notional[status])}"
            for status in sorted(status_counts.keys())
        ]

        top_traders = sorted(
            trader_counts.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:3]
        trader_parts = [
            f"{_truncate_text(trader_names.get(tid, tid[:8]), 12)} {count}/{_format_money(trader_notional.get(tid, 0.0))}"
            for tid, count in top_traders
        ]

        lines = [
            f"📈 {_bold('Autotrader Orders')} · {_escape_md(mode_label)}",
            f"{_bold('New:')} {_escape_md(str(len(regular_orders)))} · {_bold('Notional:')} {_escape_md(_format_money(notional))}",
            f"{_bold('Mix:')} {_escape_md(_compact_join(status_parts, max_items=3))}",
        ]
        if trader_parts:
            lines.append(f"{_bold('Top:')} {_escape_md(_compact_join(trader_parts, max_items=2))}")

        await self._enqueue("\n".join(lines))

    async def _send_position_close_alerts(
        self,
        *,
        session,
        orders: list[TraderOrder],
        mode: str,
    ) -> None:
        realized_rows: dict[str, TraderOrder] = {}

        for row in orders:
            if not self._is_realized_order(row):
                continue
            updated_at = _to_utc(row.updated_at) or _to_utc(row.created_at) or utcnow()
            marker = f"{str(row.status or '').lower()}@{updated_at.isoformat()}"
            order_id = str(row.id)
            if self._close_alert_markers.get(order_id) == marker:
                continue
            self._close_alert_markers[order_id] = marker
            realized_rows[order_id] = row

        if not realized_rows:
            return

        while len(self._close_alert_markers) > CLOSE_ALERT_MARKER_LIMIT:
            oldest = next(iter(self._close_alert_markers.keys()), None)
            if oldest is None:
                break
            self._close_alert_markers.pop(oldest, None)

        rows = sorted(
            realized_rows.values(),
            key=lambda row: (_to_utc(row.updated_at) or _to_utc(row.created_at) or utcnow(), str(row.id)),
        )
        mode_label = _mode_label_from_orders(rows, _mode_label(mode))
        trader_ids = {str(row.trader_id) for row in rows if row.trader_id}
        trader_names = await self._load_trader_name_map(session, trader_ids)

        title = "Autotrader Position Close" if len(rows) == 1 else "Autotrader Position Closes"
        realized_won = 0.0
        realized_lost = 0.0
        close_lines: list[str] = []
        trigger_parts: list[str] = []
        display_limit = min(CLOSE_ALERT_BATCH_LIMIT, 5)

        for index, row in enumerate(rows, start=1):
            trader_name = trader_names.get(str(row.trader_id), str(row.trader_id)[:8] if row.trader_id else "Unknown")
            status = str(row.status or "closed").lower()
            status_label = status.replace("_", " ").upper()
            pnl = float(row.actual_profit or 0.0)
            market = str(row.market_question or row.market_id or "unknown market")
            payload = row.payload_json if isinstance(row.payload_json, dict) else {}
            position_close = payload.get("position_close") if isinstance(payload, dict) else {}
            close_trigger = ""
            if isinstance(position_close, dict):
                close_trigger = str(position_close.get("close_trigger") or "").strip()

            if pnl >= 0:
                realized_won += pnl
            else:
                realized_lost += abs(pnl)

            if index <= display_limit:
                status_short = "WIN" if "win" in status else "LOSS" if "loss" in status else _truncate_text(status_label, 10)
                close_lines.append(
                    f"{_escape_md(str(index))}\\) {_escape_md(_truncate_text(trader_name, 13))} · "
                    f"{_escape_md(status_short)} · {_escape_md(_format_signed_money(pnl))}"
                )
                close_lines.append(f"   {_escape_md(_truncate_text(market, 56))}")
            if close_trigger:
                trigger_parts.append(
                    f"{index}:{_truncate_text(close_trigger, 20)}"
                )

        net_realized = realized_won - realized_lost
        lines = [
            f"✅ {_bold(title)} · {_escape_md(mode_label)}",
            (
                f"{_bold('Count:')} {_escape_md(str(len(rows)))} · "
                f"{_bold('Won:')} {_escape_md(_format_money(realized_won))} · "
                f"{_bold('Lost:')} {_escape_md(_format_money(realized_lost))} · "
                f"{_bold('Net:')} {_escape_md(_format_signed_money(net_realized))}"
            ),
        ]
        lines.extend(close_lines)
        if trigger_parts:
            lines.append(f"{_bold('Triggers:')} {_escape_md(_compact_join(trigger_parts, max_items=3))}")

        remaining = len(rows) - display_limit
        if remaining > 0:
            lines.append(f"{_bold('More:')} {_escape_md(str(remaining))} close\\(s\\)")

        await self._enqueue("\n".join(lines))

    async def _maybe_send_timeline_summary(
        self,
        *,
        session,
        control: Optional[TraderOrchestratorControl],
        snapshot: Optional[TraderOrchestratorSnapshot],
    ) -> None:
        now = utcnow()
        interval = timedelta(minutes=self._summary_interval_minutes)

        if self._last_summary_at is None:
            self._last_summary_at = now
            return

        if now - self._last_summary_at < interval:
            return

        start = self._last_summary_at
        end = now

        summary_message = await self._build_timeline_summary(
            session=session,
            start=start,
            end=end,
            control=control,
            snapshot=snapshot,
        )
        await self._enqueue(summary_message)
        self._last_summary_at = now

    async def _build_timeline_summary(
        self,
        *,
        session,
        start: datetime,
        end: datetime,
        control: Optional[TraderOrchestratorControl],
        snapshot: Optional[TraderOrchestratorSnapshot],
        title: str = "Autotrader Timeline",
    ) -> str:
        start_naive = _to_utc(start).replace(tzinfo=None)
        end_naive = _to_utc(end).replace(tzinfo=None)

        decision_rows = (
            await session.execute(
                select(TraderDecision.decision, func.count(TraderDecision.id))
                .where(
                    TraderDecision.created_at >= start_naive,
                    TraderDecision.created_at < end_naive,
                )
                .group_by(TraderDecision.decision)
            )
        ).all()
        decision_counts = {str(row[0] or "unknown").lower(): int(row[1] or 0) for row in decision_rows}

        order_rows = (
            await session.execute(
                select(
                    TraderOrder.status,
                    func.count(TraderOrder.id),
                    func.coalesce(func.sum(TraderOrder.notional_usd), 0.0),
                )
                .where(
                    TraderOrder.created_at >= start_naive,
                    TraderOrder.created_at < end_naive,
                )
                .group_by(TraderOrder.status)
            )
        ).all()
        order_counts = {str(row[0] or "unknown").lower(): int(row[1] or 0) for row in order_rows}
        total_notional = float(sum(float(row[2] or 0.0) for row in order_rows))

        realized_rows = (
            await session.execute(
                select(
                    TraderOrder.status,
                    func.count(TraderOrder.id),
                    func.coalesce(func.sum(TraderOrder.actual_profit), 0.0),
                )
                .where(
                    TraderOrder.updated_at >= start_naive,
                    TraderOrder.updated_at < end_naive,
                    TraderOrder.status.in_(tuple(REALIZED_ORDER_STATUSES)),
                )
                .group_by(TraderOrder.status)
            )
        ).all()

        realized_won = 0.0
        realized_lost = 0.0
        for row in realized_rows:
            pnl = float(row[2] or 0.0)
            if pnl >= 0:
                realized_won += pnl
            else:
                realized_lost += abs(pnl)
        realized_net = realized_won - realized_lost

        issue_count = int(
            (
                await session.execute(
                    select(func.count(TraderEvent.id)).where(
                        TraderEvent.created_at >= start_naive,
                        TraderEvent.created_at < end_naive,
                        func.lower(TraderEvent.severity).in_(("warn", "warning", "error", "critical")),
                    )
                )
            ).scalar()
            or 0
        )

        mode = _mode_label(getattr(control, "mode", "shadow"))
        traders_running = int(getattr(snapshot, "traders_running", 0) if snapshot else 0)
        traders_total = int(getattr(snapshot, "traders_total", 0) if snapshot else 0)

        decision_parts = [
            f"{key.upper()} {decision_counts[key]}"
            for key in sorted(decision_counts.keys(), key=lambda name: (-decision_counts[name], name))
        ]
        order_parts = [
            f"{str(row[0] or 'unknown').upper()} {int(row[1] or 0)}/{_format_money(float(row[2] or 0.0))}"
            for row in sorted(order_rows, key=lambda item: (-int(item[1] or 0), str(item[0] or "unknown")))
        ]
        realized_parts = [
            f"{str(row[0] or 'unknown').upper()} {int(row[1] or 0)}/{_format_signed_money(float(row[2] or 0.0))}"
            for row in sorted(realized_rows, key=lambda item: (-int(item[1] or 0), str(item[0] or "unknown")))
        ]

        lines = [
            f"🕒 {_bold(_escape_md(title))}",
            f"{_bold('Window:')} {_escape_md(_to_utc(start).strftime('%Y-%m-%d %H:%M'))} -> {_escape_md(_to_utc(end).strftime('%Y-%m-%d %H:%M'))} UTC",
            f"{_bold('Mode:')} {_escape_md(mode)} · {_bold('Traders:')} {_escape_md(str(traders_running))}/{_escape_md(str(traders_total))}",
            f"{_bold('Notional:')} {_escape_md(_format_money(total_notional))} · {_bold('Issues:')} {_escape_md(str(issue_count))}",
            (
                f"{_bold('Won:')} {_escape_md(_format_money(realized_won))} · "
                f"{_bold('Lost:')} {_escape_md(_format_money(realized_lost))} · "
                f"{_bold('Net:')} {_escape_md(_format_signed_money(realized_net))}"
            ),
        ]

        if decision_counts:
            lines.append(f"{_bold('Decisions:')} {_escape_md(_compact_join(decision_parts, max_items=4))}")
        if order_counts:
            lines.append(f"{_bold('Orders:')} {_escape_md(_compact_join(order_parts, max_items=4))}")
        if realized_rows:
            lines.append(f"{_bold('Realized:')} {_escape_md(_compact_join(realized_parts, max_items=4))}")

        if self._summary_per_trader:
            trader_lines = await self._build_per_trader_summary_lines(
                session=session,
                start_naive=start_naive,
                end_naive=end_naive,
            )
            if trader_lines:
                lines.append(_bold("Per Trader:"))
                lines.extend(trader_lines)

        return "\n".join(lines)

    async def _build_per_trader_summary_lines(
        self,
        *,
        session,
        start_naive: datetime,
        end_naive: datetime,
    ) -> list[str]:
        order_rows = (
            await session.execute(
                select(
                    TraderOrder.trader_id,
                    func.count(TraderOrder.id),
                    func.coalesce(func.sum(TraderOrder.notional_usd), 0.0),
                )
                .where(
                    TraderOrder.created_at >= start_naive,
                    TraderOrder.created_at < end_naive,
                )
                .group_by(TraderOrder.trader_id)
            )
        ).all()

        decision_rows = (
            await session.execute(
                select(
                    TraderDecision.trader_id,
                    TraderDecision.decision,
                    func.count(TraderDecision.id),
                )
                .where(
                    TraderDecision.created_at >= start_naive,
                    TraderDecision.created_at < end_naive,
                )
                .group_by(TraderDecision.trader_id, TraderDecision.decision)
            )
        ).all()

        issue_rows = (
            await session.execute(
                select(TraderEvent.trader_id, func.count(TraderEvent.id))
                .where(
                    TraderEvent.created_at >= start_naive,
                    TraderEvent.created_at < end_naive,
                    func.lower(TraderEvent.severity).in_(("warn", "warning", "error", "critical")),
                )
                .group_by(TraderEvent.trader_id)
            )
        ).all()

        trader_ids: set[str] = set()
        for row in order_rows:
            if row[0]:
                trader_ids.add(str(row[0]))
        for row in decision_rows:
            if row[0]:
                trader_ids.add(str(row[0]))
        for row in issue_rows:
            if row[0]:
                trader_ids.add(str(row[0]))

        if not trader_ids:
            return []

        names = await self._load_trader_name_map(session, trader_ids)

        decision_map: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for row in decision_rows:
            trader_id = str(row[0]) if row[0] else "unknown"
            decision = str(row[1] or "unknown").lower()
            decision_map[trader_id][decision] += int(row[2] or 0)

        issue_map = {(str(row[0]) if row[0] else "unknown"): int(row[1] or 0) for row in issue_rows}

        lines: list[str] = []
        sorted_orders = sorted(order_rows, key=lambda row: int(row[1] or 0), reverse=True)

        for row in sorted_orders[:12]:
            trader_id = str(row[0]) if row[0] else "unknown"
            order_count = int(row[1] or 0)
            notional = float(row[2] or 0.0)
            decision_counts = decision_map.get(trader_id, {})
            selected_count = int(decision_counts.get("selected", 0))
            blocked_count = int(decision_counts.get("blocked", 0))
            skipped_count = int(decision_counts.get("skipped", 0))
            issues = int(issue_map.get(trader_id, 0))

            trader_name = names.get(trader_id, trader_id[:8])
            line = (
                f"- {_escape_md(trader_name)}: "
                f"orders {_escape_md(str(order_count))}, "
                f"notional {_escape_md(_format_money(notional))}, "
                f"selected {_escape_md(str(selected_count))}, "
                f"blocked {_escape_md(str(blocked_count))}, "
                f"skipped {_escape_md(str(skipped_count))}, "
                f"issues {_escape_md(str(issues))}"
            )
            lines.append(line)

        return lines

    @staticmethod
    def _format_utc_timestamp(value: Any) -> str:
        if isinstance(value, datetime):
            dt = _to_utc(value)
            if dt is not None:
                return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} UTC"
        if isinstance(value, str) and value.strip():
            return value.strip()
        return "n/a"

    def _format_runtime_state_message(
        self,
        *,
        title: str,
        mode: str,
        traders_running: int,
        traders_total: int,
    ) -> str:
        return "\n".join(
            [
                f"ℹ️ {_bold(_escape_md(title))}",
                "",
                f"{_bold('Mode:')} {_escape_md(mode)}",
                f"{_bold('Traders Running:')} {_escape_md(str(traders_running))}/{_escape_md(str(traders_total))}",
            ]
        )

    def _format_issue_event_message(
        self,
        *,
        event: TraderEvent,
        trader_names: dict[str, str],
    ) -> str:
        severity = str(event.severity or "warn").upper()
        trader_name = "System"
        if event.trader_id:
            trader_name = trader_names.get(str(event.trader_id), str(event.trader_id)[:8])

        created_at = _to_utc(event.created_at) or utcnow()
        message = str(event.message or "No details provided")

        lines = [
            f"⚠️ {_bold('Autotrader Issue')}",
            "",
            f"{_bold('Severity:')} {_escape_md(severity)}",
            f"{_bold('Type:')} {_escape_md(str(event.event_type or 'event'))}",
            f"{_bold('Trader:')} {_escape_md(trader_name)}",
            f"{_bold('Message:')} {_escape_md(message[:400])}",
            f"{_bold('Time:')} {_escape_md(created_at.strftime('%Y-%m-%d %H:%M:%S'))} UTC",
        ]
        return "\n".join(lines)

    def _format_issue_order_message(
        self,
        *,
        order: TraderOrder,
        trader_names: dict[str, str],
    ) -> str:
        trader_name = "Unknown"
        if order.trader_id:
            trader_name = trader_names.get(str(order.trader_id), str(order.trader_id)[:8])

        status = str(order.status or "unknown").upper()
        created_at = _to_utc(order.created_at) or utcnow()
        reason = str(order.error_message or order.reason or "Order failure")
        market_question = str(order.market_question or order.market_id or "unknown market")

        lines = [
            f"❌ {_bold('Autotrader Order Issue')}",
            "",
            f"{_bold('Trader:')} {_escape_md(trader_name)}",
            f"{_bold('Status:')} {_escape_md(status)}",
            f"{_bold('Market:')} {_escape_md(market_question[:180])}",
            f"{_bold('Reason:')} {_escape_md(reason[:400])}",
            f"{_bold('Time:')} {_escape_md(created_at.strftime('%Y-%m-%d %H:%M:%S'))} UTC",
        ]
        return "\n".join(lines)

    def _format_worker_issue_message(self, error_text: str) -> str:
        lines = [
            f"❌ {_bold('Autotrader Worker Error')}",
            "",
            f"{_bold('Error:')} {_escape_md(error_text[:700])}",
            f"{_bold('Time:')} {_escape_md(utcnow().strftime('%Y-%m-%d %H:%M:%S'))} UTC",
        ]
        return "\n".join(lines)

    async def _prime_telegram_update_offset(self) -> None:
        if not self._bot_token or not self._chat_id:
            self._telegram_update_offset = None
            return
        for _ in range(TELEGRAM_UPDATE_DRAIN_LIMIT):
            updates = await self._fetch_telegram_updates(timeout_seconds=0)
            if not updates:
                break

    async def _fetch_telegram_updates(self, *, timeout_seconds: int) -> list[dict[str, Any]]:
        if not self._bot_token:
            return []
        if not self._http_client:
            self._http_client = httpx.AsyncClient(timeout=15.0)

        url = f"{TELEGRAM_API_BASE}/bot{self._bot_token}/getUpdates"
        payload: dict[str, Any] = {
            "timeout": max(0, int(timeout_seconds)),
            "limit": 100,
            "allowed_updates": ["message"],
        }
        if self._telegram_update_offset is not None:
            payload["offset"] = int(self._telegram_update_offset)

        try:
            response = await self._http_client.post(url, json=payload, timeout=timeout_seconds + 10)
            if response.status_code == 429:
                retry_after = 5
                try:
                    body = response.json()
                    retry_after = int(body.get("parameters", {}).get("retry_after", 5) or 5)
                except Exception:
                    retry_after = 5
                logger.warning("Telegram getUpdates rate limited", retry_after=retry_after)
                await asyncio.sleep(max(1, retry_after))
                return []

            if response.status_code != 200:
                logger.warning("Telegram getUpdates failed", status=response.status_code, body=response.text[:250])
                return []

            body = response.json()
            if not isinstance(body, dict) or not body.get("ok"):
                logger.warning("Telegram getUpdates returned invalid payload")
                return []

            result = body.get("result")
            if not isinstance(result, list):
                return []

            updates = [item for item in result if isinstance(item, dict)]
            if updates:
                update_ids = [int(item.get("update_id")) for item in updates if isinstance(item.get("update_id"), int)]
                if update_ids:
                    self._telegram_update_offset = max(update_ids) + 1
            return updates
        except asyncio.CancelledError:
            raise
        except httpx.TimeoutException:
            return []
        except Exception as exc:
            logger.error("Failed to poll Telegram updates", exc_info=exc)
            return []

    async def _telegram_command_loop(self) -> None:
        while self._running:
            try:
                now_monotonic = time.monotonic()
                if now_monotonic - self._last_settings_reload_monotonic >= SETTINGS_REFRESH_SECONDS:
                    await self._load_settings()
                    self._last_settings_reload_monotonic = now_monotonic

                if not self._bot_token or not self._chat_id:
                    await asyncio.sleep(COMMAND_LOOP_IDLE_SECONDS)
                    continue

                updates = await self._fetch_telegram_updates(timeout_seconds=COMMAND_POLL_TIMEOUT_SECONDS)
                for update in updates:
                    await self._process_telegram_update(update)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error("Telegram command loop failed", exc_info=exc)
                await asyncio.sleep(COMMAND_LOOP_IDLE_SECONDS)

    async def _process_telegram_update(self, update: dict[str, Any]) -> None:
        message = update.get("message")
        if not isinstance(message, dict):
            return

        chat = message.get("chat")
        if not isinstance(chat, dict):
            return
        chat_id = str(chat.get("id") or "").strip()
        if not chat_id:
            return

        expected_chat_id = str(self._chat_id or "").strip()
        if expected_chat_id and chat_id != expected_chat_id:
            logger.warning("Ignoring Telegram command from unexpected chat", chat_id=chat_id)
            return

        text = str(message.get("text") or "").strip()
        if not text:
            return

        operator = "telegram"
        sender = message.get("from")
        if isinstance(sender, dict):
            username = str(sender.get("username") or "").strip()
            sender_id = str(sender.get("id") or "").strip()
            if username:
                operator = f"telegram:{username}"
            elif sender_id:
                operator = f"telegram:{sender_id}"

        try:
            response = await self._handle_telegram_command(text=text, operator=operator)
        except Exception as exc:
            logger.error("Telegram command processing failed", exc_info=exc)
            response = "❌ Telegram command failed\\.\nPlease retry or check backend logs\\."
        if response:
            await self._enqueue(response)

    async def _handle_telegram_command(self, *, text: str, operator: str) -> str:
        stripped = str(text or "").strip()
        if not stripped:
            return ""

        parts = stripped.split()
        head = str(parts[0] or "").strip()
        args = [str(item or "").strip() for item in parts[1:] if str(item or "").strip()]

        if head.startswith("/"):
            command = head.split("@", 1)[0].lower()
        else:
            command = head.lower()

        if command in {"/help", "/commands", "help", "commands"}:
            return self._telegram_help_message()
        if command in {"/status", "status"}:
            return await self._telegram_status_message()
        if command in {"/performance", "/pnl", "performance", "pnl"}:
            hours_arg = args[0] if args else None
            return await self._telegram_performance_message(hours_arg=hours_arg)
        if command in {"/stop", "stop", "/off", "off", "/disable", "disable"}:
            return await self._telegram_stop_autotrader(operator=operator)
        if command in {"/start", "start", "/on", "on", "/enable", "enable"}:
            mode_arg = self._parse_telegram_start_args(args)
            return await self._telegram_start_autotrader(
                mode_arg=mode_arg,
                operator=operator,
            )
        if command in {"/autotrader", "autotrader"}:
            if not args:
                return "Usage: `/autotrader on [shadow|live]` or `/autotrader off`\\."
            action = args[0].strip().lower()
            action_args = args[1:]
            if action in {"on", "start", "enable", "enabled", "resume"}:
                mode_arg = self._parse_telegram_start_args(action_args)
                return await self._telegram_start_autotrader(
                    mode_arg=mode_arg,
                    operator=operator,
                )
            if action in {"off", "stop", "disable", "disabled", "pause"}:
                return await self._telegram_stop_autotrader(operator=operator)
            return "Usage: `/autotrader on [shadow|live]` or `/autotrader off`\\."
        if command in {"/killswitch", "/kill", "killswitch", "kill"}:
            if not args:
                return "Usage: `/killswitch on` or `/killswitch off`\\."
            toggle = args[0].strip().lower()
            if toggle in {"on", "1", "true", "enable", "enabled"}:
                return await self._telegram_set_kill_switch(enabled=True, operator=operator)
            if toggle in {"off", "0", "false", "disable", "disabled"}:
                return await self._telegram_set_kill_switch(enabled=False, operator=operator)
            return "Usage: `/killswitch on` or `/killswitch off`\\."

        return "Unknown command\\.\nUse `/help` for available Telegram commands\\."

    @staticmethod
    def _parse_telegram_start_args(args: list[str]) -> Optional[str]:
        mode_arg: Optional[str] = None
        if args:
            first = args[0].lower()
            if first in {"shadow", "live"}:
                mode_arg = first
        return mode_arg

    @staticmethod
    def _telegram_help_message() -> str:
        return "\n".join(
            [
                f"🤖 {_bold('Homerun Telegram Commands')}",
                "",
                "`/status` \\- show orchestrator state and core metrics",
                "`/performance [hours]` \\- timeline summary for the last N hours \\(default 24\\)",
                "`/autotrader on|off` \\- quick autotrader power toggle",
                "`/start [shadow|live]` \\- start autotrader",
                "`/stop` \\- stop autotrader",
                "`/killswitch on|off` \\- toggle orchestrator kill switch",
            ]
        )

    async def _telegram_status_message(self) -> str:
        async with AsyncSessionLocal() as session:
            control = await read_orchestrator_control(session)
            snapshot = await read_orchestrator_snapshot(session)
            cutoff = (utcnow() - timedelta(hours=24)).replace(tzinfo=None)
            realized_rows = (
                await session.execute(
                    select(
                        TraderOrder.status,
                        func.coalesce(func.sum(TraderOrder.actual_profit), 0.0),
                    )
                    .where(
                        TraderOrder.updated_at >= cutoff,
                        TraderOrder.status.in_(tuple(REALIZED_ORDER_STATUSES)),
                    )
                    .group_by(TraderOrder.status)
                )
            ).all()

        realized_won = 0.0
        realized_lost = 0.0
        for row in realized_rows:
            pnl = float(row[1] or 0.0)
            if pnl >= 0:
                realized_won += pnl
            else:
                realized_lost += abs(pnl)
        realized_24h = realized_won - realized_lost

        mode = _mode_label(control.get("mode", "shadow"))
        enabled = bool(control.get("is_enabled", False))
        paused = bool(control.get("is_paused", True))
        running = bool(snapshot.get("running", False))
        state = "ACTIVE" if enabled and not paused else "PAUSED"
        kill_switch = bool(control.get("kill_switch", False))
        traders_running = int(snapshot.get("traders_running", 0) or 0)
        traders_total = int(snapshot.get("traders_total", 0) or 0)
        decisions_count = int(snapshot.get("decisions_count", 0) or 0)
        orders_count = int(snapshot.get("orders_count", 0) or 0)
        open_orders = int(snapshot.get("open_orders", 0) or 0)
        daily_pnl = float(snapshot.get("daily_pnl", 0.0) or 0.0)
        last_run = self._format_utc_timestamp(snapshot.get("last_run_at"))
        updated_at = self._format_utc_timestamp(snapshot.get("updated_at"))

        lines = [
            f"📊 {_bold('Autotrader Status')} · {_escape_md(state)}\\/{_escape_md('running' if running else 'idle')} · {_escape_md(mode)}",
            f"{_bold('Kill:')} {_escape_md('ON' if kill_switch else 'OFF')} · {_bold('Traders:')} {_escape_md(str(traders_running))}/{_escape_md(str(traders_total))}",
            f"{_bold('Decisions:')} {_escape_md(str(decisions_count))} · {_bold('Orders:')} {_escape_md(str(orders_count))} \\({_escape_md(str(open_orders))} open\\)",
            f"{_bold('Daily:')} {_escape_md(_format_signed_money(daily_pnl))} · {_bold('24h Won:')} {_escape_md(_format_money(realized_won))}",
            f"{_bold('24h Lost:')} {_escape_md(_format_money(realized_lost))} · {_bold('24h Net:')} {_escape_md(_format_signed_money(realized_24h))}",
            f"{_bold('Last Run:')} {_escape_md(last_run)}",
            f"{_bold('Snapshot:')} {_escape_md(updated_at)}",
        ]
        return "\n".join(lines)

    async def _telegram_performance_message(self, *, hours_arg: Optional[str]) -> str:
        hours = _clamp_int(hours_arg, 1, 168, 24)
        end = utcnow()
        start = end - timedelta(hours=hours)

        async with AsyncSessionLocal() as session:
            control_row = await session.get(TraderOrchestratorControl, "default")
            snapshot_row = await session.get(TraderOrchestratorSnapshot, "latest")
            return await self._build_timeline_summary(
                session=session,
                start=start,
                end=end,
                control=control_row,
                snapshot=snapshot_row,
                title=f"Autotrader Performance ({hours}h)",
            )

    async def _telegram_start_autotrader(
        self,
        *,
        mode_arg: Optional[str],
        operator: str,
    ) -> str:
        if global_pause_state.is_paused:
            return "❌ Global pause is active\\.\nResume workers before starting autotrader\\."

        mode = str(mode_arg or "").strip().lower()
        if mode and mode not in {"shadow", "live"}:
            return "❌ Invalid mode\\.\nUse `shadow` or `live`\\."

        async with AsyncSessionLocal() as session:
            control_before = await read_orchestrator_control(session)
            selected_mode = mode or str(control_before.get("mode") or "shadow").strip().lower() or "shadow"
            if selected_mode not in {"shadow", "live"}:
                selected_mode = "shadow"
            if selected_mode == "live":
                preflight = await create_live_preflight(
                    session,
                    requested_mode="live",
                    requested_by=operator,
                )
                if str(preflight.get("status") or "").strip().lower() != "passed":
                    failed_checks = preflight.get("failed_checks") or []
                    failed_ids = [
                        str(check.get("id") or "").strip()
                        for check in failed_checks
                        if isinstance(check, dict) and str(check.get("id") or "").strip()
                    ]
                    failed_summary = ", ".join(failed_ids[:6]) if failed_ids else "preflight checks failed"
                    return (
                        "❌ Live preflight failed\\.\n"
                        f"Checks: {_escape_md(failed_summary)}\\.\n"
                        "Review live setup in the UI and retry `/autotrader on live`\\."
                    )

                arm_response = await arm_live_start(
                    session,
                    preflight_id=str(preflight.get("preflight_id") or ""),
                    ttl_seconds=300,
                    requested_by=operator,
                )
                arm_token = str(arm_response.get("arm_token") or "").strip()
                if not arm_token:
                    return "❌ Live start failed: arm token was not issued\\."
                if not await consume_live_arm_token(session, arm_token):
                    return "❌ Live start failed: arm token expired\\.\nRetry `/autotrader on live`\\."

                control = await update_orchestrator_control(
                    session,
                    is_enabled=True,
                    is_paused=False,
                    mode="live",
                    requested_run_at=utcnow(),
                )
                await write_orchestrator_snapshot(
                    session,
                    running=False,
                    enabled=True,
                    current_activity="Live start command queued from Telegram",
                    interval_seconds=int(
                        control.get("run_interval_seconds") or ORCHESTRATOR_DEFAULT_RUN_INTERVAL_SECONDS
                    ),
                )
                await create_trader_event(
                    session,
                    event_type="live_started",
                    source="telegram",
                    operator=operator,
                    message="Live trading started via Telegram",
                    payload={
                        "mode": "live",
                        "preflight_id": preflight.get("preflight_id"),
                    },
                )
                return "✅ Autotrader start queued\\.\nMode: LIVE"

            control = await update_orchestrator_control(
                session,
                is_enabled=True,
                is_paused=False,
                mode=selected_mode,
                requested_run_at=utcnow(),
            )
            await write_orchestrator_snapshot(
                session,
                running=False,
                enabled=True,
                current_activity="Start command queued from Telegram",
                interval_seconds=int(
                    control.get("run_interval_seconds") or ORCHESTRATOR_DEFAULT_RUN_INTERVAL_SECONDS
                ),
            )
            await create_trader_event(
                session,
                event_type="started",
                source="telegram",
                operator=operator,
                message=f"Trader orchestrator started in {selected_mode} mode via Telegram",
                payload={
                    "mode": selected_mode,
                },
            )

        return f"✅ Autotrader start queued\\.\nMode: {_escape_md(selected_mode.upper())}"

    async def _telegram_stop_autotrader(self, *, operator: str) -> str:
        async with AsyncSessionLocal() as session:
            control = await update_orchestrator_control(
                session,
                is_enabled=False,
                is_paused=True,
                requested_run_at=None,
            )
            await write_orchestrator_snapshot(
                session,
                running=False,
                enabled=False,
                current_activity="Manual stop requested from Telegram",
                interval_seconds=int(
                    control.get("run_interval_seconds") or ORCHESTRATOR_DEFAULT_RUN_INTERVAL_SECONDS
                ),
            )
            await create_trader_event(
                session,
                event_type="stopped",
                source="telegram",
                operator=operator,
                message="Trader orchestrator stopped via Telegram",
            )
        return "🛑 Autotrader stopped\\."

    async def _telegram_set_kill_switch(self, *, enabled: bool, operator: str) -> str:
        async with AsyncSessionLocal() as session:
            await update_orchestrator_control(session, kill_switch=bool(enabled))
            await create_trader_event(
                session,
                event_type="kill_switch",
                severity="warn" if enabled else "info",
                source="telegram",
                operator=operator,
                message="Kill switch updated via Telegram",
                payload={"enabled": bool(enabled)},
            )
        if enabled:
            return "⛔ Kill switch enabled\\."
        return "✅ Kill switch disabled\\."

    def _can_send_now(self) -> bool:
        now = time.monotonic()
        while self._send_timestamps and self._send_timestamps[0] < now - 60:
            self._send_timestamps.popleft()
        return len(self._send_timestamps) < MAX_MESSAGES_PER_MINUTE

    def _record_send(self) -> None:
        self._send_timestamps.append(time.monotonic())

    async def _enqueue(self, text: str) -> None:
        if not self._bot_token or not self._chat_id:
            return
        if not text:
            return

        body = text.strip()
        if len(body) <= 3900:
            await self._message_queue.put(body)
            return

        lines = body.splitlines()
        chunk = ""
        for line in lines:
            candidate = f"{chunk}\n{line}" if chunk else line
            if len(candidate) <= 3900:
                chunk = candidate
                continue
            if chunk:
                await self._message_queue.put(chunk)
            chunk = line[:3900]
        if chunk:
            await self._message_queue.put(chunk)

    async def _queue_worker(self) -> None:
        while self._running:
            try:
                try:
                    message = await asyncio.wait_for(self._message_queue.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    continue

                while not self._can_send_now():
                    await asyncio.sleep(1.0)
                    if not self._running:
                        return

                await self._send_telegram(message)
                self._record_send()

            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error("Queue worker error", exc_info=exc)
                await asyncio.sleep(1.0)

    async def _send_telegram(self, text: str) -> bool:
        if not self._bot_token or not self._chat_id:
            logger.debug("Telegram credentials not configured, skipping message")
            return False

        if not self._http_client:
            self._http_client = httpx.AsyncClient(timeout=15.0)

        url = f"{TELEGRAM_API_BASE}/bot{self._bot_token}/sendMessage"
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": True,
        }

        try:
            resp = await self._http_client.post(url, json=payload)
            if resp.status_code == 200:
                logger.debug("Telegram message sent successfully")
                return True

            if resp.status_code == 429:
                body = resp.json()
                retry_after = body.get("parameters", {}).get("retry_after", 5)
                logger.warning("Telegram rate limited, will retry", retry_after=retry_after)
                await asyncio.sleep(retry_after)
                await self._message_queue.put(text)
                return False

            logger.warning(
                "Telegram API error",
                status=resp.status_code,
                body=resp.text[:300],
            )
            return False

        except httpx.TimeoutException:
            logger.warning("Telegram API request timed out")
            return False
        except Exception as exc:
            logger.error("Failed to send Telegram message", exc_info=exc)
            return False

    async def send_test_message(self) -> bool:
        """Send an immediate test message using currently stored settings."""
        await self._load_settings()
        if not self._bot_token or not self._chat_id:
            return False

        text = (
            f"✅ {_bold('Homerun Autotrader Notifier')}\n\n"
            "Test message received\\.\n"
            "Autotrader Telegram alerts are configured correctly\\."
        )
        return await self._send_telegram(text)


notifier = TelegramNotifier()
