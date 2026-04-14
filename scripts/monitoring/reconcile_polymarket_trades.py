import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from sqlalchemy import func, select

import sys

sys.path.append(os.path.abspath("backend"))

from models.database import AsyncSessionLocal, TraderOrderVerificationEvent


BASE_URL = "http://localhost:8000/api"
DEFAULT_WINDOW_HOURS = 24
MATCH_TIME_WINDOW_SECONDS = 3600


@dataclass
class TradeRecord:
    timestamp: datetime
    token_id: str
    side: str
    price: float | None
    size: float | None
    title: str | None
    condition_id: str | None
    tx_hash: str | None
    raw: dict[str, Any]


@dataclass
class EventRecord:
    event_id: str
    trader_order_id: str
    timestamp: datetime
    token_id: str
    side: str
    price: float | None
    size: float | None
    trade_id: str | None
    tx_hash: str | None


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


async def _load_wallet_address(start_time: datetime, end_time: datetime) -> str | None:
    async with AsyncSessionLocal() as session:
        stmt = (
            select(
                TraderOrderVerificationEvent.execution_wallet_address,
                func.count(TraderOrderVerificationEvent.id),
            )
            .where(TraderOrderVerificationEvent.created_at >= start_time)
            .where(TraderOrderVerificationEvent.created_at <= end_time)
            .group_by(TraderOrderVerificationEvent.execution_wallet_address)
            .order_by(func.count(TraderOrderVerificationEvent.id).desc())
        )
        rows = (await session.execute(stmt)).fetchall()
    for wallet, _count in rows:
        if wallet:
            return str(wallet).lower()
    return None


async def _load_verification_events(
    start_time: datetime, end_time: datetime, wallet: str
) -> list[EventRecord]:
    async with AsyncSessionLocal() as session:
        stmt = (
            select(TraderOrderVerificationEvent)
            .where(TraderOrderVerificationEvent.execution_wallet_address == wallet)
            .where(
                func.coalesce(
                    TraderOrderVerificationEvent.trade_timestamp,
                    TraderOrderVerificationEvent.created_at,
                )
                >= start_time
            )
            .where(
                func.coalesce(
                    TraderOrderVerificationEvent.trade_timestamp,
                    TraderOrderVerificationEvent.created_at,
                )
                <= end_time
            )
        )
        rows = (await session.execute(stmt)).scalars().all()
    events: list[EventRecord] = []
    for row in rows:
        if not row.token_id or not row.side:
            continue
        ts = row.trade_timestamp or row.created_at
        if not isinstance(ts, datetime):
            continue
        events.append(
            EventRecord(
                event_id=str(row.id),
                trader_order_id=str(row.trader_order_id),
                timestamp=ts,
                token_id=str(row.token_id),
                side=str(row.side).upper(),
                price=_safe_float(row.price),
                size=_safe_float(row.size),
                trade_id=str(row.trade_id) if row.trade_id else None,
                tx_hash=str(row.tx_hash) if row.tx_hash else None,
            )
        )
    return events


def _load_polymarket_trades(wallet: str, start_time: datetime, end_time: datetime) -> list[TradeRecord]:
    resp = requests.get(
        f"{BASE_URL}/wallets/{wallet}/trades",
        params={"limit": 500},
        timeout=60,
    )
    resp.raise_for_status()
    raw_trades = resp.json().get("trades") or []
    trades: list[TradeRecord] = []
    for trade in raw_trades:
        ts = _parse_ts(trade.get("timestamp") or trade.get("createdAt") or trade.get("created_at"))
        if ts is None or ts < start_time or ts > end_time:
            continue
        token_id = trade.get("asset")
        side = trade.get("side")
        if not token_id or not side:
            continue
        trades.append(
            TradeRecord(
                timestamp=ts,
                token_id=str(token_id),
                side=str(side).upper(),
                price=_safe_float(trade.get("price")),
                size=_safe_float(trade.get("size") or trade.get("quantity") or trade.get("amount")),
                title=trade.get("title"),
                condition_id=trade.get("conditionId"),
                tx_hash=trade.get("transactionHash"),
                raw=trade,
            )
        )
    return trades


def _match(trades: list[TradeRecord], events: list[EventRecord]):
    used_events: set[str] = set()
    matches = []

    for trade in sorted(trades, key=lambda t: t.timestamp):
        best = None
        best_score = None
        for event in events:
            if event.event_id in used_events:
                continue
            if event.token_id != trade.token_id:
                continue
            if event.side != trade.side:
                continue
            dt = abs((event.timestamp - trade.timestamp).total_seconds())
            if dt > MATCH_TIME_WINDOW_SECONDS:
                continue
            size_diff = None
            if trade.size is not None and event.size is not None:
                size_diff = abs(trade.size - event.size)
            price_diff = None
            if trade.price is not None and event.price is not None:
                price_diff = abs(trade.price - event.price)
            score = dt
            if size_diff is not None:
                score += size_diff * 60.0
            if price_diff is not None:
                score += price_diff * 300.0
            if best_score is None or score < best_score:
                best_score = score
                best = event
        if best is not None:
            used_events.add(best.event_id)
            matches.append((trade, best, best_score))

    matched_trade_ids = {id(match[0]) for match in matches}
    unmatched_trades = [t for t in trades if id(t) not in matched_trade_ids]
    unmatched_events = [e for e in events if e.event_id not in used_events]
    return matches, unmatched_trades, unmatched_events


async def main():
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=DEFAULT_WINDOW_HOURS)

    wallet = await _load_wallet_address(start_time, end_time)
    if not wallet:
        print("No execution wallet found in verification events.")
        return

    events = await _load_verification_events(start_time, end_time, wallet)
    trades = _load_polymarket_trades(wallet, start_time, end_time)

    matches, unmatched_trades, unmatched_events = _match(trades, events)

    report = {
        "summary": {
            "window_start": start_time.isoformat(),
            "window_end": end_time.isoformat(),
            "wallet_address": wallet,
            "polymarket_trade_count": len(trades),
            "verification_event_count": len(events),
            "matched": len(matches),
            "unmatched_trades": len(unmatched_trades),
            "unmatched_verification_events": len(unmatched_events),
        },
        "matches": [
            {
                "trade": {
                    "timestamp": trade.timestamp.isoformat(),
                    "token_id": trade.token_id,
                    "side": trade.side,
                    "price": trade.price,
                    "size": trade.size,
                    "title": trade.title,
                    "condition_id": trade.condition_id,
                    "tx_hash": trade.tx_hash,
                },
                "event": {
                    "event_id": event.event_id,
                    "trader_order_id": event.trader_order_id,
                    "timestamp": event.timestamp.isoformat(),
                    "token_id": event.token_id,
                    "side": event.side,
                    "price": event.price,
                    "size": event.size,
                    "trade_id": event.trade_id,
                    "tx_hash": event.tx_hash,
                },
                "match_score": score,
            }
            for trade, event, score in matches
        ],
        "unmatched_trades": [
            {
                "timestamp": trade.timestamp.isoformat(),
                "token_id": trade.token_id,
                "side": trade.side,
                "price": trade.price,
                "size": trade.size,
                "title": trade.title,
                "condition_id": trade.condition_id,
                "tx_hash": trade.tx_hash,
            }
            for trade in unmatched_trades
        ],
        "unmatched_verification_events": [
            {
                "event_id": event.event_id,
                "trader_order_id": event.trader_order_id,
                "timestamp": event.timestamp.isoformat(),
                "token_id": event.token_id,
                "side": event.side,
                "price": event.price,
                "size": event.size,
                "trade_id": event.trade_id,
                "tx_hash": event.tx_hash,
            }
            for event in unmatched_events
        ],
    }

    os.makedirs("output", exist_ok=True)
    timestamp = end_time.strftime("%Y%m%d_%H%M%S")
    path = os.path.join("output", f"polymarket_reconciliation_events_{timestamp}.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)

    print(json.dumps(report["summary"], indent=2))
    print(path)


if __name__ == "__main__":
    asyncio.run(main())
