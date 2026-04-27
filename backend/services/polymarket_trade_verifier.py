"""Polymarket-truth verification for TraderOrder realized P&L.

The ONLY legitimate sources of realized P&L are:

  1. A confirmed on-chain trade record from polymarket.get_wallet_trades —
     this includes both bot-initiated SELLs and any manual user sells
     made via the Polymarket UI. The trade record carries the actual
     fill price and a transactionHash that pins the close to a specific
     on-chain event.

  2. A market-resolution payout: when a market settles, winning shares
     pay $1 and losing shares pay $0. This is deterministic and can be
     computed from the market's resolution metadata + our recorded entry
     fill size.

NO other source — wallet aggregate `realizedPnl`, `currentPrice`, etc.
— is acceptable. Inferred P&L conflates manual user trades with bot
fills and produces phantom numbers.

This module:

  * fetches the wallet's recent trades from Polymarket
  * matches sells to TraderOrder rows by (asset_id == token_id) +
    (trade.timestamp > order.entry_anchor) + side == SELL, FIFO by
    timestamp so the earliest sell consumes the earliest open order
  * computes realized_pnl = sum(sell_price * matched_size) -
    matched_cost_basis
  * writes the verified P&L back to the row with
    verification_status = wallet_activity (matched-by-trade) and the
    transactionHash recorded as verification_tx_hash

Orders that cannot be matched to any sell trade AND whose market has
not resolved are left at verification_status = summary_only with
actual_profit = NULL — the aggregation queries already exclude these.

For each row updated, an immutable TraderOrderVerificationEvent is
appended so the lineage is auditable.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.database import TraderOrder
from services.polymarket import polymarket_client
from services.trader_order_verification import (
    TRADER_ORDER_VERIFICATION_DISPUTED,
    TRADER_ORDER_VERIFICATION_VENUE_FILL,
    TRADER_ORDER_VERIFICATION_WALLET_ACTIVITY,
    apply_trader_order_verification,
    append_trader_order_verification_event,
)
from utils.converters import safe_float
from utils.utcnow import utcnow

from utils.logger import get_logger

logger = get_logger("polymarket_trade_verifier")

# A trade record from /data/trades:
#   {
#     "proxyWallet": "0x...",      # wallet address (lowercase)
#     "side": "BUY" | "SELL",
#     "asset": "<erc1155 token id>",
#     "conditionId": "0x...",
#     "size": <float>,
#     "price": <float>,             # actual fill price 0-1
#     "timestamp": <unix seconds>,  # int
#     "transactionHash": "0x...",   # on-chain tx
#     "outcomeIndex": 0 | 1,
#     ...
#   }


_RESOLVED_STATUSES = (
    "resolved",
    "resolved_win",
    "resolved_loss",
    "closed_win",
    "closed_loss",
    "win",
    "loss",
)


def _trade_token_id(trade: dict[str, Any]) -> str:
    return str(trade.get("asset") or trade.get("token_id") or "").strip()


def _trade_side(trade: dict[str, Any]) -> str:
    return str(trade.get("side") or "").strip().upper()


def _trade_timestamp(trade: dict[str, Any]) -> datetime | None:
    ts = trade.get("timestamp")
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).replace(tzinfo=None)
    except (TypeError, ValueError, OSError):
        return None


def _entry_anchor(row: TraderOrder) -> datetime | None:
    """Earliest time we believe this order was on the book.

    We use ``executed_at`` if set, otherwise ``created_at``. A trade
    must have happened at-or-after this anchor to be eligible to close
    this order.
    """
    anchor = row.executed_at or row.created_at
    if anchor is None:
        return None
    if anchor.tzinfo is not None:
        anchor = anchor.astimezone(timezone.utc).replace(tzinfo=None)
    return anchor


def _entry_token_id(row: TraderOrder) -> str:
    """Best-effort token_id from the order's payload."""
    payload = row.payload_json if isinstance(row.payload_json, dict) else {}
    candidates: list[Any] = []
    for key in (
        "token_id",
        "live_token_id",
        "outcome_token_id",
        "asset_id",
    ):
        value = payload.get(key)
        if value:
            candidates.append(value)
    market_payload = payload.get("market") if isinstance(payload, dict) else None
    if isinstance(market_payload, dict):
        for key in ("token_id", "outcome_token_id"):
            value = market_payload.get(key)
            if value:
                candidates.append(value)
    fill = payload.get("live_fill") if isinstance(payload, dict) else None
    if isinstance(fill, dict):
        value = fill.get("token_id")
        if value:
            candidates.append(value)
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text
    return ""


def _entry_fill_size(row: TraderOrder) -> float:
    payload = row.payload_json if isinstance(row.payload_json, dict) else {}
    fill = payload.get("live_fill") if isinstance(payload, dict) else None
    if isinstance(fill, dict):
        size = safe_float(fill.get("filled_size"), None)
        if size is not None and size > 0.0:
            return float(size)
    # Fallback: derive from notional / entry_price.
    notional = safe_float(row.notional_usd, None)
    price = safe_float(row.entry_price, None) or safe_float(row.effective_price, None)
    if notional is not None and notional > 0.0 and price is not None and price > 0.0:
        return float(notional / price)
    return 0.0


def _entry_cost_basis(row: TraderOrder) -> float:
    """USD cost basis for the entry — what we paid to open."""
    payload = row.payload_json if isinstance(row.payload_json, dict) else {}
    fill = payload.get("live_fill") if isinstance(payload, dict) else None
    if isinstance(fill, dict):
        notional = safe_float(fill.get("filled_notional"), None)
        if notional is not None and notional > 0.0:
            return float(notional)
    notional = safe_float(row.notional_usd, None)
    if notional is not None and notional > 0.0:
        return float(notional)
    return 0.0


class _MatchResult:
    __slots__ = (
        "matched_size",
        "matched_proceeds",
        "matched_trades",
        "tx_hashes",
    )

    def __init__(self) -> None:
        self.matched_size: float = 0.0
        self.matched_proceeds: float = 0.0
        self.matched_trades: list[dict[str, Any]] = []
        self.tx_hashes: list[str] = []


def _match_sells_to_order(
    row: TraderOrder,
    candidate_sells: list[dict[str, Any]],
    *,
    consumed_sizes_by_tx: dict[str, float],
) -> _MatchResult:
    """FIFO match SELL trades to this order.

    Each sell trade has a transactionHash and a size. A single sell can
    fill multiple orders (rare — usually 1-1) so we track how much of
    each sell has already been allocated via consumed_sizes_by_tx and
    only consume the remaining portion.
    """
    needed = _entry_fill_size(row)
    result = _MatchResult()
    if needed <= 0.0:
        return result
    anchor = _entry_anchor(row) or datetime.min
    for trade in candidate_sells:
        ts = _trade_timestamp(trade)
        if ts is None or ts < anchor:
            continue
        tx_hash = str(trade.get("transactionHash") or "").strip()
        trade_size = safe_float(trade.get("size"), 0.0) or 0.0
        if trade_size <= 0.0:
            continue
        already_consumed = consumed_sizes_by_tx.get(tx_hash, 0.0)
        available = trade_size - already_consumed
        if available <= 0.0:
            continue
        take = min(available, needed - result.matched_size)
        if take <= 0.0:
            continue
        price = safe_float(trade.get("price"), None)
        if price is None or price < 0.0:
            continue
        result.matched_size += take
        result.matched_proceeds += take * price
        if tx_hash:
            consumed_sizes_by_tx[tx_hash] = already_consumed + take
            result.tx_hashes.append(tx_hash)
        result.matched_trades.append({
            "tx_hash": tx_hash,
            "price": price,
            "size": take,
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else None,
        })
        if result.matched_size >= needed - 1e-9:
            break
    return result


async def verify_orders_against_wallet_trades(
    session: AsyncSession,
    *,
    wallet_address: str,
    order_window_start: datetime | None = None,
    order_ids: Iterable[str] | None = None,
    max_trades: int = 2000,
    commit: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Match TraderOrder closes to actual on-chain SELL trades.

    Args:
        session: open AsyncSession.
        wallet_address: proxy wallet address.
        order_window_start: only re-verify orders updated since this time
            (default: last 7 days). Pass datetime.min to verify all rows.
        order_ids: optional explicit set of order ids to verify.
        max_trades: cap on wallet_trades fetched (default 2000 — covers
            the last ~few weeks of activity for a typical bot).
        commit: whether to commit the session after updates.
        dry_run: if True, compute matches but do not write any changes.

    Returns:
        Summary dict with counts of orders examined / verified /
        unmatched, and totals.
    """
    wallet_lower = str(wallet_address or "").strip().lower()
    if not wallet_lower:
        return {"error": "missing_wallet_address", "examined": 0, "verified": 0, "unmatched": 0}

    try:
        trades_raw = await polymarket_client.get_wallet_trades_paginated(
            wallet_lower, max_trades=max_trades, page_size=500
        )
    except Exception as exc:
        logger.warning(
            "polymarket_trade_verifier: get_wallet_trades failed",
            wallet=wallet_lower,
            exc_info=exc,
        )
        return {
            "error": "wallet_trades_fetch_failed",
            "error_type": type(exc).__name__,
            "examined": 0,
            "verified": 0,
            "unmatched": 0,
        }

    sells_by_token: dict[str, list[dict[str, Any]]] = {}
    for trade in trades_raw or []:
        if not isinstance(trade, dict):
            continue
        if _trade_side(trade) != "SELL":
            continue
        token_id = _trade_token_id(trade)
        if not token_id:
            continue
        sells_by_token.setdefault(token_id, []).append(trade)
    # Sort each bucket FIFO so earliest sells consume earliest orders.
    for trades in sells_by_token.values():
        trades.sort(key=lambda t: (_trade_timestamp(t) or datetime.min))

    consumed_sizes_by_tx: dict[str, float] = {}

    query = (
        select(TraderOrder)
        .where(TraderOrder.mode == "live")
        .where(TraderOrder.status.in_(_RESOLVED_STATUSES))
    )
    if order_ids is not None:
        ids = [str(oid).strip() for oid in order_ids if str(oid).strip()]
        if not ids:
            return {"examined": 0, "verified": 0, "unmatched": 0}
        query = query.where(TraderOrder.id.in_(ids))
    else:
        if order_window_start is None:
            order_window_start = utcnow().replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        query = query.where(TraderOrder.updated_at >= order_window_start)
    query = query.order_by(TraderOrder.executed_at.asc(), TraderOrder.created_at.asc())

    rows = list((await session.execute(query)).scalars().all())

    examined = 0
    verified = 0
    unmatched = 0
    pnl_total_before = 0.0
    pnl_total_after = 0.0

    now = utcnow()

    for row in rows:
        examined += 1
        prior_profit = float(row.actual_profit) if row.actual_profit is not None else 0.0
        token_id = _entry_token_id(row)
        if not token_id:
            unmatched += 1
            continue
        candidates = sells_by_token.get(token_id) or []
        if not candidates:
            unmatched += 1
            continue
        match = _match_sells_to_order(
            row, candidates, consumed_sizes_by_tx=consumed_sizes_by_tx
        )
        if match.matched_size <= 0.0:
            unmatched += 1
            continue
        cost_basis = _entry_cost_basis(row)
        # Allocate cost proportionally to matched size if we couldn't
        # match the full entry (rare — usually we match all of it).
        entry_size = _entry_fill_size(row)
        if entry_size > 0.0:
            allocated_cost = cost_basis * min(1.0, match.matched_size / entry_size)
        else:
            allocated_cost = cost_basis
        verified_pnl = match.matched_proceeds - allocated_cost
        verified += 1
        pnl_total_before += prior_profit
        pnl_total_after += verified_pnl

        if dry_run:
            continue

        row.actual_profit = float(verified_pnl)
        row.updated_at = now
        first_tx = match.tx_hashes[0] if match.tx_hashes else None
        apply_trader_order_verification(
            row,
            verification_status=TRADER_ORDER_VERIFICATION_WALLET_ACTIVITY,
            verification_source="polymarket_wallet_trades",
            verification_reason=f"matched {len(match.matched_trades)} sell trade(s)",
            execution_wallet_address=wallet_lower,
            verification_tx_hash=first_tx,
            verified_at=now,
            force=True,
        )
        # Stamp the verified close on the payload so downstream code
        # has the trade lineage.
        payload = dict(row.payload_json or {})
        payload["verified_close"] = {
            "verified_at": now.isoformat(),
            "matched_size": match.matched_size,
            "matched_proceeds": match.matched_proceeds,
            "allocated_cost": allocated_cost,
            "realized_pnl": verified_pnl,
            "trades": match.matched_trades,
            "source": "polymarket_wallet_trades",
        }
        row.payload_json = payload
        append_trader_order_verification_event(
            session,
            trader_order_id=str(row.id),
            verification_status=TRADER_ORDER_VERIFICATION_WALLET_ACTIVITY,
            source="polymarket_wallet_trades",
            event_type="verified_close_from_wallet_trades",
            reason=f"verified_pnl={verified_pnl:.4f} matched_size={match.matched_size:.4f}",
            execution_wallet_address=wallet_lower,
            tx_hash=first_tx,
            payload_json={
                "matched_trades": match.matched_trades,
                "prior_actual_profit": prior_profit,
                "verified_actual_profit": verified_pnl,
            },
            created_at=now,
        )

    if commit and not dry_run and verified > 0:
        await session.commit()

    return {
        "examined": examined,
        "verified": verified,
        "unmatched": unmatched,
        "wallet_trades_fetched": len(trades_raw or []),
        "pnl_total_before": pnl_total_before,
        "pnl_total_after": pnl_total_after,
        "pnl_delta": pnl_total_after - pnl_total_before,
        "dry_run": dry_run,
    }
