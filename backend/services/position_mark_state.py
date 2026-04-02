"""In-memory position mark state driven by real-time WS price updates.

Replaces the DB-round-trip pattern where position lifecycle writes marks to DB
and frontend reads them via REST. Instead, PriceCache callbacks update marks
in-memory and push to frontend via WS immediately.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Callable, Optional

from utils.logger import get_logger

logger = get_logger("position_mark_state")


@dataclass
class PositionMark:
    order_id: str
    market_id: str
    token_id: str
    direction: str  # "yes" or "no"
    entry_price: float
    notional: float
    edge_percent: float = 0.0

    mark_price: float = 0.0
    mark_bid: float = 0.0
    mark_ask: float = 0.0
    mark_updated_at: float = 0.0  # epoch seconds
    mark_sequence: int = 0

    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    edge_delta_pct: float = 0.0

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "market_id": self.market_id,
            "token_id": self.token_id,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "notional": self.notional,
            "mark_price": self.mark_price,
            "mark_bid": self.mark_bid,
            "mark_ask": self.mark_ask,
            "mark_updated_at": self.mark_updated_at,
            "mark_sequence": self.mark_sequence,
            "unrealized_pnl": self.unrealized_pnl,
            "pnl_pct": self.unrealized_pnl_pct,
            "edge_delta_pct": self.edge_delta_pct,
            "mark_fresh": True,
        }


class PositionMarkState:
    """Thread-safe singleton tracking live mark prices for open positions."""

    _instance: Optional[PositionMarkState] = None
    _instance_lock = Lock()

    def __init__(self) -> None:
        self._lock = Lock()
        self._positions: dict[str, PositionMark] = {}
        # Reverse lookup: token_id -> set of order_ids watching that token
        self._token_to_orders: dict[str, set[str]] = {}
        self._sequence: int = 0
        self._on_marks_changed: Optional[Callable[[list[PositionMark]], None]] = None

    @classmethod
    def get_instance(cls) -> PositionMarkState:
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        with cls._instance_lock:
            cls._instance = None

    def set_on_marks_changed(self, callback: Callable[[list[PositionMark]], None]) -> None:
        self._on_marks_changed = callback

    def register_position(
        self,
        order_id: str,
        market_id: str,
        token_id: str,
        direction: str,
        entry_price: float,
        notional: float,
        edge_percent: float = 0.0,
    ) -> None:
        with self._lock:
            self._positions[order_id] = PositionMark(
                order_id=order_id,
                market_id=market_id,
                token_id=token_id,
                direction=direction.lower(),
                entry_price=entry_price,
                notional=notional,
                edge_percent=edge_percent,
            )
            if token_id not in self._token_to_orders:
                self._token_to_orders[token_id] = set()
            self._token_to_orders[token_id].add(order_id)

    def unregister_position(self, order_id: str) -> None:
        with self._lock:
            mark = self._positions.pop(order_id, None)
            if mark and mark.token_id in self._token_to_orders:
                self._token_to_orders[mark.token_id].discard(order_id)
                if not self._token_to_orders[mark.token_id]:
                    del self._token_to_orders[mark.token_id]

    def on_price_update(
        self,
        token_id: str,
        mid: float,
        bid: float,
        ask: float,
        exchange_ts: float,
        ingest_ts: float,
        sequence: int,
    ) -> None:
        """PriceCache on_update callback. Called from WS receive threads."""
        changed: list[PositionMark] = []
        with self._lock:
            order_ids = self._token_to_orders.get(token_id)
            if not order_ids:
                return
            self._sequence += 1
            local_seq = self._sequence
            now = ingest_ts if ingest_ts > 0 else time.time()
            for oid in order_ids:
                mark = self._positions.get(oid)
                if mark is None:
                    continue
                # Compute mark price based on direction
                if mark.direction == "yes":
                    mark_price = mid
                else:
                    mark_price = 1.0 - mid
                mark.mark_price = mark_price
                mark.mark_bid = bid
                mark.mark_ask = ask
                mark.mark_updated_at = now
                mark.mark_sequence = local_seq
                # Unrealized P&L
                if mark.entry_price > 0:
                    mark.unrealized_pnl = (mark_price - mark.entry_price) * mark.notional / mark.entry_price
                    mark.unrealized_pnl_pct = (mark_price - mark.entry_price) / mark.entry_price * 100.0
                    mark.edge_delta_pct = mark.unrealized_pnl_pct - mark.edge_percent
                changed.append(mark)

        if changed and self._on_marks_changed:
            try:
                self._on_marks_changed(changed)
            except Exception:
                pass

    def get_marks(self) -> dict[str, dict]:
        with self._lock:
            return {oid: m.to_dict() for oid, m in self._positions.items()}

    def get_mark(self, order_id: str) -> Optional[PositionMark]:
        with self._lock:
            return self._positions.get(order_id)

    def get_changed_marks_since(self, sequence: int) -> list[PositionMark]:
        with self._lock:
            return [m for m in self._positions.values() if m.mark_sequence > sequence]


def get_position_mark_state() -> PositionMarkState:
    return PositionMarkState.get_instance()
