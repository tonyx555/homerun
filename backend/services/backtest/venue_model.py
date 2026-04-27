"""Venue-specific order semantics for the backtest matching engine.

Encodes Polymarket CLOB rules (the only venue currently in scope) so the
matching engine can faithfully reproduce live behavior:

* **Time-in-force**: GTC / IOC / FAK / FOK semantics, including "would
  cross" rejection for post-only orders and immediate-cancel for IOC/FAK
  remainders.
* **Tick size**: 0.01 (1¢) on Polymarket; orders are rounded to the
  nearest legal tick toward the trader's interest before submission.
* **Min notional**: $1 floor enforced at submit time (orders below this
  are rejected by the venue, mirrored here).
* **Price clamp**: prices ∈ [tick, 1 − tick] for binary outcomes.
* **Self-cross protection**: the engine cancels any resting order that
  would self-cross with a new submission from the same trader.

The ``Venue`` interface is abstract so we can plug in Kalshi or other
venues later. The backtester defaults to ``PolymarketVenue``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Optional, Protocol

from utils.converters import safe_float


# ── TIF constants (mirror live_execution_service.OrderType) ──────────────

TIF_GTC = "GTC"
TIF_IOC = "IOC"
TIF_FAK = "FAK"
TIF_FOK = "FOK"
TIF_GTD = "GTD"

VALID_TIFS = {TIF_GTC, TIF_IOC, TIF_FAK, TIF_FOK, TIF_GTD}


@dataclass(frozen=True)
class OrderRule:
    """Per-venue rules consulted when validating + matching an order."""

    tick_size: float
    min_notional_usd: float
    max_price: float
    min_price: float
    valid_tifs: frozenset[str]
    supports_post_only: bool
    supports_partial_fills: bool
    fok_atomicity: bool  # FOK rejects unless full fill is achievable


@dataclass
class SubmitDecision:
    """Result of ``Venue.validate_submit``.

    ``ok=True`` means the order is admitted (possibly with adjustments).
    ``ok=False`` carries a ``reject_reason`` mirroring live venue rejects.
    """

    ok: bool
    adjusted_price: Optional[float] = None
    adjusted_size: Optional[float] = None
    reject_reason: Optional[str] = None
    notes: dict[str, Any] = field(default_factory=dict)


class Venue(Protocol):
    """Abstract venue interface used by the matching engine."""

    @property
    def name(self) -> str: ...

    @property
    def rules(self) -> OrderRule: ...

    def round_price(self, price: float, *, side: str) -> float: ...

    def validate_submit(
        self,
        *,
        side: str,
        price: float,
        size: float,
        tif: str,
        post_only: bool,
    ) -> SubmitDecision: ...

    def crosses_book(
        self,
        *,
        side: str,
        price: float,
        best_bid: Optional[float],
        best_ask: Optional[float],
    ) -> bool: ...


class PolymarketVenue:
    """Polymarket CLOB rules.

    Constants mirror ``services.live_execution_service.POST_ONLY_REPRICE_TICK``
    and ``settings.MIN_ORDER_SIZE_USD``. They are duplicated here (rather
    than imported) so the backtester remains usable without a live-trading
    runtime context.
    """

    name = "polymarket"
    rules = OrderRule(
        tick_size=0.01,
        min_notional_usd=1.0,
        max_price=0.99,
        min_price=0.01,
        valid_tifs=frozenset({TIF_GTC, TIF_IOC, TIF_FAK, TIF_FOK, TIF_GTD}),
        supports_post_only=True,
        supports_partial_fills=True,
        fok_atomicity=True,
    )

    def round_price(self, price: float, *, side: str) -> float:
        """Round to the venue tick.

        For BUY orders we round UP toward the inside (more aggressive bid);
        for SELL orders we round DOWN toward the inside (more aggressive
        offer). This matches the convention in
        ``live_execution_service._round_down_to_tick`` for sells.
        """
        tick = self.rules.tick_size
        side_norm = (side or "").upper()
        if tick <= 0:
            return float(price)
        steps = float(price) / tick
        if side_norm == "BUY":
            rounded = math.ceil(steps - 1e-9) * tick
        else:
            rounded = math.floor(steps + 1e-9) * tick
        return _clamp(rounded, self.rules.min_price, self.rules.max_price)

    def validate_submit(
        self,
        *,
        side: str,
        price: float,
        size: float,
        tif: str,
        post_only: bool,
    ) -> SubmitDecision:
        side_norm = (side or "").upper()
        if side_norm not in {"BUY", "SELL"}:
            return SubmitDecision(ok=False, reject_reason=f"invalid_side:{side}")
        tif_norm = (tif or TIF_GTC).upper()
        if tif_norm not in self.rules.valid_tifs:
            return SubmitDecision(ok=False, reject_reason=f"invalid_tif:{tif}")
        if post_only and tif_norm in {TIF_IOC, TIF_FAK, TIF_FOK}:
            return SubmitDecision(
                ok=False,
                reject_reason=f"post_only_with_taker_tif:{tif_norm}",
            )

        size_f = safe_float(size, 0.0) or 0.0
        if size_f <= 0:
            return SubmitDecision(ok=False, reject_reason="non_positive_size")

        price_f = safe_float(price, 0.0) or 0.0
        if price_f <= 0:
            return SubmitDecision(ok=False, reject_reason="non_positive_price")
        rounded_price = self.round_price(price_f, side=side_norm)
        notional = rounded_price * size_f
        if notional + 1e-9 < self.rules.min_notional_usd:
            return SubmitDecision(
                ok=False,
                reject_reason=(
                    f"below_min_notional:{notional:.4f}<{self.rules.min_notional_usd:.4f}"
                ),
            )
        adjusted_price = rounded_price if abs(rounded_price - price_f) > 1e-12 else None
        return SubmitDecision(ok=True, adjusted_price=adjusted_price)

    def crosses_book(
        self,
        *,
        side: str,
        price: float,
        best_bid: Optional[float],
        best_ask: Optional[float],
    ) -> bool:
        """True when a new resting order would cross the visible book.

        For post-only orders the engine treats this as a reject.
        """
        side_norm = (side or "").upper()
        if side_norm == "BUY":
            return best_ask is not None and price + 1e-12 >= float(best_ask)
        if side_norm == "SELL":
            return best_bid is not None and price - 1e-12 <= float(best_bid)
        return False

    def is_marketable(
        self,
        *,
        side: str,
        price: float,
        best_bid: Optional[float],
        best_ask: Optional[float],
    ) -> bool:
        """True when an order would immediately match against the book."""
        side_norm = (side or "").upper()
        if side_norm == "BUY":
            return best_ask is not None and price + 1e-12 >= float(best_ask)
        if side_norm == "SELL":
            return best_bid is not None and price - 1e-12 <= float(best_bid)
        return False


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


__all__ = [
    "Venue",
    "OrderRule",
    "SubmitDecision",
    "PolymarketVenue",
    "TIF_GTC",
    "TIF_IOC",
    "TIF_FAK",
    "TIF_FOK",
    "TIF_GTD",
    "VALID_TIFS",
]
