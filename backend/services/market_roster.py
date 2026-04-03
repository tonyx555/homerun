from __future__ import annotations

import hashlib
import json
from typing import Any


def _extract_market_field(market: Any, key: str) -> Any:
    if isinstance(market, dict):
        return market.get(key)
    return getattr(market, key, None)


def _normalize_market_entry(market: Any) -> dict[str, Any] | None:
    market_id = str(_extract_market_field(market, "id") or _extract_market_field(market, "market_id") or "").strip()
    if not market_id:
        return None

    token_ids = _extract_market_field(market, "clob_token_ids")
    if not isinstance(token_ids, list):
        token_ids = list(token_ids or []) if isinstance(token_ids, tuple) else []
    outcome_prices = _extract_market_field(market, "outcome_prices")
    if not isinstance(outcome_prices, list):
        outcome_prices = list(outcome_prices or []) if isinstance(outcome_prices, tuple) else []

    entry = {
        "id": market_id,
        "condition_id": str(_extract_market_field(market, "condition_id") or "").strip() or None,
        "question": str(
            _extract_market_field(market, "question") or _extract_market_field(market, "market_question") or ""
        ).strip()
        or None,
        "group_item_title": str(_extract_market_field(market, "group_item_title") or "").strip() or None,
        "event_slug": str(_extract_market_field(market, "event_slug") or "").strip() or None,
        "platform": str(_extract_market_field(market, "platform") or "polymarket").strip().lower() or "polymarket",
        "sports_market_type": str(_extract_market_field(market, "sports_market_type") or "").strip() or None,
        "line": _extract_market_field(market, "line"),
        "neg_risk": bool(_extract_market_field(market, "neg_risk")),
        "active": bool(_extract_market_field(market, "active")) if _extract_market_field(market, "active") is not None else None,
        "closed": bool(_extract_market_field(market, "closed")) if _extract_market_field(market, "closed") is not None else None,
        "accepting_orders": (
            bool(_extract_market_field(market, "accepting_orders"))
            if _extract_market_field(market, "accepting_orders") is not None
            else None
        ),
        "resolved": bool(_extract_market_field(market, "resolved")) if _extract_market_field(market, "resolved") is not None else None,
        "winner": bool(_extract_market_field(market, "winner")) if _extract_market_field(market, "winner") is not None else None,
        "winning_outcome": str(_extract_market_field(market, "winning_outcome") or "").strip() or None,
        "status": str(_extract_market_field(market, "status") or "").strip() or None,
        "clob_token_ids": [str(token_id or "").strip() for token_id in token_ids if str(token_id or "").strip()],
        "outcome_prices": outcome_prices,
    }
    return entry


def build_market_roster(
    markets: list[Any],
    *,
    scope: str,
    event_id: str | None = None,
    event_slug: str | None = None,
    event_title: str | None = None,
    category: str | None = None,
) -> dict[str, Any] | None:
    roster_markets: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for market in markets:
        entry = _normalize_market_entry(market)
        if entry is None:
            continue
        market_id = str(entry["id"])
        if market_id in seen_ids:
            continue
        seen_ids.add(market_id)
        roster_markets.append(entry)

    if not roster_markets:
        return None

    packed = json.dumps(roster_markets, sort_keys=True, separators=(",", ":"))
    roster_hash = hashlib.sha256(
        "|".join(
            [
                str(scope or "").strip().lower(),
                str(event_id or "").strip(),
                str(event_slug or "").strip(),
                packed,
            ]
        ).encode("utf-8")
    ).hexdigest()[:16]

    return {
        "scope": str(scope or "").strip().lower() or "signal",
        "event_id": str(event_id or "").strip() or None,
        "event_slug": str(event_slug or "").strip() or None,
        "event_title": str(event_title or "").strip() or None,
        "category": str(category or "").strip() or None,
        "market_count": len(roster_markets),
        "roster_hash": roster_hash,
        "markets": roster_markets,
    }


def ensure_market_roster_payload(
    payload: dict[str, Any] | None,
    *,
    markets: list[Any] | None = None,
    market_id: str | None = None,
    market_question: str | None = None,
    event_id: str | None = None,
    event_slug: str | None = None,
    event_title: str | None = None,
    category: str | None = None,
) -> dict[str, Any]:
    normalized = dict(payload or {})
    existing = normalized.get("market_roster")
    if isinstance(existing, dict) and isinstance(existing.get("markets"), list) and existing.get("markets"):
        return normalized

    roster_markets = markets
    if roster_markets is None:
        payload_markets = normalized.get("markets")
        roster_markets = payload_markets if isinstance(payload_markets, list) else []
    if not roster_markets and (market_id or market_question):
        roster_markets = [
            {
                "id": str(market_id or "").strip(),
                "question": str(market_question or "").strip(),
                "event_slug": str(event_slug or "").strip() or None,
            }
        ]

    roster = build_market_roster(
        list(roster_markets or []),
        scope="event" if (event_id or event_slug) else "signal",
        event_id=event_id,
        event_slug=event_slug,
        event_title=event_title,
        category=category,
    )
    if roster is not None:
        normalized["market_roster"] = roster
    return normalized
