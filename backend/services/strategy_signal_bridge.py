"""Bridge: convert ArbitrageOpportunity objects from strategy on_event() into TradeSignal DB rows.

Workers dispatch DataEvents to subscribed strategies via the event_dispatcher.
Strategies return lists of ArbitrageOpportunity objects.  This module converts
those opportunities into normalized TradeSignal rows using the same upsert
pattern as the existing signal_bus emit_* functions.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional, Any

from sqlalchemy.ext.asyncio import AsyncSession

from models.opportunity import ArbitrageOpportunity
from services.signal_bus import (
    build_signal_contract_from_opportunity,
    make_dedupe_key,
    refresh_trade_signal_snapshots,
    upsert_trade_signal,
)
from utils.utcnow import utcnow


async def bridge_opportunities_to_signals(
    session: AsyncSession,
    opportunities: list[ArbitrageOpportunity],
    source: str,
    *,
    default_ttl_minutes: int = 120,
    quality_filter_pipeline: Optional[Any] = None,
) -> int:
    """Convert strategy-produced ArbitrageOpportunity objects into TradeSignal rows.

    Mirrors the pattern used by ``emit_scanner_signals`` in signal_bus.py:
    each opportunity is upserted by (source, dedupe_key) so repeated
    detections update rather than duplicate.

    If ``quality_filter_pipeline`` is provided it is a ``QualityFilterPipeline``
    instance.  Each opportunity is evaluated once before upserting and the
    result is stored on the signal row via ``quality_passed`` /
    ``quality_rejection_reasons``.  Opportunities that fail are still written
    to the DB (with ``quality_passed=False``) so the orchestrator can skip them
    using the stored result rather than re-evaluating.

    Returns the number of signals upserted.
    """
    now = utcnow()
    emitted = 0

    for opp in opportunities:
        market_id, direction, entry_price, market_question, payload_json, strategy_context_json = (
            build_signal_contract_from_opportunity(opp)
        )
        if not market_id:
            continue

        dedupe_key = make_dedupe_key(
            opp.stable_id,
            opp.strategy,
            market_id,
        )
        expires = opp.resolution_date or (now + timedelta(minutes=default_ttl_minutes))

        opp_quality_passed: Optional[bool] = None
        opp_rejection_reasons: Optional[list] = None
        if quality_filter_pipeline is not None:
            report = quality_filter_pipeline.evaluate(opp)
            opp_quality_passed = bool(report.passed)
            opp_rejection_reasons = list(report.rejection_reasons) if not report.passed else None

        await upsert_trade_signal(
            session,
            source=source,
            source_item_id=opp.stable_id,
            signal_type=f"{source}_opportunity",
            strategy_type=opp.strategy,
            market_id=market_id,
            market_question=market_question,
            direction=direction,
            entry_price=entry_price,
            edge_percent=float(opp.roi_percent or 0.0),
            confidence=float(opp.confidence),
            liquidity=float(opp.min_liquidity or 0.0),
            expires_at=expires,
            payload_json=payload_json,
            strategy_context_json=strategy_context_json,
            quality_passed=opp_quality_passed,
            quality_rejection_reasons=opp_rejection_reasons,
            dedupe_key=dedupe_key,
            commit=False,
        )
        emitted += 1

    await session.commit()
    await refresh_trade_signal_snapshots(session)
    return emitted
