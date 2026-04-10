"""Bridge: publish Opportunity objects into the in-process intent runtime.

DB projection remains asynchronous for audit/UI, but runtime publication is
the authoritative hot path for strategy execution.
"""

from __future__ import annotations

from typing import Optional, Any

from models.opportunity import Opportunity
from services.intent_runtime import get_intent_runtime
from utils.logger import get_logger

logger = get_logger(__name__)


async def bridge_opportunities_to_signals(
    opportunities: list[Opportunity],
    source: str,
    *,
    signal_type_override: str | None = None,
    default_ttl_minutes: int = 120,
    quality_filter_pipeline: Optional[Any] = None,
    quality_reports: Optional[dict] = None,
    sweep_missing: bool = False,
    refresh_prices: bool = True,
) -> int:
    """Publish opportunities to the in-process intent runtime.
    """
    runtime = get_intent_runtime()
    if not runtime.started:
        await runtime.start()
    await runtime.prewarm_source_tokens(
        opportunities,
        source=str(source),
    )
    return await runtime.publish_opportunities(
        opportunities,
        source=str(source),
        signal_type_override=signal_type_override,
        default_ttl_minutes=default_ttl_minutes,
        quality_filter_pipeline=quality_filter_pipeline,
        quality_reports=quality_reports,
        sweep_missing=sweep_missing,
        refresh_prices=refresh_prices,
    )
