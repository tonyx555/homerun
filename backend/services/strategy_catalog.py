"""Unified Strategy Catalog

Single entry point for all strategy seeds — both detection (opportunity)
and execution (trader) strategies. Consolidates the two separate catalogs
into one file for easier management.

When adding a new strategy:
1. Write your strategy class (extending BaseStrategy)
2. Add a seed entry to the appropriate sub-catalog:
   - opportunity_strategy_catalog.py  for detection strategies
   - trader_orchestrator/strategy_catalog.py  for execution strategies
3. The seed routine will create/update the DB row on startup
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

# Re-export existing catalog functions and data
from services.opportunity_strategy_catalog import (
    SYSTEM_OPPORTUNITY_STRATEGY_SEEDS,
    SystemOpportunityStrategySeed,
    ensure_system_opportunity_strategies_seeded,
)
from services.trader_orchestrator.strategy_catalog import (
    SYSTEM_TRADER_STRATEGY_SEEDS,
    SystemTraderStrategySeed,
    ensure_system_trader_strategies_seeded,
)

__all__ = [
    # Opportunity (detection) catalog
    "SYSTEM_OPPORTUNITY_STRATEGY_SEEDS",
    "SystemOpportunityStrategySeed",
    "ensure_system_opportunity_strategies_seeded",
    # Trader (execution) catalog
    "SYSTEM_TRADER_STRATEGY_SEEDS",
    "SystemTraderStrategySeed",
    "ensure_system_trader_strategies_seeded",
    # Unified
    "ensure_all_strategies_seeded",
    "ALL_STRATEGY_SEEDS",
]


async def ensure_all_strategies_seeded(session: AsyncSession) -> dict:
    """Seed both opportunity and trader strategies in one call."""
    opp_result = await ensure_system_opportunity_strategies_seeded(session)
    trader_result = await ensure_system_trader_strategies_seeded(session)
    return {
        "opportunity": opp_result,
        "trader": trader_result,
    }


# Convenience: all seeds in one dict for introspection
ALL_STRATEGY_SEEDS = {
    "opportunity": SYSTEM_OPPORTUNITY_STRATEGY_SEEDS,
    "trader": SYSTEM_TRADER_STRATEGY_SEEDS,
}
