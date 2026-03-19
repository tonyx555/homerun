"""Trader SDK — stable helpers for strategy authors and routes."""

from __future__ import annotations

from typing import Any


class TradersSDK:
    """High-level APIs for trader intelligence datasets."""

    _ai_instance = None

    class _AIDescriptor:
        def __get__(self, obj, objtype=None):
            if TradersSDK._ai_instance is None:
                from services.ai import get_llm_manager
                from services.ai_sdk import AISDK

                TradersSDK._ai_instance = AISDK(get_llm_manager(), purpose="trader_intelligence")
            return TradersSDK._ai_instance

    ai = _AIDescriptor()

    @staticmethod
    async def get_firehose_signals(
        *,
        limit: int = 250,
        include_filtered: bool = False,
        include_source_context: bool = True,
    ) -> list[dict[str, Any]]:
        from services.trader_data_access import get_trader_firehose_signals

        return await get_trader_firehose_signals(
            limit=limit,
            include_filtered=include_filtered,
            include_source_context=include_source_context,
        )

    @staticmethod
    async def get_strategy_filtered_signals(
        *,
        limit: int = 50,
        include_filtered: bool = False,
    ) -> list[dict[str, Any]]:
        from services.trader_data_access import get_strategy_filtered_trader_signals

        return await get_strategy_filtered_trader_signals(
            limit=limit,
            include_filtered=include_filtered,
        )

    @staticmethod
    async def get_confluence_signals(
        *,
        min_strength: float = 0.0,
        min_tier: str = "WATCH",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        from services.trader_data_access import get_trader_confluence_signals

        return await get_trader_confluence_signals(
            min_strength=min_strength,
            min_tier=min_tier,
            limit=limit,
        )

    @staticmethod
    async def get_pooled_traders(
        *,
        limit: int = 200,
        tier: str | None = None,
        include_blacklisted: bool = True,
        tracked_only: bool = False,
    ) -> list[dict[str, Any]]:
        from services.trader_data_access import get_pooled_traders

        return await get_pooled_traders(
            limit=limit,
            tier=tier,
            include_blacklisted=include_blacklisted,
            tracked_only=tracked_only,
        )

    @staticmethod
    async def get_tracked_traders(
        *,
        limit: int = 200,
        include_recent_activity: bool = False,
        activity_hours: int = 24,
    ) -> list[dict[str, Any]]:
        from services.trader_data_access import get_tracked_traders

        return await get_tracked_traders(
            limit=limit,
            include_recent_activity=include_recent_activity,
            activity_hours=activity_hours,
        )

    @staticmethod
    async def get_groups(
        *,
        include_members: bool = False,
        member_limit: int = 25,
    ) -> list[dict[str, Any]]:
        from services.trader_data_access import get_trader_groups

        return await get_trader_groups(
            include_members=include_members,
            member_limit=member_limit,
        )

    @staticmethod
    async def get_tags() -> list[dict[str, Any]]:
        from services.trader_data_access import get_trader_tags

        return await get_trader_tags()

    @staticmethod
    async def get_traders_by_tag(tag_name: str, *, limit: int = 100) -> list[dict[str, Any]]:
        from services.trader_data_access import get_traders_by_tag

        return await get_traders_by_tag(
            tag_name=tag_name,
            limit=limit,
        )


__all__ = ["TradersSDK"]
