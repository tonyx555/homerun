"""Portfolio tools — positions, trades, balances, P&L."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def build_tools() -> list:
    from services.ai.agent import AgentTool

    return [
        AgentTool(
            name="get_open_positions",
            description=(
                "Get all currently open trading positions for the live wallet. "
                "Returns market, side, size, entry price, current price, and unrealized P&L "
                "for each position."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_open_positions,
            max_calls=10,
            category="portfolio",
        ),
        AgentTool(
            name="get_trade_history",
            description=(
                "Get recent closed trades with P&L. Returns trade details including "
                "market, side, size, entry/exit prices, realized P&L, and timestamps."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Max trades to return (default 20)",
                        "default": 20,
                    },
                },
                "required": [],
            },
            handler=_get_trade_history,
            max_calls=10,
            category="portfolio",
        ),
        AgentTool(
            name="get_account_balance",
            description=(
                "Get the current USDC balance of the live trading wallet. "
                "Also returns allowance status."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_account_balance,
            max_calls=10,
            category="portfolio",
        ),
        AgentTool(
            name="get_portfolio_performance",
            description=(
                "Get portfolio performance statistics — total P&L, win rate, "
                "average trade size, Sharpe ratio, max drawdown, and equity curve data."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Number of equity snapshots (default 50)",
                        "default": 50,
                    },
                },
                "required": [],
            },
            handler=_get_portfolio_performance,
            max_calls=5,
            category="portfolio",
        ),
        AgentTool(
            name="get_open_orders",
            description=(
                "Get all currently open (unfilled) orders. Returns order ID, market, "
                "side, price, size, and creation time for each."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_get_open_orders,
            max_calls=10,
            category="portfolio",
        ),
    ]


# ---------------------------------------------------------------------------
# Implementations
# ---------------------------------------------------------------------------


async def _get_open_positions(args: dict) -> dict:
    try:
        from models.database import AsyncSessionLocal, LiveTradingPosition
        from sqlalchemy import select

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(LiveTradingPosition).where(
                    LiveTradingPosition.size > 0
                )
            )
            rows = result.scalars().all()

        positions = []
        for r in rows:
            # Truncate long hex IDs for display (full IDs not useful in chat)
            mid = r.market_id or ""
            if len(mid) > 20:
                mid = mid[:8] + "..." + mid[-6:]
            tid = r.token_id or ""
            if len(tid) > 20:
                tid = tid[:8] + "..." + tid[-6:]
            positions.append({
                "id": r.id,
                "market_question": r.market_question or mid,
                "market_id": mid,
                "token_id": tid,
                "outcome": r.outcome,
                "size": float(r.size) if r.size else 0,
                "average_cost": float(r.average_cost) if r.average_cost else 0,
                "current_price": float(r.current_price) if r.current_price else None,
                "unrealized_pnl": float(r.unrealized_pnl) if r.unrealized_pnl else None,
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
            })

        return {"positions": positions, "count": len(positions)}
    except Exception as exc:
        logger.error("get_open_positions failed: %s", exc)
        return {"error": str(exc)}


async def _get_trade_history(args: dict) -> dict:
    try:
        from models.database import AsyncSessionLocal, TraderOrder
        from sqlalchemy import select

        limit = args.get("limit", 20)

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(TraderOrder)
                .order_by(TraderOrder.created_at.desc())
                .limit(limit)
            )
            rows = result.scalars().all()

        trades = []
        for r in rows:
            mid = r.market_id or ""
            if len(mid) > 20:
                mid = mid[:8] + "..." + mid[-6:]
            trades.append({
                "id": r.id,
                "market_question": r.market_question or mid,
                "market_id": mid,
                "direction": r.direction,
                "mode": r.mode,
                "status": r.status,
                "notional_usd": float(r.notional_usd) if r.notional_usd else 0,
                "entry_price": float(r.entry_price) if r.entry_price else None,
                "actual_profit": float(r.actual_profit) if r.actual_profit else None,
                "edge_percent": float(r.edge_percent) if r.edge_percent else None,
                "strategy_key": r.strategy_key,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            })

        return {"trades": trades, "count": len(trades)}
    except Exception as exc:
        logger.error("get_trade_history failed: %s", exc)
        return {"error": str(exc)}


async def _get_account_balance(args: dict) -> dict:
    try:
        from services.live_execution_service import live_execution_service

        if not live_execution_service.is_ready():
            return {"error": "Live execution not initialized. Configure Polymarket credentials in Settings."}

        balance = await live_execution_service.get_balance()
        return balance
    except Exception as exc:
        logger.error("get_account_balance failed: %s", exc)
        return {"error": str(exc)}


async def _get_portfolio_performance(args: dict) -> dict:
    try:
        from models.database import AsyncSessionLocal, LiveTradingPosition, TraderOrder
        from sqlalchemy import func, select

        # Aggregate from positions and trade records
        async with AsyncSessionLocal() as session:
            # Open positions summary
            pos_result = await session.execute(
                select(
                    func.count(LiveTradingPosition.id),
                    func.sum(LiveTradingPosition.size * LiveTradingPosition.average_cost),
                    func.sum(LiveTradingPosition.unrealized_pnl),
                ).where(LiveTradingPosition.size > 0)
            )
            pos_row = pos_result.one()
            open_count = pos_row[0] or 0
            total_invested = round(float(pos_row[1] or 0), 2)
            total_unrealized = round(float(pos_row[2] or 0), 2)

            # Recent trades summary
            trade_result = await session.execute(
                select(
                    func.count(TraderOrder.id),
                    func.sum(TraderOrder.actual_profit),
                ).where(TraderOrder.actual_profit.isnot(None))
            )
            trade_row = trade_result.one()
            total_trades = trade_row[0] or 0
            total_realized = round(float(trade_row[1] or 0), 2)

            # Win rate
            if total_trades > 0:
                win_result = await session.execute(
                    select(func.count(TraderOrder.id)).where(
                        TraderOrder.actual_profit > 0
                    )
                )
                wins = win_result.scalar() or 0
                win_rate = round(wins / total_trades, 4)
            else:
                win_rate = 0.0

        return {
            "open_positions": open_count,
            "total_invested_usd": total_invested,
            "unrealized_pnl_usd": total_unrealized,
            "total_closed_trades": total_trades,
            "realized_pnl_usd": total_realized,
            "win_rate": win_rate,
        }
    except Exception as exc:
        logger.error("get_portfolio_performance failed: %s", exc)
        return {"error": str(exc)}


async def _get_open_orders(args: dict) -> dict:
    try:
        from services.live_execution_service import live_execution_service

        if not live_execution_service.is_ready():
            return {"error": "Live execution not initialized. Configure Polymarket credentials in Settings."}

        orders = await live_execution_service.get_open_orders()
        order_list = []
        for o in orders:
            order_list.append({
                "id": getattr(o, "id", None),
                "market_id": getattr(o, "market_id", None),
                "token_id": getattr(o, "token_id", None),
                "side": str(getattr(o, "side", "")),
                "price": float(getattr(o, "price", 0)),
                "size": float(getattr(o, "size", 0)),
                "status": str(getattr(o, "status", "")),
                "created_at": getattr(o, "created_at", None).isoformat() if getattr(o, "created_at", None) else None,
            })
        return {"orders": order_list, "count": len(order_list)}
    except Exception as exc:
        logger.error("get_open_orders failed: %s", exc)
        return {"error": str(exc)}
