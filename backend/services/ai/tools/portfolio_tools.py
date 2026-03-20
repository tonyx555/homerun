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
            max_calls=3,
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
            max_calls=3,
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
            max_calls=3,
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
            max_calls=2,
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
            max_calls=3,
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
                    LiveTradingPosition.status == "open"
                )
            )
            rows = result.scalars().all()

        positions = []
        for r in rows:
            positions.append({
                "id": r.id,
                "market_question": getattr(r, "market_question", None) or r.condition_id,
                "condition_id": r.condition_id,
                "side": r.side,
                "size": float(r.size) if r.size else 0,
                "entry_price": float(r.entry_price) if r.entry_price else 0,
                "current_price": float(r.current_price) if getattr(r, "current_price", None) else None,
                "unrealized_pnl": float(r.unrealized_pnl) if getattr(r, "unrealized_pnl", None) else None,
                "opened_at": r.created_at.isoformat() if r.created_at else None,
            })

        return {"positions": positions, "count": len(positions)}
    except Exception as exc:
        logger.error("get_open_positions failed: %s", exc)
        return {"error": str(exc)}


async def _get_trade_history(args: dict) -> dict:
    try:
        from models.database import AsyncSessionLocal, LiveTradingPosition
        from sqlalchemy import select

        limit = args.get("limit", 20)

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(LiveTradingPosition)
                .where(LiveTradingPosition.status == "closed")
                .order_by(LiveTradingPosition.updated_at.desc())
                .limit(limit)
            )
            rows = result.scalars().all()

        trades = []
        for r in rows:
            trades.append({
                "id": r.id,
                "market_question": getattr(r, "market_question", None) or r.condition_id,
                "condition_id": r.condition_id,
                "side": r.side,
                "size": float(r.size) if r.size else 0,
                "entry_price": float(r.entry_price) if r.entry_price else 0,
                "exit_price": float(r.exit_price) if getattr(r, "exit_price", None) else None,
                "realized_pnl": float(r.realized_pnl) if getattr(r, "realized_pnl", None) else None,
                "closed_at": r.updated_at.isoformat() if r.updated_at else None,
            })

        return {"trades": trades, "count": len(trades)}
    except Exception as exc:
        logger.error("get_trade_history failed: %s", exc)
        return {"error": str(exc)}


async def _get_account_balance(args: dict) -> dict:
    try:
        from services.trading_proxy import TradingProxy

        proxy = TradingProxy.instance()
        if proxy is None:
            return {"error": "TradingProxy not initialized"}

        balance = await proxy.get_balance()
        return balance
    except Exception as exc:
        logger.error("get_account_balance failed: %s", exc)
        return {"error": str(exc)}


async def _get_portfolio_performance(args: dict) -> dict:
    try:
        from services.trading_proxy import TradingProxy

        proxy = TradingProxy.instance()
        if proxy is None:
            return {"error": "TradingProxy not initialized"}

        limit = args.get("limit", 50)
        perf = await proxy.get_live_wallet_performance(limit=limit)
        return perf
    except Exception as exc:
        logger.error("get_portfolio_performance failed: %s", exc)
        return {"error": str(exc)}


async def _get_open_orders(args: dict) -> dict:
    try:
        from services.trading_proxy import TradingProxy

        proxy = TradingProxy.instance()
        if proxy is None:
            return {"error": "TradingProxy not initialized"}

        orders = await proxy.get_open_orders()
        return {"orders": orders if isinstance(orders, list) else [orders], "count": len(orders) if isinstance(orders, list) else 1}
    except Exception as exc:
        logger.error("get_open_orders failed: %s", exc)
        return {"error": str(exc)}
