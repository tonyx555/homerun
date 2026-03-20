"""Trading execution tools — place/cancel orders.

These tools have real financial impact.  The agent framework logs all
invocations to the scratchpad for full auditability.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def build_tools() -> list:
    from services.ai.agent import AgentTool

    return [
        AgentTool(
            name="place_order",
            description=(
                "Place an order on Polymarket. Specify the token, side (BUY/SELL), "
                "price, and size. Returns the order ID on success. "
                "IMPORTANT: This executes a REAL trade with real money."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "token_id": {
                        "type": "string",
                        "description": "Token ID to trade",
                    },
                    "side": {
                        "type": "string",
                        "enum": ["BUY", "SELL"],
                        "description": "Order side",
                    },
                    "price": {
                        "type": "number",
                        "description": "Limit price (0.01 to 0.99)",
                    },
                    "size": {
                        "type": "number",
                        "description": "Order size in shares",
                    },
                },
                "required": ["token_id", "side", "price", "size"],
            },
            handler=_place_order,
            max_calls=5,
            category="trading",
        ),
        AgentTool(
            name="cancel_order",
            description=(
                "Cancel an existing open order by its order ID."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "order_id": {
                        "type": "string",
                        "description": "The order ID to cancel",
                    },
                },
                "required": ["order_id"],
            },
            handler=_cancel_order,
            max_calls=5,
            category="trading",
        ),
        AgentTool(
            name="cancel_all_orders",
            description=(
                "Cancel ALL currently open orders. Use with caution."
            ),
            parameters={"type": "object", "properties": {}, "required": []},
            handler=_cancel_all_orders,
            max_calls=1,
            category="trading",
        ),
    ]


# ---------------------------------------------------------------------------
# Implementations
# ---------------------------------------------------------------------------


async def _place_order(args: dict) -> dict:
    try:
        from services.live_execution_service import live_execution_service

        if not live_execution_service.is_ready():
            return {"error": "Live execution not initialized — configure Polymarket credentials in Settings"}

        token_id = args["token_id"]
        side = args["side"].upper()
        price = float(args["price"])
        size = float(args["size"])

        # Basic sanity checks
        if side not in ("BUY", "SELL"):
            return {"error": f"Invalid side: {side}. Must be BUY or SELL."}
        if not (0.01 <= price <= 0.99):
            return {"error": f"Price {price} out of range [0.01, 0.99]"}
        if size <= 0:
            return {"error": f"Size must be positive, got {size}"}

        from services.live_execution_service import OrderSide

        order = await live_execution_service.place_order(
            token_id=token_id,
            side=OrderSide.BUY if side == "BUY" else OrderSide.SELL,
            price=price,
            size=size,
        )
        return {
            "order_id": order.id,
            "status": str(order.status),
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
        }
    except Exception as exc:
        logger.error("place_order failed: %s", exc)
        return {"error": str(exc)}


async def _cancel_order(args: dict) -> dict:
    try:
        from services.live_execution_service import live_execution_service

        if not live_execution_service.is_ready():
            return {"error": "Live execution not initialized — configure Polymarket credentials in Settings"}

        order_id = args["order_id"]
        success = await live_execution_service.cancel_order(order_id)
        return {"order_id": order_id, "cancelled": success}
    except Exception as exc:
        logger.error("cancel_order failed: %s", exc)
        return {"error": str(exc)}


async def _cancel_all_orders(args: dict) -> dict:
    try:
        from services.live_execution_service import live_execution_service

        if not live_execution_service.is_ready():
            return {"error": "Live execution not initialized — configure Polymarket credentials in Settings"}

        result = await live_execution_service.cancel_all_orders()
        return result if isinstance(result, dict) else {"result": result}
    except Exception as exc:
        logger.error("cancel_all_orders failed: %s", exc)
        return {"error": str(exc)}
