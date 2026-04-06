"""add live trading order market ids

Revision ID: 202604050001
Revises: 202604040002
Create Date: 2026-04-05 23:55:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "202604050001"
down_revision = "202604040002"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    existing = _column_names("live_trading_orders")
    if "market_id" not in existing:
        op.add_column("live_trading_orders", sa.Column("market_id", sa.String(), nullable=True))
    op.execute(
        sa.text(
            """
            CREATE INDEX IF NOT EXISTS idx_live_trading_orders_market_id
            ON live_trading_orders (market_id)
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE live_trading_orders AS live
            SET market_id = positions.market_id
            FROM live_trading_positions AS positions
            WHERE coalesce(live.market_id, '') = ''
              AND live.wallet_address = positions.wallet_address
              AND live.token_id = positions.token_id
              AND coalesce(positions.market_id, '') <> ''
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE live_trading_orders AS live
            SET market_id = trader.market_id
            FROM trader_orders AS trader
            WHERE coalesce(live.market_id, '') = ''
              AND trader.provider_order_id = live.id
              AND coalesce(trader.market_id, '') <> ''
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE live_trading_orders AS live
            SET market_id = trader.market_id
            FROM trader_orders AS trader
            WHERE coalesce(live.market_id, '') = ''
              AND trader.provider_order_id = live.clob_order_id
              AND coalesce(trader.market_id, '') <> ''
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE live_trading_orders AS live
            SET market_id = trader.market_id
            FROM trader_orders AS trader
            WHERE coalesce(live.market_id, '') = ''
              AND trader.signal_id = live.opportunity_id
              AND coalesce(trader.market_id, '') <> ''
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE live_trading_orders AS live
            SET market_id = signal.market_id
            FROM trade_signals AS signal
            WHERE coalesce(live.market_id, '') = ''
              AND signal.id = live.opportunity_id
              AND coalesce(signal.market_id, '') <> ''
            """
        )
    )


def downgrade() -> None:
    pass
