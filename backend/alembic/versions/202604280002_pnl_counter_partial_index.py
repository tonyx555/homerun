"""partial index for live-trading runtime P&L aggregation

Revision ID: 202604280002
Revises: 202604280001
Create Date: 2026-04-28

``LiveExecutionService._derive_pnl_counters_from_orders`` aggregates
``TraderOrder.actual_profit`` per execution wallet on every runtime-state
persist call. The plain ``idx_trader_orders_execution_wallet_address``
index speeds the wallet filter, but the planner still has to evaluate
``actual_profit IS NOT NULL`` row-by-row across that wallet's history.

A partial index over only the verified-pnl rows lets the aggregation
become an index-only scan over the meaningful subset, regardless of how
many ``actual_profit IS NULL`` rows the wallet has accumulated.

Same pattern as 202604270001's functional indexes for trade signals.
"""
from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "202604280002"
down_revision = "202604280001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "idx_trader_orders_wallet_realized_pnl "
        "ON trader_orders (lower(coalesce(execution_wallet_address, '')), executed_at) "
        "WHERE actual_profit IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_trader_orders_wallet_realized_pnl")
