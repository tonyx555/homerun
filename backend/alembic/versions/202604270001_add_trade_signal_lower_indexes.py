"""add functional indexes to speed up list_unconsumed_trade_signals

Revision ID: 202604270001
Revises: 202604260003
Create Date: 2026-04-27

Production logs show ``list_unconsumed_trade_signals`` taking 2-7s
repeatedly, blowing the fast trader's 3s budget on its hot path.  The
query filters with ``func.lower(func.coalesce(TradeSignal.status, ""))``
and ``func.lower(func.coalesce(TradeSignal.source, ""))``, which defeats
the plain B-tree indexes on ``status`` and ``source``.

Also: the per-trader anti-join against ``trader_signal_consumption``
groups by ``signal_id`` to find the latest ``consumed_at`` per signal.
The existing unique constraint ``(trader_id, signal_id)`` is sufficient
for lookup but not for the GROUP BY + MAX path the planner takes.  A
composite index ``(trader_id, signal_id, consumed_at DESC)`` lets the
subquery become a simple index range scan.

Same pattern as 202603310001 (lower_wallet_address_indexes).
"""
from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "202604270001"
down_revision = "202604260003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Functional indexes on the case-folded columns the worker actually
    # queries.  COALESCE-to-empty-string mirrors the Python expression in
    # list_unconsumed_trade_signals so the planner can use the index.
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "idx_trade_signals_status_lower "
        "ON trade_signals (lower(coalesce(status, '')))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "idx_trade_signals_source_lower "
        "ON trade_signals (lower(coalesce(source, '')))"
    )
    # Composite index for the per-trader consumption anti-join.  Order
    # matters: trader_id first (equality filter), signal_id second
    # (join key), consumed_at DESC last (so MAX is an index-only scan).
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "idx_trader_signal_consumption_trader_signal_consumed "
        "ON trader_signal_consumption (trader_id, signal_id, consumed_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_trade_signals_status_lower")
    op.execute("DROP INDEX IF EXISTS idx_trade_signals_source_lower")
    op.execute("DROP INDEX IF EXISTS idx_trader_signal_consumption_trader_signal_consumed")
