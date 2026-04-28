"""add functional indexes to speed up list_unconsumed_trade_signals

Revision ID: 202604270001
Revises: 202604260003
Create Date: 2026-04-27

Production logs show ``list_unconsumed_trade_signals`` taking 2-7s
repeatedly, blowing the fast trader's 3s budget on its hot path.  The
query filters with ``func.lower(func.coalesce(TradeSignal.status, ""))``
and ``func.lower(func.coalesce(TradeSignal.source, ""))``, which defeats
the plain B-tree indexes on ``status`` and ``source``.

Also: the anti-join against ``trader_signal_consumption`` (rewritten as
``NOT EXISTS`` on top of the unique ``(trader_id, signal_id)`` constraint)
becomes an index-only scan when the index also includes ``consumed_at``
so the planner does not have to fetch the heap to evaluate
``consumed_at >= signal_sort_ts``.

Same pattern as 202603310001 (lower_wallet_address_indexes).

NOTE on locking: Plain ``CREATE INDEX`` takes a SHARE lock on the table
which blocks writers for the index build duration. ``CONCURRENTLY``
would avoid that, but cannot run inside a transaction and conflicts
with the way ``init_database`` wraps migrations in
``async_engine.begin()``. Since this migration only adds indexes (no
DML), if you need to apply it on a high-write table without blocking
writes, drop the indexes here and create them out-of-band with
``CREATE INDEX CONCURRENTLY``.
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
    # (join key), consumed_at DESC last so the NOT EXISTS subquery
    # filtering on ``consumed_at >= signal_sort_ts`` is index-only.
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "idx_trader_signal_consumption_trader_signal_consumed "
        "ON trader_signal_consumption (trader_id, signal_id, consumed_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_trade_signals_status_lower")
    op.execute("DROP INDEX IF EXISTS idx_trade_signals_source_lower")
    op.execute("DROP INDEX IF EXISTS idx_trader_signal_consumption_trader_signal_consumed")
