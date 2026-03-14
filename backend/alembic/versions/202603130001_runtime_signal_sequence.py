"""Add runtime signal sequence columns for hot-path sequencing.

Revision ID: 202603130001
Revises: 202603120001
Create Date: 2026-03-13 05:05:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from alembic_helpers import column_names, index_names


revision = "202603130001"
down_revision = "202603120001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    trade_signal_columns = column_names("trade_signals")
    if "runtime_sequence" not in trade_signal_columns:
        op.add_column("trade_signals", sa.Column("runtime_sequence", sa.BigInteger(), nullable=True))

    trader_cursor_columns = column_names("trader_signal_cursor")
    if "last_runtime_sequence" not in trader_cursor_columns:
        op.add_column("trader_signal_cursor", sa.Column("last_runtime_sequence", sa.BigInteger(), nullable=True))

    trade_signal_indexes = index_names("trade_signals")
    if "idx_trade_signals_runtime_sequence" not in trade_signal_indexes:
        op.create_index(
            "idx_trade_signals_runtime_sequence",
            "trade_signals",
            ["runtime_sequence"],
            unique=False,
        )
    if "idx_trade_signals_source_status_sequence" not in trade_signal_indexes:
        op.create_index(
            "idx_trade_signals_source_status_sequence",
            "trade_signals",
            ["source", "status", "runtime_sequence"],
            unique=False,
        )


def downgrade() -> None:
    pass
