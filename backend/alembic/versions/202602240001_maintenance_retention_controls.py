"""Add maintenance retention controls for signal emissions and wallet activity rollups.

Revision ID: 202602240001
Revises: 202602230003
Create Date: 2026-02-24 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from alembic_helpers import column_names, table_names


revision = "202602240001"
down_revision = "202602230003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table_name = "app_settings"
    if table_name not in table_names():
        return

    existing = column_names(table_name)
    columns = (
        sa.Column(
            "cleanup_trade_signal_emission_days",
            sa.Integer(),
            nullable=True,
            server_default=sa.text("21"),
        ),
        sa.Column(
            "cleanup_trade_signal_update_days",
            sa.Integer(),
            nullable=True,
            server_default=sa.text("3"),
        ),
        sa.Column(
            "cleanup_wallet_activity_rollup_days",
            sa.Integer(),
            nullable=True,
            server_default=sa.text("60"),
        ),
        sa.Column(
            "cleanup_wallet_activity_dedupe_enabled",
            sa.Boolean(),
            nullable=True,
            server_default=sa.text("true"),
        ),
    )
    for col in columns:
        if col.name not in existing:
            op.add_column(table_name, col)


def downgrade() -> None:
    return
