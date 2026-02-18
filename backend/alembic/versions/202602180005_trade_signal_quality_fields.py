"""Add quality_passed and quality_rejection_reasons to trade_signals.

Revision ID: 202602180005
Revises: 202602180004
Create Date: 2026-02-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from alembic_helpers import column_names


revision = "202602180005"
down_revision = "202602180004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    existing = column_names("trade_signals")

    additions: list[sa.Column] = [
        sa.Column("quality_passed", sa.Boolean(), nullable=True),
        sa.Column("quality_rejection_reasons", sa.JSON(), nullable=True),
    ]

    for col in additions:
        if col.name not in existing:
            op.add_column("trade_signals", col)


def downgrade() -> None:
    pass
