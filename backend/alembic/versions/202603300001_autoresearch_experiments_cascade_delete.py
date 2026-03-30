"""Add CASCADE delete to autoresearch_experiments.trader_id FK.

Revision ID: 202603300001
Revises: 202603140002
Create Date: 2026-03-30 00:00:00
"""

from __future__ import annotations

from alembic import op

revision = "202603300001"
down_revision = "202603140002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "autoresearch_experiments_trader_id_fkey",
        "autoresearch_experiments",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "autoresearch_experiments_trader_id_fkey",
        "autoresearch_experiments",
        "traders",
        ["trader_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(
        "autoresearch_experiments_trader_id_fkey",
        "autoresearch_experiments",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "autoresearch_experiments_trader_id_fkey",
        "autoresearch_experiments",
        "traders",
        ["trader_id"],
        ["id"],
    )
