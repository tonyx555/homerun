"""Add strategy runtime revision counters for worker hot-reload polling.

Revision ID: 202602180001
Revises: 202602170005
Create Date: 2026-02-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from alembic_helpers import index_names, table_names


revision = "202602180001"
down_revision = "202602170005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if "strategy_runtime_revisions" not in table_names():
        op.create_table(
            "strategy_runtime_revisions",
            sa.Column("scope", sa.String(), nullable=False),
            sa.Column("revision", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("scope"),
        )

    existing_indexes = index_names("strategy_runtime_revisions")
    if "idx_strategy_runtime_revisions_updated" not in existing_indexes:
        op.create_index(
            "idx_strategy_runtime_revisions_updated",
            "strategy_runtime_revisions",
            ["updated_at"],
            unique=False,
        )


def downgrade() -> None:
    pass
