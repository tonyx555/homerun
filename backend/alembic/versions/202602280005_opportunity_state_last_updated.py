"""Add last_updated_at timestamp for opportunity state projection.

Revision ID: 202602280005
Revises: 202602280004
Create Date: 2026-02-28
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from alembic_helpers import index_names, table_names


revision = "202602280005"
down_revision = "202602280004"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    if "opportunity_state" not in table_names():
        return

    existing_columns = _column_names("opportunity_state")
    if "last_updated_at" not in existing_columns:
        op.add_column(
            "opportunity_state",
            sa.Column("last_updated_at", sa.DateTime(), nullable=True, server_default=sa.text("now()")),
        )
        op.execute(sa.text("UPDATE opportunity_state SET last_updated_at = COALESCE(last_seen_at, now())"))
        op.alter_column("opportunity_state", "last_updated_at", nullable=False, server_default=None)

    existing_indexes = index_names("opportunity_state")
    if "idx_opportunity_state_last_updated" not in existing_indexes:
        op.create_index(
            "idx_opportunity_state_last_updated",
            "opportunity_state",
            ["last_updated_at"],
            unique=False,
        )


def downgrade() -> None:
    return
