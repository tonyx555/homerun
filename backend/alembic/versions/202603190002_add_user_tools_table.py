"""Add user_tools table.

Revision ID: 202603190002
Revises: 202603190001
Create Date: 2026-03-19 00:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from alembic_helpers import table_names


revision = "202603190002"
down_revision = "202603190001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if "user_tools" not in table_names():
        op.create_table(
            "user_tools",
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("name", sa.String(), nullable=False, unique=True),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("tool_type", sa.String(), server_default=sa.text("'function'")),
            sa.Column("parameters_schema", sa.JSON(), nullable=True),
            sa.Column("implementation", sa.Text(), nullable=True),
            sa.Column("is_builtin", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
        )


def downgrade() -> None:
    pass
