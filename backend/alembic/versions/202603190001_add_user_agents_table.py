"""Add user_agents table.

Revision ID: 202603190001
Revises: 202603140002
Create Date: 2026-03-19 00:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

from alembic_helpers import table_names


revision = "202603190001"
down_revision = "202603140002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if "user_agents" not in table_names():
        op.create_table(
            "user_agents",
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("description", sa.String(), nullable=True),
            sa.Column("system_prompt", sa.Text(), nullable=False),
            sa.Column("tools", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("model", sa.String(), nullable=True),
            sa.Column("temperature", sa.Float(), nullable=False, server_default=sa.text("0.0")),
            sa.Column("max_iterations", sa.Integer(), nullable=False, server_default=sa.text("10")),
            sa.Column("is_builtin", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
        )


def downgrade() -> None:
    pass
