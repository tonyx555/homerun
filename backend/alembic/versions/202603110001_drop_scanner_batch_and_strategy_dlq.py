"""Drop legacy scanner batch and strategy dead-letter queue tables.

Revision ID: 202603110001
Revises: 202603090001
Create Date: 2026-03-11 12:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "202603110001"
down_revision = "202603090001"
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return set(inspector.get_table_names())


def upgrade() -> None:
    existing_tables = _table_names()
    if "strategy_dead_letter_queue" in existing_tables:
        op.drop_table("strategy_dead_letter_queue")
    if "scanner_batch_queue" in existing_tables:
        op.drop_table("scanner_batch_queue")


def downgrade() -> None:
    pass
