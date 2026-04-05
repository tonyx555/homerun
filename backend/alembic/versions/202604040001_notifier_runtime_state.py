"""add notifier runtime state

Revision ID: 202604040001
Revises: 202604030002
Create Date: 2026-04-04 22:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "202604040001"
down_revision = "202604030002"
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return set(inspector.get_table_names())


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    if "notifier_runtime_state" not in _table_names():
        op.create_table(
            "notifier_runtime_state",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("close_alert_markers_json", sa.JSON(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
        return

    existing = _column_names("notifier_runtime_state")
    if "close_alert_markers_json" not in existing:
        op.add_column("notifier_runtime_state", sa.Column("close_alert_markers_json", sa.JSON(), nullable=True))
    if "updated_at" not in existing:
        op.add_column("notifier_runtime_state", sa.Column("updated_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    pass
