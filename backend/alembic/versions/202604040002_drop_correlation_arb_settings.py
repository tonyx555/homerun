"""drop correlation arb settings

Revision ID: 202604040002
Revises: 202604040001
Create Date: 2026-04-04 23:35:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "202604040002"
down_revision = "202604040001"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    existing = _column_names("app_settings")
    for column_name in (
        "correlation_arb_enabled",
        "correlation_arb_min_correlation",
        "correlation_arb_min_divergence",
    ):
        if column_name in existing:
            op.drop_column("app_settings", column_name)


def downgrade() -> None:
    pass
