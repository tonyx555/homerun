"""drop removed strategy settings columns

Revision ID: 202604030001
Revises: 202604020002
Create Date: 2026-04-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "202604030001"
down_revision = "202604020002"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    table_name = "app_settings"
    existing = _column_names(table_name)
    for column_name in (
        "bayesian_cascade_enabled",
        "bayesian_min_edge_percent",
        "bayesian_propagation_depth",
        "temporal_decay_enabled",
    ):
        if column_name in existing:
            op.drop_column(table_name, column_name)


def downgrade() -> None:
    pass
