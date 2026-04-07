"""Add scanner eligibility counts and runtime settings

Revision ID: 202604070001
Revises: 77a2c87e00b2
Create Date: 2026-04-07
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "202604070001"
down_revision = "202604050001"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    existing = _column_names("app_settings")
    for column in (
        sa.Column("scanner_skipped_signal_reactivation_cooldown_seconds", sa.Integer(), nullable=True),
        sa.Column("scanner_strict_ws_max_age_ms", sa.Integer(), nullable=True),
    ):
        if column.name not in existing:
            op.add_column("app_settings", column)

    existing = _column_names("scanner_snapshot")
    for column in (
        sa.Column("raw_detected_count", sa.Integer(), nullable=True),
        sa.Column("displayable_count", sa.Integer(), nullable=True),
        sa.Column("execution_eligible_count", sa.Integer(), nullable=True),
        sa.Column("strategy_diagnostics_json", sa.JSON(), nullable=True),
    ):
        if column.name not in existing:
            op.add_column("scanner_snapshot", column)


def downgrade() -> None:
    pass
