"""Cut over trader orchestrator simulation mode from paper to shadow.

Revision ID: 202602280008
Revises: 202602280007
Create Date: 2026-02-28
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from alembic_helpers import column_names, table_names


revision = "202602280008"
down_revision = "202602280007"
branch_labels = None
depends_on = None


def _table_has_column(table_name: str, column_name: str) -> bool:
    return table_name in table_names() and column_name in column_names(table_name)


def _normalize_mode_column(table_name: str, column_name: str) -> None:
    if not _table_has_column(table_name, column_name):
        return

    op.execute(
        sa.text(
            f"""
            UPDATE {table_name}
            SET {column_name} = LOWER(TRIM(COALESCE({column_name}, '')))
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            UPDATE {table_name}
            SET {column_name} = 'shadow'
            WHERE {column_name} IN ('', 'paper')
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            UPDATE {table_name}
            SET {column_name} = 'shadow'
            WHERE {column_name} NOT IN ('shadow', 'live')
            """
        )
    )


def upgrade() -> None:
    _normalize_mode_column("trader_orchestrator_control", "mode")
    _normalize_mode_column("traders", "mode")
    _normalize_mode_column("trader_orders", "mode")
    _normalize_mode_column("execution_sessions", "mode")
    _normalize_mode_column("trader_positions", "mode")

    if _table_has_column("trader_orchestrator_control", "settings_json"):
        op.execute(
            sa.text(
                """
                UPDATE trader_orchestrator_control
                SET settings_json = (
                    (COALESCE(settings_json, '{}'::json)::jsonb - 'paper_account_id')::json
                )
                WHERE settings_json IS NOT NULL
                """
            )
        )


def downgrade() -> None:
    return
