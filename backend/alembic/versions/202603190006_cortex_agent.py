"""Add Cortex agent tables and AppSettings columns.

Revision ID: 202603190006
Revises: 202603190005
Create Date: 2026-03-19
"""

import sqlalchemy as sa
from alembic import op

revision = "202603190006"
down_revision = "202603190005"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def _table_exists(table_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in set(inspector.get_table_names())


def upgrade() -> None:
    # -- cortex_memory table --
    if not _table_exists("cortex_memory"):
        op.create_table(
            "cortex_memory",
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("category", sa.String(), nullable=False, server_default="observation"),
            sa.Column("content", sa.Text(), nullable=False),
            sa.Column("context_json", sa.JSON(), nullable=True),
            sa.Column("importance", sa.Float(), nullable=False, server_default="0.5"),
            sa.Column("access_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.Column("expired", sa.Boolean(), nullable=False, server_default="false"),
        )
        op.create_index("idx_cortex_memory_category", "cortex_memory", ["category"])
        op.create_index("idx_cortex_memory_importance", "cortex_memory", ["importance"])
        op.create_index("idx_cortex_memory_expired", "cortex_memory", ["expired"])

    # -- cortex_run_log table --
    if not _table_exists("cortex_run_log"):
        op.create_table(
            "cortex_run_log",
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("started_at", sa.DateTime(), nullable=True),
            sa.Column("finished_at", sa.DateTime(), nullable=True),
            sa.Column("status", sa.String(), nullable=False, server_default="running"),
            sa.Column("thinking_log", sa.Text(), nullable=True),
            sa.Column("actions_taken", sa.JSON(), nullable=True),
            sa.Column("learnings_saved", sa.JSON(), nullable=True),
            sa.Column("summary", sa.Text(), nullable=True),
            sa.Column("tokens_used", sa.Integer(), nullable=True),
            sa.Column("cost_usd", sa.Float(), nullable=True),
            sa.Column("model_used", sa.String(), nullable=True),
            sa.Column("trigger", sa.String(), nullable=False, server_default="scheduled"),
        )
        op.create_index("idx_cortex_run_started", "cortex_run_log", ["started_at"])
        op.create_index("idx_cortex_run_status", "cortex_run_log", ["status"])

    # -- AppSettings cortex columns --
    existing = _column_names("app_settings")
    cortex_columns = [
        sa.Column("cortex_enabled", sa.Boolean(), server_default="false"),
        sa.Column("cortex_model", sa.String(), nullable=True),
        sa.Column("cortex_interval_seconds", sa.Integer(), server_default="300"),
        sa.Column("cortex_max_iterations", sa.Integer(), server_default="15"),
        sa.Column("cortex_temperature", sa.Float(), server_default="0.1"),
        sa.Column("cortex_mandate", sa.Text(), nullable=True),
        sa.Column("cortex_memory_limit", sa.Integer(), server_default="20"),
        sa.Column("cortex_write_actions_enabled", sa.Boolean(), server_default="false"),
        sa.Column("cortex_notify_telegram", sa.Boolean(), server_default="false"),
    ]
    for col in cortex_columns:
        if col.name not in existing:
            op.add_column("app_settings", col)


def downgrade() -> None:
    pass
