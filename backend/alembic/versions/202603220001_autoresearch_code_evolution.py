"""Add code evolution columns to autoresearch tables.

Revision ID: 202603220001
Revises: 202603210002
Create Date: 2026-03-22
"""

import sqlalchemy as sa
from alembic import op

revision = "202603220001"
down_revision = "202603210002"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    # -- autoresearch_experiments: code evolution columns --
    cols = _column_names("autoresearch_experiments")
    new_cols = [
        ("mode", sa.String(), "params"),
        ("strategy_id", sa.String(), None),
        ("best_source_code", sa.Text(), None),
        ("best_version", sa.Integer(), None),
    ]
    for col_name, col_type, default in new_cols:
        if col_name not in cols:
            op.add_column(
                "autoresearch_experiments",
                sa.Column(col_name, col_type, nullable=True, server_default=default),
            )

    # -- autoresearch_iterations: code snapshot columns --
    iter_cols = _column_names("autoresearch_iterations")
    iter_new = [
        ("source_code_snapshot", sa.Text(), None),
        ("source_diff", sa.Text(), None),
        ("validation_result_json", sa.JSON(), None),
    ]
    for col_name, col_type, default in iter_new:
        if col_name not in iter_cols:
            op.add_column(
                "autoresearch_iterations",
                sa.Column(col_name, col_type, nullable=True, server_default=default),
            )

    # -- app_settings: mode column --
    settings_cols = _column_names("app_settings")
    if "autoresearch_mode" not in settings_cols:
        op.add_column(
            "app_settings",
            sa.Column("autoresearch_mode", sa.String(), nullable=True, server_default="params"),
        )


def downgrade() -> None:
    for col in ["mode", "strategy_id", "best_source_code", "best_version"]:
        op.drop_column("autoresearch_experiments", col)
    for col in ["source_code_snapshot", "source_diff", "validation_result_json"]:
        op.drop_column("autoresearch_iterations", col)
    op.drop_column("app_settings", "autoresearch_mode")
