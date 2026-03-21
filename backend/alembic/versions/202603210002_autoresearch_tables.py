"""Add autoresearch experiment tables and AppSettings columns.

Revision ID: 202603210002
Revises: 202603210001
Create Date: 2026-03-21
"""

import sqlalchemy as sa
from alembic import op

revision = "202603210002"
down_revision = "202603210001"
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
    # -- autoresearch_experiments table --
    if not _table_exists("autoresearch_experiments"):
        op.create_table(
            "autoresearch_experiments",
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("trader_id", sa.String(), sa.ForeignKey("traders.id"), nullable=False),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("status", sa.String(), nullable=False, server_default="running"),
            sa.Column("baseline_score", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("best_score", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("best_params_json", sa.JSON(), nullable=True),
            sa.Column("iteration_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("kept_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("reverted_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("settings_json", sa.JSON(), nullable=True),
            sa.Column("started_at", sa.DateTime(), nullable=True),
            sa.Column("finished_at", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
        )
        op.create_index("idx_arx_trader_id", "autoresearch_experiments", ["trader_id"])
        op.create_index("idx_arx_status", "autoresearch_experiments", ["status"])

    # -- autoresearch_iterations table --
    if not _table_exists("autoresearch_iterations"):
        op.create_table(
            "autoresearch_iterations",
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column(
                "experiment_id",
                sa.String(),
                sa.ForeignKey("autoresearch_experiments.id"),
                nullable=False,
            ),
            sa.Column("iteration_number", sa.Integer(), nullable=False),
            sa.Column("proposed_params_json", sa.JSON(), nullable=True),
            sa.Column("baseline_score", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("new_score", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("score_delta", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("decision", sa.String(), nullable=False),
            sa.Column("reasoning", sa.Text(), nullable=True),
            sa.Column("backtest_result_json", sa.JSON(), nullable=True),
            sa.Column("changed_params_json", sa.JSON(), nullable=True),
            sa.Column("duration_seconds", sa.Float(), nullable=False, server_default="0.0"),
            sa.Column("tokens_used", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("created_at", sa.DateTime(), nullable=True),
        )
        op.create_index("idx_ari_experiment_id", "autoresearch_iterations", ["experiment_id"])
        op.create_index(
            "idx_ari_experiment_iteration",
            "autoresearch_iterations",
            ["experiment_id", "iteration_number"],
        )

    # -- AppSettings: autoresearch columns --
    cols = _column_names("app_settings")
    new_cols = [
        ("autoresearch_model", sa.String(), None),
        ("autoresearch_max_iterations", sa.Integer(), "50"),
        ("autoresearch_interval_seconds", sa.Integer(), "600"),
        ("autoresearch_temperature", sa.Float(), "0.2"),
        ("autoresearch_mandate", sa.Text(), None),
        ("autoresearch_auto_apply", sa.Boolean(), "true"),
        ("autoresearch_walk_forward_windows", sa.Integer(), "5"),
        ("autoresearch_train_ratio", sa.Float(), "0.7"),
    ]
    for col_name, col_type, default in new_cols:
        if col_name not in cols:
            op.add_column(
                "app_settings",
                sa.Column(col_name, col_type, nullable=True, server_default=default),
            )


def downgrade() -> None:
    op.drop_table("autoresearch_iterations")
    op.drop_table("autoresearch_experiments")
    for col_name in [
        "autoresearch_model",
        "autoresearch_max_iterations",
        "autoresearch_interval_seconds",
        "autoresearch_temperature",
        "autoresearch_mandate",
        "autoresearch_auto_apply",
        "autoresearch_walk_forward_windows",
        "autoresearch_train_ratio",
    ]:
        op.drop_column("app_settings", col_name)
