"""drop legacy machine learning tables and training artifacts

Revision ID: 202604020002
Revises: 202604020001
Create Date: 2026-04-02
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "202604020002"
down_revision = "202604020001"
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return set(inspector.get_table_names())


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def _index_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {index["name"] for index in inspector.get_indexes(table_name)}


def upgrade() -> None:
    tables = _table_names()

    if "ml_training_snapshots" in tables and "task_key" not in _column_names("ml_training_snapshots"):
        op.add_column(
            "ml_training_snapshots",
            sa.Column(
                "task_key",
                sa.String(),
                nullable=False,
                server_default=sa.text("'crypto_directional'"),
            ),
        )
        op.execute(
            "UPDATE ml_training_snapshots SET task_key = 'crypto_directional' "
            "WHERE task_key IS NULL OR btrim(task_key) = ''"
        )

    if "ml_training_snapshots" in tables and "idx_mlt_task_asset_tf_ts" not in _index_names("ml_training_snapshots"):
        op.create_index(
            "idx_mlt_task_asset_tf_ts",
            "ml_training_snapshots",
            ["task_key", "asset", "timeframe", "timestamp"],
        )

    for table_name in (
        "ml_model_weights",
        "ml_prediction_log",
        "ml_training_jobs",
        "ml_trained_models",
    ):
        if table_name in tables:
            op.drop_table(table_name)


def downgrade() -> None:
    pass
