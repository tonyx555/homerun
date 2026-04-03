"""add machine learning sdk artifact and deployment schema

Revision ID: 202604020001
Revises: 202604010001
Create Date: 2026-04-02
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "202604020001"
down_revision = "202604010001"
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

    if "task_key" not in _column_names("ml_training_snapshots"):
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

    training_snapshot_indexes = _index_names("ml_training_snapshots")
    if "idx_mlt_task_asset_tf_ts" not in training_snapshot_indexes:
        op.create_index(
            "idx_mlt_task_asset_tf_ts",
            "ml_training_snapshots",
            ["task_key", "asset", "timeframe", "timestamp"],
        )

    if "machine_learning_model_artifacts" not in tables:
        op.create_table(
            "machine_learning_model_artifacts",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("task_key", sa.String(), nullable=False),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("backend", sa.String(), nullable=False),
            sa.Column("status", sa.String(), nullable=False, server_default=sa.text("'ready'")),
            sa.Column("artifact_path", sa.String(), nullable=False),
            sa.Column("artifact_sha256", sa.String(), nullable=False),
            sa.Column("manifest_json", sa.JSON(), nullable=False),
            sa.Column("metrics_json", sa.JSON(), nullable=True),
            sa.Column("source_json", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.Column("archived_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("task_key", "name", name="uq_mla_task_name"),
        )

    model_indexes = _index_names("machine_learning_model_artifacts")
    if "idx_mla_task_status" not in model_indexes:
        op.create_index(
            "idx_mla_task_status",
            "machine_learning_model_artifacts",
            ["task_key", "status"],
        )
    if "idx_mla_created" not in model_indexes:
        op.create_index("idx_mla_created", "machine_learning_model_artifacts", ["created_at"])

    if "machine_learning_adapter_artifacts" not in tables:
        op.create_table(
            "machine_learning_adapter_artifacts",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("task_key", sa.String(), nullable=False),
            sa.Column("base_model_id", sa.String(), nullable=False),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("adapter_type", sa.String(), nullable=False),
            sa.Column("status", sa.String(), nullable=False, server_default=sa.text("'ready'")),
            sa.Column("artifact_path", sa.String(), nullable=False),
            sa.Column("artifact_sha256", sa.String(), nullable=False),
            sa.Column("manifest_json", sa.JSON(), nullable=False),
            sa.Column("metrics_json", sa.JSON(), nullable=True),
            sa.Column("training_source_json", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.Column("archived_at", sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(
                ["base_model_id"],
                ["machine_learning_model_artifacts.id"],
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("task_key", "name", name="uq_mlad_task_name"),
        )

    adapter_indexes = _index_names("machine_learning_adapter_artifacts")
    if "idx_mlad_task_status" not in adapter_indexes:
        op.create_index(
            "idx_mlad_task_status",
            "machine_learning_adapter_artifacts",
            ["task_key", "status"],
        )
    if "idx_mlad_base_model" not in adapter_indexes:
        op.create_index("idx_mlad_base_model", "machine_learning_adapter_artifacts", ["base_model_id"])
    if "idx_mlad_created" not in adapter_indexes:
        op.create_index("idx_mlad_created", "machine_learning_adapter_artifacts", ["created_at"])

    if "machine_learning_deployments" not in tables:
        op.create_table(
            "machine_learning_deployments",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("task_key", sa.String(), nullable=False),
            sa.Column("base_model_id", sa.String(), nullable=False),
            sa.Column("adapter_id", sa.String(), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.Column("activated_at", sa.DateTime(), nullable=False),
            sa.ForeignKeyConstraint(
                ["base_model_id"],
                ["machine_learning_model_artifacts.id"],
            ),
            sa.ForeignKeyConstraint(
                ["adapter_id"],
                ["machine_learning_adapter_artifacts.id"],
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("task_key", name="uq_mld_task_key"),
        )

    deployment_indexes = _index_names("machine_learning_deployments")
    if "idx_mld_updated" not in deployment_indexes:
        op.create_index("idx_mld_updated", "machine_learning_deployments", ["updated_at"])

    if "machine_learning_jobs" not in tables:
        op.create_table(
            "machine_learning_jobs",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("task_key", sa.String(), nullable=False),
            sa.Column("job_type", sa.String(), nullable=False),
            sa.Column("status", sa.String(), nullable=False, server_default=sa.text("'queued'")),
            sa.Column("target_id", sa.String(), nullable=True),
            sa.Column("message", sa.String(), nullable=True),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("payload_json", sa.JSON(), nullable=True),
            sa.Column("result_json", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("started_at", sa.DateTime(), nullable=True),
            sa.Column("finished_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )

    job_indexes = _index_names("machine_learning_jobs")
    if "idx_mlj_task_created" not in job_indexes:
        op.create_index("idx_mlj_task_created", "machine_learning_jobs", ["task_key", "created_at"])
    if "idx_mlj_status" not in job_indexes:
        op.create_index("idx_mlj_status", "machine_learning_jobs", ["status"])
    if "idx_mlj_created" not in job_indexes:
        op.create_index("idx_mlj_created", "machine_learning_jobs", ["created_at"])


def downgrade() -> None:
    pass
