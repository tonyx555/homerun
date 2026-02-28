"""Add strategy dead-letter queue and scanner SLO incident persistence.

Revision ID: 202602280006
Revises: 202602280005
Create Date: 2026-02-28
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from alembic_helpers import column_names, index_names, table_names


revision = "202602280006"
down_revision = "202602280005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    tables = table_names()

    if "strategy_dead_letter_queue" not in tables:
        op.create_table(
            "strategy_dead_letter_queue",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("source", sa.String(), nullable=False, server_default=sa.text("'scanner'")),
            sa.Column("batch_id", sa.String(), nullable=True),
            sa.Column("strategy_type", sa.String(), nullable=False, server_default=sa.text("'unknown'")),
            sa.Column("opportunities_json", sa.JSON(), nullable=False, server_default=sa.text("'[]'::json")),
            sa.Column("status_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
            sa.Column("first_failed_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("last_failed_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("lease_owner", sa.String(), nullable=True),
            sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
            sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("processed_at", sa.DateTime(), nullable=True),
            sa.Column("terminal", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("error", sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )

    strategy_dlq_indexes = index_names("strategy_dead_letter_queue")
    if "idx_strategy_dead_letter_pending" not in strategy_dlq_indexes:
        op.create_index(
            "idx_strategy_dead_letter_pending",
            "strategy_dead_letter_queue",
            ["processed_at", "first_failed_at"],
            unique=False,
        )
    if "idx_strategy_dead_letter_lease" not in strategy_dlq_indexes:
        op.create_index(
            "idx_strategy_dead_letter_lease",
            "strategy_dead_letter_queue",
            ["lease_expires_at"],
            unique=False,
        )
    if "idx_strategy_dead_letter_strategy" not in strategy_dlq_indexes:
        op.create_index(
            "idx_strategy_dead_letter_strategy",
            "strategy_dead_letter_queue",
            ["strategy_type", "processed_at"],
            unique=False,
        )

    if "scanner_slo_incidents" not in tables:
        op.create_table(
            "scanner_slo_incidents",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("metric", sa.String(), nullable=False),
            sa.Column("severity", sa.String(), nullable=False, server_default=sa.text("'warning'")),
            sa.Column("status", sa.String(), nullable=False, server_default=sa.text("'open'")),
            sa.Column("threshold_value", sa.Float(), nullable=True),
            sa.Column("observed_value", sa.Float(), nullable=True),
            sa.Column("details_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
            sa.Column("opened_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("last_seen_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("resolved_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )

    scanner_slo_indexes = index_names("scanner_slo_incidents")
    if "ix_scanner_slo_incidents_metric" not in scanner_slo_indexes:
        op.create_index("ix_scanner_slo_incidents_metric", "scanner_slo_incidents", ["metric"], unique=False)
    if "idx_scanner_slo_incidents_status" not in scanner_slo_indexes:
        op.create_index(
            "idx_scanner_slo_incidents_status",
            "scanner_slo_incidents",
            ["status", "opened_at"],
            unique=False,
        )
    if "idx_scanner_slo_incidents_metric_status" not in scanner_slo_indexes:
        op.create_index(
            "idx_scanner_slo_incidents_metric_status",
            "scanner_slo_incidents",
            ["metric", "status"],
            unique=False,
        )
    if "idx_scanner_slo_incidents_last_seen" not in scanner_slo_indexes:
        op.create_index(
            "idx_scanner_slo_incidents_last_seen",
            "scanner_slo_incidents",
            ["last_seen_at"],
            unique=False,
        )

    if "scanner_control" in tables:
        control_columns = column_names("scanner_control")
        if "heavy_lane_forced_degraded" not in control_columns:
            op.add_column(
                "scanner_control",
                sa.Column(
                    "heavy_lane_forced_degraded",
                    sa.Boolean(),
                    nullable=True,
                    server_default=sa.text("false"),
                ),
            )
            op.execute(sa.text("UPDATE scanner_control SET heavy_lane_forced_degraded = false WHERE heavy_lane_forced_degraded IS NULL"))
            op.alter_column(
                "scanner_control",
                "heavy_lane_forced_degraded",
                nullable=False,
                server_default=None,
            )

        control_columns = column_names("scanner_control")
        if "heavy_lane_degraded_reason" not in control_columns:
            op.add_column("scanner_control", sa.Column("heavy_lane_degraded_reason", sa.Text(), nullable=True))

        control_columns = column_names("scanner_control")
        if "heavy_lane_degraded_until" not in control_columns:
            op.add_column("scanner_control", sa.Column("heavy_lane_degraded_until", sa.DateTime(), nullable=True))


def downgrade() -> None:
    return
