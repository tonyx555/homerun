"""trader order verification cutover

Revision ID: 202604030002
Revises: 202604030001
Create Date: 2026-04-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "202604030002"
down_revision = "202604030001"
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


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
    table_name = "trader_orders"
    existing = _column_names(table_name)
    indexes = _index_names(table_name)

    columns = (
        sa.Column("execution_wallet_address", sa.String(), nullable=True),
        sa.Column("provider_order_id", sa.String(), nullable=True),
        sa.Column("provider_clob_order_id", sa.String(), nullable=True),
        sa.Column("verification_status", sa.String(), nullable=False, server_default=sa.text("'local'")),
        sa.Column("verification_source", sa.String(), nullable=True),
        sa.Column("verification_reason", sa.Text(), nullable=True),
        sa.Column("verification_tx_hash", sa.String(), nullable=True),
        sa.Column("verified_at", sa.DateTime(), nullable=True),
    )
    for column in columns:
        if column.name not in existing:
            op.add_column(table_name, column)

    if "idx_trader_orders_execution_wallet_address" not in indexes:
        op.create_index("idx_trader_orders_execution_wallet_address", table_name, ["execution_wallet_address"], unique=False)
    if "idx_trader_orders_provider_order_id" not in indexes:
        op.create_index("idx_trader_orders_provider_order_id", table_name, ["provider_order_id"], unique=False)
    if "idx_trader_orders_provider_clob_order_id" not in indexes:
        op.create_index("idx_trader_orders_provider_clob_order_id", table_name, ["provider_clob_order_id"], unique=False)
    if "idx_trader_orders_verification_status" not in indexes:
        op.create_index("idx_trader_orders_verification_status", table_name, ["verification_status"], unique=False)
    if "idx_trader_orders_verification_tx_hash" not in indexes:
        op.create_index("idx_trader_orders_verification_tx_hash", table_name, ["verification_tx_hash"], unique=False)

    if "trader_order_verification_events" not in _table_names():
        op.create_table(
            "trader_order_verification_events",
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column(
                "trader_order_id",
                sa.String(),
                sa.ForeignKey("trader_orders.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("verification_status", sa.String(), nullable=False, server_default=sa.text("'local'")),
            sa.Column("source", sa.String(), nullable=True),
            sa.Column("event_type", sa.String(), nullable=False),
            sa.Column("reason", sa.Text(), nullable=True),
            sa.Column("provider_order_id", sa.String(), nullable=True),
            sa.Column("provider_clob_order_id", sa.String(), nullable=True),
            sa.Column("execution_wallet_address", sa.String(), nullable=True),
            sa.Column("tx_hash", sa.String(), nullable=True),
            sa.Column("payload_json", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        )

    verification_indexes = _index_names("trader_order_verification_events")
    if "idx_tove_trader_order_id" not in verification_indexes:
        op.create_index("idx_tove_trader_order_id", "trader_order_verification_events", ["trader_order_id"], unique=False)
    if "idx_tove_verification_status" not in verification_indexes:
        op.create_index("idx_tove_verification_status", "trader_order_verification_events", ["verification_status"], unique=False)
    if "idx_tove_source" not in verification_indexes:
        op.create_index("idx_tove_source", "trader_order_verification_events", ["source"], unique=False)
    if "idx_tove_event_type" not in verification_indexes:
        op.create_index("idx_tove_event_type", "trader_order_verification_events", ["event_type"], unique=False)
    if "idx_tove_provider_order_id" not in verification_indexes:
        op.create_index("idx_tove_provider_order_id", "trader_order_verification_events", ["provider_order_id"], unique=False)
    if "idx_tove_provider_clob_order_id" not in verification_indexes:
        op.create_index("idx_tove_provider_clob_order_id", "trader_order_verification_events", ["provider_clob_order_id"], unique=False)
    if "idx_tove_execution_wallet_address" not in verification_indexes:
        op.create_index("idx_tove_execution_wallet_address", "trader_order_verification_events", ["execution_wallet_address"], unique=False)
    if "idx_tove_tx_hash" not in verification_indexes:
        op.create_index("idx_tove_tx_hash", "trader_order_verification_events", ["tx_hash"], unique=False)
    if "idx_tove_order_created" not in verification_indexes:
        op.create_index("idx_tove_order_created", "trader_order_verification_events", ["trader_order_id", "created_at"], unique=False)
    if "idx_tove_status_created" not in verification_indexes:
        op.create_index("idx_tove_status_created", "trader_order_verification_events", ["verification_status", "created_at"], unique=False)


def downgrade() -> None:
    pass
