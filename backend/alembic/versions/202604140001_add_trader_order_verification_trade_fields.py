"""Add trade fields to trader_order_verification_events.

Revision ID: 202604140001
Revises: 202604100002
Create Date: 2026-04-14
"""

from alembic import op
import sqlalchemy as sa


revision = "202604140001"
down_revision = "202604100002"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    existing = _column_names("trader_order_verification_events")
    columns = [
        sa.Column("token_id", sa.String(), nullable=True),
        sa.Column("side", sa.String(), nullable=True),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("size", sa.Float(), nullable=True),
        sa.Column("trade_timestamp", sa.DateTime(), nullable=True),
        sa.Column("trade_id", sa.String(), nullable=True),
    ]
    for column in columns:
        if column.name not in existing:
            op.add_column("trader_order_verification_events", column)

    if "token_id" not in existing:
        op.create_index(
            "idx_trader_order_verification_events_token_id",
            "trader_order_verification_events",
            ["token_id"],
        )
    if "trade_timestamp" not in existing:
        op.create_index(
            "idx_trader_order_verification_events_trade_ts",
            "trader_order_verification_events",
            ["trade_timestamp"],
        )
    if "trade_id" not in existing:
        op.create_index(
            "idx_trader_order_verification_events_trade_id",
            "trader_order_verification_events",
            ["trade_id"],
        )


def downgrade() -> None:
    pass
