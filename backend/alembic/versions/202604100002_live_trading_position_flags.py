"""Add live trading position settlement visibility fields.

Revision ID: 202604100002
Revises: 202604080001
Create Date: 2026-04-10
"""

from alembic import op
import sqlalchemy as sa


revision = "202604100002"
down_revision = "202604080001"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    existing = _column_names("live_trading_positions")
    columns = [
        sa.Column("redeemable", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("counts_as_open", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("end_date", sa.String(), nullable=True),
    ]
    for column in columns:
        if column.name not in existing:
            op.add_column("live_trading_positions", column)


def downgrade() -> None:
    pass
