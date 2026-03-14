"""Drop trader runtime foreign keys to projected trade signals.

Revision ID: 202603110002
Revises: 202603110001
Create Date: 2026-03-11 12:30:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "202603110002"
down_revision = "202603110001"
branch_labels = None
depends_on = None


def _drop_trade_signal_fk(table_name: str, column_name: str = "signal_id") -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_names = set(inspector.get_table_names())
    if table_name not in table_names:
        return
    for foreign_key in inspector.get_foreign_keys(table_name):
        constrained_columns = set(foreign_key.get("constrained_columns") or [])
        referred_table = str(foreign_key.get("referred_table") or "")
        constraint_name = foreign_key.get("name")
        if referred_table != "trade_signals":
            continue
        if column_name not in constrained_columns:
            continue
        if not constraint_name:
            continue
        op.drop_constraint(constraint_name, table_name, type_="foreignkey")


def upgrade() -> None:
    for table_name in (
        "trader_decisions",
        "trader_orders",
        "execution_sessions",
        "trader_signal_consumption",
        "strategy_experiment_assignments",
    ):
        _drop_trade_signal_fk(table_name)


def downgrade() -> None:
    pass
