"""Preserve trader order history when deleting traders.

Revision ID: 202602230001
Revises: 202602220004
Create Date: 2026-02-23 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from alembic_helpers import table_names


revision = "202602230001"
down_revision = "202602220004"
branch_labels = None
depends_on = None


def _trader_order_trader_fk_names() -> list[str]:
    inspector = sa.inspect(op.get_bind())
    if "trader_orders" not in set(inspector.get_table_names()):
        return []

    names: list[str] = []
    for fk in inspector.get_foreign_keys("trader_orders"):
        constrained_columns = tuple(fk.get("constrained_columns") or ())
        referred_table = str(fk.get("referred_table") or "")
        if constrained_columns != ("trader_id",):
            continue
        if referred_table != "traders":
            continue

        name = fk.get("name")
        if isinstance(name, str) and name:
            names.append(name)
    return names


def upgrade() -> None:
    if "trader_orders" not in table_names():
        return

    for constraint_name in _trader_order_trader_fk_names():
        op.drop_constraint(constraint_name, "trader_orders", type_="foreignkey")


def downgrade() -> None:
    pass
