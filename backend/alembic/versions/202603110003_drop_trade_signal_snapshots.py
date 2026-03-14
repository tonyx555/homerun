"""Drop projected trade signal snapshot table.

Revision ID: 202603110003
Revises: 202603110002
Create Date: 2026-03-11 13:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "202603110003"
down_revision = "202603110002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "trade_signal_snapshots" in set(inspector.get_table_names()):
        op.drop_table("trade_signal_snapshots")


def downgrade() -> None:
    pass
