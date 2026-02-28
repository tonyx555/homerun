"""Add autotrader position-close notification toggle.

Revision ID: 202602280007
Revises: 202602280006
Create Date: 2026-02-28
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from alembic_helpers import column_names


revision = "202602280007"
down_revision = "202602280006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table_name = "app_settings"
    existing = column_names(table_name)

    if "notify_autotrader_closes" not in existing:
        op.add_column(
            table_name,
            sa.Column(
                "notify_autotrader_closes",
                sa.Boolean(),
                nullable=True,
                server_default=sa.true(),
            ),
        )


def downgrade() -> None:
    return
