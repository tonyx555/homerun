"""Add allow_network_access setting to app_settings.

Revision ID: 202603060001
Revises: 202603050001
"""

from alembic import op
import sqlalchemy as sa

from alembic_helpers import column_names


revision = "202603060001"
down_revision = "202603050001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    existing = column_names("app_settings")
    if "allow_network_access" not in existing:
        op.add_column(
            "app_settings",
            sa.Column("allow_network_access", sa.Boolean(), nullable=False, server_default=sa.false()),
        )


def downgrade() -> None:
    pass
