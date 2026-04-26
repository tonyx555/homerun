"""Add Chainlink Data Streams credentials to app_settings.

Revision ID: 202604250002
Revises: 202604250001
Create Date: 2026-04-25

Adds the two columns that back the direct Chainlink Data Streams feed
introduced in ``services/chainlink_direct_feed.py``. These are stored
encrypted via the same ``decrypt_secret`` path the other API key
columns use.
"""

from alembic import op
import sqlalchemy as sa


revision = "202604250002"
down_revision = "202604250001"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    columns = _column_names("app_settings")
    if "chainlink_direct_api_key" not in columns:
        op.add_column(
            "app_settings",
            sa.Column("chainlink_direct_api_key", sa.String(), nullable=True),
        )
    if "chainlink_direct_user_secret" not in columns:
        op.add_column(
            "app_settings",
            sa.Column("chainlink_direct_user_secret", sa.String(), nullable=True),
        )


def downgrade() -> None:
    columns = _column_names("app_settings")
    if "chainlink_direct_user_secret" in columns:
        op.drop_column("app_settings", "chainlink_direct_user_secret")
    if "chainlink_direct_api_key" in columns:
        op.drop_column("app_settings", "chainlink_direct_api_key")
