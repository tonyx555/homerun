"""Add hot-path indexes for tracked trader worker queries.

Revision ID: 202603110004
Revises: 202603110003
Create Date: 2026-03-11 20:15:00
"""

from __future__ import annotations

from alembic import op

from alembic_helpers import index_names


revision = "202603110004"
down_revision = "202603110003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    wallet_activity_indexes = index_names("wallet_activity_rollups")
    if "idx_war_traded_wallet_market" not in wallet_activity_indexes:
        op.create_index(
            "idx_war_traded_wallet_market",
            "wallet_activity_rollups",
            ["traded_at", "wallet_address", "market_id"],
            unique=False,
        )

    confluence_indexes = index_names("market_confluence_signals")
    if "idx_confluence_market_outcome" not in confluence_indexes:
        op.create_index(
            "idx_confluence_market_outcome",
            "market_confluence_signals",
            ["market_id", "outcome"],
            unique=False,
        )


def downgrade() -> None:
    pass
