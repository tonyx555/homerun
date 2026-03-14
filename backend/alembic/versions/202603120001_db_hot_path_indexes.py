"""Add DB hot-path indexes for rescore and market-time lookups.

Revision ID: 202603120001
Revises: 202603110004
Create Date: 2026-03-12 09:35:00
"""

from __future__ import annotations

from alembic import op

from alembic_helpers import index_names


revision = "202603120001"
down_revision = "202603110004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    context = op.get_context()

    discovered_indexes = index_names("discovered_wallets")
    if "idx_discovered_insider_rescore_queue" not in discovered_indexes:
        with context.autocommit_block():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_discovered_insider_rescore_queue
                ON discovered_wallets (
                    last_analyzed_at DESC NULLS LAST,
                    insider_last_scored_at,
                    total_trades
                )
                WHERE total_trades >= 30
                """
            )

    wallet_activity_indexes = index_names("wallet_activity_rollups")
    if "idx_war_market_traded" not in wallet_activity_indexes:
        with context.autocommit_block():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_war_market_traded
                ON wallet_activity_rollups (market_id, traded_at)
                """
            )

    news_finding_indexes = index_names("news_workflow_findings")
    if "idx_news_finding_market_created" not in news_finding_indexes:
        with context.autocommit_block():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_news_finding_market_created
                ON news_workflow_findings (market_id, created_at)
                """
            )


def downgrade() -> None:
    pass
