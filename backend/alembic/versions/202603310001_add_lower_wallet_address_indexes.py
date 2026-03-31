"""add functional indexes for wallet address and source_flags queries

Revision ID: 202603310001
Revises: 77a2c87e00b2
Create Date: 2026-03-31

The trader_data_access queries use func.lower(wallet_address) IN (...) which
defeats the plain B-tree index on wallet_address.  Adding expression indexes
lets Postgres use an index scan instead of a sequential scan + sort, fixing
the QueryCanceledError (statement timeout) on trader_group_members.

Also adds a GIN index on discovered_wallets.source_flags cast to jsonb,
fixing the full-table-scan timeout when the smart_wallet_pool checks for
pool_manual_include/pool_manual_exclude/pool_blacklisted flags.
"""
from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "202603310001"
down_revision = "77a2c87e00b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "idx_trader_group_member_wallet_lower "
        "ON trader_group_members (lower(wallet_address))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "idx_discovered_wallet_address_lower "
        "ON discovered_wallets (lower(address))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "idx_tracked_wallet_address_lower "
        "ON tracked_wallets (lower(address))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS "
        "idx_discovered_wallet_source_flags_gin "
        "ON discovered_wallets "
        "USING gin (CAST(COALESCE(source_flags, '{}'::json) AS jsonb))"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_trader_group_member_wallet_lower")
    op.execute("DROP INDEX IF EXISTS idx_discovered_wallet_address_lower")
    op.execute("DROP INDEX IF EXISTS idx_tracked_wallet_address_lower")
    op.execute("DROP INDEX IF EXISTS idx_discovered_wallet_source_flags_gin")
