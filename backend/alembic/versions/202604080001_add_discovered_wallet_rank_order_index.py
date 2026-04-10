"""Add discovered wallet ranking order index

Revision ID: 202604080001
Revises: 202604070002
Create Date: 2026-04-08
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "202604080001"
down_revision = "202604070002"
branch_labels = None
depends_on = None


def _index_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {index["name"] for index in inspector.get_indexes(table_name)}


def upgrade() -> None:
    table_name = "discovered_wallets"
    index_name = "idx_discovered_pool_rank_order"
    if index_name in _index_names(table_name):
        return

    bind = op.get_bind()
    dialect = str(getattr(bind.dialect, "name", "") or "").strip().lower()
    if dialect == "postgresql":
        with op.get_context().autocommit_block():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY idx_discovered_pool_rank_order
                ON discovered_wallets (
                    composite_score DESC NULLS LAST,
                    rank_score DESC NULLS LAST,
                    total_pnl DESC NULLS LAST,
                    address ASC
                )
                """
            )
        return

    op.create_index(
        index_name,
        table_name,
        ["composite_score", "rank_score", "total_pnl", "address"],
        unique=False,
    )


def downgrade() -> None:
    pass
