"""Restore trade_signal_emissions.signal_id index required by FK enforcement.

The preceding hygiene migration dropped this index based on idx_scan = 0, but
pg_stat_user_indexes does not count FK-enforcement lookups as scans. Without
the index, every DELETE of a trade_signals row seq-scans 11M+ emissions to
apply the ON DELETE SET NULL trigger.

Revision ID: 202604180002
Revises: 202604180001
Create Date: 2026-04-18
"""

from alembic import op
import sqlalchemy as sa


revision = "202604180002"
down_revision = "202604180001"
branch_labels = None
depends_on = None


def _index_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {idx["name"] for idx in inspector.get_indexes(table_name)}


def upgrade() -> None:
    if "ix_trade_signal_emissions_signal_id" not in _index_names("trade_signal_emissions"):
        op.create_index(
            "ix_trade_signal_emissions_signal_id",
            "trade_signal_emissions",
            ["signal_id"],
        )


def downgrade() -> None:
    pass
