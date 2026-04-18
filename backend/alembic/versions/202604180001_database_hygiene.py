"""Database hygiene: drop dead opportunity_events, add trade_signal retention, drop unused indexes.

Revision ID: 202604180001
Revises: 202604140001
Create Date: 2026-04-18
"""

from alembic import op
import sqlalchemy as sa


revision = "202604180001"
down_revision = "202604140001"
branch_labels = None
depends_on = None


def _table_exists(name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return name in set(inspector.get_table_names())


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def _index_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {idx["name"] for idx in inspector.get_indexes(table_name)}


# Indexes confirmed unused by pg_stat_user_indexes (idx_scan = 0, stats never
# reset). Grouped by parent table for readability.
_UNUSED_INDEXES: list[tuple[str, str]] = [
    ("trade_signal_emissions", "idx_trade_signal_emissions_signal_created"),
    ("trade_signal_emissions", "idx_trade_signal_emissions_source_created"),
    ("trade_signal_emissions", "ix_trade_signal_emissions_signal_id"),
    ("trade_signal_emissions", "ix_trade_signal_emissions_market_id"),
    ("trade_signal_emissions", "ix_trade_signal_emissions_source"),
    ("trade_signal_emissions", "ix_trade_signal_emissions_event_type"),
    ("wallet_monitor_events", "idx_wme_tx_log"),
    ("wallet_monitor_events", "idx_wme_tx_hash"),
    ("wallet_monitor_events", "idx_wme_order_hash"),
    ("wallet_activity_rollups", "idx_war_market_side_time"),
    ("discovered_wallets", "idx_discovered_win_rate"),
    ("discovered_wallets", "idx_discovered_pnl"),
    ("discovered_wallets", "idx_discovered_insider_score"),
    ("data_source_records", "idx_data_source_records_slug_ingested"),
]


def upgrade() -> None:
    # 1. Drop opportunity_events. The writer was removed in a prior refactor;
    # the table has been stale since April 6 and the reader fallback has been
    # deleted in the same changeset that ships this migration.
    if _table_exists("opportunity_events"):
        op.drop_table("opportunity_events")

    # 2. Add trade_signals retention setting (nullable, defaults to 30 on new rows).
    if "cleanup_trade_signal_days" not in _column_names("app_settings"):
        op.add_column(
            "app_settings",
            sa.Column(
                "cleanup_trade_signal_days",
                sa.Integer(),
                nullable=True,
                server_default=sa.text("30"),
            ),
        )

    # 3. Drop unused secondary indexes. Reclaims ~4 GB on the production DB.
    for table_name, index_name in _UNUSED_INDEXES:
        if index_name in _index_names(table_name):
            op.drop_index(index_name, table_name=table_name)


def downgrade() -> None:
    pass
