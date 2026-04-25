"""Add market microstructure snapshots for execution replay.

Revision ID: 202604250001
Revises: 202604180003
Create Date: 2026-04-25
"""

from alembic import op
import sqlalchemy as sa


revision = "202604250001"
down_revision = "202604180003"
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return set(inspector.get_table_names())


def _index_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in _table_names():
        return set()
    return {idx["name"] for idx in inspector.get_indexes(table_name)}


def upgrade() -> None:
    if "market_microstructure_snapshots" not in _table_names():
        op.create_table(
            "market_microstructure_snapshots",
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("provider", sa.String(), nullable=False, server_default=sa.text("'polymarket'")),
            sa.Column("token_id", sa.String(), nullable=False),
            sa.Column("snapshot_type", sa.String(), nullable=False),
            sa.Column("observed_at", sa.DateTime(), nullable=False),
            sa.Column("exchange_ts_ms", sa.BigInteger(), nullable=True),
            sa.Column("sequence", sa.BigInteger(), nullable=True),
            sa.Column("best_bid", sa.Float(), nullable=True),
            sa.Column("best_ask", sa.Float(), nullable=True),
            sa.Column("spread_bps", sa.Float(), nullable=True),
            sa.Column("bids_json", sa.JSON(), nullable=True),
            sa.Column("asks_json", sa.JSON(), nullable=True),
            sa.Column("trade_price", sa.Float(), nullable=True),
            sa.Column("trade_size", sa.Float(), nullable=True),
            sa.Column("trade_side", sa.String(), nullable=True),
            sa.Column("payload_json", sa.JSON(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        )

    indexes = _index_names("market_microstructure_snapshots")
    if "ix_market_microstructure_snapshots_provider" not in indexes:
        op.create_index(
            "ix_market_microstructure_snapshots_provider",
            "market_microstructure_snapshots",
            ["provider"],
        )
    if "ix_market_microstructure_snapshots_token_id" not in indexes:
        op.create_index(
            "ix_market_microstructure_snapshots_token_id",
            "market_microstructure_snapshots",
            ["token_id"],
        )
    if "ix_market_microstructure_snapshots_snapshot_type" not in indexes:
        op.create_index(
            "ix_market_microstructure_snapshots_snapshot_type",
            "market_microstructure_snapshots",
            ["snapshot_type"],
        )
    if "ix_market_microstructure_snapshots_observed_at" not in indexes:
        op.create_index(
            "ix_market_microstructure_snapshots_observed_at",
            "market_microstructure_snapshots",
            ["observed_at"],
        )
    if "idx_mms_token_observed" not in indexes:
        op.create_index(
            "idx_mms_token_observed",
            "market_microstructure_snapshots",
            ["token_id", "observed_at"],
        )
    if "idx_mms_token_type_observed" not in indexes:
        op.create_index(
            "idx_mms_token_type_observed",
            "market_microstructure_snapshots",
            ["token_id", "snapshot_type", "observed_at"],
        )


def downgrade() -> None:
    pass
