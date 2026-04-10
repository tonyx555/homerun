"""Split scanner market history out of scanner snapshot

Revision ID: 202604070002
Revises: 202604070001
Create Date: 2026-04-07
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "202604070002"
down_revision = "202604070001"
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    tables = _table_names()
    if "scanner_market_history" not in tables:
        op.create_table(
            "scanner_market_history",
            sa.Column("market_id", sa.String(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.Column("points_json", sa.JSON(), nullable=True),
            sa.PrimaryKeyConstraint("market_id"),
        )

    if "scanner_snapshot" not in tables:
        return
    columns = _column_names("scanner_snapshot")
    if "market_history_json" not in columns:
        return

    snapshot = sa.table(
        "scanner_snapshot",
        sa.column("id", sa.String()),
        sa.column("market_history_json", sa.JSON()),
    )
    row = bind.execute(
        sa.select(snapshot.c.market_history_json).where(snapshot.c.id == "latest")
    ).first()
    market_history = row[0] if row else None
    if isinstance(market_history, dict):
        history_table = sa.table(
            "scanner_market_history",
            sa.column("market_id", sa.String()),
            sa.column("updated_at", sa.DateTime()),
            sa.column("points_json", sa.JSON()),
        )
        now = bind.execute(sa.select(sa.func.now())).scalar_one()
        rows: list[dict[str, object]] = []
        for raw_market_id, raw_points in market_history.items():
            market_id = str(raw_market_id or "").strip().lower()
            points = [dict(point) for point in raw_points if isinstance(point, dict)] if isinstance(raw_points, list) else []
            if not market_id or len(points) < 2:
                continue
            rows.append(
                {
                    "market_id": market_id,
                    "updated_at": now,
                    "points_json": points,
                }
            )
        if rows:
            bind.execute(sa.insert(history_table), rows)

    op.drop_column("scanner_snapshot", "market_history_json")


def downgrade() -> None:
    pass
