"""Add latency_class column to traders.

Introduces a per-trader latency tier (``fast`` / ``normal`` / ``slow``) that
drives every downstream execution budget: cycle soft/hard timeouts, DB pool
isolation (``FastAsyncSessionLocal``), statement_timeout, and whether the
fast-tier runtime (fast_trader_runtime) owns the trader instead of the
shared orchestrator loop.

Default is ``normal`` — pre-existing traders keep the current execution
path and timeouts unchanged.  A trader must be explicitly flipped to
``fast`` or ``slow`` to change behaviour.

Revision ID: 202604180003
Revises: 202604180002
Create Date: 2026-04-18
"""

from alembic import op
import sqlalchemy as sa


revision = "202604180003"
down_revision = "202604180002"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {col["name"] for col in inspector.get_columns(table_name)}


def upgrade() -> None:
    existing = _column_names("traders")
    if "latency_class" not in existing:
        op.add_column(
            "traders",
            sa.Column(
                "latency_class",
                sa.String(),
                nullable=False,
                server_default=sa.text("'normal'"),
            ),
        )
        op.execute(
            sa.text(
                """
                UPDATE traders
                   SET latency_class = 'normal'
                 WHERE latency_class IS NULL
                    OR latency_class = ''
                """
            )
        )


def downgrade() -> None:
    pass
