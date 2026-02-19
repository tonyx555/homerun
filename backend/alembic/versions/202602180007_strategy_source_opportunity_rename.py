"""Replace deprecated ArbitrageOpportunity imports in strategy source code.

Revision ID: 202602180007
Revises: 202602180006
Create Date: 2026-02-18 12:05:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from alembic_helpers import table_names


# revision identifiers, used by Alembic.
revision = "202602180007"
down_revision = "202602180006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if "strategies" not in table_names():
        return

    bind = op.get_bind()
    strategies = sa.table(
        "strategies",
        sa.column("source_code", sa.Text()),
        sa.column("status", sa.String()),
        sa.column("error_message", sa.Text()),
        sa.column("version", sa.Integer()),
    )

    bind.execute(
        strategies.update()
        .where(strategies.c.source_code.like("%ArbitrageOpportunity%"))
        .values(
            source_code=sa.func.replace(strategies.c.source_code, "ArbitrageOpportunity", "Opportunity"),
            status=sa.case((strategies.c.status == "disabled", "disabled"), else_="unloaded"),
            error_message=None,
            version=sa.func.coalesce(strategies.c.version, 0) + 1,
        )
    )


def downgrade() -> None:
    return
