"""Collapse multi-entry trader source_configs to a single entry.

Revision ID: 202604260001
Revises: 202604250002
Create Date: 2026-04-26

A trader runs exactly one strategy on one source. The schema previously
allowed an array of per-source configs but no part of the runtime ever
used more than the first entry meaningfully. Tighten the invariant by
collapsing any existing rows with len(source_configs_json) > 1 down to
their first entry. The API + service-layer validators reject any future
multi-entry payloads.
"""

from __future__ import annotations

import json

import sqlalchemy as sa
from alembic import op


revision = "202604260001"
down_revision = "202604250002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    trader_table = sa.table(
        "traders",
        sa.column("id", sa.String()),
        sa.column("source_configs_json", sa.JSON()),
    )

    rows = (
        bind.execute(
            sa.select(trader_table.c.id, trader_table.c.source_configs_json)
        )
        .mappings()
        .all()
    )

    for row in rows:
        configs = row.get("source_configs_json")
        if isinstance(configs, str):
            try:
                configs = json.loads(configs)
            except (ValueError, TypeError):
                continue
        if not isinstance(configs, list) or len(configs) <= 1:
            continue
        bind.execute(
            trader_table.update()
            .where(trader_table.c.id == row["id"])
            .values(source_configs_json=[configs[0]])
        )


def downgrade() -> None:
    # Lossy collapse cannot be reversed. The dropped per-source configs
    # are gone; leaving the rows as-is is the best we can do.
    pass
