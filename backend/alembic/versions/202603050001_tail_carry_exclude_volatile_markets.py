"""Backfill tail_end_carry keyword exclusions for live-event volatility.

Adds lol:, win on, and counter-strike to the exclude_market_keywords list
for all traders using the tail_end_carry strategy.  These market categories
suffer from intra-event price inversions that trigger the 0.50 stop on
positions that would otherwise resolve profitably.

Revision ID: 202603050001
Revises: 202603030001
Create Date: 2026-03-05
"""

from __future__ import annotations

import json

import sqlalchemy as sa
from alembic import op
from alembic_helpers import table_names


revision = "202603050001"
down_revision = "202603030001"
branch_labels = None
depends_on = None

_NEW_KEYWORDS = ["lol:", "win on", "counter-strike"]


def upgrade() -> None:
    if "traders" not in table_names():
        return

    conn = op.get_bind()
    rows = conn.execute(
        sa.text(
            "SELECT id, source_configs_json FROM traders "
            "WHERE source_configs_json::text ILIKE '%tail_end_carry%'"
        )
    ).fetchall()

    for row in rows:
        trader_id = row[0]
        configs = row[1] if isinstance(row[1], list) else json.loads(row[1]) if isinstance(row[1], str) else []

        changed = False
        for cfg in configs:
            if not isinstance(cfg, dict):
                continue
            if cfg.get("strategy_key") != "tail_end_carry":
                continue
            params = cfg.setdefault("strategy_params", {})
            existing = params.get("exclude_market_keywords", [])
            if isinstance(existing, str):
                existing = [t.strip() for t in existing.split(",") if t.strip()]
            existing_lower = {kw.lower() for kw in existing}
            for kw in _NEW_KEYWORDS:
                if kw.lower() not in existing_lower:
                    existing.append(kw)
                    changed = True
            if changed:
                params["exclude_market_keywords"] = existing

        if changed:
            conn.execute(
                sa.text("UPDATE traders SET source_configs_json = :cfg WHERE id = :tid"),
                {"cfg": json.dumps(configs), "tid": trader_id},
            )


def downgrade() -> None:
    return
