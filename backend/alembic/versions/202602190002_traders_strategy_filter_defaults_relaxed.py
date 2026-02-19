"""Relax legacy traders_confluence default hard filters.

Revision ID: 202602190002
Revises: 202602190001
Create Date: 2026-02-19 11:00:00.000000
"""

from __future__ import annotations

from datetime import datetime, timezone
import json

from alembic import op
import sqlalchemy as sa
from alembic_helpers import column_names, table_names


revision = "202602190002"
down_revision = "202602190001"
branch_labels = None
depends_on = None


_LEGACY_TRADER_DEFAULTS = {
    "min_confidence": 0.45,
    "min_tier": "high",
    "min_wallet_count": 2,
    "max_entry_price": 0.85,
    "firehose_require_tradable_market": True,
    "firehose_exclude_crypto_markets": True,
    "firehose_require_qualified_source": True,
    "firehose_max_age_minutes": 180,
    "firehose_source_scope": "all",
    "firehose_side_filter": "all",
}


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_config(value: object) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return dict(parsed)
        except Exception:
            return {}
    return {}


def _matches_legacy_defaults(config: dict) -> bool:
    for key, expected in _LEGACY_TRADER_DEFAULTS.items():
        if config.get(key) != expected:
            return False
    return True


def upgrade() -> None:
    if "strategies" not in table_names():
        return

    existing_columns = set(column_names("strategies"))
    required = {"id", "slug", "config"}
    if not required.issubset(existing_columns):
        return

    bind = op.get_bind()
    rows = bind.execute(
        sa.text("SELECT id, config FROM strategies WHERE lower(slug) = :slug"),
        {"slug": "traders_confluence"},
    ).fetchall()
    if not rows:
        return

    has_updated_at = "updated_at" in existing_columns
    now_value = _utcnow_naive()

    for row in rows:
        strategy_id = str(row[0] or "").strip()
        config = _parse_config(row[1])
        if not strategy_id or not _matches_legacy_defaults(config):
            continue

        config["firehose_require_tradable_market"] = False
        config["firehose_exclude_crypto_markets"] = False
        config["firehose_max_age_minutes"] = 720
        payload = json.dumps(config, ensure_ascii=True, separators=(",", ":"), sort_keys=True)

        if has_updated_at:
            bind.execute(
                sa.text("UPDATE strategies SET config = :config, updated_at = :updated_at WHERE id = :id"),
                {"id": strategy_id, "config": payload, "updated_at": now_value},
            )
        else:
            bind.execute(
                sa.text("UPDATE strategies SET config = :config WHERE id = :id"),
                {"id": strategy_id, "config": payload},
            )


def downgrade() -> None:
    return
