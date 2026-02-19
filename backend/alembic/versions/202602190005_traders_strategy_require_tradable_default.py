"""Require tradable market by default for relaxed traders_confluence configs.

Revision ID: 202602190005
Revises: 202602190004
Create Date: 2026-02-19 06:00:00.000000
"""

from __future__ import annotations

from datetime import datetime, timezone
import json

from alembic import op
import sqlalchemy as sa
from alembic_helpers import column_names, table_names


revision = "202602190005"
down_revision = "202602190004"
branch_labels = None
depends_on = None


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


def _matches_relaxed_defaults(config: dict) -> bool:
    if config.get("min_confidence") != 0.45:
        return False
    if config.get("min_wallet_count") != 2:
        return False
    if config.get("max_entry_price") != 0.85:
        return False
    if config.get("min_tier") != "low":
        return False
    if config.get("firehose_require_active_signal") is not True:
        return False
    if config.get("firehose_require_tradable_market") is not False:
        return False
    if config.get("firehose_exclude_crypto_markets") is not False:
        return False
    if config.get("firehose_require_qualified_source") is not True:
        return False
    if config.get("firehose_source_scope") != "all":
        return False
    if config.get("firehose_side_filter") != "all":
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
        if not strategy_id or not _matches_relaxed_defaults(config):
            continue

        config["firehose_require_tradable_market"] = True
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
