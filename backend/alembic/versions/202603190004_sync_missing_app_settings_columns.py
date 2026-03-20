"""Sync any app_settings columns present in ORM model but missing from DB.

Fixes crash: column app_settings.search_polymarket_enabled does not exist.
Uses the same dynamic-sync approach as 202602130009.

Revision ID: 202603190004
Revises: 202603190003
Create Date: 2026-03-19
"""

from __future__ import annotations

import sys
from pathlib import Path

from alembic import op
import sqlalchemy as sa
from alembic_helpers import column_names

BACKEND_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


# revision identifiers, used by Alembic.
revision = "202603190004"
down_revision = "202603190003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    from models.database import AppSettings
    from models.model_registry import register_all_models

    register_all_models()

    table_name = AppSettings.__tablename__
    existing = column_names(table_name)

    if not existing:
        return

    mapper = sa.inspect(AppSettings)
    for attr in mapper.columns:
        if attr.name in existing:
            continue

        col_type = attr.type.copy() if hasattr(attr.type, "copy") else type(attr.type)()
        nullable = attr.nullable if attr.nullable is not None else True

        server_default = None
        if attr.default is not None and attr.default.is_scalar:
            py_val = attr.default.arg
            if isinstance(py_val, bool):
                server_default = sa.true() if py_val else sa.false()
            elif isinstance(py_val, (int, float)):
                server_default = sa.text(str(py_val))
            elif isinstance(py_val, str):
                server_default = sa.text(f"'{py_val}'")

        new_col = sa.Column(
            attr.name,
            col_type,
            nullable=nullable,
            server_default=server_default,
        )
        op.add_column(table_name, new_col)


def downgrade() -> None:
    pass
