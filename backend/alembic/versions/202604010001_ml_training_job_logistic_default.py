"""align ml training job default model type with supported backend

Revision ID: 202604010001
Revises: 202603310001
Create Date: 2026-04-01

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "202604010001"
down_revision = "202603310001"
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    if table_name not in set(inspector.get_table_names()):
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    if "model_type" not in _column_names("ml_training_jobs"):
        return
    op.execute("ALTER TABLE ml_training_jobs ALTER COLUMN model_type SET DEFAULT 'logistic'")
    op.execute("UPDATE ml_training_jobs SET model_type = 'logistic' WHERE model_type IS NULL OR btrim(model_type) = ''")


def downgrade() -> None:
    pass
