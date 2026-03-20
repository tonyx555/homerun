"""Add discovery_profiles and discovery_profile_versions tables.

Revision ID: 202603190003
Revises: 202603190002
Create Date: 2026-03-19
"""

import sqlalchemy as sa
from alembic import op

revision = "202603190003"
down_revision = "202603190002"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in set(inspector.get_table_names())


def upgrade() -> None:
    if not _table_exists("discovery_profile_tombstones"):
        op.create_table(
            "discovery_profile_tombstones",
            sa.Column("slug", sa.String(), primary_key=True),
            sa.Column("deleted_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column("reason", sa.String(), nullable=True),
        )

    if not _table_exists("discovery_profiles"):
        op.create_table(
            "discovery_profiles",
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("slug", sa.String(), unique=True, nullable=False),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("source_code", sa.Text(), nullable=False),
            sa.Column("class_name", sa.String(), nullable=True),
            sa.Column("is_system", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("enabled", sa.Boolean(), server_default=sa.text("true")),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("status", sa.String(), server_default=sa.text("'unloaded'")),
            sa.Column("error_message", sa.Text(), nullable=True),
            sa.Column("config", sa.JSON(), server_default=sa.text("'{}'")),
            sa.Column("config_schema", sa.JSON(), server_default=sa.text("'{}'")),
            sa.Column("profile_kind", sa.String(), server_default=sa.text("'python'")),
            sa.Column("version", sa.Integer(), server_default=sa.text("1")),
            sa.Column("sort_order", sa.Integer(), server_default=sa.text("0")),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        )
        op.create_index("idx_discovery_profile_slug", "discovery_profiles", ["slug"])
        op.create_index("idx_discovery_profile_enabled", "discovery_profiles", ["enabled"])
        op.create_index("idx_discovery_profile_is_active", "discovery_profiles", ["is_active"])
        op.create_index("idx_discovery_profile_status", "discovery_profiles", ["status"])

    if not _table_exists("discovery_profile_versions"):
        op.create_table(
            "discovery_profile_versions",
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column(
                "profile_id",
                sa.String(),
                sa.ForeignKey("discovery_profiles.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("profile_slug", sa.String(), nullable=False),
            sa.Column("version", sa.Integer(), nullable=False),
            sa.Column("is_latest", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("source_code", sa.Text(), nullable=False),
            sa.Column("class_name", sa.String(), nullable=True),
            sa.Column("config", sa.JSON(), server_default=sa.text("'{}'")),
            sa.Column("config_schema", sa.JSON(), server_default=sa.text("'{}'")),
            sa.Column("profile_kind", sa.String(), server_default=sa.text("'python'")),
            sa.Column("enabled", sa.Boolean(), server_default=sa.text("true")),
            sa.Column("is_system", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("sort_order", sa.Integer(), server_default=sa.text("0")),
            sa.Column("parent_version", sa.Integer(), nullable=True),
            sa.Column("created_by", sa.String(), nullable=True),
            sa.Column("reason", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint("profile_id", "version", name="uq_discovery_profile_versions_profile_version"),
        )
        op.create_index("idx_discovery_profile_versions_profile_id", "discovery_profile_versions", ["profile_id"])
        op.create_index("idx_discovery_profile_versions_slug_version", "discovery_profile_versions", ["profile_slug", "version"])
        op.create_index("idx_discovery_profile_versions_profile_created", "discovery_profile_versions", ["profile_id", "created_at"])
        op.create_index("idx_discovery_profile_versions_latest", "discovery_profile_versions", ["profile_id", "is_latest"])


def downgrade() -> None:
    pass
