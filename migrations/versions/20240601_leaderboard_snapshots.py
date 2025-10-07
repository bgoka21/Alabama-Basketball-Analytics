"""Upgrade cached leaderboard snapshots schema.

Revision ID: 20240601_leaderboard_snapshots
Revises: 20240520_add_cached_leaderboard_index
Create Date: 2024-06-01 00:00:00
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "20240601_leaderboard_snapshots"
down_revision = "20240520_add_cached_leaderboard_index"
branch_labels = None
depends_on = None


def _utcnow() -> datetime:
    return datetime.utcnow()


def upgrade() -> None:
    op.add_column(
        "cached_leaderboards",
        sa.Column("schema_version", sa.Integer(), nullable=True),
    )
    op.add_column(
        "cached_leaderboards",
        sa.Column("formatter_version", sa.Integer(), nullable=True),
    )
    op.add_column(
        "cached_leaderboards",
        sa.Column("etag", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "cached_leaderboards",
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.add_column(
        "cached_leaderboards",
        sa.Column("build_manifest", sa.Text(), nullable=True),
    )

    conn = op.get_bind()
    rows = conn.execute(
        sa.text(
            "SELECT id, payload_json, updated_at, season_id, stat_key "
            "FROM cached_leaderboards"
        )
    ).fetchall()

    for row in rows:
        payload = {}
        try:
            payload = json.loads(row.payload_json or "{}")
        except json.JSONDecodeError:
            payload = {}

        schema_version = payload.get("schema_version")
        if schema_version is None:
            schema_version = 0

        formatter_version = payload.get("formatter_version")
        if formatter_version is None:
            formatter_version = 0

        etag_source = row.payload_json or ""
        etag = hashlib.sha256(etag_source.encode("utf-8")).hexdigest()

        created_at = row.updated_at or _utcnow()

        conn.execute(
            sa.text(
                "UPDATE cached_leaderboards "
                "SET schema_version = :schema_version, "
                "formatter_version = :formatter_version, "
                "etag = :etag, "
                "created_at = :created_at "
                "WHERE id = :id"
            ),
            {
                "schema_version": int(schema_version),
                "formatter_version": int(formatter_version),
                "etag": etag,
                "created_at": created_at,
                "id": row.id,
            },
        )

    op.alter_column(
        "cached_leaderboards",
        "schema_version",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.alter_column(
        "cached_leaderboards",
        "formatter_version",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.alter_column(
        "cached_leaderboards",
        "etag",
        existing_type=sa.String(length=64),
        nullable=False,
    )
    op.alter_column(
        "cached_leaderboards",
        "created_at",
        existing_type=sa.DateTime(),
        nullable=False,
    )
    op.alter_column(
        "cached_leaderboards",
        "season_id",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.alter_column(
        "cached_leaderboards",
        "stat_key",
        existing_type=sa.String(length=128),
        type_=sa.String(length=64),
        nullable=False,
    )

    op.drop_constraint(
        "uq_cached_leaderboards_season_stat",
        "cached_leaderboards",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_cached_leaderboards_version",
        "cached_leaderboards",
        ["season_id", "stat_key", "etag"],
    )

    with op.batch_alter_table("cached_leaderboards") as batch_op:
        batch_op.drop_column("updated_at")


def downgrade() -> None:
    op.add_column(
        "cached_leaderboards",
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    op.drop_constraint(
        "uq_cached_leaderboards_version",
        "cached_leaderboards",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_cached_leaderboards_season_stat",
        "cached_leaderboards",
        ["season_id", "stat_key"],
    )

    op.alter_column(
        "cached_leaderboards",
        "stat_key",
        existing_type=sa.String(length=64),
        type_=sa.String(length=128),
        nullable=False,
    )
    op.alter_column(
        "cached_leaderboards",
        "season_id",
        existing_type=sa.Integer(),
        nullable=True,
    )

    op.drop_column("build_manifest")
    op.drop_column("created_at")
    op.drop_column("etag")
    op.drop_column("formatter_version")
    op.drop_column("schema_version")
