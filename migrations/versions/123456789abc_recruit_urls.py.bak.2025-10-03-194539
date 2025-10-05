"""Make espn_url nullable, add/allow s247_url, add case-sensitive name index (SQLite-safe)

Revision ID: 123456789abc
Revises: 18ec91f6d3af
Create Date: 2024-01-01 00:00:00
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "123456789abc"
down_revision = "18ec91f6d3af"
branch_labels = None
depends_on = None


def _safe_inspector():
    """Return a SQLA Inspector when running online; None in offline (e.g., --sql)."""
    try:
        bind = op.get_bind()
        # Alembic offline mode supplies a MockConnection that can't be inspected
        insp = inspect(bind)
        # Poke it to ensure it's real/online
        _ = getattr(insp, "get_table_names", None)
        if _ is None:
            return None
        return insp
    except Exception:
        return None


def upgrade():
    insp = _safe_inspector()
    existing_cols = set()
    existing_indexes = set()

    if insp:
        try:
            existing_cols = {c["name"] for c in insp.get_columns("recruit")}
        except Exception:
            existing_cols = set()
        try:
            existing_indexes = {ix["name"] for ix in insp.get_indexes("recruit")}
        except Exception:
            existing_indexes = set()

    # Alter/add columns in batch (SQLite-safe)
    with op.batch_alter_table("recruit") as batch:
        # Make espn_url nullable if present; if inspector unavailable, assume present
        if not existing_cols or "espn_url" in existing_cols:
            try:
                batch.alter_column(
                    "espn_url",
                    existing_type=sa.String(length=255),
                    nullable=True,
                    existing_nullable=False,
                )
            except Exception:
                # If column missing or already nullable, ignore
                pass

        # Ensure s247_url exists and is nullable
        if existing_cols and "s247_url" in existing_cols:
            try:
                batch.alter_column(
                    "s247_url",
                    existing_type=sa.String(length=255),
                    nullable=True,
                )
            except Exception:
                pass
        else:
            try:
                batch.add_column(sa.Column("s247_url", sa.String(length=255), nullable=True))
            except Exception:
                # If column already exists, ignore
                pass

    # Named index on name (idempotent when inspector is available)
    if "ix_recruit_name_cs" not in existing_indexes:
        try:
            op.create_index("ix_recruit_name_cs", "recruit", ["name"])
        except Exception:
            # If index exists (e.g., created by another branch), ignore
            pass


def downgrade():
    # Drop index if present
    try:
        op.drop_index("ix_recruit_name_cs", table_name="recruit")
    except Exception:
        pass

    with op.batch_alter_table("recruit") as batch:
        # Revert espn_url to NOT NULL if possible
        try:
            batch.alter_column(
                "espn_url",
                existing_type=sa.String(length=255),
                nullable=False,
                existing_nullable=True,
            )
        except Exception:
            pass

        # If s247_url was added here, drop it; if it pre-existed, make NOT NULL
        try:
            batch.drop_column("s247_url")
        except Exception:
            try:
                batch.alter_column(
                    "s247_url",
                    existing_type=sa.String(length=255),
                    nullable=False,
                    existing_nullable=True,
                )
            except Exception:
                pass
