"""add composite index for cached leaderboard lookups

Revision ID: 20240520_add_cached_leaderboard_index
Revises: 20231010_cached_leaderboards
Create Date: 2024-05-20 00:00:00.000000

"""
from alembic import op

revision = '20240520_add_cached_leaderboard_index'
down_revision = '20231010_cached_leaderboards'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_cached_leaderboards_season_stat ON cached_leaderboards (season_id, stat_key)"
    )


def downgrade():
    op.execute(
        "DROP INDEX IF EXISTS idx_cached_leaderboards_season_stat"
    )
