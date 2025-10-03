"""add leaderboard snapshot table"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1c5f2b234567'
down_revision = (
    '1ac7a7797b5c',
    '1f9735f5da8c',
    '25eb7f71b29b',
    '4d5e5f0b9a4b',
    'b4c50f8517aa',
    'e3fa7b9224ab',
)
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'leaderboard_snapshots',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('season_id', sa.Integer(), nullable=False),
        sa.Column('stat_key', sa.String(length=64), nullable=False),
        sa.Column('start_date', sa.Date(), nullable=True),
        sa.Column('end_date', sa.Date(), nullable=True),
        sa.Column('label_key', sa.String(length=255), nullable=False, server_default=''),
        sa.Column('label_values', sa.JSON(), nullable=False, server_default='[]'),
        sa.Column('player_totals', sa.JSON(), nullable=False, server_default='{}'),
        sa.Column('shot_details', sa.JSON(), nullable=False, server_default='{}'),
        sa.Column('team_totals', sa.JSON(), nullable=True),
        sa.Column('player_keys', sa.JSON(), nullable=False, server_default='[]'),
        sa.Column('leaderboard_rows', sa.JSON(), nullable=False, server_default='[]'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['season_id'], ['season.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'season_id', 'stat_key', 'start_date', 'end_date', 'label_key',
            name='uq_leaderboard_snapshot_filters',
        ),
    )
    op.create_index(
        'ix_leaderboard_snapshots_season_stat',
        'leaderboard_snapshots',
        ['season_id', 'stat_key'],
    )


def downgrade():
    op.drop_index('ix_leaderboard_snapshots_season_stat', table_name='leaderboard_snapshots')
    op.drop_table('leaderboard_snapshots')
