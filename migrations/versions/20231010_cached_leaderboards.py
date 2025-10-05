
"""Create cached_leaderboards table

Revision ID: 20231010_cached_leaderboards
Revises: 25c03a46ac35
Create Date: 2025-10-10 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20231010_cached_leaderboards'
down_revision = '25c03a46ac35'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'cached_leaderboards',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('season_id', sa.Integer(), nullable=True),
        sa.Column('stat_key', sa.String(length=128), nullable=False),
        sa.Column('payload_json', sa.Text(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('season_id', 'stat_key', name='uq_cached_leaderboards_season_stat'),
    )
    op.create_index(op.f('ix_cached_leaderboards_season_id'), 'cached_leaderboards', ['season_id'], unique=False)
    op.create_index(op.f('ix_cached_leaderboards_stat_key'), 'cached_leaderboards', ['stat_key'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_cached_leaderboards_stat_key'), table_name='cached_leaderboards')
    op.drop_index(op.f('ix_cached_leaderboards_season_id'), table_name='cached_leaderboards')
    op.drop_table('cached_leaderboards')
