"""Create PnRStats table

Revision ID: fa05b941a8bd
Revises: dfc35024adde
Create Date: 2025-06-19 12:39:06.122471
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'fa05b941a8bd'
down_revision = 'dfc35024adde'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'pnr_stats',
        sa.Column('id',             sa.Integer(), primary_key=True),
        sa.Column('game_id',        sa.Integer(), nullable=True),
        sa.Column('practice_id',    sa.Integer(), nullable=True),
        sa.Column('possession_id',  sa.Integer(), nullable=False),
        sa.Column('player_id',      sa.Integer(), nullable=False),
        sa.Column('role',           sa.String(length=50), nullable=False),
        sa.Column('advantage_created', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('direct',           sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('points_scored',    sa.Integer(), nullable=False, server_default='0'),
        sa.Column('turnover_occurred', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('assist_occurred',   sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('start_time',      sa.Float(), nullable=False),
        sa.Column('duration',        sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(['game_id'],       ['game.id']),
        sa.ForeignKeyConstraint(['practice_id'],   ['practice.id']),
        sa.ForeignKeyConstraint(['possession_id'], ['possession.id']),
        sa.ForeignKeyConstraint(['player_id'],     ['player.id']),
    )


def downgrade():
    op.drop_table('pnr_stats')
