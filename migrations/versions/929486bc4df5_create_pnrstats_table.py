"""Create PnRStats table"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '<the new file’s revision id>'      # e.g. 'f3b2a1c4d5e6'
down_revision = 'dfc35024adde'                  # your existing head
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
