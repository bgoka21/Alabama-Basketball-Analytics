"""create player development plan

Revision ID: ff5ca5bde710
Revises: 90323e37708f
Create Date: 2025-07-16 18:43:52.664647

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ff5ca5bde710'
down_revision = '90323e37708f'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if not bind.dialect.has_table(bind, 'player_development_plan'):
        op.create_table(
            'player_development_plan',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('player_name', sa.String(length=100), nullable=False),
            sa.Column('season_id', sa.Integer(), sa.ForeignKey('season.id')),
            sa.Column('stat_1_name', sa.String(length=64)),
            sa.Column('stat_1_goal', sa.Float()),
            sa.Column('stat_2_name', sa.String(length=64)),
            sa.Column('stat_2_goal', sa.Float()),
            sa.Column('stat_3_name', sa.String(length=64)),
            sa.Column('stat_3_goal', sa.Float()),
            sa.Column('note_1', sa.Text()),
            sa.Column('note_2', sa.Text()),
            sa.Column('note_3', sa.Text())
        )


def downgrade():
    op.drop_table('player_development_plan')
