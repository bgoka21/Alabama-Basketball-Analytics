"""Add PlayerDraftStock with bio fields

Revision ID: 2093eeb34640
Revises: 123456789abc
Create Date: 2025-07-14 15:45:22.253874

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2093eeb34640'
down_revision = '123456789abc'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'player_draft_stock',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('coach', sa.String(length=128)),
        sa.Column('coach_current_team', sa.String(length=128)),
        sa.Column('player', sa.String(length=128)),
        sa.Column('player_class', sa.String(length=32)),
        sa.Column('age', sa.Float),
        sa.Column('team', sa.String(length=128)),
        sa.Column('conference', sa.String(length=128)),
        sa.Column('year', sa.Integer),
        sa.Column('projected_pick', sa.String(length=32)),
        sa.Column('actual_pick', sa.String(length=32)),
        sa.Column('projected_money', sa.Float),
        sa.Column('actual_money', sa.Float),
        sa.Column('net', sa.Float),
        sa.Column('high_school', sa.String(length=128)),
        sa.Column('hometown_city', sa.String(length=128)),
        sa.Column('hometown_state', sa.String(length=64)),
        sa.Column('height', sa.String(length=32)),
        sa.Column('weight', sa.Float),
        sa.Column('position', sa.String(length=64))
    )


def downgrade():
    op.drop_table('player_draft_stock')
