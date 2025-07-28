"""Create Session model

Revision ID: ddbf220420b8
Revises: cccf734637a8
Create Date: 2025-07-28 13:08:35.416552

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ddbf220420b8'
down_revision = 'cccf734637a8'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'session',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('season_id', sa.Integer(), sa.ForeignKey('season.id'), nullable=False),
        sa.Column('name', sa.String(length=50), nullable=False),
        sa.Column('start_date', sa.Date(), nullable=False),
        sa.Column('end_date', sa.Date(), nullable=False),
        sa.UniqueConstraint('season_id', 'name', name='_season_session_uc')
    )


def downgrade():
    op.drop_table('session')
