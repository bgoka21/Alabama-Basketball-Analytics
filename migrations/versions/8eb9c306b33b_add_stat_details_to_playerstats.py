"""Add stat_details column to PlayerStats

Revision ID: 8eb9c306b33b
Revises: dfc35024adde
Create Date: 2025-06-07 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '8eb9c306b33b'
down_revision = 'dfc35024adde'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('player_stats', schema=None) as batch_op:
        batch_op.add_column(sa.Column('stat_details', sa.Text(), nullable=True))


def downgrade():
    with op.batch_alter_table('player_stats', schema=None) as batch_op:
        batch_op.drop_column('stat_details')
