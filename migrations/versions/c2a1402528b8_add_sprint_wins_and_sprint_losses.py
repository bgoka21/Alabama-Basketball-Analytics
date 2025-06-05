"""Add sprint_wins and sprint_losses to PlayerStats

Revision ID: c2a1402528b8
Revises: 768821f51dd5
Create Date: 2025-06-02 15:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'c2a1402528b8'
down_revision = '768821f51dd5'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('player_stats', schema=None) as batch_op:
        batch_op.add_column(sa.Column('sprint_wins', sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column('sprint_losses', sa.Integer(), nullable=True))


def downgrade():
    with op.batch_alter_table('player_stats', schema=None) as batch_op:
        batch_op.drop_column('sprint_losses')
        batch_op.drop_column('sprint_wins')

