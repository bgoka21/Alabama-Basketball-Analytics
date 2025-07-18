"""Add drill_labels column to Possession

Revision ID: eeeee
Revises: ff5ca5bde710
Create Date: 2025-07-01 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = 'eeeee'
down_revision = 'ff5ca5bde710'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('possession', schema=None) as batch_op:
        batch_op.add_column(sa.Column('drill_labels', sa.String(length=255), nullable=True))


def downgrade():
    with op.batch_alter_table('possession', schema=None) as batch_op:
        batch_op.drop_column('drill_labels')
