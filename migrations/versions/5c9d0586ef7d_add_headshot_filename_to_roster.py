"""add headshot_filename to roster

Revision ID: 5c9d0586ef7d
Revises: ff5ca5bde710
Create Date: 2025-08-20 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '5c9d0586ef7d'
down_revision = 'ff5ca5bde710'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('roster', schema=None) as batch_op:
        batch_op.add_column(sa.Column('headshot_filename', sa.String(length=255), nullable=True))


def downgrade():
    with op.batch_alter_table('roster', schema=None) as batch_op:
        batch_op.drop_column('headshot_filename')
