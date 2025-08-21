"""Merge multiple heads

Revision ID: 25c03a46ac35
Revises: bb12ed1e23f4, eeeee
Create Date: 2025-07-28 15:41:03.868334

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '25c03a46ac35'
down_revision = ('bb12ed1e23f4', 'eeeee')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
