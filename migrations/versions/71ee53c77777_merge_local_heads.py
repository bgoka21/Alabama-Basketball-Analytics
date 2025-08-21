"""Merge local heads

Revision ID: 71ee53c77777
Revises: 25c03a46ac35, a99d84b087da
Create Date: 2025-07-28 18:53:27.499190

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '71ee53c77777'
down_revision = ('25c03a46ac35', 'a99d84b087da')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
