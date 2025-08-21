"""Merge heads after EYBL additions

Revision ID: 97bd00f61704
Revises: 71ee53c77777, bc1d6e7f8e9a
Create Date: 2025-08-21 15:41:23.218772

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '97bd00f61704'
down_revision = ('71ee53c77777', 'bc1d6e7f8e9a')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
