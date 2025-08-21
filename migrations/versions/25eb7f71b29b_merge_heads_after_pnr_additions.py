"""Merge heads after PnR additions

Revision ID: 25eb7f71b29b
Revises: 97bd00f61704, 1b2a3c4d5e67
Create Date: 2025-08-21 17:30:45.486711

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '25eb7f71b29b'
down_revision = ('97bd00f61704', '1b2a3c4d5e67')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
