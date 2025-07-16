"""merge heads

Revision ID: 90323e37708f
Revises: abcdef123456, 2093eeb34640
Create Date: 2025-07-16 18:42:01.471374

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '90323e37708f'
down_revision = ('abcdef123456', '2093eeb34640')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
