"""Create setting table for configurable defaults.

Revision ID: d2f7b1a4c6b9
Revises: 97bd00f61704
Create Date: 2024-11-24 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd2f7b1a4c6b9'
down_revision = '97bd00f61704'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'setting',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('key', sa.String(length=64), nullable=False, unique=True),
        sa.Column('value', sa.String(length=255), nullable=True),
    )


def downgrade():
    op.drop_table('setting')
