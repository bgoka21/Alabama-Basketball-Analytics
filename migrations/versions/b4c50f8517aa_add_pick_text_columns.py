"""add pick text columns

Revision ID: b4c50f8517aa
Revises: 108e107ba16c
Create Date: 2025-09-04 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'b4c50f8517aa'
down_revision = '108e107ba16c'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('prospects', sa.Column('projected_pick_text', sa.String(length=32), nullable=True))
    op.add_column('prospects', sa.Column('actual_pick_text', sa.String(length=32), nullable=True))

def downgrade():
    op.drop_column('prospects', 'actual_pick_text')
    op.drop_column('prospects', 'projected_pick_text')
