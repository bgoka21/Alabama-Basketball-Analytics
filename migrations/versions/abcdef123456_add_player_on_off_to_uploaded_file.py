"""Add player_on_off to UploadedFile

Revision ID: abcdef123456
Revises: fa05b941a8bd
Create Date: 2025-06-20 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = 'abcdef123456'
down_revision = 'fa05b941a8bd'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('uploaded_files', schema=None) as batch_op:
        batch_op.add_column(sa.Column('player_on_off', sa.Text(), nullable=True))


def downgrade():
    with op.batch_alter_table('uploaded_files', schema=None) as batch_op:
        batch_op.drop_column('player_on_off')
