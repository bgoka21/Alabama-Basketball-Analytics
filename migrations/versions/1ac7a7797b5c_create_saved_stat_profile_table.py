"""create saved stat profile table

Revision ID: 1ac7a7797b5c
Revises: ff5ca5bde710
Create Date: 2025-10-05 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1ac7a7797b5c'
down_revision = 'ff5ca5bde710'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'saved_stat_profile',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(length=128), nullable=False),
        sa.Column('fields_json', sa.Text(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column('mode_default', sa.String(length=32), nullable=False, server_default=sa.text("'totals'")),
        sa.Column('source_default', sa.String(length=32), nullable=False, server_default=sa.text("'practice'")),
        sa.Column('owner_id', sa.Integer(), nullable=True),
        sa.Column('visibility', sa.String(length=16), nullable=False, server_default=sa.text("'team'")),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index('ix_saved_stat_profile_name', 'saved_stat_profile', ['name'])


def downgrade():
    op.drop_index('ix_saved_stat_profile_name', table_name='saved_stat_profile')
    op.drop_table('saved_stat_profile')
