"""add PnR columns to unified_stats

Revision ID: 1b2a3c4d5e67
Revises: ff5ca5bde710
Create Date: 2025-08-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = '1b2a3c4d5e67'
down_revision = 'ff5ca5bde710'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('unified_stats', sa.Column('pnr_poss', sa.Integer(), nullable=True))
    op.add_column('unified_stats', sa.Column('pnr_ppp', sa.Float(), nullable=True))
    op.add_column('unified_stats', sa.Column('pnr_to_pct', sa.Float(), nullable=True))
    op.add_column('unified_stats', sa.Column('pnr_score_pct', sa.Float(), nullable=True))


def downgrade():
    op.drop_column('unified_stats', 'pnr_score_pct')
    op.drop_column('unified_stats', 'pnr_to_pct')
    op.drop_column('unified_stats', 'pnr_ppp')
    op.drop_column('unified_stats', 'pnr_poss')
