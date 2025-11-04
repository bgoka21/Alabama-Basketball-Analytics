"""add total_fouls_drawn column to team_stats"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '3f5964e91732'
down_revision = '0a9b2e5c2d94'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'team_stats',
        sa.Column('total_fouls_drawn', sa.Integer(), nullable=False, server_default='0')
    )
    op.alter_column(
        'team_stats',
        'total_fouls_drawn',
        existing_type=sa.Integer(),
        server_default=None,
    )


def downgrade():
    op.drop_column('team_stats', 'total_fouls_drawn')
