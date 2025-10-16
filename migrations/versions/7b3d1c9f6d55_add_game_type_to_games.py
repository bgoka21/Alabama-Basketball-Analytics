"""Add game_type column to games"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '7b3d1c9f6d55'
down_revision = '25c03a46ac35'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('game', sa.Column('game_type', sa.String(length=32), nullable=True))


def downgrade():
    op.drop_column('game', 'game_type')
