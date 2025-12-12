"""Add family and series columns to scout possessions"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '1d9fd3e939b2'
down_revision = '5c1a9b64bb2d'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('scout_possessions', sa.Column('family', sa.String(length=255), nullable=True))
    op.add_column('scout_possessions', sa.Column('series', sa.String(length=255), nullable=True))


def downgrade():
    op.drop_column('scout_possessions', 'series')
    op.drop_column('scout_possessions', 'family')
