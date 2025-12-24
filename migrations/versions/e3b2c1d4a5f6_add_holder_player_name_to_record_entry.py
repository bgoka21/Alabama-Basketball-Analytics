"""Add holder_player_name to record entries."""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'e3b2c1d4a5f6'
down_revision = 'd1f2e3a4b5c6'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('record_entry', sa.Column('holder_player_name', sa.String(length=128), nullable=True))


def downgrade():
    op.drop_column('record_entry', 'holder_player_name')
