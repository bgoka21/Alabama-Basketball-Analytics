"""Add unique constraint to scout possessions"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '5c1a9b64bb2d'
down_revision = '9bb7c5a2d7c1'
branch_labels = None
depends_on = None


def upgrade():
    op.create_unique_constraint(
        'uq_scout_possessions_game_instance',
        'scout_possessions',
        ['scout_game_id', 'instance_number'],
    )


def downgrade():
    op.drop_constraint('uq_scout_possessions_game_instance', 'scout_possessions', type_='unique')
