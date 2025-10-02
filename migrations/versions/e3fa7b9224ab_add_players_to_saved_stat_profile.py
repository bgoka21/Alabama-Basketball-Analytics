"""Add players_json to saved stat profiles"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e3fa7b9224ab'
down_revision = 'd2f7b1a4c6b9'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'saved_stat_profile',
        sa.Column('players_json', sa.Text(), nullable=False, server_default=sa.text("'[]'")),
    )
    op.execute("UPDATE saved_stat_profile SET players_json = '[]' WHERE players_json IS NULL OR players_json = ''")


def downgrade():
    op.drop_column('saved_stat_profile', 'players_json')
