"""Create scout tables for isolated playcall uploads"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '9bb7c5a2d7c1'
down_revision = '0a9b2e5c2d94'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'scout_teams',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(length=255), nullable=False, unique=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        'scout_games',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('scout_team_id', sa.Integer(), sa.ForeignKey('scout_teams.id'), nullable=False),
        sa.Column('uploaded_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('original_filename', sa.String(length=255)),
        sa.Column('stored_filename', sa.String(length=255)),
        sa.Column('game_date', sa.Date()),
        sa.Column('opponent', sa.String(length=255)),
        sa.Column('notes', sa.Text()),
    )

    op.create_table(
        'scout_possessions',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('scout_game_id', sa.Integer(), sa.ForeignKey('scout_games.id')),
        sa.Column('instance_number', sa.String(length=255)),
        sa.Column('playcall', sa.String(length=255)),
        sa.Column('bucket', sa.String(length=32)),
        sa.Column('points', sa.Integer()),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )


def downgrade():
    op.drop_table('scout_possessions')
    op.drop_table('scout_games')
    op.drop_table('scout_teams')
