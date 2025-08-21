"""add external_identity_map and unified_stats tables

Revision ID: bc1d6e7f8e9a
Revises: a99d84b087da
Create Date: 2025-07-30 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'bc1d6e7f8e9a'
down_revision = 'a99d84b087da'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'external_identity_map',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('recruit_id', sa.Integer(), sa.ForeignKey('recruit.id'), nullable=True),
        sa.Column('source_system', sa.String(length=64), nullable=False, server_default='synergy_portal_csv'),
        sa.Column('external_key', sa.String(length=128), nullable=False),
        sa.Column('player_name_external', sa.String(length=128), nullable=False),
        sa.Column('team_external', sa.String(length=128), nullable=False),
        sa.Column('circuit', sa.String(length=32), nullable=False),
        sa.Column('season_year', sa.Integer(), nullable=True),
        sa.Column('season_type', sa.String(length=32), nullable=True, server_default='AAU'),
        sa.Column('match_confidence', sa.Float(), nullable=False, server_default='0'),
        sa.Column('is_verified', sa.Boolean(), nullable=False, server_default=sa.text('0')),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.UniqueConstraint('external_key', name='uq_external_identity_map_external_key'),
    )
    op.create_index('ix_ext_ident_recruit_circuit_season', 'external_identity_map', ['recruit_id', 'circuit', 'season_year', 'season_type'])

    op.create_table(
        'unified_stats',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('recruit_id', sa.Integer(), sa.ForeignKey('recruit.id'), nullable=False),
        sa.Column('circuit', sa.String(length=32), nullable=False),
        sa.Column('season_year', sa.Integer(), nullable=True),
        sa.Column('season_type', sa.String(length=32), nullable=True, server_default='AAU'),
        sa.Column('team_name', sa.String(length=128), nullable=True),
        sa.Column('gp', sa.Float(), nullable=True),
        sa.Column('ppg', sa.Float(), nullable=True),
        sa.Column('ast', sa.Float(), nullable=True),
        sa.Column('tov', sa.Float(), nullable=True),
        sa.Column('fg_pct', sa.Float(), nullable=True),
        sa.Column('ppp', sa.Float(), nullable=True),
        sa.Column('source_system', sa.String(length=64), nullable=False, server_default='synergy_portal_csv'),
        sa.Column('ingested_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column('original_filenames', sa.Text(), nullable=True),
        sa.UniqueConstraint('recruit_id', 'circuit', 'season_year', 'season_type', 'team_name', name='uq_unified_stats_unique'),
    )
    op.create_index('ix_unified_stats_circuit_season', 'unified_stats', ['circuit', 'season_year'])


def downgrade():
    op.drop_index('ix_unified_stats_circuit_season', table_name='unified_stats')
    op.drop_table('unified_stats')
    op.drop_index('ix_ext_ident_recruit_circuit_season', table_name='external_identity_map')
    op.drop_table('external_identity_map')
