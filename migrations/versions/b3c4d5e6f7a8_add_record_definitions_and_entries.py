"""Add record definition and entry tables"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'b3c4d5e6f7a8'
down_revision = ('8d3f5c2c9b1f', '3f5964e91732')
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'record_definition',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('category', sa.String(length=32), nullable=False),
        sa.Column('entity_type', sa.String(length=16), nullable=False),
        sa.Column('scope', sa.String(length=16), nullable=False),
        sa.Column('stat_key', sa.String(length=64), nullable=False),
        sa.Column('compare', sa.String(length=8), nullable=False, server_default='MAX'),
        sa.Column('qualifier_stat_key', sa.String(length=64), nullable=True),
        sa.Column('qualifier_threshold_override', sa.Float(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("compare = 'MAX'", name='ck_record_definition_compare_max'),
    )
    op.create_index('ix_record_definition_stat_key', 'record_definition', ['stat_key'])

    op.create_table(
        'record_entry',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('record_definition_id', sa.Integer(), nullable=False),
        sa.Column('holder_entity_type', sa.String(length=16), nullable=False),
        sa.Column('holder_player_id', sa.Integer(), nullable=True),
        sa.Column('holder_opponent_name', sa.String(length=128), nullable=True),
        sa.Column('value', sa.Float(), nullable=False),
        sa.Column('scope', sa.String(length=16), nullable=False),
        sa.Column('season_year', sa.Integer(), nullable=True),
        sa.Column('game_id', sa.Integer(), nullable=True),
        sa.Column('occurred_on', sa.Date(), nullable=True),
        sa.Column('source_type', sa.String(length=16), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('is_current', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_forced_current', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('auto_key', sa.String(length=128), nullable=True, unique=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['record_definition_id'], ['record_definition.id']),
        sa.ForeignKeyConstraint(['holder_player_id'], ['roster.id']),
        sa.ForeignKeyConstraint(['game_id'], ['game.id']),
    )
    op.create_index('ix_record_entry_record_definition_id', 'record_entry', ['record_definition_id'])
    op.create_index('ix_record_entry_game_id', 'record_entry', ['game_id'])


def downgrade():
    op.drop_index('ix_record_entry_game_id', table_name='record_entry')
    op.drop_index('ix_record_entry_record_definition_id', table_name='record_entry')
    op.drop_table('record_entry')

    op.drop_index('ix_record_definition_stat_key', table_name='record_definition')
    op.drop_table('record_definition')
