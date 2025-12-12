"""Add playcall mapping table"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func

# revision identifiers, used by Alembic.
revision = '8d3f5c2c9b1f'
down_revision = '1d9fd3e939b2'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'scout_playcall_mappings',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('playcall', sa.String(length=255), nullable=False),
        sa.Column('playcall_key', sa.String(length=255), nullable=False),
        sa.Column('canonical_series', sa.String(length=255), nullable=True),
        sa.Column('canonical_family', sa.String(length=255), nullable=True),
        sa.Column(
            'updated_at',
            sa.DateTime(),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
        ),
    )
    op.create_unique_constraint(
        'uq_scout_playcall_mappings_key',
        'scout_playcall_mappings',
        ['playcall_key'],
    )
    op.create_index(
        'ix_scout_playcall_mappings_playcall_key',
        'scout_playcall_mappings',
        ['playcall_key'],
        unique=False,
    )


def downgrade():
    op.drop_index('ix_scout_playcall_mappings_playcall_key', table_name='scout_playcall_mappings')
    op.drop_constraint('uq_scout_playcall_mappings_key', 'scout_playcall_mappings', type_='unique')
    op.drop_table('scout_playcall_mappings')
