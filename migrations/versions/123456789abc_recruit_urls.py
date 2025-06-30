"""recruit_urls

Revision ID: 123456789abc
Revises: 18ec91f6d3af
Create Date: 2025-06-30 00:25:21.652346

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '123456789abc'
down_revision = '18ec91f6d3af'
branch_labels = None
depends_on = None


def upgrade():
    # Drop old NOT NULL espn_url
    op.drop_column('recruit', 'espn_url')
    # Allow s247_url to be nullable
    op.alter_column(
        'recruit',
        's247_url',
        existing_type=sa.String(length=255),
        nullable=True
    )
    # Re-add espn_url as nullable
    op.add_column(
        'recruit',
        sa.Column('espn_url', sa.String(length=255), nullable=True)
    )
    # Reinforce case-sensitive unique on name
    op.create_index(
        'ix_recruit_name_collate',
        'recruit',
        ['name'],
        unique=True
    )


def downgrade():
    op.drop_index('ix_recruit_name_collate', table_name='recruit')
    op.drop_column('recruit', 'espn_url')
    op.alter_column(
        'recruit',
        's247_url',
        existing_type=sa.String(length=255),
        nullable=False
    )
    op.add_column(
        'recruit',
        sa.Column('espn_url', sa.String(length=255), nullable=False)
    )
