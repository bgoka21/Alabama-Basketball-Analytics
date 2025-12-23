"""Add is_active to record entries."""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'd1f2e3a4b5c6'
down_revision = 'c4f1d2e3a9b1'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('record_entry', sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.true()))
    op.alter_column('record_entry', 'is_active', server_default=None)


def downgrade():
    op.drop_column('record_entry', 'is_active')
