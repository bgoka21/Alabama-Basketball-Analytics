"""Add admin notes to record definitions."""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'c4f1d2e3a9b1'
down_revision = 'b3c4d5e6f7a8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('record_definition', sa.Column('admin_notes', sa.Text(), nullable=True))


def downgrade():
    op.drop_column('record_definition', 'admin_notes')
