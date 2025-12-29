"""Add series column to scout possessions."""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'f4c2a1b0d9e8'
down_revision = 'e3b2c1d4a5f6'
branch_labels = None
depends_on = None


def _column_names(table_name: str) -> set[str]:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return {column["name"] for column in inspector.get_columns(table_name)}


def upgrade():
    if "series" not in _column_names("scout_possessions"):
        op.add_column('scout_possessions', sa.Column('series', sa.String(length=255), nullable=True))


def downgrade():
    if "series" in _column_names("scout_possessions"):
        op.drop_column('scout_possessions', 'series')
