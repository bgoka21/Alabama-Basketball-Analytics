"""Make espn_url nullable, allow s247_url, add case-sensitive name index"""

revision = '123456789abc'
down_revision = '18ec91f6d3af'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa

def upgrade():
    with op.batch_alter_table('recruit', reflect=True) as batch_op:
        # allow s247_url to be nullable
        batch_op.alter_column(
            's247_url',
            existing_type=sa.String(length=255),
            nullable=True
        )
        # drop old non-null espn_url if it exists
        try:
            batch_op.drop_column('espn_url')
        except Exception:
            pass
        # re-add espn_url as nullable
        batch_op.add_column(
            sa.Column('espn_url', sa.String(length=255), nullable=True)
        )
        # enforce case-sensitive unique on name
        batch_op.create_index(
            'ix_recruit_name_cs_unique',
            ['name'],
            unique=True
        )

def downgrade():
    with op.batch_alter_table('recruit', reflect=True) as batch_op:
        batch_op.drop_index('ix_recruit_name_cs_unique')
        batch_op.drop_column('espn_url')
        batch_op.alter_column(
            's247_url',
            existing_type=sa.String(length=255),
            nullable=False
        )
        batch_op.add_column(
            sa.Column('espn_url', sa.String(length=255), nullable=False)
        )
