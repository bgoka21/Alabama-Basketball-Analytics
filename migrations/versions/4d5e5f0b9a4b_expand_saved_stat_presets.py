"""Expand saved stat presets with types and dates"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4d5e5f0b9a4b'
down_revision = 'ff5ca5bde710'
branch_labels = None
depends_on = None


PRESET_TYPE_DEFAULT = sa.text("'combined'")


def upgrade():
    op.add_column(
        'saved_stat_profile',
        sa.Column('preset_type', sa.String(length=16), nullable=True, server_default=PRESET_TYPE_DEFAULT),
    )
    op.add_column('saved_stat_profile', sa.Column('date_from', sa.Date(), nullable=True))
    op.add_column('saved_stat_profile', sa.Column('date_to', sa.Date(), nullable=True))

    op.alter_column(
        'saved_stat_profile',
        'fields_json',
        existing_type=sa.Text(),
        nullable=True,
        existing_server_default=sa.text("'[]'"),
    )
    op.alter_column(
        'saved_stat_profile',
        'players_json',
        existing_type=sa.Text(),
        nullable=True,
        existing_server_default=sa.text("'[]'"),
    )

    op.execute("UPDATE saved_stat_profile SET preset_type = 'combined' WHERE preset_type IS NULL OR preset_type = ''")

    op.alter_column(
        'saved_stat_profile',
        'preset_type',
        existing_type=sa.String(length=16),
        nullable=False,
        server_default=PRESET_TYPE_DEFAULT,
    )


def downgrade():
    op.alter_column(
        'saved_stat_profile',
        'players_json',
        existing_type=sa.Text(),
        nullable=False,
        server_default=sa.text("'[]'"),
    )
    op.alter_column(
        'saved_stat_profile',
        'fields_json',
        existing_type=sa.Text(),
        nullable=False,
        server_default=sa.text("'[]'"),
    )

    op.drop_column('saved_stat_profile', 'date_to')
    op.drop_column('saved_stat_profile', 'date_from')
    op.drop_column('saved_stat_profile', 'preset_type')
