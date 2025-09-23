"""Add unique index for practice season/date"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "7d9fd4041e13"
down_revision = "add_practice_rebounding_gap_pnr_fields"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "ux_practice_season_date",
        "practice",
        ["season_id", "date"],
        unique=True,
    )


def downgrade():
    op.drop_index("ux_practice_season_date", table_name="practice")
