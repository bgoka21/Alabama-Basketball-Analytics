"""add contest pass counters to practice stats"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "add_practice_pass_contest_fields"
down_revision = "add_practice_rebounding_gap_pnr_fields"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("player_stats", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("pass_contest_positive", sa.Integer(), nullable=False, server_default="0")
        )
        batch_op.add_column(
            sa.Column("pass_contest_missed", sa.Integer(), nullable=False, server_default="0")
        )


def downgrade():
    with op.batch_alter_table("player_stats", schema=None) as batch_op:
        batch_op.drop_column("pass_contest_missed")
        batch_op.drop_column("pass_contest_positive")
