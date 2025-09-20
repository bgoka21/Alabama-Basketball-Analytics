"""add practice rebounding + gap help + pnr grade fields"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "add_practice_rebounding_gap_pnr_fields"
down_revision = "b4c50f8517aa"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("player_stats", schema=None) as batch_op:
        # --- Offensive Rebounding Opportunities ---
        batch_op.add_column(sa.Column("crash_positive", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("crash_missed", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("back_man_positive", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("back_man_missed", sa.Integer(), nullable=False, server_default="0"))

        # --- Defensive Rebounding Opportunities ---
        batch_op.add_column(sa.Column("box_out_positive", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("box_out_missed", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("off_reb_given_up", sa.Integer(), nullable=False, server_default="0"))

        # --- Collision Gap Help (Crimson + White aggregated) ---
        batch_op.add_column(sa.Column("collision_gap_positive", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("collision_gap_missed", sa.Integer(), nullable=False, server_default="0"))

        # --- PnR Gap Help & Low ---
        batch_op.add_column(sa.Column("pnr_gap_positive", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("pnr_gap_missed", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("low_help_positive", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("low_help_missed", sa.Integer(), nullable=False, server_default="0"))

        # --- PnR Grade (Defense) ---
        batch_op.add_column(sa.Column("close_window_positive", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("close_window_missed", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("shut_door_positive", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column("shut_door_missed", sa.Integer(), nullable=False, server_default="0"))


def downgrade():
    with op.batch_alter_table("player_stats", schema=None) as batch_op:
        # PnR Grade
        batch_op.drop_column("shut_door_missed")
        batch_op.drop_column("shut_door_positive")
        batch_op.drop_column("close_window_missed")
        batch_op.drop_column("close_window_positive")

        # PnR Gap Help & Low
        batch_op.drop_column("low_help_missed")
        batch_op.drop_column("low_help_positive")
        batch_op.drop_column("pnr_gap_missed")
        batch_op.drop_column("pnr_gap_positive")

        # Collision Gap
        batch_op.drop_column("collision_gap_missed")
        batch_op.drop_column("collision_gap_positive")

        # Defensive Rebounding
        batch_op.drop_column("off_reb_given_up")
        batch_op.drop_column("box_out_missed")
        batch_op.drop_column("box_out_positive")

        # Offensive Rebounding
        batch_op.drop_column("back_man_missed")
        batch_op.drop_column("back_man_positive")
        batch_op.drop_column("crash_missed")
        batch_op.drop_column("crash_positive")
