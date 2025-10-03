"""Add structured player shot detail tables."""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "add_player_shot_detail_table"
down_revision = "add_practice_pass_contest_fields"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "player_shot_detail",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("player_stats_id", sa.Integer(), nullable=False),
        sa.Column("shot_class", sa.String(length=8), nullable=False),
        sa.Column("result", sa.String(length=10), nullable=False),
        sa.Column("possession_type", sa.String(length=64), nullable=True),
        sa.Column("is_assisted", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("shot_location", sa.String(length=64), nullable=True),
        sa.Column("drill_labels", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(["player_stats_id"], ["player_stats.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_player_shot_detail_player_stats_id",
        "player_shot_detail",
        ["player_stats_id"],
    )
    op.create_index(
        "ix_player_shot_detail_shot_class",
        "player_shot_detail",
        ["shot_class"],
    )

    op.create_table(
        "player_shot_detail_label",
        sa.Column("shot_detail_id", sa.Integer(), nullable=False),
        sa.Column("label", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(["shot_detail_id"], ["player_shot_detail.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("shot_detail_id", "label"),
    )
    op.create_index(
        "ix_player_shot_detail_label",
        "player_shot_detail_label",
        ["label"],
    )


def downgrade():
    op.drop_index("ix_player_shot_detail_label", table_name="player_shot_detail_label")
    op.drop_table("player_shot_detail_label")
    op.drop_index("ix_player_shot_detail_shot_class", table_name="player_shot_detail")
    op.drop_index("ix_player_shot_detail_player_stats_id", table_name="player_shot_detail")
    op.drop_table("player_shot_detail")
