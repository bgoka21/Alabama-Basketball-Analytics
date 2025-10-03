"""Allow practice possessions to omit game ids."""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "make_possession_game_id_nullable"
down_revision = "add_player_shot_detail_table"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("possession") as batch_op:
        batch_op.alter_column(
            "game_id",
            existing_type=sa.Integer(),
            nullable=True,
        )

    op.execute("UPDATE possession SET game_id = NULL WHERE game_id = 0")


def downgrade():
    op.execute("UPDATE possession SET game_id = 0 WHERE game_id IS NULL")

    with op.batch_alter_table("possession") as batch_op:
        batch_op.alter_column(
            "game_id",
            existing_type=sa.Integer(),
            nullable=False,
        )
