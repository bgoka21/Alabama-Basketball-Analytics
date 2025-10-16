"""Replace single game_type with tag table"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0a9b2e5c2d94'
down_revision = '7b3d1c9f6d55'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'game_type_tag',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('game_id', sa.Integer(), sa.ForeignKey('game.id', ondelete='CASCADE'), nullable=False),
        sa.Column('tag', sa.String(length=32), nullable=False),
        sa.UniqueConstraint('game_id', 'tag', name='uq_game_type_tag_game_id_tag'),
    )
    op.create_index('ix_game_type_tag_game_id', 'game_type_tag', ['game_id'])
    op.create_index('ix_game_type_tag_tag', 'game_type_tag', ['tag'])

    connection = op.get_bind()
    game_table = sa.table('game', sa.column('id', sa.Integer()), sa.column('game_type', sa.String(length=32)))
    tag_table = sa.table('game_type_tag', sa.column('game_id', sa.Integer()), sa.column('tag', sa.String(length=32)))

    results = connection.execute(
        sa.select(game_table.c.id, game_table.c.game_type).where(game_table.c.game_type.isnot(None))
    )
    rows = results.fetchall()
    if rows:
        connection.execute(
            tag_table.insert(),
            [
                {'game_id': game_id, 'tag': tag}
                for game_id, tag in rows
                if tag is not None
            ],
        )

    op.drop_column('game', 'game_type')


def downgrade():
    op.add_column('game', sa.Column('game_type', sa.String(length=32), nullable=True))

    connection = op.get_bind()
    game_table = sa.table('game', sa.column('id', sa.Integer()), sa.column('game_type', sa.String(length=32)))
    tag_table = sa.table('game_type_tag', sa.column('game_id', sa.Integer()), sa.column('tag', sa.String(length=32)))

    results = connection.execute(
        sa.select(tag_table.c.game_id, tag_table.c.tag)
    )
    rows = results.fetchall()
    for game_id, tag in rows:
        connection.execute(
            game_table.update().where(game_table.c.id == game_id).values(game_type=tag)
        )

    op.drop_table('game_type_tag')
