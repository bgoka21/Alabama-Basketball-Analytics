"""Add contest shot-type breakdown fields"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '1f9735f5da8c'
down_revision = '69327cb88861'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('player_stats', schema=None) as batch_op:
        batch_op.add_column(sa.Column('atr_contest_attempts', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('atr_contest_makes', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('atr_late_attempts', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('atr_late_makes', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('atr_no_contest_attempts', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('atr_no_contest_makes', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg2_contest_attempts', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg2_contest_makes', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg2_late_attempts', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg2_late_makes', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg2_no_contest_attempts', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg2_no_contest_makes', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg3_contest_attempts', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg3_contest_makes', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg3_late_attempts', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg3_late_makes', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg3_no_contest_attempts', sa.Integer(), nullable=False, server_default='0'))
        batch_op.add_column(sa.Column('fg3_no_contest_makes', sa.Integer(), nullable=False, server_default='0'))


def downgrade():
    with op.batch_alter_table('player_stats', schema=None) as batch_op:
        batch_op.drop_column('fg3_no_contest_makes')
        batch_op.drop_column('fg3_no_contest_attempts')
        batch_op.drop_column('fg3_late_makes')
        batch_op.drop_column('fg3_late_attempts')
        batch_op.drop_column('fg3_contest_makes')
        batch_op.drop_column('fg3_contest_attempts')
        batch_op.drop_column('fg2_no_contest_makes')
        batch_op.drop_column('fg2_no_contest_attempts')
        batch_op.drop_column('fg2_late_makes')
        batch_op.drop_column('fg2_late_attempts')
        batch_op.drop_column('fg2_contest_makes')
        batch_op.drop_column('fg2_contest_attempts')
        batch_op.drop_column('atr_no_contest_makes')
        batch_op.drop_column('atr_no_contest_attempts')
        batch_op.drop_column('atr_late_makes')
        batch_op.drop_column('atr_late_attempts')
        batch_op.drop_column('atr_contest_makes')
        batch_op.drop_column('atr_contest_attempts')
