"""Add recruits, recruit_shot_type_stat, recruit_top_school"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'bb12ed1e23f4'
down_revision = '5c9d0586ef7d'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'recruit',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(length=128), nullable=False),
        sa.Column('graduation_year', sa.Integer()),
        sa.Column('position', sa.String(length=32)),
        sa.Column('height', sa.String(length=16)),
        sa.Column('weight', sa.Integer()),
        sa.Column('high_school', sa.String(length=128)),
        sa.Column('hometown', sa.String(length=128)),
        sa.Column('rating', sa.Integer()),
        sa.Column('ranking', sa.Integer()),
        sa.Column('camp_performance', sa.Text()),
        sa.Column('offer_status', sa.String(length=32)),
        sa.Column('offer_date', sa.Date()),
        sa.Column('commit_date', sa.Date()),
        sa.Column('email', sa.String(length=128)),
        sa.Column('phone', sa.String(length=32)),
        sa.Column('profile_image_url', sa.String(length=256)),
        sa.Column('notes', sa.Text()),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False)
    )
    op.create_table(
        'recruit_shot_type_stat',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('recruit_id', sa.Integer(), sa.ForeignKey('recruit.id'), nullable=False),
        sa.Column('shot_type_details', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False)
    )
    op.create_table(
        'recruit_top_school',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('recruit_id', sa.Integer(), sa.ForeignKey('recruit.id'), nullable=False),
        sa.Column('school_name', sa.String(length=128), nullable=False),
        sa.Column('rank', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False)
    )


def downgrade():
    op.drop_table('recruit_top_school')
    op.drop_table('recruit_shot_type_stat')
    op.drop_table('recruit')
