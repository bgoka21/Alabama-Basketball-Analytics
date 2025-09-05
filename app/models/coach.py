from app import db
import sqlalchemy as sa


class Coach(db.Model):
    __tablename__ = "coaches"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(128), unique=True, nullable=False)
    current_team = sa.Column(sa.String(128))
    current_conference = sa.Column(sa.String(64))

    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())
