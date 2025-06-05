from models.database import db
from flask_login import UserMixin

class User(db.Model, UserMixin):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)  # This field is missing in the DB
    is_admin = db.Column(db.Boolean, default=False)
    is_player = db.Column(db.Boolean, default=False)
    player_name = db.Column(db.String(100), nullable=True)
