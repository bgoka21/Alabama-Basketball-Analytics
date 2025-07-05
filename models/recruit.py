from datetime import datetime
from models.database import db

class Recruit(db.Model):
    __tablename__ = 'recruits'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)
    position = db.Column(db.String(32))
    height = db.Column(db.String(16))
    weight = db.Column(db.String(16))
    school = db.Column(db.String(128))
    rating = db.Column(db.String(16))
    year = db.Column(db.Integer)
    source = db.Column(db.String(32))  # 'HS' or 'Transfer'
    ppg = db.Column(db.Float)
    rpg = db.Column(db.Float)
    apg = db.Column(db.Float)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
