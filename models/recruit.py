from datetime import datetime
from models.database import db

class Recruit(db.Model):
    __tablename__ = 'recruits'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    position = db.Column(db.String(50), nullable=True)
    school = db.Column(db.String(100), nullable=True)
    espn_url = db.Column(db.String(255), nullable=False)
    s247_url = db.Column(db.String(255), nullable=False)

    three_fg_pct = db.Column(db.Float, nullable=True)
    assists = db.Column(db.Float, nullable=True)
    turnovers = db.Column(db.Float, nullable=True)
    assist_turnover_ratio = db.Column(db.Float, nullable=True)
    ft_pct = db.Column(db.Float, nullable=True)
    ppg = db.Column(db.Float, nullable=True)
    rpg = db.Column(db.Float, nullable=True)
    apg = db.Column(db.Float, nullable=True)

    s247_overall_rank = db.Column(db.Integer, nullable=True)
    s247_position_rank = db.Column(db.Integer, nullable=True)

    last_updated = db.Column(db.DateTime, default=datetime.utcnow)

