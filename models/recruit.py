from datetime import datetime
from models.database import db

class Recruit(db.Model):
    __tablename__ = 'recruit'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(
        db.String(100, collation="BINARY"),
        unique=True,
        nullable=False,
        # enforce case-sensitive uniqueness (SQLite)
    )
    position = db.Column(db.String(50), nullable=False)
    school = db.Column(db.String(100), nullable=False)
    s247_url = db.Column(db.String(255), nullable=True)
    espn_url = db.Column(db.String(255), nullable=True)
    synergy_player_id = db.Column(db.String(255), nullable=True)
    off_rating = db.Column(db.Float, nullable=True)
    def_rating = db.Column(db.Float, nullable=True)
    minutes_played = db.Column(db.Float, nullable=True)
    three_fg_pct = db.Column(db.Float, nullable=True)
    ft_pct = db.Column(db.Float, nullable=True)
    assists = db.Column(db.Float, nullable=True)
    turnovers = db.Column(db.Float, nullable=True)
    ast_to_to_ratio = db.Column(db.Float, nullable=True)
    s247_overall_rank = db.Column(db.Integer, nullable=True)
    s247_position_rank = db.Column(db.Integer, nullable=True)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
