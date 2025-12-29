from datetime import datetime
from models.database import db

UNKNOWN_SERIES = "(Unknown Series)"


def normalize_playcall(playcall: str) -> str:
    """Normalize a playcall for consistent lookups and storage."""

    return (playcall or '').strip().lower()


class ScoutTeam(db.Model):
    __tablename__ = 'scout_teams'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    games = db.relationship('ScoutGame', back_populates='team', cascade='all, delete-orphan')


class ScoutGame(db.Model):
    __tablename__ = 'scout_games'

    id = db.Column(db.Integer, primary_key=True)
    scout_team_id = db.Column(db.Integer, db.ForeignKey('scout_teams.id'), nullable=False)
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    original_filename = db.Column(db.String(255))
    stored_filename = db.Column(db.String(255))
    game_date = db.Column(db.Date)
    opponent = db.Column(db.String(255))
    notes = db.Column(db.Text)

    team = db.relationship('ScoutTeam', back_populates='games')
    possessions = db.relationship('ScoutPossession', back_populates='game', cascade='all, delete-orphan')


class ScoutPossession(db.Model):
    __tablename__ = 'scout_possessions'
    __table_args__ = (
        db.UniqueConstraint('scout_game_id', 'instance_number', name='uq_scout_possessions_game_instance'),
    )

    id = db.Column(db.Integer, primary_key=True)
    scout_game_id = db.Column(db.Integer, db.ForeignKey('scout_games.id'))
    instance_number = db.Column(db.String(255))
    playcall = db.Column(db.String(255))
    family = db.Column(db.String(255), nullable=True, default=None)
    series = db.Column(db.String(255), nullable=True)
    bucket = db.Column(db.String(32))
    points = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    game = db.relationship('ScoutGame', back_populates='possessions')


class ScoutPlaycallMapping(db.Model):
    __tablename__ = 'scout_playcall_mappings'

    id = db.Column(db.Integer, primary_key=True)
    playcall = db.Column(db.String(255), nullable=False)
    playcall_key = db.Column(db.String(255), nullable=False, unique=True, index=True)
    canonical_series = db.Column(db.String(255), nullable=True)
    canonical_family = db.Column(db.String(255), nullable=True)
    updated_at = db.Column(
        db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    @classmethod
    def from_playcall(cls, playcall: str) -> 'ScoutPlaycallMapping':
        normalized = normalize_playcall(playcall)
        existing = cls.query.filter_by(playcall_key=normalized).first()
        if existing:
            return existing

        instance = cls(playcall=playcall.strip(), playcall_key=normalized)
        db.session.add(instance)
        return instance
