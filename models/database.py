from datetime import date
from flask_sqlalchemy import SQLAlchemy

# Initialize SQLAlchemy
db = SQLAlchemy()


class Season(db.Model):
    id          = db.Column(db.Integer, primary_key=True)
    season_name = db.Column(db.String(20), unique=True, nullable=False)
    start_date  = db.Column(db.Date)
    end_date    = db.Column(db.Date)

    games       = db.relationship('Game',   backref='season', lazy=True)
    roster      = db.relationship('Roster', backref='season', lazy=True)


class Game(db.Model):
    id                       = db.Column(db.Integer, primary_key=True)
    season_id                = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=False)
    game_date                = db.Column(db.Date, nullable=False)
    opponent_name            = db.Column(db.String(100), nullable=False)
    home_or_away             = db.Column(db.String(10), nullable=False)
    result                   = db.Column(db.String(10))
    csv_filename             = db.Column(db.String(255))

    teams                    = db.relationship('TeamStats',             backref='game', lazy=True)
    players                  = db.relationship('PlayerStats',           backref='game', lazy=True)
    blue_collar_stats        = db.relationship('BlueCollarStats',       backref='game', lazy=True)
    opponent_blue_coll_stats = db.relationship('OpponentBlueCollarStats', backref='game', lazy=True)
    possessions              = db.relationship('Possession',             backref='game', lazy=True)


class Practice(db.Model):
    __tablename__   = 'practice'
    id              = db.Column(db.Integer, primary_key=True)
    season_id       = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=False)
    date            = db.Column(db.Date, nullable=False)
    category        = db.Column(db.String(20), nullable=False)  # Summer Workouts, Fall Workouts, etc.

    team_stats      = db.relationship('TeamStats',               backref='practice', lazy=True)
    player_stats    = db.relationship('PlayerStats',             backref='practice', lazy=True)
    blue_collar     = db.relationship('BlueCollarStats',         backref='practice', lazy=True)
    opp_blue_collar = db.relationship('OpponentBlueCollarStats', backref='practice', lazy=True)
    possessions     = db.relationship('Possession',              backref='practice', lazy=True)

    def __repr__(self):
        return f"<Practice {self.date} [{self.category}]>"


class TeamStats(db.Model):
    id                   = db.Column(db.Integer, primary_key=True)
    game_id              = db.Column(db.Integer, db.ForeignKey('game.id'), nullable=False, index=True)
    practice_id          = db.Column(db.Integer, db.ForeignKey('practice.id'), nullable=True, index=True)
    player_id            = db.Column(db.Integer, db.ForeignKey('roster.id'), nullable=True)
    season_id            = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=False, index=True)

    total_points         = db.Column(db.Integer)
    total_assists        = db.Column(db.Integer)
    total_turnovers      = db.Column(db.Integer)
    total_second_assists = db.Column(db.Integer)
    total_pot_assists    = db.Column(db.Integer)
    total_atr_makes      = db.Column(db.Integer, default=0)
    total_atr_attempts   = db.Column(db.Integer, default=0)
    total_fg2_makes      = db.Column(db.Integer, default=0)
    total_fg2_attempts   = db.Column(db.Integer, default=0)
    total_fg3_makes      = db.Column(db.Integer, default=0)
    total_fg3_attempts   = db.Column(db.Integer, default=0)
    total_fta            = db.Column(db.Integer)
    total_ftm            = db.Column(db.Integer)
    total_blue_collar    = db.Column(db.Integer, default=0)
    total_possessions    = db.Column(db.Integer)

    assist_pct           = db.Column(db.Float, nullable=False, default=0.0)
    turnover_pct         = db.Column(db.Float, nullable=False, default=0.0)
    tcr_pct              = db.Column(db.Float, nullable=False, default=0.0)
    oreb_pct             = db.Column(db.Float, nullable=False, default=0.0)
    ft_rate              = db.Column(db.Float, nullable=False, default=0.0)
    good_shot_pct        = db.Column(db.Float, nullable=False, default=0.0)
    is_opponent          = db.Column(db.Boolean, default=False)
    total_atr_fouled     = db.Column(db.Integer, default=0)
    total_fg2_fouled     = db.Column(db.Integer, default=0)
    total_fg3_fouled     = db.Column(db.Integer, default=0)
    wins                 = db.Column(db.Integer, nullable=False, default=0)
    losses               = db.Column(db.Integer, nullable=False, default=0)


class SkillEntry(db.Model):
    __tablename__ = 'skill_entries'

    id = db.Column(db.Integer, primary_key=True)

    # foreign key to Roster.id; always required
    player_id = db.Column(db.Integer, db.ForeignKey('roster.id'), nullable=False)

    # every entry—generic skill, shot drill, or NBA100—needs a date
    date = db.Column(db.Date, nullable=False)

    # for “generic” skills and NBA100, we use skill_name + value
    skill_name = db.Column(db.String(64), nullable=False, default="")
    value      = db.Column(db.Integer, nullable=False, default=0)

    # for “shot‐drill” entries (ATR, Floater, 3FG, FT), we use these fields:
    shot_class  = db.Column(db.String(20), nullable=True)   # 'atr','floater','3fg','ft'
    subcategory = db.Column(db.String(50), nullable=True)   # e.g. 'Right Hand', 'Free Throw'
    makes       = db.Column(db.Integer, nullable=False, default=0)
    attempts    = db.Column(db.Integer, nullable=False, default=0)

    @property
    def fg_pct(self):
        """Returns makes/attempts as a percentage (0.0 if attempts=0)."""
        return round(self.makes / self.attempts * 100, 1) if self.attempts else 0.0

    # Relationship back to Roster
    player = db.relationship('Roster', back_populates='skill_entries')


class PlayerStats(db.Model):
    id             = db.Column(db.Integer, primary_key=True)
    game_id        = db.Column(db.Integer, db.ForeignKey('game.id'), nullable=True, index=True)
    practice_id    = db.Column(db.Integer, db.ForeignKey('practice.id'), nullable=True, index=True)
    season_id      = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=False, index=True)
    player_name    = db.Column(db.String(100), nullable=False, index=True)
    jersey_number  = db.Column(db.Integer)

    atr_makes         = db.Column(db.Integer, default=0)
    atr_attempts      = db.Column(db.Integer, default=0)
    fg2_makes         = db.Column(db.Integer, default=0)
    fg2_attempts      = db.Column(db.Integer, default=0)
    fg3_makes         = db.Column(db.Integer, default=0)
    fg3_attempts      = db.Column(db.Integer, default=0)
    points            = db.Column(db.Integer)
    assists           = db.Column(db.Integer)
    turnovers         = db.Column(db.Integer)
    second_assists    = db.Column(db.Integer)
    pot_assists       = db.Column(db.Integer)
    fta               = db.Column(db.Integer)
    ftm               = db.Column(db.Integer)

    foul_by           = db.Column(db.Integer)
    contest_front     = db.Column(db.Integer)
    contest_side      = db.Column(db.Integer)
    contest_behind    = db.Column(db.Integer)
    contest_late      = db.Column(db.Integer)
    contest_no        = db.Column(db.Integer)
    contest_early     = db.Column(db.Integer)
    bump_positive     = db.Column(db.Integer)
    bump_missed       = db.Column(db.Integer)
    blowby_total      = db.Column(db.Integer)
    blowby_triple_threat = db.Column(db.Integer)
    blowby_closeout    = db.Column(db.Integer)
    blowby_isolation   = db.Column(db.Integer)
    atr_fouled         = db.Column(db.Integer, default=0)
    fg2_fouled         = db.Column(db.Integer, default=0)
    fg3_fouled         = db.Column(db.Integer, default=0)
    shot_type_details  = db.Column(db.Text, nullable=True)
    stat_details       = db.Column(db.Text, nullable=True)
    practice_wins   = db.Column(db.Integer, default=0)
    practice_losses = db.Column(db.Integer, default=0)
    sprint_wins     = db.Column(db.Integer, default=0)
    sprint_losses   = db.Column(db.Integer, default=0)



class BlueCollarStats(db.Model):
    id                = db.Column(db.Integer, primary_key=True)
    game_id           = db.Column(db.Integer, db.ForeignKey('game.id'), nullable=True,  index=True)
    practice_id       = db.Column(db.Integer, db.ForeignKey('practice.id'), nullable=True, index=True)
    season_id         = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=False, index=True)
    player_id         = db.Column(db.Integer, db.ForeignKey('roster.id'), nullable=True, index=True)
    def_reb           = db.Column(db.Integer)
    off_reb           = db.Column(db.Integer)
    misc              = db.Column(db.Integer)
    deflection        = db.Column(db.Integer)
    steal             = db.Column(db.Integer)
    block             = db.Column(db.Integer)
    floor_dive        = db.Column(db.Integer)
    charge_taken      = db.Column(db.Integer)
    reb_tip           = db.Column(db.Integer)
    total_blue_collar = db.Column(db.Integer)


class OpponentBlueCollarStats(db.Model):
    id                = db.Column(db.Integer, primary_key=True)
    game_id           = db.Column(db.Integer, db.ForeignKey('game.id'), nullable=True,  index=True)
    practice_id       = db.Column(db.Integer, db.ForeignKey('practice.id'), nullable=True, index=True)
    season_id         = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=False, index=True)
    player_id         = db.Column(db.Integer, db.ForeignKey('roster.id'), nullable=True, index=True)
    def_reb           = db.Column(db.Integer)
    off_reb           = db.Column(db.Integer)
    misc              = db.Column(db.Integer)
    deflection        = db.Column(db.Integer)
    steal             = db.Column(db.Integer)
    block             = db.Column(db.Integer)
    floor_dive        = db.Column(db.Integer)
    charge_taken      = db.Column(db.Integer)
    reb_tip           = db.Column(db.Integer)
    total_blue_collar = db.Column(db.Integer)


class Possession(db.Model):
    id                  = db.Column(db.Integer, primary_key=True)
    game_id             = db.Column(db.Integer, db.ForeignKey('game.id'), nullable=False, index=True)
    practice_id         = db.Column(db.Integer, db.ForeignKey('practice.id'), nullable=True, index=True)
    season_id           = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=False, index=True)
    time_segment        = db.Column(db.String(20))
    possession_side     = db.Column(db.String(20))
    player_combinations = db.Column(db.String(255))
    possession_start    = db.Column(db.String(50))
    possession_type     = db.Column(db.String(50))
    paint_touches       = db.Column(db.String(10))
    shot_clock          = db.Column(db.String(10))
    shot_clock_pt       = db.Column(db.String(10))
    points_scored       = db.Column(db.Integer, default=0)


class PlayerPossession(db.Model):
    id             = db.Column(db.Integer, primary_key=True)
    possession_id  = db.Column(db.Integer, db.ForeignKey('possession.id'), nullable=False, index=True)
    player_id      = db.Column(db.Integer, db.ForeignKey('roster.id'), nullable=False, index=True)


class Roster(db.Model):
    id           = db.Column(db.Integer, primary_key=True)
    season_id    = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=False)
    player_name  = db.Column(db.String(100), nullable=False)

    __table_args__ = (
        db.UniqueConstraint('season_id', 'player_name', name='_season_player_uc'),
    )

    # Relationship for skill development entries
    skill_entries = db.relationship(
        'SkillEntry',
        back_populates='player',
        cascade='all, delete-orphan'
    )

    def __repr__(self):
        return f"<Roster(season_id={self.season_id}, player_name='{self.player_name}')>"

# Re-export for compatibility with older imports
from .uploaded_file import UploadedFile
