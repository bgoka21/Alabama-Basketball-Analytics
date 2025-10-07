from datetime import date, datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint
from sqlalchemy.sql import func

# Initialize SQLAlchemy
db = SQLAlchemy()


class CachedLeaderboard(db.Model):
    __tablename__ = 'cached_leaderboards'

    id = db.Column(db.Integer, primary_key=True)
    season_id = db.Column(db.Integer, index=True, nullable=True)
    stat_key = db.Column(db.String(128), index=True, nullable=False)
    schema_version = db.Column(db.Integer, nullable=False)
    formatter_version = db.Column(db.Integer, nullable=False)
    etag = db.Column(db.String(64), nullable=False)
    payload_json = db.Column(db.Text, nullable=False)
    created_at = db.Column(
        db.DateTime,
        nullable=False,
        default=func.now(),
    )
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=func.now(),
        onupdate=func.now(),
    )
    build_manifest = db.Column(db.Text, nullable=True)

    __table_args__ = (
        db.UniqueConstraint(
            'season_id', 'stat_key', name='uq_cached_leaderboards_season_stat'
        ),
    )



class Season(db.Model):
    id          = db.Column(db.Integer, primary_key=True)
    season_name = db.Column(db.String(20), unique=True, nullable=False)
    start_date  = db.Column(db.Date)
    end_date    = db.Column(db.Date)

    games       = db.relationship('Game',   backref='season', lazy=True)
    roster      = db.relationship('Roster', backref='season', lazy=True)


class Session(db.Model):
    __tablename__ = 'session'
    id = db.Column(db.Integer, primary_key=True)
    season_id = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=False)
    name = db.Column(db.String(50), nullable=False)
    start_date = db.Column(db.Date, nullable=False)
    end_date = db.Column(db.Date, nullable=False)
    __table_args__ = (
        UniqueConstraint('season_id', 'name', name='_season_session_uc'),
    )
    season = db.relationship('Season', backref=db.backref('sessions', lazy=True))


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
    date            = db.Column(db.Date, nullable=True)
    category        = db.Column(db.String(20), nullable=False)  # Summer Workouts, Fall Workouts, etc.
    created_at      = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        server_default=func.now(),
        nullable=True,
    )

    team_stats      = db.relationship('TeamStats',               backref='practice', lazy=True)
    player_stats    = db.relationship('PlayerStats',             backref='practice', lazy=True)
    blue_collar     = db.relationship('BlueCollarStats',         backref='practice', lazy=True)
    opp_blue_collar = db.relationship('OpponentBlueCollarStats', backref='practice', lazy=True)
    possessions     = db.relationship('Possession',              backref='practice', lazy=True)

    def __repr__(self):
        return f"<Practice {self.date} [{self.category}]>"


class TeamStats(db.Model):
    id                   = db.Column(db.Integer, primary_key=True)
    game_id              = db.Column(db.Integer, db.ForeignKey('game.id'), nullable=True, index=True)
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
    atr_contest_attempts      = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    atr_contest_makes         = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    atr_late_attempts         = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    atr_late_makes            = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    atr_no_contest_attempts   = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    atr_no_contest_makes      = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg2_contest_attempts      = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg2_contest_makes         = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg2_late_attempts         = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg2_late_makes            = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg2_no_contest_attempts   = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg2_no_contest_makes      = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg3_contest_attempts      = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg3_contest_makes         = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg3_late_attempts         = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg3_late_makes            = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg3_no_contest_attempts   = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    fg3_no_contest_makes      = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    pass_contest_positive = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    pass_contest_missed   = db.Column(db.Integer, nullable=False, default=0, server_default="0")
    bump_positive     = db.Column(db.Integer)
    bump_missed       = db.Column(db.Integer)
    # --- Offensive Rebounding Opportunities ---
    crash_positive        = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # Off +
    crash_missed          = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # Off -
    back_man_positive     = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # BM +
    back_man_missed       = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # BM -

    # --- Defensive Rebounding Opportunities ---
    box_out_positive      = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # Def +
    box_out_missed        = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # Def -
    off_reb_given_up      = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # Given Up

    # --- Collision Gap Help (Crimson + White aggregated) ---
    collision_gap_positive = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # Gap +
    collision_gap_missed   = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # Gap -

    # --- PnR Gap Help & Low ---
    pnr_gap_positive      = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # Gap +
    pnr_gap_missed        = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # Gap -
    low_help_positive     = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # Low +
    low_help_missed       = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # Low -

    # --- PnR Grade (Defense) ---
    close_window_positive = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # CW +
    close_window_missed   = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # CW -
    shut_door_positive    = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # SD +
    shut_door_missed      = db.Column(db.Integer, nullable=False, default=0, server_default="0")  # SD -
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
    drill_labels       = db.Column(db.String(255))


class PlayerPossession(db.Model):
    id             = db.Column(db.Integer, primary_key=True)
    possession_id  = db.Column(db.Integer, db.ForeignKey('possession.id'), nullable=False, index=True)
    player_id      = db.Column(db.Integer, db.ForeignKey('roster.id'), nullable=False, index=True)


class ShotDetail(db.Model):
    """Detailed event or shot occurring within a possession."""
    id            = db.Column(db.Integer, primary_key=True)
    possession_id = db.Column(db.Integer, db.ForeignKey('possession.id'), nullable=False, index=True)
    event_type    = db.Column(db.String(64), nullable=False)


class PnRStats(db.Model):
    __tablename__ = 'pnr_stats'
    id                = db.Column(db.Integer, primary_key=True)
    game_id           = db.Column(db.Integer, db.ForeignKey('game.id'), nullable=True)
    practice_id       = db.Column(db.Integer, db.ForeignKey('practice.id'), nullable=True)
    possession_id     = db.Column(db.Integer, nullable=False)
    player_id         = db.Column(db.Integer, db.ForeignKey('roster.id'), nullable=False)
    role              = db.Column(db.String(10), nullable=False)        # "BH" or "Screener"
    advantage_created = db.Column(db.String(5), nullable=False)         # "Adv+" or "Adv-"
    direct            = db.Column(db.Boolean, nullable=False)
    points_scored     = db.Column(db.Integer, nullable=True)
    turnover_occurred = db.Column(db.Boolean, nullable=True)
    assist_occurred   = db.Column(db.Boolean, nullable=True)
    start_time        = db.Column(db.Float, nullable=False)
    duration          = db.Column(db.Float, nullable=False)


class Roster(db.Model):
    id           = db.Column(db.Integer, primary_key=True)
    season_id    = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=False)
    player_name  = db.Column(db.String(100), nullable=False)
    headshot_filename = db.Column(db.String(255))

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


class PageView(db.Model):
    __tablename__ = 'page_view'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    endpoint = db.Column(db.String(128), nullable=False)
    path = db.Column(db.String(256), nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    user_agent = db.Column(db.String(256), nullable=True)


class Setting(db.Model):
    __tablename__ = 'setting'

    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(64), unique=True, nullable=False)
    value = db.Column(db.String(255), nullable=True)


class PlayerDraftStock(db.Model):
    __tablename__ = 'player_draft_stock'
    id                = db.Column(db.Integer, primary_key=True)
    coach             = db.Column(db.String(128))
    coach_current_team= db.Column(db.String(128))
    player            = db.Column(db.String(128))
    player_class      = db.Column(db.String(32))
    age               = db.Column(db.Float)
    team              = db.Column(db.String(128))
    conference        = db.Column(db.String(128))
    year              = db.Column(db.Integer)
    projected_pick    = db.Column(db.String(32))
    actual_pick       = db.Column(db.String(32))
    projected_money   = db.Column(db.Float)
    actual_money      = db.Column(db.Float)
    net               = db.Column(db.Float)
    # ── New Bio Fields ──
    high_school       = db.Column(db.String(128))
    hometown_city     = db.Column(db.String(128))
    hometown_state    = db.Column(db.String(64))
    height            = db.Column(db.String(32))
    weight            = db.Column(db.Float)
    position          = db.Column(db.String(64))


class PlayerDevelopmentPlan(db.Model):
    __tablename__ = 'player_development_plan'

    id = db.Column(db.Integer, primary_key=True)
    player_name = db.Column(db.String(100), nullable=False)
    season_id = db.Column(db.Integer, db.ForeignKey('season.id'))

    stat_1_name = db.Column(db.String(64))
    stat_1_goal = db.Column(db.Float)
    stat_2_name = db.Column(db.String(64))
    stat_2_goal = db.Column(db.Float)
    stat_3_name = db.Column(db.String(64))
    stat_3_goal = db.Column(db.Float)

    note_1 = db.Column(db.Text)
    note_2 = db.Column(db.Text)
    note_3 = db.Column(db.Text)


class SavedStatProfile(db.Model):
    __tablename__ = "saved_stat_profile"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), index=True, nullable=False)

    # JSON of field keys in the order chosen (e.g., ["pts", "fg", "fg3", "ft", "bcp_total"])
    fields_json = db.Column(db.Text, nullable=True, default="[]")
    # JSON array of player ids included in the preset (e.g., [12, 45, 99])
    players_json = db.Column(db.Text, nullable=True, default="[]")

    preset_type = db.Column(db.String(16), nullable=False, default="combined")
    date_from = db.Column(db.Date, nullable=True)
    date_to = db.Column(db.Date, nullable=True)

    # MVP fixed defaults for now
    mode_default = db.Column(db.String(32), nullable=False, default="totals")     # "totals" | "per_practice"
    source_default = db.Column(db.String(32), nullable=False, default="practice") # "practice" | "game" | "both"

    owner_id = db.Column(db.Integer, nullable=True)  # creator's user id (if available)
    visibility = db.Column(db.String(16), nullable=False, default="team")  # "team" or "private"

    created_at = db.Column(db.DateTime, server_default=func.now(), nullable=False)
    updated_at = db.Column(db.DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

# Re-export for compatibility with older imports
from .uploaded_file import UploadedFile
