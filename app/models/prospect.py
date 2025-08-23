from app import db
import sqlalchemy as sa

class Prospect(db.Model):
    __tablename__ = "prospects"

    id = sa.Column(sa.Integer, primary_key=True)

    # Provenance
    sheet = sa.Column(sa.String(64), index=True)  # e.g., SEC, Big Ten, ACC, BIG 12, etc.

    # Coach / program (denormed from workbook for Money Board rollups)
    coach = sa.Column(sa.String(128), index=True, nullable=False)
    coach_current_team = sa.Column(sa.String(128))
    coach_current_conference = sa.Column(sa.String(64))

    # Player identifiers
    player = sa.Column(sa.String(128), index=True, nullable=False)
    player_class = sa.Column(sa.String(32))
    age = sa.Column(sa.Float)
    team = sa.Column(sa.String(128))
    player_conference = sa.Column(sa.String(64))
    year = sa.Column(sa.Integer, index=True)

    # Money (all in same units as workbook)
    projected_money = sa.Column(sa.Float)
    actual_money = sa.Column(sa.Float)
    net = sa.Column(sa.Float)  # actual_money - projected_money

    # Picks (raw text + parsed number)
    projected_pick_raw = sa.Column(sa.String(32))
    actual_pick_raw    = sa.Column(sa.String(32))
    projected_pick     = sa.Column(sa.Float)
    actual_pick        = sa.Column(sa.Float)

    # Measurements (raw strings + normalized inches)
    height_raw = sa.Column(sa.String(16))
    wingspan_raw = sa.Column(sa.String(16))
    height_in = sa.Column(sa.Float)
    wingspan_in = sa.Column(sa.Float)
    ws_minus_h_in = sa.Column(sa.Float)

    # Hometown / geo (lat/lon to be populated later)
    home_city = sa.Column(sa.String(128))
    home_state = sa.Column(sa.String(64))
    country = sa.Column(sa.String(64))
    latitude = sa.Column(sa.Float)
    longitude = sa.Column(sa.Float)
    geocode_status = sa.Column(sa.String(32))  # ok | partial | missing

    # Timestamps
    created_at = sa.Column(sa.DateTime, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime, onupdate=sa.func.now())

    __table_args__ = (
        sa.UniqueConstraint("player", "team", "year", name="uq_prospect_player_team_year"),
    )
