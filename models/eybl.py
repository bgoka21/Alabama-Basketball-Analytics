from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Float, Text, ForeignKey, UniqueConstraint, Index
from .database import db
from .recruit import Recruit


class ExternalIdentityMap(db.Model):
    __tablename__ = "external_identity_map"

    id = Column(Integer, primary_key=True)
    recruit_id = Column(Integer, ForeignKey("recruit.id"), nullable=True)
    source_system = Column(String(64), nullable=False, default="synergy_portal_csv")
    external_key = Column(String(128), nullable=False, unique=True)
    player_name_external = Column(String(128), nullable=False)
    team_external = Column(String(128), nullable=False)
    circuit = Column(String(32), nullable=False)
    season_year = Column(Integer, nullable=True)
    season_type = Column(String(32), nullable=True, default="AAU")
    match_confidence = Column(Float, nullable=False, default=0.0)
    is_verified = Column(db.Boolean, nullable=False, default=False)
    created_at = Column(DateTime, server_default=db.func.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('external_key', name='uq_external_identity_map_external_key'),
        Index('ix_ext_ident_recruit_circuit_season', 'recruit_id', 'circuit', 'season_year', 'season_type'),
    )


class UnifiedStats(db.Model):
    __tablename__ = "unified_stats"

    id = Column(Integer, primary_key=True)
    recruit_id = Column(Integer, ForeignKey("recruit.id"), nullable=False)
    circuit = Column(String(32), nullable=False)
    season_year = Column(Integer, nullable=True)
    season_type = Column(String(32), nullable=True, default="AAU")
    team_name = Column(String(128), nullable=True)
    gp = Column(Float, nullable=True)
    ppg = Column(Float, nullable=True)
    ast = Column(Float, nullable=True)
    tov = Column(Float, nullable=True)
    fg_pct = Column(Float, nullable=True)
    ppp = Column(Float, nullable=True)
    pnr_poss = Column(Integer, nullable=True)
    pnr_ppp = Column(Float, nullable=True)
    pnr_to_pct = Column(Float, nullable=True)
    pnr_score_pct = Column(Float, nullable=True)
    source_system = Column(String(64), nullable=False, default="synergy_portal_csv")
    ingested_at = Column(DateTime, server_default=db.func.now(), nullable=False)
    original_filenames = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint('recruit_id', 'circuit', 'season_year', 'season_type', 'team_name',
                         name='uq_unified_stats_unique'),
        Index('ix_unified_stats_circuit_season', 'circuit', 'season_year'),
    )


class IdentitySynonym(db.Model):
    """Simple mapping to normalize external player or team names."""

    __tablename__ = "identity_synonym"

    id = Column(Integer, primary_key=True)
    kind = Column(String(16), nullable=False)  # 'name' or 'team'
    source_value = Column(String(128), nullable=False)
    normalized_value = Column(String(128), nullable=False)
    created_at = Column(DateTime, server_default=db.func.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('kind', 'source_value', name='uq_identity_synonym_kind_source'),
    )

