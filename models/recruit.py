from datetime import datetime
from sqlalchemy import Column, Integer, String, Date, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship
from .database import db

class Recruit(db.Model):
    __tablename__ = "recruit"
    id                = Column(Integer, primary_key=True)
    school            = Column(String(128), nullable=True, default="")  # legacy, ignored by form
    name              = Column(String(128), nullable=False)
    graduation_year   = Column(Integer)
    position          = Column(String(32))
    height            = Column(String(16))
    weight            = Column(Integer)
    high_school       = Column(String(128))
    hometown          = Column(String(128))
    rating            = Column(Integer)
    ranking           = Column(Integer)
    camp_performance  = Column(Text)
    offer_status      = Column(String(32))
    offer_date        = Column(Date)
    commit_date       = Column(Date)
    email             = Column(String(128))
    phone             = Column(String(32))
    profile_image_url = Column(String(256))
    notes             = Column(Text)
    created_at        = Column(DateTime, default=datetime.utcnow)
    updated_at        = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    shot_type_stats = relationship(
        "RecruitShotTypeStat",
        back_populates="recruit",
        cascade="all, delete-orphan"
    )
    top_schools = relationship(
        "RecruitTopSchool",
        back_populates="recruit",
        cascade="all, delete-orphan",
        order_by="RecruitTopSchool.rank"
    )

class RecruitShotTypeStat(db.Model):
    __tablename__ = "recruit_shot_type_stat"
    id                = Column(Integer, primary_key=True)
    recruit_id        = Column(Integer, ForeignKey("recruit.id"), nullable=False)
    shot_type_details = Column(Text, nullable=False)
    created_at        = Column(DateTime, default=datetime.utcnow)
    updated_at        = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    recruit = relationship("Recruit", back_populates="shot_type_stats")

class RecruitTopSchool(db.Model):
    __tablename__ = "recruit_top_school"
    id         = Column(Integer, primary_key=True)
    recruit_id = Column(Integer, ForeignKey("recruit.id"), nullable=False)
    school_name= Column(String(128), nullable=False)
    rank       = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    recruit = relationship("Recruit", back_populates="top_schools")
