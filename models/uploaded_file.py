# models/uploaded_file.py  (or wherever your UploadedFile is defined)
from .database import db
from datetime import datetime, date   # ← add `date` here

class UploadedFile(db.Model):
    __tablename__ = 'uploaded_files'

    id             = db.Column(db.Integer, primary_key=True)
    season_id      = db.Column(db.Integer, db.ForeignKey('season.id'), nullable=False)
    recruit_id     = db.Column(db.Integer, db.ForeignKey('recruit.id'), nullable=True)
    filename       = db.Column(db.String(255), nullable=False)
    upload_date    = db.Column(db.DateTime, default=datetime.utcnow)

    # new: the date of the CSV contents
    file_date      = db.Column(db.Date, nullable=False)

    # Parsing status and logs
    parse_status   = db.Column(db.String(50), default='Not Parsed')
    parse_log      = db.Column(db.Text,   nullable=True)
    parse_error    = db.Column(db.Text,   nullable=True)
    last_parsed    = db.Column(db.DateTime, nullable=True)

    # relationship back to Season
    season         = db.relationship('Season', backref=db.backref('uploaded_files', lazy=True))
    recruit        = db.relationship('Recruit', backref=db.backref('uploaded_files', lazy=True))

    # Breakdown data stored as JSON strings
    offensive_breakdown = db.Column(db.Text, nullable=True)
    defensive_breakdown = db.Column(db.Text, nullable=True)
    lineup_efficiencies = db.Column(db.Text, nullable=True)
    player_on_off       = db.Column(db.Text, nullable=True)

    # ✅ Category filter
    category       = db.Column(db.String(50), nullable=True)
