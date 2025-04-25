from .database import db
from datetime import datetime

class UploadedFile(db.Model):
    __tablename__ = 'uploaded_files'

    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    upload_date = db.Column(db.DateTime, default=datetime.utcnow)

    # Parsing status and logs
    parse_status = db.Column(db.String(50), default='Not Parsed')
    parse_log = db.Column(db.Text, nullable=True)
    parse_error = db.Column(db.Text, nullable=True)  # Error message if parsing fails
    last_parsed = db.Column(db.DateTime, nullable=True)

    # Breakdown data stored as JSON strings
    offensive_breakdown = db.Column(db.Text, nullable=True)
    defensive_breakdown = db.Column(db.Text, nullable=True)
    lineup_efficiencies = db.Column(db.Text, nullable=True)

    # ✅ Category filter
    category = db.Column(db.String(50), nullable=True)  # e.g., 'Game' or 'Practice'
