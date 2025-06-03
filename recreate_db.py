# recreate_db.py
from app import create_app, db

app = create_app()
with app.app_context():
    # Drop all existing tables
    db.drop_all()
    # Recreate all tables based on your models
    db.create_all()
    print("All tables dropped and recreated.")
