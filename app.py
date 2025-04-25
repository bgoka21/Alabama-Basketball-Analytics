# app.py

import os
from flask import Flask, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_migrate import Migrate  # Newly added for migrations
from models.database import db  # Your existing db object
from models.user import User    # User model, ensure user.py is inside models/
from admin.routes import admin_bp

# Optional: Import auth if it exists (Fix for auth.login issue)
try:
    from auth.routes import auth_bp
    AUTH_EXISTS = True
except ImportError:
    AUTH_EXISTS = False

def create_app():
    app = Flask(__name__)

    # --- Basic Configuration ---
    app.config['SECRET_KEY'] = 'your_secret_key_here'

    # Corrected DB Path Setup (Absolute Path)
    basedir = os.path.abspath(os.path.dirname(__file__))
    instance_path = os.path.join(basedir, 'instance')
    db_path = os.path.join(instance_path, 'database.db')

    

    # Ensure instance directory exists
    if not os.path.exists(instance_path):
        os.makedirs(instance_path)

    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + db_path

    # Folder for uploaded CSV files
    upload_folder = os.path.join(basedir, 'data', 'uploads')
    app.config['UPLOAD_FOLDER'] = upload_folder
    os.makedirs(upload_folder, exist_ok=True)

    # --- Initialize Database ---
    db.init_app(app)
    
    # --- Initialize Flask-Migrate ---
    migrate = Migrate(app, db)

    # --- Flask-Login Setup ---
    login_manager = LoginManager()
    login_manager.init_app(app)
    
    def grade_atr2fg_pct(pct, attempts):
        if not attempts:
            return ""
        try:
            v = float(pct)
        except:
            v = 0.0

        if v == 0:
            base = (100, 26, 36)
        else:
            bins = [
                (35,  (100, 26, 36)),
                (40,  (100, 44, 54)),
                (45,  (100, 62, 72)),
                (50,  (100,100, 85)),
                (55,  (100,100, 70)),
                (60,  (100,100, 50)),
                (65,  ( 80,100, 90)),
                (70,  ( 71,100, 81)),
                (75,  ( 62,100, 72)),
                (100, (  0,100,  0)),
            ]
            for thresh, rgb in bins:
                if v <= thresh:
                    base = rgb
                    break

        # correct 0–255 mapping
        r, g, b = (min(round(c * 2.55), 255) for c in base)
        return f"background-color: rgb({r},{g},{b});"


    def grade_3fg_pct(pct, attempts):
        if not attempts:
            return ""
        try:
            v = float(pct)
        except:
            v = 0.0

        # new code – 0 % with attempts → lowest‑red bucket
        if v == 0:
            base = (100,26,36)

        else:
            bins = [
                (18,  (100, 26, 36)),
                (21,  (100, 44, 54)),
                (24,  (100, 62, 72)),
                (27,  (100,100, 85)),
                (30,  (100,100, 70)),
                (33,  (100,100, 50)),
                (36,  ( 80,100, 90)),
                (39,  ( 71,100, 81)),
                (42,  ( 62,100, 72)),
                (45,  ( 53,100, 63)),
                (100, (  0,100,  0)),
            ]
            for thresh, rgb in bins:
                if v <= thresh:
                    base = rgb
                    break

        r, g, b = (min(round(c * 2.55), 255) for c in base)
        return f"background-color: rgb({r},{g},{b});"


    app.jinja_env.filters['grade_atr2fg_pct'] = grade_atr2fg_pct
    app.jinja_env.filters['grade_3fg_pct']  = grade_3fg_pct




    # Fix: Ensure login route exists before assigning it
    if AUTH_EXISTS:
        login_manager.login_view = 'auth.login'  # Redirects to auth.login only if auth exists
    else:
        login_manager.login_view = 'admin.dashboard'  # Default to admin dashboard if no auth

    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    # --- Basic Home Route ---
    @app.route('/')
    def home():
        return "🏀 Basketball Analytics Home - Flask App is Running!"
    

    # --- Blueprint Registration ---
    app.register_blueprint(admin_bp, url_prefix='/admin')
    
    # Register auth blueprint if it exists
    if AUTH_EXISTS:
        app.register_blueprint(auth_bp, url_prefix='/auth')

    return app


if __name__ == "__main__":
    app = create_app()

    with app.app_context():
        # Create database tables. Note: If you use migrations, the tables will be created/updated via flask db upgrade.
        db.create_all()

    # Run the Flask app
    app.run(debug=True)
