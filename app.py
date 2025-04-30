import os
from flask import Flask, redirect, url_for, render_template
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_migrate import Migrate

from models.database import db
from models.user import User
from admin.routes import admin_bp

# Optional: Import auth blueprint if it exists
try:
    from auth.routes import auth_bp
    AUTH_EXISTS = True
except ImportError:
    AUTH_EXISTS = False


def create_app():
    app = Flask(__name__)

    # --- Basic Configuration ---
    app.config['SECRET_KEY'] = 'your_secret_key_here'

    # Database path setup
    basedir = os.path.abspath(os.path.dirname(__file__))
    instance_path = os.path.join(basedir, 'instance')
    db_path = os.path.join(instance_path, 'database.db')
    if not os.path.exists(instance_path):
        os.makedirs(instance_path)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + db_path

    # Upload folder configuration
    upload_folder = os.path.join(basedir, 'data', 'uploads')
    app.config['UPLOAD_FOLDER'] = upload_folder
    os.makedirs(upload_folder, exist_ok=True)

    # --- Initialize Extensions ---
    db.init_app(app)
    Migrate(app, db)
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login' if AUTH_EXISTS else 'admin.login'

    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    # --- Jinja2 Filters ---
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

        r, g, b = (min(round(c * 2.55), 255) for c in base)
        return f"background-color: rgb({r},{g},{b});"

    def grade_3fg_pct(pct, attempts):
        if not attempts:
            return ""
        try:
            v = float(pct)
        except:
            v = 0.0

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
    app.jinja_env.filters['grade_3fg_pct'] = grade_3fg_pct

    # --- Register Blueprints ---
    from public.routes import public_bp
    app.register_blueprint(public_bp)

    app.register_blueprint(admin_bp, url_prefix='/admin')

    if AUTH_EXISTS:
        app.register_blueprint(auth_bp, url_prefix='/auth')

    # --- Public Home Route ---
    @app.route('/')
    def home():
        return render_template('public/home.html')

    return app

# Create the app instance for CLI & WSGI
app = create_app()

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=True)
