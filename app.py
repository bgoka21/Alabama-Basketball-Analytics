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
    


    # --- Jinja2 Filters for coloring percentages ---
    @app.template_filter()
    def grade_atr2fg_pct(pct, attempts):
        """
        Return an inline background-color style based on ATR/2FG percentage.
        If there are no attempts, returns an empty string.
        """
        if not attempts:
            return ""
        # pick a base RGB tuple by threshold
        if pct >= 70:
            base = (0, 128, 0)    # green
        elif pct >= 50:
            base = (255, 165, 0)  # orange
        else:
            base = (255, 0, 0)    # red
        r, g, b = base
        return f"background-color: rgb({r},{g},{b});"

    @app.template_filter()
    def grade_3fg_pct(pct, attempts):
        """
        Return an inline background-color style based on 3FG percentage.
        If there are no attempts, returns an empty string.
        """
        if not attempts:
            return ""
        if pct >= 40:
            base = (0, 128, 0)
        elif pct >= 30:
            base = (255, 165, 0)
        else:
            base = (255, 0, 0)
        r, g, b = base
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
