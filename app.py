import os
from flask import Flask, redirect, url_for, render_template
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_migrate import Migrate

from models.database import db
from models.user import User
from admin.routes import admin_bp
from merge_app.app import merge_bp

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
        """Return a gradient background-color style for ATR/2FG percentage.

        The color mapping mirrors ``grade_pps`` by converting the field goal
        percentage into points per shot (value of a made 2-pointer).
        """
        if not attempts:
            return ""

        pps = (pct / 100.0) * 2
        return grade_pps(pps, attempts)

    @app.template_filter()
    def grade_3fg_pct(pct, attempts):
        """Return a gradient background-color style for 3FG percentage.

        The gradient is calculated using ``grade_pps`` with the 3-point shot
        value so FG% and PPS share the same color logic.
        """
        if not attempts:
            return ""

        pps = (pct / 100.0) * 3
        return grade_pps(pps, attempts)

    @app.template_filter()
    def grade_pps(pps, attempts):
        """Return an inline background-color style for points per shot.

        Shades of green, yellow and red are used for good, average and
        poor efficiency respectively. No style is returned when there are
        no attempts.
        """
        if not attempts:
            return ""

        def interpolate(start, end, factor):
            return tuple(
                round(s + (e - s) * max(0.0, min(factor, 1.0)))
                for s, e in zip(start, end)
            )

        if pps >= 1.1:
            start, end = (200, 255, 200), (0, 128, 0)
            factor = min((pps - 1.1) / 0.5, 1.0)
        elif pps >= 1.0:
            start, end = (255, 255, 224), (255, 215, 0)
            factor = (pps - 1.0) / 0.1
        else:
            start, end = (255, 200, 200), (255, 0, 0)
            factor = min((1.0 - pps) / 0.5, 1.0)

        r, g, b = interpolate(start, end, factor)
        return f"background-color: rgb({r},{g},{b});"


    app.jinja_env.filters['grade_atr2fg_pct'] = grade_atr2fg_pct
    app.jinja_env.filters['grade_3fg_pct'] = grade_3fg_pct
    app.jinja_env.filters['grade_pps'] = grade_pps

    # --- Register Blueprints ---
    from public.routes import public_bp
    app.register_blueprint(public_bp)

    app.register_blueprint(admin_bp, url_prefix='/admin')

    # Register merge tool blueprint under /merge
    app.register_blueprint(merge_bp, url_prefix='/merge')

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
