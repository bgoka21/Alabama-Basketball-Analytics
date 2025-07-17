import os
from flask import Flask, redirect, url_for, render_template, request
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, current_user
from flask_migrate import Migrate
from flask_apscheduler import APScheduler
import pdfkit

from datetime import datetime
from models.database import db, PageView
from models.user import User
from admin.routes import admin_bp
from merge_app.app import merge_bp
from scraping.recruit_scraper import run_full_refresh

scheduler = APScheduler()

try:
    PDFKIT_CONFIG = pdfkit.configuration()
except OSError:
    PDFKIT_CONFIG = None
PDF_OPTIONS = {
    'page-size': 'Letter',
    'margin-top': '0.75in',
    'margin-right': '0.75in',
    'margin-bottom': '0.75in',
    'margin-left': '0.75in',
    'encoding': 'UTF-8',
    'enable-local-file-access': None
}

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
    app.config['SYNERGY_API_KEY'] = "0vBg4oX7mqNx"
    app.config['SYNERGY_CLIENT_ID'] = os.environ.get('SYNERGY_CLIENT_ID', 'client.basketball.alabamambb')
    app.config['SYNERGY_CLIENT_SECRET'] = os.environ.get('SYNERGY_CLIENT_SECRET', '0vBg4oX7mqNx')

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

    headshot_folder = os.path.join(app.static_folder, 'headshots')
    os.makedirs(headshot_folder, exist_ok=True)

    # --- Initialize Extensions ---
    db.init_app(app)
    Migrate(app, db)
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login' if AUTH_EXISTS else 'admin.login'

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))
    


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

    if scheduler.state == 0:
        scheduler.init_app(app)
        scheduler.start()
        scheduler.add_job(
            id='refresh_recruits',
            func=lambda: run_full_refresh(years=[2025, 2024]),
            trigger='cron', hour=3, minute=0
        )

    if AUTH_EXISTS:
        app.register_blueprint(auth_bp, url_prefix='/auth')

    # Ensure all tables exist when the application starts
    with app.app_context():
        db.create_all()

    @app.before_request
    def log_page_view():
        if request.endpoint in ('static', None):
            return
        pv = PageView(
            user_id=current_user.get_id() if current_user.is_authenticated else None,
            endpoint=request.endpoint,
            path=request.path,
            timestamp=datetime.utcnow(),
            user_agent=request.user_agent.string,
        )
        db.session.add(pv)
        db.session.commit()

    # --- Public Home Route ---
    @app.route('/')
    def home():
        return render_template('public/home.html')

    # Register routes defined outside this factory so Flask sees them even when
    # using ``flask --app app:create_app`` to run the app. Expose the app
    # variable first so ``routes`` can import it without a circular import.
    globals()['app'] = app
    import routes  # noqa: F401
    from routes import draft_upload  # noqa: F401

    return app

# Create the app instance for CLI & WSGI
app = create_app()

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=True)
