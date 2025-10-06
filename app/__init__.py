import os
import json
import click
from flask import Flask, redirect, url_for, render_template, request, flash, current_app
from flask.json.provider import DefaultJSONProvider
from types import SimpleNamespace
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, current_user
from flask_migrate import Migrate
from apscheduler.schedulers.background import BackgroundScheduler
from flask.cli import with_appcontext
from sqlalchemy import inspect
import pdfkit

from datetime import datetime, date
from models.database import db, PageView, SavedStatProfile
from models.user import User
from merge_app.app import merge_bp
from utils.auth import PLAYER_ALLOWED_ENDPOINTS
from app.utils.schema import ensure_columns
from app.utils.formatting import fmt_money, posneg_class

# Allow JSON serialization of SimpleNamespace values across all Flask apps
_orig_json_default = DefaultJSONProvider.default
def _ns_default(self, obj):
    if isinstance(obj, SimpleNamespace):
        return obj.__dict__
    return _orig_json_default(self, obj)
DefaultJSONProvider.default = _ns_default


def init_scheduler(app: Flask) -> None:
    if getattr(app, "_apscheduler_started", False):
        return

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.start()
    app.apscheduler = scheduler
    app.extensions.setdefault("apscheduler", scheduler)
    app._apscheduler_started = True
    app.logger.info("APScheduler started")


from admin.routes import admin_bp

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


def ensure_saved_stat_profile_table(app):
    with app.app_context():
        insp = inspect(db.engine)
        if 'saved_stat_profile' not in insp.get_table_names():
            SavedStatProfile.__table__.create(bind=db.engine, checkfirst=True)


# Optional: Import auth blueprint if it exists
try:
    from auth.routes import auth_bp
    AUTH_EXISTS = True
except ImportError:
    AUTH_EXISTS = False


def create_app():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    app = Flask(
        __name__,
        static_folder=os.path.join(repo_root, 'static'),
        template_folder=os.path.join(repo_root, 'templates'),
    )

    try:
        from flask_compress import Compress
        Compress(app)
    except Exception:
        app.logger.warning("Flask-Compress not available; skipping compression")

    # Allow JSON serialization of SimpleNamespace objects
    from types import SimpleNamespace
    _json_default = app.json.default
    def _default(obj):
        if isinstance(obj, SimpleNamespace):
            return obj.__dict__
        return _json_default(obj)
    app.json.default = _default

    # --- Basic Configuration ---
    app.config['SECRET_KEY'] = 'your_secret_key_here'
    app.config['SYNERGY_API_KEY'] = "0vBg4oX7mqNx"
    app.config['SYNERGY_CLIENT_ID'] = os.environ.get('SYNERGY_CLIENT_ID', 'client.basketball.alabamambb')
    app.config['SYNERGY_CLIENT_SECRET'] = os.environ.get('SYNERGY_CLIENT_SECRET', '0vBg4oX7mqNx')

    # Database path setup
    basedir = repo_root
    instance_path = os.path.join(basedir, 'instance')
    db_path = os.path.join(instance_path, 'database.db')
    if not os.path.exists(instance_path):
        os.makedirs(instance_path)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + db_path

    # Ingest directories for EYBL/AAU stats previews and snapshots
    ingest_previews = os.path.join(instance_path, 'ingest_previews')
    ingest_snapshots = os.path.join(instance_path, 'ingest_snapshots')
    os.makedirs(ingest_previews, exist_ok=True)
    os.makedirs(ingest_snapshots, exist_ok=True)
    app.config['INGEST_PREVIEWS_DIR'] = ingest_previews
    app.config['INGEST_SNAPSHOTS_DIR'] = ingest_snapshots

    # Upload folder configuration
    upload_folder = os.path.join(basedir, 'data', 'uploads')
    app.config['UPLOAD_FOLDER'] = upload_folder
    os.makedirs(upload_folder, exist_ok=True)

    headshot_folder = os.path.join(app.static_folder, 'headshots')
    os.makedirs(headshot_folder, exist_ok=True)

    # Allow up to 32 MB uploads (tune as needed)
    app.config.setdefault("MAX_CONTENT_LENGTH", 32 * 1024 * 1024)

    # --- Initialize Extensions ---
    db.init_app(app)
    ensure_saved_stat_profile_table(app)
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
    app.jinja_env.filters["fmt_money"] = fmt_money
    app.jinja_env.filters["posneg"] = posneg_class

    def display_pick(value):
        try:
            num, text = value
        except Exception:
            num, text = value, None
        if text and str(text).strip():
            return str(text).strip()
        if num is not None:
            try:
                return str(int(num))
            except Exception:
                return str(num)
        return ""

    app.jinja_env.filters['display_pick'] = display_pick

    # --- Register Blueprints ---
    from public.routes import public_bp
    app.register_blueprint(public_bp)

    from recruits import recruits_bp
    app.register_blueprint(recruits_bp, url_prefix='/recruits')

    app.register_blueprint(admin_bp, url_prefix='/admin')

    from recruits.admin_logo import bp_logo
    app.register_blueprint(bp_logo)

    # Register merge tool blueprint under /merge
    app.register_blueprint(merge_bp, url_prefix='/merge')

    if AUTH_EXISTS:
        app.register_blueprint(auth_bp, url_prefix='/auth')

    # Ensure all tables exist when the application starts unless skipped
    if not os.environ.get('SKIP_CREATE_ALL'):
        from app.models import prospect, coach  # noqa: F401  # ensure models are registered
        with app.app_context():
            db.create_all()
            # SAFE add: projected/actual pick fields (raw + numeric)
            ensure_columns(db.engine, "prospects", [
                ("projected_pick",     "REAL"),
                ("actual_pick",        "REAL"),
                ("projected_pick_raw", "TEXT"),
                ("actual_pick_raw",    "TEXT"),
                ("projected_pick_text","TEXT"),
                ("actual_pick_text",   "TEXT"),
            ])

    @app.before_request
    def restrict_player_routes():
        if request.endpoint in ('static', None):
            return
        if current_user.is_authenticated and current_user.is_player:
            if request.endpoint not in PLAYER_ALLOWED_ENDPOINTS:
                flash('You do not have permission to view that page.', 'error')
                if current_user.player_name:
                    target = url_for('admin.player_detail', player_name=current_user.player_name)
                else:
                    target = url_for('public.homepage')
                return redirect(target)

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

    from services.eybl_ingest import eybl_import_command
    app.cli.add_command(eybl_import_command)

    from app.cli.import_draft_stock import import_draft_stock
    app.cli.add_command(import_draft_stock)

    # --- BEGIN: cache CLI commands ---

    @app.cli.group("cache")
    def cache_cli():
        """Cache utilities."""
        pass

    @cache_cli.command("rebuild")
    @click.option("--season", "season_id", required=True, type=int, help="Season ID to rebuild")
    def cache_rebuild_cmd(season_id: int):
        """Rebuild all leaderboard caches for a season (formatted payloads)."""
        from services.cache_leaderboard import rebuild_leaderboards_for_season

        out = rebuild_leaderboards_for_season(season_id=season_id)
        if current_app:
            current_app.logger.info(
                "Rebuilt %s leaderboard keys for season %s", len(out), season_id
            )
        click.echo(f"Rebuilt {len(out)} leaderboard keys for season {season_id}")

    @cache_cli.command("rebuild-one")
    @click.option("--season", "season_id", required=True, type=int)
    @click.option("--key", "stat_key", required=True, type=str)
    def cache_rebuild_one_cmd(season_id: int, stat_key: str):
        """Rebuild a single leaderboard key for a season."""
        from services.cache_leaderboard import cache_build_one, _import_compute_leaderboard

        compute_fn = _import_compute_leaderboard()
        cache_build_one(stat_key, season_id=season_id, compute_fn=compute_fn)
        if current_app:
            current_app.logger.info("Rebuilt %s for season %s", stat_key, season_id)
        click.echo(f"Rebuilt {stat_key} (season {season_id})")

    # --- END: cache CLI commands ---

    @app.cli.command("seed-presets")
    @with_appcontext
    def seed_presets_command():
        """Seed example saved stat presets if they do not exist."""
        seeds = [
            {
                "name": "Starting Guards",
                "preset_type": "players",
                "player_ids": [3, 7, 11],
                "fields": [],
            },
            {
                "name": "Shooting Splits",
                "preset_type": "stats",
                "fields": ["ATR_make", "FG2_make", "FG3_make", "FT_att", "PPP"],
                "player_ids": [],
            },
            {
                "name": "Conference Window",
                "preset_type": "dates",
                "fields": [],
                "player_ids": [],
                "date_from": date(2025, 1, 1),
                "date_to": date(2025, 3, 31),
            },
        ]

        created = 0
        for seed in seeds:
            existing = SavedStatProfile.query.filter_by(
                name=seed["name"], preset_type=seed["preset_type"]
            ).first()
            if existing:
                continue

            profile = SavedStatProfile(
                name=seed["name"],
                preset_type=seed["preset_type"],
                fields_json=json.dumps(seed.get("fields", [])),
                players_json=json.dumps(seed.get("player_ids", [])),
                date_from=seed.get("date_from"),
                date_to=seed.get("date_to"),
                visibility="team",
            )
            db.session.add(profile)
            created += 1

        db.session.commit()
        print(f"Seeded {created} presets.")

    init_scheduler(app)

    return app

# Create the app instance for CLI & WSGI
app = None  # type: ignore
if os.environ.get("FLASK_CREATE_APP") == "1":
    app = create_app()

if __name__ == "__main__":
    if not os.environ.get('SKIP_CREATE_ALL'):
        with app.app_context():
            db.create_all()
    app.run(debug=True)
