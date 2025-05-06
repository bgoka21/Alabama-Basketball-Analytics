import os
import io
import re
import datetime
import traceback
import zipfile
import pandas as pd  # Added pandas import for CSV parsing and NaN handling
import json
from types import SimpleNamespace

from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, send_file, current_app, session, make_response
)
from flask_login import login_required, current_user, confirm_login, login_user, logout_user
from utils.auth       import admin_required
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

from models.database import (
    db,
    Game,
    TeamStats,
    PlayerStats,
    BlueCollarStats,
    OpponentBlueCollarStats,
    Possession,
    PlayerPossession,
    Season,
    Roster
)

from models.uploaded_file import UploadedFile
from models.user import User

from sqlalchemy import func
from test_parse import get_possession_breakdown_detailed

# --- Helper Functions at the top ---

def safe_str(value):
    """Safely convert a value to a string, returning an empty string if the value is None."""
    return "" if value is None else str(value)

def extract_tokens(text):
    """
    Extract tokens from the given text string.
    If the input is not a string or is NaN, it safely converts the value to a string or returns an empty list.
    """
    if pd.isna(text):
        return []
    if not isinstance(text, str):
        text = str(text)
    tokens = text.replace(',', ' ').split()
    return tokens

admin_bp = Blueprint('admin', __name__, template_folder='../templates/admin')

try:
    from auth.routes import auth_bp
    AUTH_EXISTS = True
except ImportError:
    AUTH_EXISTS = False

ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@admin_bp.before_request
def admin_bp_before_request():
    # Always allow the login page & static assets
    if request.endpoint == 'admin.login' or request.endpoint.startswith('static'):
        return

    # First, everyone must be logged in to see any /admin pages
    if not current_user.is_authenticated:
        flash("Please log in to continue.", "error")
        return redirect(url_for('admin.login'))

    # Now lock down only the truly admin-only endpoints:
    admin_only = {
        'admin.dashboard',
        'admin.files_view_unique',
        'admin.upload_file',
        'admin.parse_file',
        'admin.delete_file',
        'admin.users_list',
        'admin.add_user',
        'admin.edit_user',
        'admin.delete_user',
        'admin.edit_roster',
        'admin.delete_roster',
        # … add any other admin-only endpoints here …
    }

    if request.endpoint in admin_only and not current_user.is_admin:
        flash("You do not have permission to view that page.", "error")
        return redirect(url_for('admin.login'))

    # Everything else under admin_bp (e.g. game_reports, game_stats, players_list, player_shot_type, etc.)
    # is now only gated by login_required (via this before_request), not by admin status.



@admin_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user = User.query.filter_by(username=username).first()

        if user and check_password_hash(user.password_hash, password):
            login_user(user, remember=True)
            flash("Login successful!", "success")
            # Send admins to the dashboard, everyone else out to the public home
            if user.is_admin:
                return redirect(url_for('admin.dashboard'))
            else:
                return redirect(url_for('public.homepage'))

        flash("Invalid credentials. Please try again.", "error")
        return redirect(url_for('admin.login'))

    return render_template('admin/login.html')

@admin_bp.route('/dashboard', methods=['GET'])
@admin_required
def dashboard():
    uploaded_files = UploadedFile.query.order_by(UploadedFile.upload_date.desc()).all()
    return render_template('admin/dashboard.html', uploaded_files=uploaded_files, active_page='dashboard')

@admin_bp.route('/users', methods=['GET'])
@admin_required
def users_list():
    """Show all users for admin to manage."""
    users = User.query.order_by(User.username).all()
    return render_template('admin/users.html', users=users, active_page='users')

@admin_bp.route('/users/add', methods=['GET', 'POST'])
@admin_required
def add_user():
    """Admin: create a new user account."""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        is_admin = bool(request.form.get('is_admin'))

        # Basic validation
        if not username or not password:
            flash('Username and password are required.', 'error')
        elif User.query.filter_by(username=username).first():
            flash('That username is already taken.', 'error')
        else:
            # Create & save
            hashed = generate_password_hash(password)
            new = User(username=username, password_hash=hashed, is_admin=is_admin)
            db.session.add(new)
            db.session.commit()
            flash(f'User "{username}" created.', 'success')
            return redirect(url_for('admin.users_list'))

    # GET or failed POST: render form
    return render_template('admin/add_user.html', active_page='users')

@admin_bp.route('/users/<int:user_id>/edit', methods=['GET', 'POST'])
@admin_required
def edit_user(user_id):
    """Admin: edit an existing user."""
    user = User.query.get_or_404(user_id)

    if request.method == 'POST':
        new_username = request.form.get('username', '').strip()
        new_password = request.form.get('password', '')
        is_admin = bool(request.form.get('is_admin'))

        if not new_username:
            flash('Username cannot be blank.', 'error')
        elif new_username != user.username and User.query.filter_by(username=new_username).first():
            flash('That username is already taken.', 'error')
        else:
            user.username = new_username
            user.is_admin = is_admin
            if new_password:
                user.password_hash = generate_password_hash(new_password)
            db.session.commit()
            flash(f'User "{user.username}" updated.', 'success')
            return redirect(url_for('admin.users_list'))

    return render_template('admin/edit_user.html', user=user, active_page='users')

@admin_bp.route('/users/<int:user_id>/delete', methods=['POST'])
@admin_required
def delete_user(user_id):
    """Admin: delete a user."""
    user = User.query.get_or_404(user_id)

    if user.id == current_user.id:
        flash("You can't delete yourself!", 'error')
    else:
        db.session.delete(user)
        db.session.commit()
        flash(f'User "{user.username}" deleted.', 'success')

    return redirect(url_for('admin.users_list'))

@admin_bp.route('/logout')
@login_required
def logout():
    """Log the current user out."""
    logout_user()
    flash("Logged out successfully.", "success")
    return redirect(url_for('public.homepage'))



@admin_bp.route('/upload', methods=['POST'])
@admin_required
def upload_file():
    if 'file' not in request.files:
        flash('No file part in request', 'error')
        return redirect(url_for('admin.dashboard'))

    files = request.files.getlist('file')
    if not files or files[0].filename == '':
        flash('No selected files', 'error')
        return redirect(url_for('admin.dashboard'))

    for file in files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            upload_path = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
            file.save(upload_path)

            category = request.form.get('category')

            new_upload = UploadedFile(
                filename=filename,
                parse_status='Not Parsed',
                category=category
            )
            db.session.add(new_upload)
    db.session.commit()

    flash("Files uploaded successfully!", "success")
    return redirect(url_for('admin.dashboard'))

@admin_bp.route('/parse/<int:file_id>', methods=['POST'])
@admin_required
def parse_file(file_id):
    uploaded_file = UploadedFile.query.get_or_404(file_id)
    filename      = uploaded_file.filename
    upload_path   = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)

    # 1) file must exist
    if not os.path.exists(upload_path):
        flash(f"File '{filename}' not found on server.", "error")
        return redirect(url_for('admin.files_view_unique'))

    try:
        current_app.logger.debug(
            f"Starting parse for file '{filename}' at '{upload_path}'"
        )
        from test_parse import parse_csv

        # 2) run your parser (now returns lineup_efficiencies too)
        results = parse_csv(upload_path, 1, 1)
        current_app.logger.debug("Parsing completed successfully.")

        # 2a) make lineup_efficiencies JSON-friendly
        raw_lineups = results.get('lineup_efficiencies', {})
        json_lineups = {}
        for size, sides in raw_lineups.items():
            # e.g. size = 2,3,4,5
            json_lineups[size] = {}
            for side, combos in sides.items():
                # side = 'offense' or 'defense'
                json_lineups[size][side] = {
                    ",".join(combo): ppp
                    for combo, ppp in combos.items()
                }

        # 3) update status, timestamp, and store all three JSON payloads
        uploaded_file.parse_status        = 'Parsed Successfully'
        uploaded_file.last_parsed         = datetime.datetime.utcnow()
        uploaded_file.offensive_breakdown = json.dumps(
            results.get('offensive_breakdown', {}))
        uploaded_file.defensive_breakdown = json.dumps(
            results.get('defensive_breakdown', {}))
        uploaded_file.lineup_efficiencies = json.dumps(json_lineups)

        db.session.commit()

        # 4) redirect into the game editor
        game = Game.query.filter_by(csv_filename=filename).first()
        if not game:
            flash(
                f"Parsed OK but couldn’t find Game record for '{filename}'",
                "warning"
            )
            return redirect(url_for('admin.dashboard'))

        flash(
            f"File '{filename}' parsed successfully! You can now edit the game.",
            "success"
        )
        return redirect(url_for('admin.edit_game', game_id=game.id))

    except Exception as e:
        # 5) on any error, record it and flip the status
        current_app.logger.exception("Error parsing CSV")
        uploaded_file.parse_status = 'Error'
        uploaded_file.parse_error  = str(e)
        db.session.commit()

        flash(f"Parsing failed for '{filename}': {e}", "error")
        return redirect(url_for('admin.files_view_unique'))


@admin_bp.route('/logs/<int:file_id>', methods=['GET'])
@admin_required
def view_logs(file_id):
    uploaded_file = UploadedFile.query.get_or_404(file_id)
    return render_template('admin/logs.html', uploaded_file=uploaded_file)

@admin_bp.route('/delete/<int:file_id>', methods=['POST'])
@admin_required
def delete_file(file_id):
    uploaded_file = UploadedFile .query.get_or_404(file_id)
    filename = uploaded_file.filename
    upload_path = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)

    db.session.delete(uploaded_file)
    db.session.commit()

    if os.path.exists(upload_path):
        os.remove(upload_path)

    flash(f"File '{filename}' has been deleted.", "success")
    return redirect(url_for('admin.files_view_unique'))

@admin_bp.route('/bulk-action', methods=['POST'], endpoint='bulk_action')
@admin_required
def bulk_action_view():
    selected_ids = request.form.getlist('selected_files')
    action = request.form.get('action')

    if not selected_ids:
        flash("No files selected.", "error")
        return redirect(url_for('admin.files_view_unique'))

    files = UploadedFile.query.filter(UploadedFile.id.in_(selected_ids)).all()

    if action == 'delete':
        for file in files:
            filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], file.filename)
            db.session.delete(file)
            if os.path.exists(filepath):
                os.remove(filepath)
        db.session.commit()
        flash(f"Deleted {len(files)} files.", "success")
    elif action == 'download':
        memory_file = io.BytesIO()
        with zipfile.ZipFile(memory_file, 'w') as zf:
            for file in files:
                file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], file.filename)
                if os.path.exists(file_path):
                    zf.write(file_path, arcname=file.filename)
        memory_file.seek(0)
        return send_file(memory_file, download_name="downloaded_files.zip", as_attachment=True)

    return redirect(url_for('admin.files_view_unique'))

@admin_bp.route('/download/<int:file_id>', methods=['GET'])
@admin_required
def download_file(file_id):
    uploaded_file = UploadedFile.query.get_or_404(file_id)
    file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], uploaded_file.filename)

    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        flash(f"File '{uploaded_file.filename}' not found.", "error")
        return redirect(url_for('admin.files_view_unique'))

@admin_bp.route('/reset-db', methods=['POST'])
@admin_required
def reset_db():
    if not current_user.is_admin:
        flash("You are not authorized to reset the database.", "error")
        return redirect(url_for('admin.dashboard'))

    confirmation = request.form.get('confirmation', '')
    if confirmation != "RESET":
        flash("You must type 'RESET' to confirm.", "error")
        return redirect(url_for('admin.dashboard'))

    try:
        engine = db.engine

        # never drop users, seasons, or roster tables
        exclude = {
            'users',
            Season.__table__.name,   # e.g. "seasons"
            Roster.__table__.name    # e.g. "roster"
        }
        to_drop = [
            t for t in db.metadata.sorted_tables
            if t.name not in exclude
        ]

        # drop everything except those three
        db.metadata.drop_all(bind=engine, tables=to_drop)
        # recreate any missing tables
        db.create_all()

        flash("Database reset complete (users, seasons & rosters preserved).", "success")
    except Exception as e:
        flash(f"Error resetting DB: {e}", "error")

    return redirect(url_for('admin.dashboard'))



@admin_bp.route('/game-reports')
@login_required
def game_reports():
    games = Game.query.order_by(Game.game_date.desc()).all()
    return render_template('admin/game_reports.html', games=games, active_page='game_reports')



@admin_bp.route('/files', methods=['GET'], endpoint='files_view_unique')
@admin_required
def files_view():
    category_filter = request.args.get('category')
    if category_filter:
        files = UploadedFile.query.filter_by(category=category_filter).order_by(UploadedFile.upload_date.desc()).all()
    else:
        files = UploadedFile.query.order_by(UploadedFile.upload_date.desc()).all()
    return render_template('files.html', files=files, selected_category=category_filter)


@admin_bp.route('/stats/<int:game_id>')
@login_required
def game_stats(game_id):
    # ─── Load Game & Stored TeamStats ─────────────────────────────────────────
    game = Game.query.get_or_404(game_id)
    team_stats = TeamStats.query.filter_by(game_id=game_id, is_opponent=False).first()
    opponent_stats = TeamStats.query.filter_by(game_id=game_id, is_opponent=True).first()

    # Dummy fallback
    def default_stats():
        class D:
            total_atr_attempts = total_fg2_attempts = total_fg3_attempts = 0
            total_atr_makes    = total_fg2_makes    = total_fg3_makes    = 0
            total_ftm = total_fta = total_possessions = 0
            total_assists = total_turnovers = 0
            total_second_assists = total_pot_assists = 0
            total_blue_collar = 0
        return D()
    if not team_stats:     team_stats     = default_stats()
    if not opponent_stats: opponent_stats = default_stats()

    # ─── Load DB rows you’ll need in the template ──────────────────────────────
    player_stats            = PlayerStats.query.filter_by(game_id=game_id).all()
    blue_collar_stats       = BlueCollarStats.query.filter_by(game_id=game_id).all()
    opponent_blue_breakdown = db.session.query(
        func.sum(OpponentBlueCollarStats.def_reb).label('def_reb'),
        func.sum(OpponentBlueCollarStats.off_reb).label('off_reb'),
        func.sum(OpponentBlueCollarStats.misc).label('misc'),
        func.sum(OpponentBlueCollarStats.deflection).label('deflection'),
        func.sum(OpponentBlueCollarStats.steal).label('steal'),
        func.sum(OpponentBlueCollarStats.block).label('block'),
        func.sum(OpponentBlueCollarStats.floor_dive).label('floor_dive'),
        func.sum(OpponentBlueCollarStats.charge_taken).label('charge_taken'),
        func.sum(OpponentBlueCollarStats.reb_tip).label('reb_tip')
    ).filter(OpponentBlueCollarStats.game_id == game_id).one()
    possessions = Possession.query.filter_by(game_id=game_id).all()

    # ─── LOAD CSV & TAG PERIOD ────────────────────────────────────────────────
    csv_path = os.path.join(current_app.config['UPLOAD_FOLDER'], game.csv_filename)
    df = pd.read_csv(csv_path)
    # Grab just “1st Half”, “2nd Half”, or “Overtime”
    df['Period'] = (
        df['GAME SPLITS']
          .fillna('')
          .str.split(',', n=1).str[0]
          .str.strip()
    )

    # ─── POSSESSION BREAKDOWNS & LINEUPS (UNCHANGED) ──────────────────────────
    offensive_breakdown, defensive_breakdown, periodic_offense, periodic_defense = \
        get_possession_breakdown_detailed(df)
    uploaded_file = UploadedFile.query.filter_by(filename=game.csv_filename).first()
    lineup_efficiencies = (
        json.loads(uploaded_file.lineup_efficiencies)
        if uploaded_file and uploaded_file.lineup_efficiencies else {}
    )
    best_offense = {}
    worst_offense = {}
    best_defense = {}
    worst_defense = {}
    for size, sides in lineup_efficiencies.items():
        # Offense
        off = sides.get('offense', {})
        best_offense[size]  = sorted(off.items(), key=lambda x: x[1], reverse=True)[:5]
        worst_offense[size] = sorted(off.items(), key=lambda x: x[1])[:5]
        # Defense
        dfens = sides.get('defense', {})
        best_defense[size]  = sorted(dfens.items(), key=lambda x: x[1])[:5]
        worst_defense[size] = sorted(dfens.items(), key=lambda x: x[1], reverse=True)[:5]

    # ─── DEFENSIVE SECONDARY METRICS ──────────────────────────────────────────
    # We'll treat the opponent’s offense as “Defense rows” in the CSV:
    defense_rows = df[df['Row'] == "Defense"]

    # Helper: count tokens in the OPP STATS column
    def count_def_tokens(rows, tokens):
        return sum(
            1
            for _, r in rows.iterrows()
            for tok in extract_tokens(r.get("OPP STATS", ""))
            if tok in tokens
        )

    # 1) OREB % Allowed
    opp_atr_miss   = opponent_stats.total_atr_attempts - opponent_stats.total_atr_makes
    opp_fg2_miss   = opponent_stats.total_fg2_attempts - opponent_stats.total_fg2_makes
    opp_fg3_miss   = opponent_stats.total_fg3_attempts - opponent_stats.total_fg3_makes
    opp_reb_chance = opp_atr_miss + opp_fg2_miss + opp_fg3_miss
    opp_oreb_pct = (
        round(opponent_blue_breakdown.off_reb / opp_reb_chance * 100, 1)
        if opp_reb_chance > 0 else 0.0
    )

    # 2) FT Rate Allowed (FTA ÷ possessions)
    # count opponent free‐throw attempts
    opp_fta = opponent_stats.total_fta
    # opponent field‐goal attempts = ATR + 2FG + 3FG attempts
    opp_fga = (
        opponent_stats.total_atr_attempts
        + opponent_stats.total_fg2_attempts
        + opponent_stats.total_fg3_attempts
    )
    opp_ft_rate = (
        round(opp_fta / opp_fga * 100, 1)
        if opp_fga > 0 else 0.0
    )

    # 3) Good Shot % Allowed
    opp_good = (
        opponent_stats.total_fta
      + opponent_stats.total_atr_makes + opp_atr_miss
      + opponent_stats.total_fg3_makes + opp_fg3_miss
    )
    opp_bad = opponent_stats.total_fg2_makes + opp_fg2_miss
    opp_den = opp_good + opp_bad
    opp_good_shot_pct = (
        round(opp_good / opp_den * 100, 2)
        if opp_den > 0 else 0.0
    )

    # 4) Assist % Allowed (assists ÷ made FGs)
    opp_fgm_made   = (
        opponent_stats.total_atr_makes
      + opponent_stats.total_fg2_makes
      + opponent_stats.total_fg3_makes
    )
    opp_assist_pct = (
        round(opponent_stats.total_assists / opp_fgm_made * 100, 1)
        if opp_fgm_made > 0 else 0.0
    )

    # 5) Turnover % Allowed (TOs ÷ possessions)
    opp_turnover_pct = (
        round(opponent_stats.total_turnovers / opponent_stats.total_possessions * 100, 1)
        if opponent_stats.total_possessions > 0 else 0.0
    )

    # 6) PPP Allowed (points allowed ÷ possessions)
    opp_ppp = (
        round(opponent_stats.total_points / opponent_stats.total_possessions, 2)
        if opponent_stats.total_possessions > 0 else 0.0
    )

    # 7) TCR Allowed (transition conversions ÷ transition opportunities)
    # Denominator: made+missed FG + steals (from OPP STATS), minus neutrals
    made   = count_def_tokens(defense_rows, ("ATR+", "2FG+", "3FG+"))
    missed = count_def_tokens(defense_rows, ("ATR-", "2FG-", "3FG-"))
    steals = count_def_tokens(defense_rows, ("Steal",))
    neutrals = defense_rows[defense_rows['TEAM'].fillna('').str.contains("Neutral")]
    made_neu   = count_def_tokens(neutrals, ("ATR+", "2FG+", "3FG+"))
    missed_neu = count_def_tokens(neutrals, ("ATR-", "2FG-", "3FG-"))
    steals_neu = count_def_tokens(neutrals, ("Steal",))
    trans_opps = (made + missed + steals) - (made_neu + missed_neu + steals_neu)

    # Numerator: any OPP stat in transition that’s a conversion
    trans_rows = defense_rows[
        defense_rows['POSSESSION TYPE'].fillna('').str.contains("Transition")
    ]
    conv = count_def_tokens(trans_rows, (
        "ATR+", "ATR-",
        "2FG+", "2FG-",
        "3FG+", "3FG-",
        "FT+",   # free throws made
        "Fouled"
    ))
    opp_tcr_pct = (
        round(conv / trans_opps * 100, 1)
        if trans_opps > 0 else 0.0
    )

    # ─── RENDER ───────────────────────────────────────────────────────────────
    return render_template(
        'admin/game_stats.html',
        active_page='stats',
        game=game,
        team_stats=team_stats,
        opponent_stats=opponent_stats,
        player_stats=player_stats,
        blue_collar_stats=blue_collar_stats,
        opponent_blue_coll_stats=opponent_blue_breakdown,
        possessions=possessions,

        # breakdowns
        offensive_breakdown=offensive_breakdown,
        defensive_breakdown=defensive_breakdown,
        periodic_offense=periodic_offense,
        periodic_defense=periodic_defense,

        # lineup efficiencies
        lineup_efficiencies=lineup_efficiencies,
        best_offense=best_offense,
        worst_offense=worst_offense,
        best_defense=best_defense,
        worst_defense=worst_defense,

        # defensive secondary metrics
        opp_oreb_pct=opp_oreb_pct,
        opp_ft_rate=opp_ft_rate,
        opp_good_shot_pct=opp_good_shot_pct,
        opp_assist_pct=opp_assist_pct,
        opp_turnover_pct=opp_turnover_pct,
        opp_ppp=opp_ppp,
        opp_tcr_pct=opp_tcr_pct,
    )





@admin_bp.route('/game/<int:game_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_game(game_id):
    game = Game.query.get_or_404(game_id)  # load the game record
    if request.method == 'POST':
        try:
            date_str = request.form.get('game_date')
            if date_str:
                from datetime import datetime
                game.game_date = datetime.strptime(date_str, "%Y-%m-%d").date()

            game.opponent_name = request.form.get('opponent_name')
            game.result = request.form.get('result')

            db.session.commit()
            flash("Game updated successfully!", "success")
            return redirect(url_for('admin.game_reports'))

        except Exception as e:
            db.session.rollback()
            flash(f"Error updating game: {e}", "error")

    return render_template('admin/edit_game.html', game=game)


# admin/routes.py  (only the player_detail view shown)
@admin_bp.route('/player/<player_name>')
@login_required
def player_detail(player_name):
    # 1) Fetch per‑game rows
    stats_records = PlayerStats.query.filter_by(player_name=player_name).all()
    if not stats_records:
        flash("No stats found for this player.", "error")
        return redirect(url_for('admin.players_list'))

    # 2) Season aggregates
    aggregated_dict = {
        "points":         sum(s.points or 0    for s in stats_records),
        "assists":        sum(s.assists or 0   for s in stats_records),
        "turnovers":      sum(s.turnovers or 0 for s in stats_records),
        "atr_attempts":   sum(s.atr_attempts or 0 for s in stats_records),
        "atr_makes":      sum(s.atr_makes    or 0 for s in stats_records),
        "fg2_attempts":   sum(s.fg2_attempts or 0 for s in stats_records),
        "fg2_makes":      sum(s.fg2_makes    or 0 for s in stats_records),
        "fg3_attempts":   sum(s.fg3_attempts or 0 for s in stats_records),
        "fg3_makes":      sum(s.fg3_makes    or 0 for s in stats_records),
        "fta":            sum(s.fta          or 0 for s in stats_records),
        "ftm":            sum(s.ftm          or 0 for s in stats_records),
        "second_assists": sum(s.second_assists or 0 for s in stats_records),
        "pot_assists":    sum(s.pot_assists    or 0 for s in stats_records),
    }
    total_shots = (
        aggregated_dict["atr_attempts"]
      + aggregated_dict["fg2_attempts"]
      + aggregated_dict["fg3_attempts"]
    )
    if total_shots:
        efg = (
            aggregated_dict["atr_makes"]
          + aggregated_dict["fg2_makes"]
          + 1.5 * aggregated_dict["fg3_makes"]
        ) / total_shots
        aggregated_dict["efg_pct"]         = f"{efg * 100:.1f}%"
        aggregated_dict["points_per_shot"] = f"{efg * 2:.2f}"
    else:
        aggregated_dict["efg_pct"]         = "N/A"
        aggregated_dict["points_per_shot"] = "N/A"

    if aggregated_dict["turnovers"]:
        aggregated_dict["assist_turnover_ratio"]     = f"{aggregated_dict['assists'] / aggregated_dict['turnovers']:.2f}"
        total_ast = (
            aggregated_dict["assists"]
          + aggregated_dict["second_assists"]
          + aggregated_dict["pot_assists"]
        )
        aggregated_dict["adj_assist_turnover_ratio"] = f"{total_ast / aggregated_dict['turnovers']:.2f}"
    else:
        aggregated_dict["assist_turnover_ratio"]     = "N/A"
        aggregated_dict["adj_assist_turnover_ratio"] = "N/A"

    aggregated = SimpleNamespace(**aggregated_dict)

    # 3) Blue‑collar totals
    bc = db.session.query(
        func.coalesce(func.sum(BlueCollarStats.def_reb),      0).label('def_reb'),
        func.coalesce(func.sum(BlueCollarStats.off_reb),      0).label('off_reb'),
        func.coalesce(func.sum(BlueCollarStats.misc),         0).label('misc'),
        func.coalesce(func.sum(BlueCollarStats.deflection),   0).label('deflection'),
        func.coalesce(func.sum(BlueCollarStats.steal),        0).label('steal'),
        func.coalesce(func.sum(BlueCollarStats.block),        0).label('block'),
        func.coalesce(func.sum(BlueCollarStats.floor_dive),   0).label('floor_dive'),
        func.coalesce(func.sum(BlueCollarStats.charge_taken), 0).label('charge_taken'),
        func.coalesce(func.sum(BlueCollarStats.reb_tip),      0).label('reb_tip'),
        func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0).label('total_blue_collar'),
    ).join(PlayerStats, BlueCollarStats.player_id == PlayerStats.id) \
     .filter(PlayerStats.player_name == player_name).one()

    player_blue_breakdown = SimpleNamespace(
        def_reb      = bc.def_reb,
        off_reb      = bc.off_reb,
        misc         = bc.misc,
        deflection   = bc.deflection,
        steal        = bc.steal,
        block        = bc.block,
        floor_dive   = bc.floor_dive,
        charge_taken = bc.charge_taken,
        reb_tip      = bc.reb_tip,
        total_blue_collar = bc.total_blue_collar
    )

    # 4) Shot‑Type season totals
    shot_type_totals = SimpleNamespace(
        atr= SimpleNamespace(
            makes    = aggregated.atr_makes,
            attempts = aggregated.atr_attempts,
            fg_pct   = (aggregated.atr_makes/aggregated.atr_attempts*100)
                       if aggregated.atr_attempts else 0
        ),
        fg2= SimpleNamespace(
            makes    = aggregated.fg2_makes,
            attempts = aggregated.fg2_attempts,
            fg_pct   = (aggregated.fg2_makes/aggregated.fg2_attempts*100)
                       if aggregated.fg2_attempts else 0
        ),
        fg3= SimpleNamespace(
            makes    = aggregated.fg3_makes,
            attempts = aggregated.fg3_attempts,
            fg_pct   = (aggregated.fg3_makes/aggregated.fg3_attempts*100)
                       if aggregated.fg3_attempts else 0
        )
    )

    # 5) Gather every shot‐detail record
    all_details = []
    for rec in stats_records:
        raw = rec.shot_type_details
        if raw:
            details = json.loads(raw) if isinstance(raw, str) else raw
            all_details.extend(details)

    # 6) Build detail_counts with total / transition / halfcourt
    detail_counts = {'atr': {}, 'fg2': {}, 'fg3': {}}
    cls_map      = {'atr':'atr','2fg':'fg2','3fg':'fg3'}

    for shot in all_details:
        cls_key = shot.get('shot_class','').lower()
        cls = cls_map.get(cls_key)
        if not cls:
            continue

        made    = (shot.get('result') == 'made')
        context = shot.get('context', 'total')   # must be 'total', 'transition' or 'halfcourt'

        for key, val in shot.items():
            if key in ('shot_class','result','context') or not val:
                continue

            if isinstance(val, (list, tuple)):
                labels = val
            else:
                labels = [
                    label.strip()
                    for label in re.split(r'[,/]', str(val))
                    if label.strip()
                ]

            for lbl in labels:
                ent = detail_counts[cls].setdefault(
                  lbl,
                  {
                    'total':      {'attempts':0,'makes':0},
                    'transition': {'attempts':0,'makes':0},
                    'halfcourt':  {'attempts':0,'makes':0},
                  }
                )
                # increment overall + context
                ent['total']['attempts']      += 1
                ent[context]['attempts']      += 1
                if made:
                    ent['total']['makes']     += 1
                    ent[context]['makes']     += 1

    # 7) Compute fg_pct, freq, pps for each context
    for cls, bucket in detail_counts.items():
        for lbl, data in bucket.items():
            # avoid div/0
            base = data['total']['attempts'] or 1
            pts  = 2 if cls in ('atr','fg2') else 3

            for ctx in ('total','transition','halfcourt'):
                a = data[ctx]['attempts']
                m = data[ctx]['makes']
                data[ctx]['fg_pct'] = (m / a) if a else 0
                data[ctx]['pps']    = (pts * m / a) if a else 0
                data[ctx]['freq']   = (a / base)

    # 8) Wrap into shot_summaries
    # 8) Ensure default categories exist, then wrap into shot_summaries
    shot_summaries = {}
    for shot_type, bucket in detail_counts.items():
        # always have these two keys so template lookup never fails
        for default_label in ['Assisted', 'Non‑Assisted']:
            bucket.setdefault(default_label, {
                'total':      {'attempts': 0, 'makes': 0, 'fg_pct': 0, 'pps': 0, 'freq': 0},
                'transition': {'attempts': 0, 'makes': 0, 'fg_pct': 0, 'pps': 0, 'freq': 0},
                'halfcourt':  {'attempts': 0, 'makes': 0, 'fg_pct': 0, 'pps': 0, 'freq': 0},
            })
            # build per‐category namespaces
            cats = {}
            for lbl, data in bucket.items():
                cats[lbl] = SimpleNamespace(
                    total      = SimpleNamespace(**data['total']),
                    transition = SimpleNamespace(**data['transition']),
                    halfcourt  = SimpleNamespace(**data['halfcourt']),
                )

            # season summary totals for this shot type
            ta = sum(d['total']['attempts'] for d in bucket.values()) or 1
            tm = sum(d['total']['makes']    for d in bucket.values())
            tp = sum(d['total']['pps'] * d['total']['attempts'] for d in bucket.values())

            shot_summaries[shot_type] = SimpleNamespace(
                total = SimpleNamespace(
                    attempts = ta,
                    makes    = tm,
                    fg_pct   = tm / ta,
                    pps      = tp / ta
                ),
                cats = cats
            )

    
    # 8.a) season‑wide transition + halfcourt aggregates on each summary
    for key, summary in shot_summaries.items():
        # sum up all category‐level transition & halfcourt makes/attempts
        t_att = sum(cat.transition.attempts for cat in summary.cats.values())
        t_mks = sum(cat.transition.makes    for cat in summary.cats.values())
        h_att = sum(cat.halfcourt.attempts  for cat in summary.cats.values())
        h_mks = sum(cat.halfcourt.makes     for cat in summary.cats.values())

        summary.transition = SimpleNamespace(
            attempts = t_att,
            makes    = t_mks,
            fg_pct   = (t_mks / t_att * 100) if t_att else 0
        )
        summary.halfcourt = SimpleNamespace(
            attempts = h_att,
            makes    = h_mks,
            fg_pct   = (h_mks / h_att * 100) if h_att else 0
        )



    # 9) Game‑by‑Game breakdown (unchanged)
    fields = [
      "points","assists","turnovers",
      "atr_makes","atr_attempts",
      "fg2_makes","fg2_attempts",
      "fg3_makes","fg3_attempts",
      "ftm","fta","pot_assists","second_assists"
    ]
    game_breakdown, game_details = {}, {}
    for s in stats_records:
        gid = s.game_id
        if gid not in game_breakdown:
            game_breakdown[gid] = {f:0 for f in fields}
        for f in fields:
            game_breakdown[gid][f] += getattr(s, f, 0) or 0

    for gid, data in game_breakdown.items():
        g = Game.query.get(gid)
        game_details[gid] = {
            'opponent_name': g.opponent_name if g else 'Unknown',
            'game_date':     g.game_date.strftime("%b %d") if g and g.game_date else '',
            'sort_date':     g.game_date.strftime("%Y%m%d") if g and g.game_date else '0'
        }

    # 10) Render template
    # … after you build shot_summaries …
    return render_template(
        'admin/player_detail.html',
        player_name           = player_name,
        stats_records         = stats_records,
        aggregated            = aggregated,
        game_breakdown        = game_breakdown,
        game_details          = game_details,
        player_blue_breakdown = player_blue_breakdown,
        shot_type_totals      = shot_type_totals,
        shot_summaries        = shot_summaries,
    )

# ... [remaining routes unchanged below] ...

@admin_bp.route('/roster/edit', methods=['GET', 'POST'])
@admin_required
def edit_roster():
    seasons = Season.query.order_by(Season.season_name.desc()).all()

    if request.method == 'POST':
        season_id = request.form.get('season_id', type=int)
        new_season_name = request.form.get('new_season', '').strip()

        if new_season_name:
            season = Season(season_name=new_season_name)
            db.session.add(season)
            db.session.commit()
            season_id = season.id
        else:
            season = Season.query.get(season_id)
            if not season:
                flash("Invalid season selected.", "error")
                return redirect(url_for('admin.edit_roster'))
        
        players_text = request.form.get('players_list', '')
        players = [line.strip() for line in players_text.split('\n') if line.strip()]

        Roster.query.filter_by(season_id=season_id).delete()
        for player in players:
            roster_entry = Roster(season_id=season_id, player_name=player)
            db.session.add(roster_entry)
        db.session.commit()
        flash("Roster updated successfully!", "success")
        return redirect(url_for('admin.players_list', season_id=season_id))

    season_id = request.args.get('season_id', type=int)
    current_roster_text = ""
    if season_id:
        roster_entries = Roster.query.filter_by(season_id=season_id).all()
        current_roster_text = "\n".join([entry.player_name for entry in roster_entries])
        
    return render_template('admin/edit_roster.html', 
                           seasons=seasons, 
                           selected_season=season_id, 
                           current_roster=current_roster_text)


@admin_bp.route('/roster/<int:season_id>/delete', methods=['POST'])
@admin_required
def delete_roster(season_id):
    # remove all roster entries
    Roster.query.filter_by(season_id=season_id).delete()
    # then remove the season altogether
    Season.query.filter_by(id=season_id).delete()
    db.session.commit()
    flash("Season and its roster have been deleted.", "success")
    return redirect(url_for('admin.edit_roster'))



@admin_bp.route('/players')
@login_required
def players_list():
    season_id = request.args.get('season_id', type=int)
    seasons = Season.query.order_by(Season.id.desc()).all()
    
    if not season_id and seasons:
        season_id = seasons[0].id

    import re
    def sort_key(name):
        m = re.match(r'#(\d+)', name)
        return int(m.group(1)) if m else 9999

    if season_id:
        roster_entries = Roster.query.filter_by(season_id=season_id).all()
        players = [r.player_name for r in roster_entries]
    else:
        players = [p[0] for p in db.session.query(PlayerStats.player_name).distinct().all()]

    players.sort(key=sort_key)
        
    return render_template('admin/players.html',
                           players=players,
                           seasons=seasons,
                           selected_season=season_id,
                           active_page='players')


@admin_bp.route('/player/<player_name>/shot-type')
@login_required
def player_shot_type(player_name):
    """
    Render the per–player shot type breakdown page.
    """
    # Fetch all the PlayerStats rows so we can reassemble their shot_type_details
    stats_records = PlayerStats.query.filter_by(player_name=player_name).all()
    if not stats_records:
        flash("No stats found for this player.", "error")
        return redirect(url_for('admin.players_list'))

    # Collect every shot_type_details entry across every game
    shot_details = []
    for s in stats_records:
        if s.shot_type_details:
            shot_details.extend(s.shot_type_details)

    # Now you can group or filter shot_details however you like:
    atr_details  = [d for d in shot_details if d.get('shot_class') == 'ATR']
    fg2_details  = [d for d in shot_details if d.get('shot_class') == '2FG']
    fg3_details  = [d for d in shot_details if d.get('shot_class') == '3FG']

    return render_template(
        'admin/player_shot_type.html',
        player_name=player_name,
        atr_details=atr_details,
        fg2_details=fg2_details,
        fg3_details=fg3_details,
    )
