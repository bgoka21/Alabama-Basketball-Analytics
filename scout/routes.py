import os
import uuid
from datetime import datetime
from functools import wraps
from typing import Optional

from flask import abort, current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename

from . import scout_bp
from models.database import db
from models.scout import ScoutGame, ScoutTeam
from scout.parsers import store_scout_playcalls


def _staff_required(view_func):
    @wraps(view_func)
    @login_required
    def wrapper(*args, **kwargs):
        if current_user.is_player:
            abort(403)
        return view_func(*args, **kwargs)

    return wrapper


def _ensure_scout_upload_dir() -> str:
    upload_dir = current_app.config.get('SCOUT_UPLOAD_FOLDER')
    if not upload_dir:
        upload_dir = os.path.abspath(
            os.path.join(current_app.root_path, os.pardir, 'uploads', 'scout')
        )
    os.makedirs(upload_dir, exist_ok=True)
    return upload_dir


@scout_bp.route('/playcalls')
@_staff_required
def scout_playcalls():
    teams = ScoutTeam.query.order_by(ScoutTeam.name).all()
    team_id = request.args.get('team_id', type=int)
    selected_team = ScoutTeam.query.get(team_id) if team_id else None
    games = []
    if selected_team:
        games = (
            ScoutGame.query.filter_by(scout_team_id=selected_team.id)
            .order_by(ScoutGame.uploaded_at.desc())
            .all()
        )

    selected_game_ids: set[int] = set()
    game_ids_param = request.args.get('game_ids', '') or ''
    if game_ids_param:
        for raw_id in game_ids_param.split(','):
            try:
                selected_game_ids.add(int(raw_id))
            except (TypeError, ValueError):
                continue
    for raw_id in request.args.getlist('game_ids'):
        try:
            selected_game_ids.add(int(raw_id))
        except (TypeError, ValueError):
            continue

    min_runs = request.args.get('min_runs', default=1, type=int)

    return render_template(
        'scout/scout_playcalls.html',
        teams=teams,
        selected_team=selected_team,
        games=games,
        selected_game_ids=selected_game_ids,
        min_runs=min_runs,
    )


@scout_bp.route('/upload_playcalls_csv', methods=['POST'])
@_staff_required
def upload_playcalls_csv():
    team_id = request.form.get('team_id', type=int)
    new_team_name = (request.form.get('new_team_name') or '').strip()

    team: Optional[ScoutTeam] = None
    if new_team_name:
        team = ScoutTeam.query.filter_by(name=new_team_name).first()
        if not team:
            team = ScoutTeam(name=new_team_name)
            db.session.add(team)
            db.session.commit()
    elif team_id:
        team = ScoutTeam.query.get(team_id)

    if not team:
        flash('Please select a team or add a new one.', 'error')
        return redirect(url_for('scout.scout_playcalls'))

    file = request.files.get('file')
    if not file or not file.filename:
        flash('Please upload a CSV file.', 'error')
        return redirect(url_for('scout.scout_playcalls', team_id=team.id))

    filename = secure_filename(file.filename) or 'playcalls.csv'
    upload_dir = _ensure_scout_upload_dir()
    stored_filename = f"{uuid.uuid4().hex}_{filename}"
    file_path = os.path.join(upload_dir, stored_filename)
    file.save(file_path)

    game = ScoutGame(
        scout_team_id=team.id,
        uploaded_at=datetime.utcnow(),
        original_filename=filename,
        stored_filename=stored_filename,
    )
    db.session.add(game)
    db.session.commit()

    try:
        created_count = store_scout_playcalls(file_path, game)
        flash(
            f'Playcalls file uploaded for scout team. Parsed {created_count} possessions.',
            'success',
        )
    except Exception:  # pragma: no cover - defensive logging
        current_app.logger.exception('Failed to parse scout playcalls CSV')
        flash('Playcalls file uploaded, but parsing failed.', 'error')

    return redirect(url_for('scout.scout_playcalls', team_id=team.id))
