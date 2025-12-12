import csv
import os
import uuid
from datetime import datetime
from functools import wraps
from io import StringIO
from typing import Optional

from flask import (
    Response,
    abort,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename

from . import scout_bp
from models.database import db
from models.scout import ScoutGame, ScoutPossession, ScoutTeam
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


def _parse_scout_filters():
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

    if games:
        valid_game_ids = {game.id for game in games}
        selected_game_ids = {game_id for game_id in selected_game_ids if game_id in valid_game_ids}

    min_runs = request.args.get('min_runs', default=1, type=int) or 1
    min_runs = max(1, min_runs)

    return teams, selected_team, games, selected_game_ids, min_runs


def _build_report_rows(selected_game_ids: set[int], min_runs: int):
    report_rows = {"STANDARD": [], "BOB": [], "SOB": []}
    if not selected_game_ids:
        return report_rows

    possession_key_expr = (
        db.cast(ScoutPossession.scout_game_id, db.String)
        + db.literal('-')
        + db.cast(ScoutPossession.instance_number, db.String)
    )
    times_run_expr = db.func.count(db.func.distinct(possession_key_expr))
    total_points_expr = db.func.coalesce(db.func.sum(ScoutPossession.points), 0)

    query = (
        db.session.query(
            ScoutPossession.bucket,
            ScoutPossession.playcall,
            times_run_expr.label('times_run'),
            total_points_expr.label('total_points'),
        )
        .filter(ScoutPossession.scout_game_id.in_(selected_game_ids))
        .filter(ScoutPossession.playcall.isnot(None))
        .filter(ScoutPossession.playcall != '')
        .group_by(ScoutPossession.bucket, ScoutPossession.playcall)
    )

    if min_runs and min_runs > 1:
        query = query.having(times_run_expr >= min_runs)

    excluded_prefixes = ('eog', 'ft', 'vs')

    for row in query.all():
        playcall = (row.playcall or '').strip()
        playcall_lower = playcall.lower()
        if any(playcall_lower.startswith(prefix) for prefix in excluded_prefixes):
            continue

        times_run = int(row.times_run or 0)
        total_points = int(row.total_points or 0)
        ppc = round(total_points / times_run, 2) if times_run else 0

        bucket_key = (row.bucket or 'STANDARD').upper()
        if bucket_key not in report_rows:
            report_rows[bucket_key] = []

        report_rows[bucket_key].append(
            {
                'bucket': bucket_key,
                'playcall': playcall or '(Unknown)',
                'times_run': times_run,
                'total_points': total_points,
                'ppc': ppc,
            }
        )

    for bucket_key, rows in report_rows.items():
        rows.sort(key=lambda row: (-row['times_run'], -row['ppc']))

    return report_rows


@scout_bp.route('/playcalls')
@_staff_required
def scout_playcalls():
    teams, selected_team, games, selected_game_ids, min_runs = _parse_scout_filters()

    report_rows = _build_report_rows(selected_game_ids, min_runs)

    return render_template(
        'scout/scout_playcalls.html',
        teams=teams,
        selected_team=selected_team,
        games=games,
        selected_game_ids=selected_game_ids,
        min_runs=min_runs,
        report_rows=report_rows,
    )


@scout_bp.route('/playcalls/export')
@_staff_required
def export_playcalls_csv():
    _teams, selected_team, _, selected_game_ids, min_runs = _parse_scout_filters()

    if not selected_team:
        flash('Select a team to export a scout report.', 'error')
        return redirect(url_for('scout.scout_playcalls'))

    if not selected_game_ids:
        flash('Select one or more scout games to export.', 'error')
        return redirect(
            url_for(
                'scout.scout_playcalls',
                team_id=selected_team.id,
                min_runs=min_runs,
            )
        )

    report_rows = _build_report_rows(selected_game_ids, min_runs)
    if not any(report_rows.values()):
        flash('No playcalls match the selected games and filters to export.', 'info')
        return redirect(
            url_for(
                'scout.scout_playcalls',
                team_id=selected_team.id,
                min_runs=min_runs,
                game_ids=','.join(str(game_id) for game_id in sorted(selected_game_ids)),
            )
        )

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Bucket', 'Playcall', 'Times Run', 'Total Points', 'PPC'])

    preferred_buckets = ['STANDARD', 'BOB', 'SOB']
    ordered_buckets = []
    seen_buckets: set[str] = set()
    for bucket_key in preferred_buckets + list(report_rows.keys()):
        if bucket_key in report_rows and bucket_key not in seen_buckets:
            ordered_buckets.append(bucket_key)
            seen_buckets.add(bucket_key)

    for bucket_key in ordered_buckets:
        for row in report_rows[bucket_key]:
            writer.writerow(
                [
                    bucket_key,
                    row['playcall'],
                    row['times_run'],
                    row['total_points'],
                    f"{row['ppc']:.2f}",
                ]
            )

    output.seek(0)
    filename = f"scout_playcalls_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'},
    )


@scout_bp.route('/playcalls/upload')
@_staff_required
def scout_playcalls_upload():
    teams = ScoutTeam.query.order_by(ScoutTeam.name).all()
    team_id = request.args.get('team_id', type=int)
    selected_team = ScoutTeam.query.get(team_id) if team_id else None

    return render_template(
        'scout/scout_playcalls_upload.html',
        teams=teams,
        selected_team=selected_team,
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
        return redirect(url_for('scout.scout_playcalls_upload', team_id=team_id))

    file = request.files.get('file')
    if not file or not file.filename:
        flash('Please upload a CSV file.', 'error')
        return redirect(
            url_for(
                'scout.scout_playcalls_upload',
                team_id=team.id if team else team_id,
            )
        )

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
