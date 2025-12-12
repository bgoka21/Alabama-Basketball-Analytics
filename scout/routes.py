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
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename

from . import scout_bp
from models.database import db
from models.scout import (
    ScoutGame,
    ScoutPlaycallMapping,
    ScoutPossession,
    ScoutTeam,
    normalize_playcall,
)
from scout.parsers import store_scout_playcalls
from scout.schema import ensure_scout_possession_schema


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

    selected_series: list[str] = []
    for raw_series in request.args.getlist('series'):
        cleaned = (raw_series or '').strip()
        if cleaned:
            selected_series.append(cleaned)

    selected_families: list[str] = []
    for raw_family in request.args.getlist('family'):
        cleaned = (raw_family or '').strip()
        if cleaned:
            selected_families.append(cleaned)

    return (
        teams,
        selected_team,
        games,
        selected_game_ids,
        min_runs,
        selected_series,
        selected_families,
    )


def _parse_int_set(raw_values):
    parsed_values: set[int] = set()
    for value in raw_values:
        try:
            parsed_values.add(int(value))
        except (TypeError, ValueError):
            continue
    return parsed_values


def _collect_unique_playcalls(selected_game_ids: set[int]):
    if not selected_game_ids:
        return []

    ensure_scout_possession_schema(db.engine)

    possession_key_expr = (
        db.cast(ScoutPossession.scout_game_id, db.String)
        + db.literal('-')
        + db.cast(ScoutPossession.instance_number, db.String)
    )
    times_run_expr = db.func.count(db.func.distinct(possession_key_expr))
    series_expr = db.func.nullif(db.func.trim(ScoutPossession.series), '')
    family_expr = db.func.nullif(db.func.trim(ScoutPossession.family), '')

    query = (
        db.session.query(
            db.func.trim(ScoutPossession.playcall).label('playcall'),
            times_run_expr.label('times_run'),
            db.func.max(series_expr).label('series'),
            db.func.max(family_expr).label('family'),
        )
        .filter(ScoutPossession.scout_game_id.in_(selected_game_ids))
        .filter(ScoutPossession.playcall.isnot(None))
        .filter(ScoutPossession.playcall != '')
        .filter(db.func.length(db.func.trim(ScoutPossession.playcall)) > 0)
        .group_by(db.func.trim(ScoutPossession.playcall))
    )

    excluded_prefixes = ('eog', 'ft', 'vs')
    unique_rows = []
    discovered_playcalls: list[str] = []
    for row in query.all():
        playcall = (row.playcall or '').strip()
        if not playcall:
            continue
        playcall_lower = playcall.lower()
        if any(playcall_lower.startswith(prefix) for prefix in excluded_prefixes):
            continue

        discovered_playcalls.append(playcall)

        unique_rows.append(
            {
                'playcall': playcall,
                'series': (row.series or '').strip(),
                'family': (row.family or '').strip(),
                'times_run': int(row.times_run or 0),
            }
        )

    if discovered_playcalls:
        playcall_keys = {normalize_playcall(playcall) for playcall in discovered_playcalls}
        mappings = {
            mapping.playcall_key: mapping
            for mapping in ScoutPlaycallMapping.query.filter(
                ScoutPlaycallMapping.playcall_key.in_(playcall_keys)
            ).all()
        }

        for row in unique_rows:
            mapping = mappings.get(normalize_playcall(row['playcall']))
            row['canonical_series'] = (mapping.canonical_series or '').strip() if mapping else ''
            row['canonical_family'] = (mapping.canonical_family or '').strip() if mapping else ''
            row['playcall_label'] = mapping.playcall if mapping else row['playcall']

    unique_rows.sort(key=lambda row: (row['playcall'].lower(), -row['times_run']))
    return unique_rows


def _save_playcall_mapping(
    playcall: str,
    series: str,
    family: str,
    selected_game_ids: set[int],
    apply_globally: bool,
) -> int:
    mapping = ScoutPlaycallMapping.from_playcall(playcall)
    mapping.playcall = playcall.strip() or mapping.playcall
    if series:
        mapping.canonical_series = series
    if family:
        mapping.canonical_family = family

    update_fields: dict[str, str] = {}
    if series:
        update_fields['series'] = series
    if family:
        update_fields['family'] = family

    if not update_fields:
        return 0

    playcall_key = normalize_playcall(playcall)
    possession_query = ScoutPossession.query.filter(
        db.func.lower(db.func.trim(ScoutPossession.playcall)) == playcall_key
    )
    if not apply_globally:
        possession_query = possession_query.filter(
            ScoutPossession.scout_game_id.in_(selected_game_ids)
        )

    updated_count = possession_query.update(update_fields, synchronize_session=False)
    return updated_count


def _build_report_rows(
    selected_game_ids: set[int], min_runs: int, selected_series: Optional[set[str]] = None
):
    base_totals = {'times_run': 0, 'total_points': 0, 'ppc': 0.0}
    report_rows: dict[str, object] = {
        'series_options': [],
        'series_rows': {},
        'visible_series': [],
        'all_rows': [],
        'all_totals': base_totals.copy(),
    }

    ensure_scout_possession_schema(db.engine)

    if not selected_game_ids:
        return report_rows

    possession_key_expr = (
        db.cast(ScoutPossession.scout_game_id, db.String)
        + db.literal('-')
        + db.cast(ScoutPossession.instance_number, db.String)
    )
    times_run_expr = db.func.count(db.func.distinct(possession_key_expr))
    total_points_expr = db.func.coalesce(db.func.sum(ScoutPossession.points), 0)
    series_label_expr = db.func.coalesce(
        db.func.nullif(db.func.trim(ScoutPossession.series), ''),
        db.func.nullif(db.func.trim(ScoutPossession.family), ''),
        db.literal('Unknown'),
    )

    query = (
        db.session.query(
            series_label_expr.label('series'),
            ScoutPossession.bucket,
            ScoutPossession.playcall,
            times_run_expr.label('times_run'),
            total_points_expr.label('total_points'),
        )
        .filter(ScoutPossession.scout_game_id.in_(selected_game_ids))
        .filter(ScoutPossession.playcall.isnot(None))
        .filter(ScoutPossession.playcall != '')
        .filter(db.func.length(db.func.trim(ScoutPossession.playcall)) > 0)
        .group_by(series_label_expr, ScoutPossession.bucket, ScoutPossession.playcall)
    )

    if min_runs and min_runs > 1:
        query = query.having(times_run_expr >= min_runs)

    excluded_prefixes = ('eog', 'ft', 'vs')

    series_options: set[str] = set()
    for row in query.all():
        playcall = (row.playcall or '').strip()
        if not playcall:
            continue
        if playcall.lower() == '(unknown)':
            # Ignore placeholder values from previous imports.
            continue
        playcall_lower = playcall.lower()
        if any(playcall_lower.startswith(prefix) for prefix in excluded_prefixes):
            continue

        series_name = (row.series or 'Unknown').strip() or 'Unknown'
        series_options.add(series_name)

        times_run = int(row.times_run or 0)
        total_points = int(row.total_points or 0)
        ppc = round(total_points / times_run, 2) if times_run else 0.0

        bucket_key = (row.bucket or 'STANDARD').upper()
        series_rows = report_rows['series_rows'].setdefault(
            series_name,
            {'rows': [], 'totals': base_totals.copy()},
        )

        series_rows['rows'].append(
            {
                'series': series_name,
                'bucket': bucket_key,
                'playcall': playcall,
                'times_run': times_run,
                'total_points': total_points,
                'ppc': ppc,
            }
        )

        series_rows['totals']['times_run'] += times_run
        series_rows['totals']['total_points'] += total_points

    if not series_options:
        return report_rows

    selected_set = {value for value in (selected_series or set()) if value}

    include_all_series = not selected_set or 'ALL' in selected_set
    ordered_series = sorted(series_options)
    report_rows['visible_series'] = (
        ordered_series if include_all_series else [series for series in ordered_series if series in selected_set]
    )

    for series_name in ordered_series:
        series_rows = report_rows['series_rows'].get(series_name, {})
        row_entries = series_rows.get('rows') if isinstance(series_rows, dict) else []
        if isinstance(row_entries, list):
            row_entries.sort(key=lambda row: (-row['times_run'], -row['ppc'], row['playcall']))
        totals_payload = series_rows.get('totals') if isinstance(series_rows, dict) else None
        if isinstance(totals_payload, dict):
            total_runs = totals_payload.get('times_run') or 0
            totals_payload['ppc'] = round(
                (totals_payload.get('total_points') or 0) / total_runs, 2
            ) if total_runs else 0.0

    for series_name in report_rows['visible_series']:
        series_rows = report_rows['series_rows'].get(series_name)
        if not isinstance(series_rows, dict):
            continue
        for row in series_rows.get('rows', []):
            report_rows['all_rows'].append(row)
        totals_payload = series_rows.get('totals') if isinstance(series_rows, dict) else None
        if isinstance(totals_payload, dict):
            report_rows['all_totals']['times_run'] += totals_payload.get('times_run', 0) or 0
            report_rows['all_totals']['total_points'] += totals_payload.get('total_points', 0) or 0

    total_runs_all = report_rows['all_totals']['times_run'] or 0
    report_rows['all_totals']['ppc'] = (
        round(report_rows['all_totals']['total_points'] / total_runs_all, 2)
        if total_runs_all
        else 0.0
    )

    report_rows['all_rows'].sort(
        key=lambda row: (row['series'], row['bucket'], -row['times_run'], -row['ppc'], row['playcall'])
    )

    report_rows['series_options'] = ['ALL'] + ordered_series

    return report_rows


@scout_bp.route('/playcalls')
@_staff_required
def scout_playcalls():
    (
        teams,
        selected_team,
        games,
        selected_game_ids,
        min_runs,
        selected_series,
        selected_families,
    ) = _parse_scout_filters()

    selected_series_set = {value for value in selected_series}
    report_rows = _build_report_rows(selected_game_ids, min_runs, selected_series_set)
    series_options = report_rows.get('series_options', []) if isinstance(report_rows, dict) else []
    if not selected_series and series_options:
        selected_series = series_options

    unique_playcalls = _collect_unique_playcalls(selected_game_ids)
    selectable_series = [option for option in series_options if option != 'ALL']
    selectable_families = sorted(
        {
            row.get('family')
            for row in unique_playcalls
            if isinstance(row, dict) and (row.get('family') or '').strip()
        }
    )

    return render_template(
        'scout/scout_playcalls.html',
        teams=teams,
        selected_team=selected_team,
        games=games,
        selected_game_ids=selected_game_ids,
        min_runs=min_runs,
        report_rows=report_rows,
        series_options=series_options,
        selected_series=selected_series,
        selected_families=selected_families,
        unique_playcalls=unique_playcalls,
        selectable_series=selectable_series,
        selectable_families=selectable_families,
    )


@scout_bp.route('/playcalls/export')
@_staff_required
def export_playcalls_csv():
    (
        _teams,
        selected_team,
        _,
        selected_game_ids,
        min_runs,
        selected_series,
        selected_families,
    ) = _parse_scout_filters()

    selected_series_set = {value for value in selected_series}

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

    report_rows = _build_report_rows(selected_game_ids, min_runs, selected_series_set)
    if not report_rows.get('all_rows'):
        flash('No playcalls match the selected games and filters to export.', 'info')
        return redirect(
            url_for(
                'scout.scout_playcalls',
                team_id=selected_team.id,
                min_runs=min_runs,
                game_ids=','.join(str(game_id) for game_id in sorted(selected_game_ids)),
                series=selected_series,
                family=selected_families,
            )
        )

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['Series', 'Bucket', 'Playcall', 'Times Run', 'Total Points', 'PPC'])

    series_order = report_rows.get('visible_series', []) if isinstance(report_rows, dict) else []
    series_rows = report_rows.get('series_rows', {}) if isinstance(report_rows, dict) else {}

    for series_name in series_order:
        payload = series_rows.get(series_name) if isinstance(series_rows, dict) else None
        if not isinstance(payload, dict):
            continue
        for row in payload.get('rows', []):
            writer.writerow(
                [
                    row['series'],
                    row['bucket'],
                    row['playcall'],
                    row['times_run'],
                    row['total_points'],
                    f"{row['ppc']:.2f}",
                ]
            )
        totals_payload = payload.get('totals') if isinstance(payload, dict) else None
        if isinstance(totals_payload, dict):
            writer.writerow(
                [
                    series_name,
                    'Totals',
                    'Totals',
                    totals_payload.get('times_run', 0),
                    totals_payload.get('total_points', 0),
                    f"{(totals_payload.get('ppc') or 0):.2f}",
                ]
            )

    all_totals = report_rows.get('all_totals', {}) if isinstance(report_rows, dict) else {}
    if report_rows.get('all_rows'):
        writer.writerow([])
        writer.writerow(['ALL', '', 'Totals', all_totals.get('times_run', 0), all_totals.get('total_points', 0), f"{(all_totals.get('ppc') or 0):.2f}"])

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


@scout_bp.route('/playcalls/series', methods=['POST', 'PUT'])
@_staff_required
def update_playcall_series():
    payload = request.get_json(silent=True) if request.is_json else None

    if payload:
        playcall = (payload.get('playcall') or '').strip()
        new_series = (
            payload.get('new_series')
            or payload.get('existing_series')
            or ''
        ).strip()
        new_family = (
            payload.get('new_family')
            or payload.get('existing_family')
            or ''
        ).strip()
        selected_game_ids = _parse_int_set(payload.get('game_ids') or [])
        min_runs = payload.get('min_runs') or 1
        selected_series = payload.get('series') or payload.get('selected_series') or []
        selected_families = (
            payload.get('family') or payload.get('families') or payload.get('selected_families') or []
        )
        team_id = payload.get('team_id')
        apply_globally_param = payload.get('apply_globally', True)
    else:
        playcall = (request.form.get('playcall') or '').strip()
        new_series = (
            request.form.get('new_series')
            or request.form.get('existing_series')
            or ''
        ).strip()
        new_family = (
            request.form.get('new_family')
            or request.form.get('existing_family')
            or ''
        ).strip()
        selected_game_ids = _parse_int_set(request.form.getlist('game_ids'))
        min_runs = request.form.get('min_runs', type=int) or 1
        selected_series = [value for value in request.form.getlist('series') if value]
        selected_families = [value for value in request.form.getlist('family') if value]
        team_id = request.form.get('team_id', type=int)
        apply_globally_param = request.form.get('apply_globally', default='1')

    try:
        min_runs = int(min_runs)
    except (TypeError, ValueError):
        min_runs = 1

    apply_globally = str(apply_globally_param).lower() not in {'false', '0', 'off', 'no'}

    update_fields: dict[str, str] = {}
    if new_series:
        update_fields['series'] = new_series
    if new_family:
        update_fields['family'] = new_family

    if not playcall or not update_fields:
        message = 'Provide a playcall and a series or family value to save.'
        if payload:
            return jsonify({'status': 'error', 'message': message}), 400
        flash(message, 'error')
        return redirect(url_for('scout.scout_playcalls'))

    if not apply_globally and not selected_game_ids:
        message = 'Select at least one game or choose to apply changes to all possessions.'
        if payload:
            return jsonify({'status': 'error', 'message': message}), 400
        flash(message, 'error')
        return redirect(url_for('scout.scout_playcalls'))

    updated_count = _save_playcall_mapping(
        playcall,
        update_fields.get('series', ''),
        update_fields.get('family', ''),
        selected_game_ids,
        apply_globally,
    )
    db.session.commit()

    saved_parts = []
    if 'series' in update_fields:
        saved_parts.append(f'series "{update_fields["series"]}"')
    if 'family' in update_fields:
        saved_parts.append(f'family "{update_fields["family"]}"')
    saved_descriptor = ' and '.join(saved_parts) if saved_parts else 'updates'
    scope_label = 'all possessions with this playcall' if apply_globally else 'selected games'
    success_message = f'Saved {saved_descriptor} for {updated_count} possessions ({scope_label}).'
    if payload:
        return (
            jsonify(
                {
                    'status': 'ok',
                    'updated_count': updated_count,
                    'series': update_fields.get('series'),
                    'family': update_fields.get('family'),
                    'apply_globally': apply_globally,
                }
            ),
            200,
        )

    flash(success_message, 'success')

    query_params = {'min_runs': min_runs}
    if team_id:
        query_params['team_id'] = team_id
    if selected_game_ids:
        query_params['game_ids'] = ','.join(str(game_id) for game_id in sorted(selected_game_ids))
    if selected_series:
        query_params['series'] = selected_series
    if selected_families:
        query_params['family'] = selected_families

    return redirect(url_for('scout.scout_playcalls', **query_params))


@scout_bp.route('/games/<int:game_id>/delete', methods=['POST'])
@_staff_required
def delete_scout_game(game_id: int):
    game = ScoutGame.query.get_or_404(game_id)

    selected_game_ids: set[int] = set()
    for raw_id in request.form.getlist('game_ids'):
        try:
            parsed_id = int(raw_id)
        except (TypeError, ValueError):
            continue

        if parsed_id != game.id:
            selected_game_ids.add(parsed_id)

    min_runs = request.form.get('min_runs', type=int) or 1
    selected_series = [value for value in request.form.getlist('series') if value]
    team_id = request.form.get('team_id', type=int) or game.scout_team_id

    db.session.delete(game)
    db.session.commit()

    flash('Scout game deleted along with its possessions.', 'success')

    query_params = {'team_id': team_id, 'min_runs': min_runs}
    if selected_game_ids:
        query_params['game_ids'] = ','.join(str(game_id) for game_id in sorted(selected_game_ids))
    if selected_series:
        query_params['series'] = selected_series

    return redirect(url_for('scout.scout_playcalls', **query_params))
