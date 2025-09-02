import os
import json
import time
import csv
import io
from collections import defaultdict, Counter
from flask import render_template, request, redirect, url_for, flash, current_app, abort, Response
from datetime import date
from flask_login import login_required, current_user
from types import SimpleNamespace
from io import BytesIO
from werkzeug.utils import secure_filename
from sqlalchemy import func, desc, and_
from models.database import db
from models.recruit import Recruit, RecruitShotTypeStat, RecruitTopSchool
from models.eybl import UnifiedStats
from . import recruits_bp
from utils.auth import PLAYER_ALLOWED_ENDPOINTS
from admin.routes import compute_team_shot_details
from services.circuit_stats import (
    get_circuit_stats_for_recruit,
    get_latest_circuit_stat,
)
import importlib.util
from pathlib import Path
from app.utils.coach_names import normalize_coach_name, get_alias_variants

# --- START PATCH: imports for workbook manager ---
import os, json, time
from datetime import datetime
from flask import current_app, request, redirect, url_for, flash, send_file
from werkzeug.utils import secure_filename
# try to import your importer(s)
_import_money_board = None
_import_money_board_alt = None
# --- END PATCH ---

_fmt_spec = importlib.util.spec_from_file_location(
    "app.utils.formatting", Path(__file__).resolve().parents[1] / "app" / "utils" / "formatting.py"
)
_fmt_module = importlib.util.module_from_spec(_fmt_spec)
_fmt_spec.loader.exec_module(_fmt_module)

ALLOWED_HEADSHOT_EXTS = {"jpg", "jpeg", "png", "webp", "gif", "svg"}
UPLOAD_SUBDIR_RECRUITS = "uploads/recruits"
ALLOWED_EXT = {".xlsx", ".xlsm", ".xls"}

# --- START PATCH: workbook file helpers ---
ALLOWED_WORKBOOK_EXTS = {'.xlsx', '.xls', '.csv'}

def _money_dir():
    # store in instance/money_board
    d = os.path.join(current_app.instance_path, 'money_board')
    os.makedirs(d, exist_ok=True)
    return d

def _wb_path():
    # normalize to a single canonical filename (we'll preserve original in manifest)
    return os.path.join(_money_dir(), 'money_board.xlsx')

def _manifest_path():
    return os.path.join(_money_dir(), 'manifest.json')

def _load_manifest():
    p = _manifest_path()
    if os.path.exists(p):
        try:
            with open(p, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_manifest(data: dict):
    with open(_manifest_path(), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _has_workbook():
    return os.path.exists(_wb_path())

def _workbook_info():
    """Return dict with current file info or None if not present."""
    if not _has_workbook():
        return None
    st = os.stat(_wb_path())
    m = _load_manifest()
    return {
        "path": _wb_path(),
        "original_filename": m.get("original_filename") or os.path.basename(_wb_path()),
        "size_bytes": st.st_size,
        "uploaded_at": m.get("uploaded_at") or datetime.fromtimestamp(st.st_mtime).isoformat(timespec='seconds'),
        "mtime": st.st_mtime,
    }

def _run_import(file_path: str):
    """Call whichever importer your project exposes."""
    global _import_money_board, _import_money_board_alt
    if _import_money_board is None:
        try:
            from app.services.money_board_importer import import_money_board as _import_money_board  # type: ignore
        except Exception:
            _import_money_board = None
    if _import_money_board is not None:
        return _import_money_board(file_path)
    if _import_money_board_alt is None:
        try:
            from app.services.draft_stock_importer import import_workbook as _import_money_board_alt  # type: ignore
        except Exception:
            _import_money_board_alt = None
    if _import_money_board_alt is not None:
        return _import_money_board_alt(file_path)
    # If neither importer exists, just log and return
    current_app.logger.warning("[Workbook] No money board importer function found.")
    return None
# --- END PATCH ---


def _allowed_ext(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_HEADSHOT_EXTS


def _save_headshot_and_get_url(file_storage, recruit_id: int) -> str:
    """
    Saves the uploaded file into static/uploads/recruits and returns a web path like:
    /static/uploads/recruits/<recruit_id>-<ts>.<ext>
    """
    filename = secure_filename(file_storage.filename)
    ext = filename.rsplit(".", 1)[1].lower()
    ts = int(time.time())
    new_name = f"{recruit_id}-{ts}.{ext}"

    static_root = current_app.static_folder
    target_dir = os.path.join(static_root, UPLOAD_SUBDIR_RECRUITS)
    os.makedirs(target_dir, exist_ok=True)

    abs_path = os.path.join(target_dir, new_name)
    file_storage.save(abs_path)

    return f"/static/{UPLOAD_SUBDIR_RECRUITS}/{new_name}"


def _maybe_delete_old_headshot(old_url: str):
    """
    Best-effort cleanup: if old_url looks like /static/uploads/recruits/..., delete the file.
    Ignore external URLs and placeholders.
    """
    if not old_url:
        return
    prefix = "/static/" + UPLOAD_SUBDIR_RECRUITS + "/"
    if old_url.startswith(prefix):
        static_root = current_app.static_folder
        rel = old_url[len("/static/"):]
        abs_path = os.path.join(static_root, rel)
        try:
            if os.path.exists(abs_path):
                os.remove(abs_path)
        except Exception:
            pass


def recruits_before_request():
    if not hasattr(current_app, 'login_manager'):
        return
    if current_user.is_authenticated and current_user.is_player:
        if request.endpoint not in PLAYER_ALLOWED_ENDPOINTS:
            flash('You do not have permission to view that page.', 'error')
            if current_user.player_name:
                target = url_for('admin.player_detail', player_name=current_user.player_name)
            else:
                target = url_for('public.homepage')
            return redirect(target)

if not getattr(recruits_bp, '_got_registered_once', False):
    recruits_bp.before_request(recruits_before_request)


@recruits_bp.route('/')
def list_recruits():
    circuit = request.args.get('circuit')
    query = Recruit.query
    if circuit and circuit != 'All':
        query = query.filter(
            db.session.query(UnifiedStats.id)
            .filter(UnifiedStats.recruit_id == Recruit.id,
                    UnifiedStats.circuit == circuit)
            .exists()
        )
    recruits = query.order_by(Recruit.name).all()
    for r in recruits:
        r.overall_pps = compute_overall_pps_for_recruit(r)
        r.circuit_stats = get_circuit_stats_for_recruit(r.id)
        if r.circuit_stats:
            r.latest_circuit = r.circuit_stats[0]['circuit']
            circuits = {s['circuit'] for s in r.circuit_stats}
            r.additional_circuits = [c for c in circuits if c != r.latest_circuit]
        else:
            r.latest_circuit = None
            r.additional_circuits = []

    circuit_counts = {
        'All': db.session.query(UnifiedStats.recruit_id).distinct().count(),
        'EYBL': db.session.query(UnifiedStats.recruit_id).filter_by(circuit='EYBL').distinct().count(),
        '3SSB': db.session.query(UnifiedStats.recruit_id).filter_by(circuit='3SSB').distinct().count(),
        'UA': db.session.query(UnifiedStats.recruit_id).filter_by(circuit='UA').distinct().count(),
    }
    return render_template(
        'recruits/list.html',
        recruits=recruits,
        circuit_filter=circuit or 'All',
        circuit_counts=circuit_counts,
    )


@recruits_bp.route("/import", methods=["GET", "POST"])
@login_required
def import_recruits_workbook():
    # mirror other recruits access control
    if getattr(current_user, "is_player", False):
        return render_template("errors/403.html"), 403

    if request.method == "POST":
        f = request.files.get("file")
        if not f or not f.filename:
            flash("Please choose an Excel file.", "warning")
            return render_template("recruits/import.html"), 400

        filename = secure_filename(f.filename)
        ext = ("." + filename.rsplit(".", 1)[-1].lower()) if "." in filename else ""
        if ext not in ALLOWED_EXT:
            flash("Only .xlsx, .xlsm, .xls files are allowed.", "warning")
            return render_template("recruits/import.html"), 400

        data = f.read()
        if not data:
            flash("Uploaded file is empty.", "warning")
            return render_template("recruits/import.html"), 400

        try:
            from app.services.draft_stock_importer import import_workbook
            current_app.logger.info("IMPORT start: %s (%d bytes)", filename, len(data))
            summary = import_workbook(BytesIO(data), strict=True, commit_batch=500)
            current_app.logger.info("IMPORT done: %s summary=%s", filename, summary)
        except Exception as e:
            # surfaces issues like “missing required columns: ['Player']”
            current_app.logger.exception("IMPORT FAILED for %s", filename)
            flash(f"Import failed: {e}", "danger")
            return render_template("recruits/import.html"), 400

        flash(f"Import complete: {summary['upserts']} upserts across {summary['sheets']} sheets.", "success")
        return redirect(url_for("recruits.money_board"))

    return render_template("recruits/import.html")

@recruits_bp.route('/new', methods=['GET', 'POST'])
def new_recruit():
    if request.method == 'POST':
        offer_date_str = request.form.get("offer_date")
        commit_date_str = request.form.get("commit_date")

        def _parse_int(field_name):
            value = request.form.get(field_name)
            try:
                return int(value) if value not in (None, "") else None
            except ValueError:
                return None

        r = Recruit(
            name=request.form["name"],
            graduation_year=_parse_int("graduation_year"),
            position=request.form.get("position"),
            height=request.form.get("height"),
            weight=_parse_int("weight"),
            high_school=request.form.get("high_school"),
            hometown=request.form.get("hometown"),
            rating=_parse_int("rating"),
            ranking=_parse_int("ranking"),
            camp_performance=request.form.get("camp_performance"),
            offer_status=request.form.get("offer_status"),
            offer_date=date.fromisoformat(offer_date_str) if offer_date_str else None,
            commit_date=date.fromisoformat(commit_date_str) if commit_date_str else None,
            email=request.form.get("email"),
            phone=request.form.get("phone"),
            notes=request.form.get("notes"),
        )
        aau_team = request.form.get("aau_team") or None
        ppg_raw = request.form.get("ppg")
        try:
            ppg = float(ppg_raw) if ppg_raw not in (None, "") else None
        except ValueError:
            ppg = None

        r.aau_team = aau_team
        r.ppg = ppg
        db.session.add(r)
        db.session.flush()

        for idx, school_name in enumerate(request.form.getlist('top_schools'), start=1):
            school_name = school_name.strip()
            if not school_name:
                continue
            ts = RecruitTopSchool(
                recruit_id=r.id,
                school_name=school_name,
                rank=idx
            )
            db.session.add(ts)

        db.session.commit()

        headshot = request.files.get("headshot_file")
        if headshot and headshot.filename and _allowed_ext(headshot.filename):
            new_url = _save_headshot_and_get_url(headshot, r.id)
            r.profile_image_url = new_url
        else:
            manual_url = (request.form.get("profile_image_url") or "").strip()
            if manual_url:
                r.profile_image_url = manual_url

        db.session.commit()
        return redirect(url_for('recruits.list_recruits'))
    return render_template('recruits/new.html')


def compute_shot_type_totals_for_recruit(recruit):
    latest = (
        RecruitShotTypeStat.query
        .filter_by(recruit_id=recruit.id)
        .order_by(RecruitShotTypeStat.created_at.desc())
        .first()
    )
    records = []
    if latest:
        records = [SimpleNamespace(shot_type_details=latest.shot_type_details)]

    # Delegate to admin.routes.compute_team_shot_details for full sub-category support
    totals, _ = compute_team_shot_details(records, set())
    return totals


def compute_overall_pps_for_recruit(recruit):
    """Return overall points per shot across all shot types."""
    totals = compute_shot_type_totals_for_recruit(recruit)
    attempts = (
        totals.atr.attempts
        + totals.fg2.attempts
        + totals.fg3.attempts
    )
    if attempts == 0:
        return 0.0
    points = (
        totals.atr.makes * 2
        + totals.fg2.makes * 2
        + totals.fg3.makes * 3
    )
    return round(points / attempts, 2)


def compute_shot_summaries_for_recruit(recruit):
    latest = (
        RecruitShotTypeStat.query
        .filter_by(recruit_id=recruit.id)
        .order_by(RecruitShotTypeStat.created_at.desc())
        .first()
    )
    records = []
    if latest:
        records = [SimpleNamespace(shot_type_details=latest.shot_type_details)]

    # Delegate to admin.routes.compute_team_shot_details for full sub-category support
    _, summaries = compute_team_shot_details(records, set())
    return summaries


@recruits_bp.route('/<int:id>')
def detail_recruit(id):
    r = Recruit.query.get_or_404(id)
    shot_type_totals = compute_shot_type_totals_for_recruit(r)
    shot_summaries = compute_shot_summaries_for_recruit(r)
    overall_pps = compute_overall_pps_for_recruit(r)
    circuit_stats = get_circuit_stats_for_recruit(r.id)
    stats_by_circuit = {}
    for stat in circuit_stats:
        stats_by_circuit.setdefault(stat['circuit'], []).append(stat)
    latest_by_circuit = {c: stats[0] for c, stats in stats_by_circuit.items()}
    return render_template(
        'recruits/detail.html',
        recruit=r,
        shot_type_totals=shot_type_totals,
        shot_summaries=shot_summaries,
        overall_pps=overall_pps,
        circuit_stats=circuit_stats,
        stats_by_circuit=stats_by_circuit,
        latest_by_circuit=latest_by_circuit,
    )




@recruits_bp.route('/<int:id>/top_schools', methods=['POST'])
def add_school(id):
    r = Recruit.query.get_or_404(id)
    school = RecruitTopSchool(
        recruit_id=r.id,
        school_name=request.form['school_name'],
        rank=request.form.get('rank', len(r.top_schools) + 1),
    )
    db.session.add(school)
    db.session.commit()
    return redirect(url_for('recruits.detail_recruit', id=id))


@recruits_bp.route('/<int:id>/top_schools/<int:school_id>/delete', methods=['POST'])
def delete_school(id, school_id):
    s = RecruitTopSchool.query.get_or_404(school_id)
    db.session.delete(s)
    db.session.commit()
    return redirect(url_for('recruits.detail_recruit', id=id))

@recruits_bp.route('/<int:id>/edit', methods=['GET', 'POST'])
def edit_recruit(id):
    r = Recruit.query.get_or_404(id)
    if request.method == 'POST':
        offer_date_str = request.form.get("offer_date")
        commit_date_str = request.form.get("commit_date")

        def _parse_int(field_name):
            value = request.form.get(field_name)
            try:
                return int(value) if value not in (None, "") else None
            except ValueError:
                return None

        r.name = request.form.get("name", r.name)
        r.graduation_year = _parse_int("graduation_year")
        r.position = request.form.get("position")
        r.height = request.form.get("height")
        r.weight = _parse_int("weight")
        r.high_school = request.form.get("high_school")
        r.hometown = request.form.get("hometown")
        r.rating = _parse_int("rating")
        r.ranking = _parse_int("ranking")
        r.camp_performance = request.form.get("camp_performance")
        aau_team = request.form.get("aau_team") or None
        ppg_raw = request.form.get("ppg")
        try:
            ppg = float(ppg_raw) if ppg_raw not in (None, "") else None
        except ValueError:
            ppg = None
        r.aau_team = aau_team
        r.ppg = ppg
        r.offer_status = request.form.get("offer_status")
        r.offer_date = date.fromisoformat(offer_date_str) if offer_date_str else None
        r.commit_date = date.fromisoformat(commit_date_str) if commit_date_str else None
        r.email = request.form.get("email")
        r.phone = request.form.get("phone")
        r.notes = request.form.get("notes")

        headshot = request.files.get("headshot_file")
        if headshot and headshot.filename and _allowed_ext(headshot.filename):
            _maybe_delete_old_headshot(r.profile_image_url)
            new_url = _save_headshot_and_get_url(headshot, r.id)
            r.profile_image_url = new_url
        else:
            manual_url = (request.form.get("profile_image_url") or "").strip()
            if manual_url:
                r.profile_image_url = manual_url

        db.session.commit()
        return redirect(url_for('recruits.detail_recruit', id=id))
    return render_template('recruits/edit.html', recruit=r)


@recruits_bp.route('/<int:id>/delete', methods=['POST'])
def delete_recruit(id):
    r = Recruit.query.get_or_404(id)
    db.session.delete(r)
    db.session.commit()
    flash('Recruit deleted.', 'success')
    return redirect(url_for('recruits.list_recruits'))


@recruits_bp.route('/money')
def money_board():
    """
    Coach leaderboard by money outcomes with filters.
    Defaults: order by NET desc.
    """
    from app.models.prospect import Prospect
    # hide Import button if table has any rows
    has_data = db.session.query(func.count(Prospect.id)).scalar() > 0
    # ---- Filters from querystring ----
    year_min = request.args.get('year_min', type=int)
    year_max = request.args.get('year_max', type=int)
    sheet = request.args.get('sheet')  # conference tab label, e.g. "SEC"
    conf = request.args.get('conf')    # coach_current_conference
    min_recruits = request.args.get('min_recruits', default=1, type=int)
    sort = request.args.get('sort', default='net_desc')

    # Distinct lists for filter controls
    years = [y for (y,) in db.session.query(Prospect.year)
                                      .filter(Prospect.year.isnot(None))
                                      .distinct().order_by(Prospect.year).all()]
    sheets = [s for (s,) in db.session.query(Prospect.sheet)
                                      .filter(Prospect.sheet.isnot(None))
                                      .distinct().order_by(Prospect.sheet).all()]
    confs = [c for (c,) in db.session.query(Prospect.coach_current_conference)
                                     .filter(Prospect.coach_current_conference.isnot(None))
                                     .distinct().order_by(Prospect.coach_current_conference).all()]

    # ---- Base query with filters applied BEFORE grouping ----
    base = db.session.query(Prospect)
    if year_min is not None:
        base = base.filter(Prospect.year >= year_min)
    if year_max is not None:
        base = base.filter(Prospect.year <= year_max)
    if sheet:
        base = base.filter(Prospect.sheet == sheet)
    if conf:
        base = base.filter(Prospect.coach_current_conference == conf)

    # ---- Aggregation by coach ----
    sub = base.subquery()
    q = (db.session.query(
            sub.c.coach.label('coach'),
            func.coalesce(sub.c.coach_current_team, '').label('coach_team'),
            func.coalesce(sub.c.coach_current_conference, '').label('coach_conf'),
            func.count(sub.c.id).label('recruits'),
            func.sum(func.coalesce(sub.c.projected_money, 0)).label('proj_sum'),
            func.sum(func.coalesce(sub.c.actual_money, 0)).label('act_sum'),
            func.sum(func.coalesce(sub.c.net, 0)).label('net_sum'),
        )
        .select_from(sub)
        .group_by(sub.c.coach, sub.c.coach_current_team, sub.c.coach_current_conference)
    )

    if min_recruits:
        q = q.having(func.count() >= min_recruits)

    # Sorting
    if sort == 'actual_desc':
        q = q.order_by(desc('act_sum'))
    elif sort == 'proj_desc':
        q = q.order_by(desc('proj_sum'))
    elif sort == 'avg_net_desc':
        # We'll sort in Python on avg_net to avoid SQL dialect differences
        pass
    else:
        q = q.order_by(desc('net_sum'))

    rows = q.all()
    # Fold any aliases by canonical coach key
    merged: dict[str, dict] = {}
    for r in rows:
        key, disp = normalize_coach_name(r.coach)
        entry = merged.get(key)
        if not entry:
            entry = {
                "coach": disp,
                "coach_team": r.coach_team,
                "coach_conf": r.coach_conf,
                "recruits": int(r.recruits),
                "proj_sum": float(r.proj_sum or 0),
                "act_sum": float(r.act_sum or 0),
                "net_sum": float(r.net_sum or 0),
            }
            merged[key] = entry
        else:
            entry["recruits"] += int(r.recruits)
            entry["proj_sum"] += float(r.proj_sum or 0)
            entry["act_sum"] += float(r.act_sum or 0)
            entry["net_sum"] += float(r.net_sum or 0)
            if not entry["coach_team"] and r.coach_team:
                entry["coach_team"] = r.coach_team
            if not entry["coach_conf"] and r.coach_conf:
                entry["coach_conf"] = r.coach_conf

    data = []
    for e in merged.values():
        avg_net = (e["net_sum"] / e["recruits"]) if e["recruits"] else 0.0
        e["avg_net"] = float(avg_net)
        data.append(e)

    if sort == 'actual_desc':
        data.sort(key=lambda x: x['act_sum'], reverse=True)
    elif sort == 'proj_desc':
        data.sort(key=lambda x: x['proj_sum'], reverse=True)
    elif sort == 'avg_net_desc':
        data.sort(key=lambda x: x['avg_net'], reverse=True)
    else:
        data.sort(key=lambda x: x['net_sum'], reverse=True)

    # Top card
    top = data[0] if data else None

    return render_template(
        'recruits/money_board.html',
        rows=data,
        top=top,
        years=years,
        sheets=sheets,
        confs=confs,
        # echo filters
        f_year_min=year_min, f_year_max=year_max, f_sheet=sheet, f_conf=conf,
        f_min_recruits=min_recruits, f_sort=sort,
        has_data=has_data
    )


@recruits_bp.route('/money/coach/<coach_name>')
def money_coach(coach_name):
    """
    Drilldown for a single coach: totals, by-year summary, and player list.
    """
    from app.models.prospect import Prospect
    # Exact match; if you prefer case-insensitive, use ilike and first()
    totals_row = (db.session.query(
                    func.sum(func.coalesce(Prospect.projected_money, 0)).label('proj_sum'),
                    func.sum(func.coalesce(Prospect.actual_money, 0)).label('act_sum'),
                    func.sum(func.coalesce(Prospect.net, 0)).label('net_sum'),
                    func.count(Prospect.id).label('recruits'),
                    func.max(Prospect.coach_current_team).label('coach_team'),
                    func.max(Prospect.coach_current_conference).label('coach_conf'),
                 )
                 .filter(Prospect.coach == coach_name)
                 .one())

    if totals_row.recruits == 0:
        abort(404)

    coach_summary = {
        "coach": coach_name,
        "coach_team": totals_row.coach_team or "",
        "coach_conf": totals_row.coach_conf or "",
        "recruits": int(totals_row.recruits or 0),
        "proj_sum": float(totals_row.proj_sum or 0),
        "act_sum": float(totals_row.act_sum or 0),
        "net_sum": float(totals_row.net_sum or 0),
        "avg_net": float((totals_row.net_sum or 0) / totals_row.recruits) if totals_row.recruits else 0.0,
    }

    # --- START PATCH: sanity assertions (optional, remove in prod) ---
    assert 'team' not in coach_summary, "coach_summary must not contain a 'team' key"
    # --- END PATCH ---

    by_year = (db.session.query(
                    Prospect.year,
                    func.count(Prospect.id).label('n'),
                    func.sum(func.coalesce(Prospect.projected_money, 0)).label('proj_sum'),
                    func.sum(func.coalesce(Prospect.actual_money, 0)).label('act_sum'),
                    func.sum(func.coalesce(Prospect.net, 0)).label('net_sum'),
               )
               .filter(Prospect.coach == coach_name)
               .group_by(Prospect.year)
               .order_by(Prospect.year.asc())
               .all())

    players = (Prospect.query
               .filter(Prospect.coach == coach_name)
               .order_by(desc(Prospect.net), desc(Prospect.actual_money), Prospect.year.desc())
               .all())

    return render_template(
        'recruits/coach_money.html',
        coach_name=coach_name,
        coach_summary=coach_summary,
        by_year=by_year,
        players=players,
    )


def _get_coach_names():
    """Return sorted canonical coach names for picker UI."""
    from app.models.prospect import Prospect
    raw = [c for (c,) in db.session.query(Prospect.coach)
                               .filter(Prospect.coach.isnot(None))
                               .distinct().all()]
    seen: dict[str, str] = {}
    for name in raw:
        key, disp = normalize_coach_name(name)
        seen[key] = disp
    return sorted(seen.values())


@recruits_bp.route('/money/compare', methods=['GET'])
def money_compare():
    """Compare projected/actual money totals for up to ten coaches."""

    raw_selected = request.args.getlist('coaches')
    current_app.logger.info(f"[Compare] coaches={raw_selected}")
    seen_keys: set[str] = set()
    selected: list[str] = []
    for raw in raw_selected:
        key, disp = normalize_coach_name(raw)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        selected.append(disp)

    MAX_COMPARE = 10
    if len(selected) > MAX_COMPARE:
        selected = selected[:MAX_COMPARE]
        flash(f"You can compare up to {MAX_COMPARE} coaches at a time.", "warning")

    variant_set: set[str] = set()
    for name in selected:
        variant_set.update(get_alias_variants(name))

    from app.models.prospect import Prospect

    # Extract filters
    year_min = request.args.get("year_min", type=int)
    year_max = request.args.get("year_max", type=int)
    sheet    = (request.args.get("sheet") or "").strip()
    conf     = (request.args.get("conf") or "").strip()
    min_rec  = request.args.get("min_recruits", type=int)
    sort     = request.args.get("sort") or ""

    filters = []
    if year_min is not None:
        filters.append(Prospect.year >= year_min)
    if year_max is not None:
        filters.append(Prospect.year <= year_max)
    if sheet:
        filters.append(Prospect.sheet == sheet)
    if conf:
        filters.append(Prospect.coach_current_conference == conf)

    # Totals query with filters
    agg_q = (
        db.session.query(
            Prospect.coach.label('coach'),
            func.count(Prospect.id).label('recruits'),
            func.sum(func.coalesce(Prospect.projected_money, 0)).label('proj_sum'),
            func.sum(func.coalesce(Prospect.actual_money, 0)).label('act_sum'),
            func.sum(func.coalesce(Prospect.net, 0)).label('net_sum'),
        )
        .filter(func.lower(Prospect.coach).in_(variant_set))
    )
    if filters:
        agg_q = agg_q.filter(and_(*filters))

    rows = agg_q.group_by(Prospect.coach).all()

    merged: dict[str, dict] = {}
    for r in rows:
        key, disp = normalize_coach_name(r.coach)
        entry = merged.get(key)
        if not entry:
            entry = {
                "coach": disp,
                "recruits": int(r.recruits or 0),
                "proj_sum": float(r.proj_sum or 0),
                "act_sum": float(r.act_sum or 0),
                "net_sum": float(r.net_sum or 0),
            }
            merged[key] = entry
        else:
            entry["recruits"] += int(r.recruits or 0)
            entry["proj_sum"] += float(r.proj_sum or 0)
            entry["act_sum"] += float(r.act_sum or 0)
            entry["net_sum"] += float(r.net_sum or 0)

    comps = []
    for name in selected:
        key, _ = normalize_coach_name(name)
        e = merged.get(key)
        if not e:
            continue
        avg_net = (e["net_sum"] / e["recruits"]) if e["recruits"] else 0
        e["avg_net"] = float(avg_net or 0)
        comps.append(e)

    if min_rec:
        comps = [c for c in comps if c["recruits"] >= min_rec]

    if sort == 'actual_desc':
        comps.sort(key=lambda c: c["act_sum"], reverse=True)
    elif sort == 'proj_desc':
        comps.sort(key=lambda c: c["proj_sum"], reverse=True)
    elif sort == 'avg_net_desc':
        comps.sort(key=lambda c: c["avg_net"], reverse=True)
    else:
        comps.sort(key=lambda c: c["net_sum"], reverse=True)

    # Players query
    players_q = Prospect.query.filter(func.lower(Prospect.coach).in_(variant_set))
    if filters:
        players_q = players_q.filter(and_(*filters))
    players_q = players_q.order_by(Prospect.coach.asc(), Prospect.net.desc().nullslast())
    players = players_q.all()

    players_by_coach = defaultdict(list)
    for p in players:
        _, disp = normalize_coach_name(p.coach)
        players_by_coach[disp].append(p)

    if min_rec:
        valid = {c["coach"] for c in comps}
        players_by_coach = {k: v for k, v in players_by_coach.items() if k in valid}

    coach_list = _get_coach_names()

    not_enough = bool(raw_selected) and len(selected) < 2

    return render_template(
        'recruits/money_compare.html',
        coaches=coach_list,
        selected=selected,
        comps=comps,
        players_by_coach=players_by_coach,
        year_min=year_min, year_max=year_max, sheet=sheet, conf=conf, min_recruits=min_rec, sort=sort,
        not_enough=not_enough,
    )


@recruits_bp.route("/money/compare.csv")
def money_compare_csv():
    raw_selected = request.args.getlist("coaches")
    seen_keys: set[str] = set()
    selected: list[str] = []
    for raw in raw_selected:
        key, disp = normalize_coach_name(raw)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        selected.append(disp)
    MAX_COMPARE = 10
    if len(selected) > MAX_COMPARE:
        selected = selected[:MAX_COMPARE]
    if not selected:
        return Response("No coaches selected", status=400)

    variant_set: set[str] = set()
    for name in selected:
        variant_set.update(get_alias_variants(name))

    from app.models.prospect import Prospect

    year_min = request.args.get("year_min", type=int)
    year_max = request.args.get("year_max", type=int)
    sheet    = (request.args.get("sheet") or "").strip()
    conf     = (request.args.get("conf") or "").strip()
    min_rec  = request.args.get("min_recruits", type=int)

    filters = [func.lower(Prospect.coach).in_(variant_set)]
    if year_min is not None:
        filters.append(Prospect.year >= year_min)
    if year_max is not None:
        filters.append(Prospect.year <= year_max)
    if sheet:
        filters.append(Prospect.sheet == sheet)
    if conf:
        filters.append(Prospect.coach_current_conference == conf)

    q = Prospect.query.filter(and_(*filters)).order_by(Prospect.coach.asc(), Prospect.net.desc().nullslast())
    rows = q.all()

    if min_rec:
        counts = Counter(normalize_coach_name(p.coach)[0] for p in rows)
        rows = [p for p in rows if counts[normalize_coach_name(p.coach)[0]] >= min_rec]

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "Coach", "Coach Team", "Coach Conf",
        "Player", "Team", "Year",
        "Projected Pick (raw)", "Projected Pick (#)",
        "Actual Pick (raw)", "Actual Pick (#)",
        "Projected $", "Actual $", "NET $"
    ])

    for p in rows:
        _, disp = normalize_coach_name(p.coach)
        w.writerow([
            disp,
            p.coach_current_team or "",
            p.coach_current_conference or "",
            p.player,
            p.team or "",
            p.year,
            p.projected_pick_raw or "",
            p.projected_pick or "",
            p.actual_pick_raw or "",
            p.actual_pick or "",
            ("" if p.projected_money is None else f"{p.projected_money:.2f}"),
            ("" if p.actual_money    is None else f"{p.actual_money:.2f}"),
            ("" if p.net             is None else f"{p.net:.2f}"),
        ])

    csv_data = buf.getvalue()
    buf.close()

    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=money_compare.csv"}
    )

# --- START PATCH: workbook manager routes ---

@recruits_bp.get("/money/workbook")
def money_workbook():
    """
    Manager page: show current workbook info + forms to upload/delete/download.
    """
    info = _workbook_info()
    return render_template("recruits/money_workbook.html", info=info)


@recruits_bp.post("/money/workbook/upload")
def money_workbook_upload():
    """
    Upload a replacement workbook and immediately import it.
    """
    f = request.files.get("file")
    if not f or not f.filename:
        flash("Please choose a file.", "warning")
        return redirect(url_for("recruits.money_workbook"))

    filename = secure_filename(f.filename)
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_WORKBOOK_EXTS:
        flash("Unsupported file type. Please upload .xlsx, .xls, or .csv", "warning")
        return redirect(url_for("recruits.money_workbook"))

    # Save to canonical path
    dest = _wb_path()
    # write to temp then replace to avoid partial writes
    tmp = dest + ".tmp"
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    f.save(tmp)
    os.replace(tmp, dest)

    # Update manifest
    _save_manifest({
        "original_filename": filename,
        "uploaded_at": datetime.now().isoformat(timespec='seconds'),
    })

    # Run importer
    try:
        _run_import(dest)
        flash(f"Workbook '{filename}' uploaded and imported successfully.", "success")
    except Exception as e:
        current_app.logger.exception(f"[Workbook] Import failed: {e}")
        flash(f"Upload saved, but import failed: {e}", "danger")

    return redirect(url_for("recruits.money_workbook"))


@recruits_bp.post("/money/workbook/delete")
def money_workbook_delete():
    """
    Delete the current workbook file (does not modify DB).
    """
    if _has_workbook():
        try:
            os.remove(_wb_path())
        except Exception as e:
            current_app.logger.exception(f"[Workbook] Delete failed: {e}")
            flash(f"Could not delete file: {e}", "danger")
            return redirect(url_for("recruits.money_workbook"))
    # clear manifest too
    try:
        if os.path.exists(_manifest_path()):
            os.remove(_manifest_path())
    except Exception:
        pass
    flash("Workbook file deleted.", "success")
    return redirect(url_for("recruits.money_workbook"))


@recruits_bp.get("/money/workbook/download")
def money_workbook_download():
    """
    Download the current workbook file.
    """
    if not _has_workbook():
        flash("No workbook on file.", "warning")
        return redirect(url_for("recruits.money_workbook"))
    info = _workbook_info() or {}
    as_name = info.get("original_filename") or "money_board.xlsx"
    return send_file(_wb_path(), as_attachment=True, download_name=as_name)
# --- END PATCH ---
