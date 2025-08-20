import os
import json
import time
from flask import render_template, request, redirect, url_for, flash, current_app
from datetime import date
from flask_login import current_user
from types import SimpleNamespace
from werkzeug.utils import secure_filename
from yourapp import db
from models.recruit import Recruit, RecruitShotTypeStat, RecruitTopSchool
from . import recruits_bp
from utils.auth import PLAYER_ALLOWED_ENDPOINTS
from flask import current_app
from admin.routes import compute_team_shot_details

ALLOWED_HEADSHOT_EXTS = {"jpg", "jpeg", "png", "webp", "gif", "svg"}
UPLOAD_SUBDIR_RECRUITS = "uploads/recruits"


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


@recruits_bp.before_request
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


@recruits_bp.route('/')
def list_recruits():
    recruits = Recruit.query.order_by(Recruit.name).all()
    for r in recruits:
        r.overall_pps = compute_overall_pps_for_recruit(r)
    return render_template('recruits/list.html', recruits=recruits)


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
    return render_template(
        'recruits/detail.html',
        recruit=r,
        shot_type_totals=shot_type_totals,
        shot_summaries=shot_summaries,
        overall_pps=overall_pps,
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
