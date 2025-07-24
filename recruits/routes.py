import os
import json
from flask import render_template, request, redirect, url_for, flash, current_app
from datetime import date
from flask_login import current_user
from types import SimpleNamespace
from yourapp import db
from models.recruit import Recruit, RecruitShotTypeStat, RecruitTopSchool
from . import recruits_bp
from utils.auth import PLAYER_ALLOWED_ENDPOINTS
from flask import current_app


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
            profile_image_url=request.form.get("profile_image_url"),
            notes=request.form.get("notes"),
        )
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

    # flatten JSON into list of shot dicts
    shots = []
    for rec in records:
        try:
            shots.extend(json.loads(rec.shot_type_details))
        except Exception:
            continue

    if not shots:
        return {}

    # find all shot classes present
    shot_classes = sorted({s.get('shot_class') for s in shots if s.get('shot_class')})

    totals = {}
    for cls in shot_classes:
        cls_shots = [s for s in shots if s.get('shot_class') == cls]
        sample = cls_shots[0] if cls_shots else {}
        prefix = f"{cls}_"
        subkeys = sorted(k for k in sample.keys() if k.startswith(prefix))
        # count each subkey
        totals[cls] = {key: sum(1 for s in cls_shots if s.get(key)) for key in subkeys}
    return totals


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

    shots = []
    for rec in records:
        try:
            shots.extend(json.loads(rec.shot_type_details))
        except Exception:
            continue

    if not shots:
        return {}

    shot_classes = sorted({s.get('shot_class') for s in shots if s.get('shot_class')})

    summaries = {}
    for cls in shot_classes:
        cls_shots = [s for s in shots if s.get('shot_class') == cls]
        count_cls = len(cls_shots)
        sample = cls_shots[0] if cls_shots else {}
        prefix = f"{cls}_"
        subkeys = sorted(k for k in sample.keys() if k.startswith(prefix))
        # percentage for each subkey
        summaries[cls] = {
            key: (sum(1 for s in cls_shots if s.get(key)) / count_cls * 100) if count_cls else 0
            for key in subkeys
        }
    return summaries


@recruits_bp.route('/<int:id>')
def detail_recruit(id):
    r = Recruit.query.get_or_404(id)
    shot_type_totals = compute_shot_type_totals_for_recruit(r)
    shot_summaries = compute_shot_summaries_for_recruit(r)
    return render_template(
        'recruits/detail.html',
        recruit=r,
        shot_type_totals=shot_type_totals,
        shot_summaries=shot_summaries,
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
        r.offer_status = request.form.get("offer_status")
        r.offer_date = date.fromisoformat(offer_date_str) if offer_date_str else None
        r.commit_date = date.fromisoformat(commit_date_str) if commit_date_str else None
        r.email = request.form.get("email")
        r.phone = request.form.get("phone")
        r.profile_image_url = request.form.get("profile_image_url")
        r.notes = request.form.get("notes")
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
