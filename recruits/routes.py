import os
import json
from flask import render_template, request, redirect, url_for, flash, current_app
from datetime import date
from werkzeug.utils import secure_filename
from flask_login import current_user
from yourapp import db
from models.recruit import Recruit, RecruitShotTypeStat, RecruitTopSchool
from parse_recruits_csv import parse_recruits_csv
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
        db.session.commit()
        return redirect(url_for('recruits.list_recruits'))
    return render_template('recruits/new.html')


@recruits_bp.route('/<int:id>')
def detail_recruit(id):
    r = Recruit.query.get_or_404(id)
    latest_stat = (
        RecruitShotTypeStat.query.filter_by(recruit_id=id)
        .order_by(RecruitShotTypeStat.created_at.desc())
        .first()
    )
    shot_data = []
    totals = {
        'atr': {'makes': 0, 'attempts': 0},
        'fg2': {'makes': 0, 'attempts': 0},
        'fg3': {'makes': 0, 'attempts': 0},
    }
    if latest_stat:
        shot_data = json.loads(latest_stat.shot_type_details)
        for s in shot_data:
            cls = s.get('shot_class')
            if cls not in totals:
                continue
            totals[cls]['attempts'] += 1
            if s.get('result') == 'made':
                totals[cls]['makes'] += 1
    for v in totals.values():
        if v['attempts']:
            v['pct'] = v['makes'] / v['attempts'] * 100
        else:
            v['pct'] = 0
    return render_template('recruits/detail.html', recruit=r, stat=latest_stat, shot_data=shot_data, totals=totals)


@recruits_bp.route('/<int:id>/upload', methods=['POST'])
def upload_csv(id):
    file = request.files['csv_file']
    filename = secure_filename(file.filename)
    path = os.path.join(current_app.config['UPLOAD_FOLDER'], 'recruits', filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    file.save(path)
    parse_recruits_csv(path, id)
    flash('Shot-type CSV parsed and saved.', 'success')
    return redirect(url_for('recruits.detail_recruit', id=id))


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
