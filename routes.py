from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from datetime import datetime
from models.recruit import Recruit
from models.database import db
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func


recruit_bp = Blueprint('recruit', __name__, url_prefix='/recruiting')

@recruit_bp.route('/')
def list_recruits():
    recs = Recruit.query.order_by(Recruit.last_updated.desc()).all()
    return render_template('recruiting.html', recruits=recs)


@recruit_bp.route('/search_synergy')
def search_synergy():
    q = request.args.get('q', '')
    dummy = [
        {'id': '123', 'name': 'John Doe', 'off_rating': 75.2, 'def_rating': 82.1, 'mpg': 32.5},
        {'id': '456', 'name': 'Jane Smith', 'off_rating': 68.4, 'def_rating': 70.3, 'mpg': 29.0},
    ]
    results = [p for p in dummy if q.lower() in p['name'].lower()]
    return jsonify(results)

@recruit_bp.route('/add', methods=['GET', 'POST'])
def add_recruit():
    if request.method == 'POST':
        name = request.form['name'].strip()
        position = request.form.get('position')
        school = request.form.get('school')

        rec = Recruit(name=name, position=position, school=school)
        db.session.add(rec)
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            error = 'Recruit already exists.'
            return render_template('add_recruit.html', error=error)

        return redirect(url_for('recruit.list_recruits'))

    return render_template('add_recruit.html')

@recruit_bp.route('/<int:id>/delete', methods=['POST'])
def delete_recruit(id):
    r = Recruit.query.get_or_404(id)
    db.session.delete(r)
    db.session.commit()
    return redirect(url_for('recruit.list_recruits'))


@recruit_bp.route('/<int:id>/url', methods=['POST'])
def update_recruit_url(id):
    r = Recruit.query.get_or_404(id)
    r.s247_url = request.form['s247_url']
    db.session.commit()
    return redirect(url_for('recruit.list_recruits'))


# ---- API Endpoints ----

@recruit_bp.route('/api/recruits', methods=['GET', 'POST'])
def recruits_api():
    """List recruits or add a new recruit via JSON."""
    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        name = (data.get('name') or '').strip()
        if not name:
            return jsonify({'error': 'name required'}), 400
        rec = Recruit(name=name,
                      position=data.get('position'),
                      school=data.get('school'))
        db.session.add(rec)
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            return jsonify({'error': 'Recruit already exists.'}), 400
        return jsonify({'id': rec.id,
                        'name': rec.name,
                        'position': rec.position,
                        'school': rec.school}), 201

    recs = Recruit.query.order_by(Recruit.last_updated.desc()).all()
    return jsonify([
        {
            'id': r.id,
            'name': r.name,
            'position': r.position,
            'school': r.school,
        }
        for r in recs
    ])
