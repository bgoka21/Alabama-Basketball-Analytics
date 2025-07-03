from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from datetime import datetime
from models.recruit import Recruit
from models.database import db
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
from synergy_client import SynergyClient

synergy = SynergyClient()


recruiting_bp = Blueprint('recruiting', __name__, url_prefix='/recruiting')
# maintain old name for backward compatibility
recruit_bp = recruiting_bp

@recruiting_bp.route('/')
def list_recruits():
    recs = Recruit.query.order_by(Recruit.last_updated.desc()).all()
    return render_template('recruiting.html', recruits=recs, active_page='recruits')


@recruiting_bp.route('/search_synergy', methods=['POST'])
def search_synergy():
    query = request.form['query']
    results = synergy.search(query)
    return jsonify(results)

@recruiting_bp.route('/add', methods=['GET', 'POST'])
def add_recruit():
    if request.method == 'POST':
        name = request.form['name'].strip()
        position = request.form.get('position') or ''
        school = request.form.get('school') or ''
        s247_url = request.form.get('s247_url')
        espn_url = request.form.get('espn_url')
        synergy_player_id = request.form.get('synergy_player_id')

        rec = Recruit(
            name=name,
            position=position,
            school=school,
            s247_url=s247_url,
            espn_url=espn_url,
            synergy_player_id=synergy_player_id,
        )

        if rec.synergy_player_id:
            stats = synergy.get_player_stats(rec.synergy_player_id)
            rec.off_rating      = stats.get('off_rating')
            rec.def_rating      = stats.get('def_rating')
            rec.minutes_played  = stats.get('minutes_played')
            rec.three_fg_pct    = stats.get('three_fg_pct')
            rec.ft_pct          = stats.get('ft_pct')
            rec.assists         = stats.get('assists')
            rec.turnovers       = stats.get('turnovers')
            rec.ast_to_to_ratio = stats.get('ast_to_to_ratio')

        db.session.add(rec)
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            error = 'Recruit already exists.'
            return render_template('add_recruit.html', error=error)

        return redirect(url_for('recruiting.list_recruits'))

    return render_template('add_recruit.html')

@recruiting_bp.route('/<int:id>/delete', methods=['POST'])
def delete_recruit(id):
    r = Recruit.query.get_or_404(id)
    db.session.delete(r)
    db.session.commit()
    return redirect(url_for('recruiting.list_recruits'))


@recruiting_bp.route('/<int:id>/url', methods=['POST'])
def update_recruit_url(id):
    r = Recruit.query.get_or_404(id)
    r.s247_url = request.form.get('s247_url')
    r.espn_url = request.form.get('espn_url')
    db.session.commit()
    return redirect(url_for('recruiting.list_recruits'))

@recruiting_bp.route('/<int:id>/update_synergy', methods=['POST'])
def update_synergy(id):
    recruit = Recruit.query.get_or_404(id)
    recruit.synergy_player_id = request.form['synergy_player_id']
    db.session.commit()
    return redirect(url_for('recruiting.list_recruits'))


# ---- API Endpoints ----

@recruiting_bp.route('/api/recruits', methods=['GET', 'POST'])
def recruits_api():
    """List recruits or add a new recruit via JSON."""
    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        name = (data.get('name') or '').strip()
        if not name:
            return jsonify({'error': 'name required'}), 400
        rec = Recruit(name=name,
                      position=data.get('position') or '',
                      school=data.get('school') or '')
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
