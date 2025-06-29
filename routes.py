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
        synergy_player_id = request.form.get('synergy_player_id')
        off_rating = float(request.form.get('off_rating') or 0.0)
        def_rating = float(request.form.get('def_rating') or 0.0)
        minutes_played = float(request.form.get('minutes_played') or 0.0)

        # Check for existing recruit to avoid IntegrityError
        if Recruit.query.filter(func.lower(Recruit.name) == name.lower()).first():
            error = 'Recruit already exists.'
            return render_template('add_recruit.html', error=error)

        rec = Recruit(
            name=name,
            synergy_player_id=synergy_player_id,
            off_rating=off_rating,
            def_rating=def_rating,
            minutes_played=minutes_played,
            last_updated=datetime.utcnow(),
        )
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
