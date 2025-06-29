from flask import Blueprint, render_template, request, redirect, url_for, flash
from datetime import datetime
from models.recruit import Recruit
from models.database import db
from clients.synergy_client import SynergyAPI
from scrapers.s247_scraper import scrape_247_stats

recruit_bp = Blueprint('recruit', __name__, url_prefix='/recruiting')

@recruit_bp.route('/')
def list_recruits():
    recs = Recruit.query.order_by(Recruit.last_updated.desc()).all()
    return render_template('recruiting.html', recruits=recs)

@recruit_bp.route('/add', methods=['GET','POST'])
def add_recruit():
    synergy = SynergyAPI()
    if request.method == 'POST' and request.form.get('action') == 'save':
        name     = request.form['name']
        school   = request.form.get('school')
        position = request.form.get('position')
        s247_url = request.form.get('s247_url')

        pid   = synergy.find_player_id(name)
        stats = synergy.get_player_stats(pid) if pid else {}
        s247  = scrape_247_stats(s247_url) if s247_url else {}

        rec = Recruit.query.filter_by(name=name).first() or Recruit(name=name)
        rec.school              = school
        rec.position            = position
        rec.synergy_player_id   = pid
        rec.off_rating          = stats.get('offensive_rating')
        rec.def_rating          = stats.get('defensive_rating')
        rec.minutes_played      = stats.get('minutes_played')
        rec.three_fg_pct        = stats.get('three_fg_pct')
        rec.ft_pct              = stats.get('ft_pct')
        rec.assists             = stats.get('assists')
        rec.turnovers           = stats.get('turnovers')
        rec.ast_to_to_ratio     = stats.get('ast_to_to_ratio')
        rec.s247_overall_rank   = s247.get('overall_rank')
        rec.s247_position_rank  = s247.get('position_rank')
        rec.last_updated        = datetime.utcnow()

        db.session.add(rec)
        db.session.commit()
        flash(f'Recruit “{name}” added/updated.', 'success')
        return redirect(url_for('recruit.list_recruits'))

    stats = None
    if request.method == 'POST' and request.form.get('action') == 'search':
        name = request.form['name']
        pid  = synergy.find_player_id(name)
        stats = synergy.get_player_stats(pid) if pid else {}
        if not pid:
            flash(f'No Synergy player found for "{name}".', 'danger')

    return render_template(
        'add_recruit.html',
        stats=stats,
        name=request.form.get('name', request.args.get('name','')),
        school=request.form.get('school', request.args.get('school','')),
        position=request.form.get('position', request.args.get('position','')),
        s247_url=request.form.get('s247_url', request.args.get('s247_url',''))
    )

@recruit_bp.route('/delete/<int:id>', methods=['POST'])
def delete_recruit(id):
    rec = Recruit.query.get_or_404(id)
    db.session.delete(rec)
    db.session.commit()
    flash(f'Recruit “{rec.name}” deleted.', 'warning')
    return redirect(url_for('recruit.list_recruits'))
