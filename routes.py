from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for

from models.database import db
from models.recruit import Recruit
from scrapers.espn_scraper import scrape_espn_stats
from scrapers.s247_scraper import scrape_247_stats

recruit_bp = Blueprint('recruit', __name__)


@recruit_bp.route('/recruiting')
def recruiting_home():
    recruits = Recruit.query.order_by(Recruit.last_updated.desc()).all()
    return render_template('recruiting.html', recruits=recruits)


@recruit_bp.route('/recruiting/add', methods=['GET', 'POST'])
def add_recruit():
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        position = request.form.get('position', '').strip() or None
        school = request.form.get('school', '').strip() or None
        espn_url = request.form.get('espn_url', '').strip()
        s247_url = request.form.get('s247_url', '').strip()

        espn_data = scrape_espn_stats(espn_url) if espn_url else {}
        s247_data = scrape_247_stats(s247_url) if s247_url else {}

        recruit = Recruit.query.filter_by(espn_url=espn_url).first()
        if not recruit:
            recruit = Recruit(espn_url=espn_url, s247_url=s247_url)
            db.session.add(recruit)

        recruit.name = name
        recruit.position = position
        recruit.school = school
        recruit.espn_url = espn_url
        recruit.s247_url = s247_url

        for key, val in {**espn_data, **s247_data}.items():
            setattr(recruit, key, val)

        recruit.last_updated = datetime.utcnow()
        db.session.commit()
        return redirect(url_for('recruit.recruiting_home'))

    return render_template('add_recruit.html')

