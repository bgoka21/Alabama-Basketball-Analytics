import json
from contextlib import contextmanager
from datetime import date
import pytest
from flask import Flask
from flask.signals import template_rendered
from flask_login import LoginManager
from werkzeug.security import generate_password_hash
from pathlib import Path
from bs4 import BeautifulSoup

import admin.routes as admin_routes
from models.database import db, Season, Session, Practice, PlayerStats, Roster
from models.user import User
from admin.routes import admin_bp
from utils.shottype import persist_player_shot_details

@pytest.fixture
def app():
    template_root = Path(__file__).resolve().parents[1] / 'templates'
    app = Flask(__name__, template_folder=str(template_root))
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SECRET_KEY'] = 'test'
    app.config['TESTING'] = True
    db.init_app(app)

    lm = LoginManager()
    lm.init_app(app)
    lm.login_view = 'admin.login'

    @lm.user_loader
    def load_user(uid):
        return db.session.get(User, int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add(season)
        session_record = Session(
            id=1,
            season_id=1,
            name='Official Practice',
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
        )
        db.session.add(session_record)
        pr1 = Practice(id=1, season_id=1, date=date(2024, 1, 2), category='Official')
        pr2 = Practice(id=2, season_id=1, date=date(2024, 1, 5), category='Official')
        db.session.add_all([pr1, pr2])
        roster = Roster(id=1, season_id=1, player_name='#1 Test')
        db.session.add(roster)
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        shots1 = [{'shot_class':'2fg','result':'made','drill_labels':['SCRIMMAGE']}]
        shots2 = [{'shot_class':'2fg','result':'made','drill_labels':['4V4 DRILLS']}]
        ps1 = PlayerStats(practice_id=1, season_id=1, player_name='#1 Test', points=5, shot_type_details=json.dumps(shots1))
        db.session.add(ps1)
        persist_player_shot_details(ps1, shots1, replace=True)
        ps2 = PlayerStats(practice_id=2, season_id=1, player_name='#1 Test', points=7, shot_type_details=json.dumps(shots2))
        db.session.add(ps2)
        persist_player_shot_details(ps2, shots2, replace=True)
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()

@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username':'admin','password':'pw'})
        yield client


@contextmanager
def captured_templates(app):
    recorded = []

    def record(sender, template, context, **extra):
        recorded.append((template, context))

    template_rendered.connect(record, app)
    try:
        yield recorded
    finally:
        template_rendered.disconnect(record, app)

def _points_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    if not table:
        return 0

    for row in table.select('tbody tr'):
        player_cell = row.find('td', {'data-key': 'player'})
        if not player_cell:
            continue
        if player_cell.get_text(strip=True) != '#1 Test':
            continue
        value_cell = row.find('td', {'data-key': 'points'})
        if not value_cell:
            continue
        text = value_cell.get_text(strip=True)
        try:
            return int(float(text))
        except ValueError:
            return 0
    return 0

def test_leaderboard_label_filter(client, app):
    with captured_templates(app) as templates:
        resp = client.get('/admin/leaderboard', query_string={'season_id':1, 'stat':'points'})
        html = resp.data.decode('utf-8')

    assert resp.status_code == 200
    assert _points_from_html(html) == 12

    selected_context = None
    for template, context in templates:
        if template.name == 'admin/leaderboard.html':
            selected_context = context
            break

    assert selected_context is not None
    assert selected_context['selected_session'] == 'Official Practice'
    assert 'Official Practice' in selected_context['sessions']

    resp = client.get('/admin/leaderboard', query_string={'season_id':1, 'stat':'points', 'label':'4V4 DRILLS'})
    html = resp.data.decode('utf-8')
    assert _points_from_html(html) == 7


def test_dual_leaderboard_defaults_official_session(app, monkeypatch):
    captured = {}

    def fake_render(template_name, **ctx):
        captured.update(ctx)
        return 'ok'

    monkeypatch.setattr(admin_routes, 'render_template', fake_render)
    monkeypatch.setattr(admin_routes, 'build_dual_context', lambda **kwargs: {})
    monkeypatch.setattr(admin_routes, 'prepare_dual_context', lambda ctx, stat_key: ctx)
    monkeypatch.setattr(
        admin_routes,
        'resolve_scope',
        lambda args, season_id, session_range=None: ('season', None, None),
    )

    with app.test_request_context('/admin/leaderboard/fake?season_id=1'):
        admin_routes._render_dual_leaderboard(
            'fake.html',
            page_title='Fake',
            compute_fn=lambda **kwargs: ({}, []),
            stat_key='fake',
        )

    assert captured['selected_session'] == 'Official Practice'
    assert 'Official Practice' in captured['sessions']
