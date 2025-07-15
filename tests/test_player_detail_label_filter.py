import json
from datetime import date
import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db, Season, Practice, PlayerStats, Roster
from models.user import User
from admin.routes import admin_bp


@pytest.fixture
def app():
    app = Flask(__name__)
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
        season = Season(id=1, season_name='2024', start_date=date(2024,1,1))
        db.session.add(season)
        practice = Practice(id=1, season_id=1, date=date(2024,1,2), category='Official Practices')
        db.session.add(practice)
        roster = Roster(season_id=1, player_name='#1 Test')
        db.session.add(roster)
        admin_user = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin_user)
        shots = [
            {"shot_class":"atr","result":"made","possession_type":"SCRIMMAGE","drill_labels":["SCRIMMAGE"]},
            {"shot_class":"2fg","result":"miss","possession_type":"SCRIMMAGE","drill_labels":["SCRIMMAGE"]},
            {"shot_class":"3fg","result":"made","possession_type":"SCRIMMAGE","drill_labels":["SCRIMMAGE"]},
            {"shot_class":"atr","result":"miss","possession_type":"4V4 DRILLS","drill_labels":["4V4 DRILLS"]},
            {"shot_class":"3fg","result":"made","possession_type":"4V4 DRILLS","drill_labels":["4V4 DRILLS"]},
        ]
        db.session.add(PlayerStats(
            practice_id=1,
            season_id=1,
            player_name='#1 Test',
            atr_makes=0,
            atr_attempts=0,
            fg2_makes=0,
            fg2_attempts=0,
            fg3_makes=0,
            fg3_attempts=0,
            points=0,
            assists=0,
            turnovers=0,
            second_assists=0,
            pot_assists=0,
            fta=0,
            ftm=0,
            shot_type_details=json.dumps(shots)
        ))
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username':'admin','password':'pw'})
        yield client


def test_label_filter_updates_metrics(client):
    resp = client.get('/admin/player/%231%20Test', query_string={'mode':'practice', 'label':'4V4 DRILLS'})
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert '1.5' in html
    assert '75.0' in html
