import json
from pathlib import Path
import pytest
from flask import Flask
from flask_login import LoginManager
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.database import db
from models.recruit import Recruit, RecruitShotTypeStat, RecruitTopSchool
from models.user import User
from recruits import recruits_bp
from admin.routes import admin_bp


@pytest.fixture
def app(tmp_path):
    template_root = Path(__file__).resolve().parents[1] / 'templates'
    app = Flask(__name__, template_folder=str(template_root))
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SECRET_KEY'] = 'test'
    app.config['TESTING'] = True
    app.config['UPLOAD_FOLDER'] = str(tmp_path)
    db.init_app(app)
    lm = LoginManager()
    lm.init_app(app)
    lm.login_view = 'admin.login'

    @lm.user_loader
    def load_user(uid):
        return db.session.get(User, int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(recruits_bp, url_prefix='/recruits')
    app.jinja_env.globals['view_exists'] = lambda n: n in app.view_functions
    class DummyUser:
        is_authenticated = False
        is_player = False
    app.jinja_env.globals['current_user'] = DummyUser()
    with app.app_context():
        db.create_all()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    return app.test_client()


def test_create_upload_and_delete_school(client, app, tmp_path):
    # create recruit
    resp = client.post('/recruits/new', data={'name': 'John Doe'})
    assert resp.status_code == 302
    with app.app_context():
        recruit = Recruit.query.filter_by(name='John Doe').first()
        rid = recruit.id

    # upload csv
    csv_path = tmp_path / 'r.csv'
    csv_path.write_text('Row,POSSESSION TYPE,Shot Location,John Doe\nJohn Doe,Halfcourt,Wing,"ATR+"\n')
    with csv_path.open('rb') as f:
        resp = client.post(f'/recruits/{rid}/upload', data={'csv_file': (f, 'r.csv')})
    assert resp.status_code == 302
    with app.app_context():
        stat = RecruitShotTypeStat.query.filter_by(recruit_id=rid).first()
        assert stat is not None

    # add school
    resp = client.post(f'/recruits/{rid}/top_schools', data={'school_name': 'Alabama', 'rank': 1})
    assert resp.status_code == 302
    with app.app_context():
        school = RecruitTopSchool.query.filter_by(recruit_id=rid).first()
        sid = school.id
        assert school.school_name == 'Alabama'

    # delete school
    resp = client.post(f'/recruits/{rid}/top_schools/{sid}/delete')
    assert resp.status_code == 302
    with app.app_context():
        assert RecruitTopSchool.query.get(sid) is None


def test_blank_numeric_fields(client, app):
    resp = client.post('/recruits/new', data={
        'name': 'Jane Doe',
        'graduation_year': '',
        'weight': '',
        'rating': '',
        'ranking': '',
    })
    assert resp.status_code == 302
    with app.app_context():
        recruit = Recruit.query.filter_by(name='Jane Doe').first()
        assert recruit is not None
        assert recruit.graduation_year is None
        assert recruit.weight is None
        assert recruit.rating is None
        assert recruit.ranking is None

def test_edit_and_delete_recruit(client, app):
    # create recruit
    resp = client.post('/recruits/new', data={'name': 'Edit Me', 'graduation_year': '2024'})
    assert resp.status_code == 302
    with app.app_context():
        rec = Recruit.query.filter_by(name='Edit Me').first()
        rid = rec.id

    # GET edit form
    resp = client.get(f'/recruits/{rid}/edit')
    assert resp.status_code == 200

    # POST edit
    resp = client.post(f'/recruits/{rid}/edit', data={'name': 'Edited', 'graduation_year': '2025'})
    assert resp.status_code == 302
    with app.app_context():
        rec = Recruit.query.get(rid)
        assert rec.name == 'Edited'
        assert rec.graduation_year == 2025

    # delete
    resp = client.post(f'/recruits/{rid}/delete')
    assert resp.status_code == 302
    with app.app_context():
        assert Recruit.query.get(rid) is None
