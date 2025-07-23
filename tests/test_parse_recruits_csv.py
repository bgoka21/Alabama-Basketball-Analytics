import json
import pytest
from flask import Flask
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.database import db
from models.recruit import Recruit, RecruitShotTypeStat
from models.user import User
from parse_recruits_csv import parse_recruits_csv


@pytest.fixture
def app(tmp_path):
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['TESTING'] = True
    db.init_app(app)
    with app.app_context():
        db.create_all()
        recruit = Recruit(id=1, name='John Doe')
        db.session.add(recruit)
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


def test_parse_recruits_csv(app, tmp_path):
    csv_path = tmp_path / 'recruit.csv'
    csv_content = (
        'Row,POSSESSION TYPE,Shot Location,John Doe\n'
        'John Doe,Halfcourt,Wing,"ATR+,3FG-"\n'
    )
    csv_path.write_text(csv_content)

    with app.app_context():
        stat = parse_recruits_csv(str(csv_path), recruit_id=1)
        assert stat.recruit_id == 1
        data = json.loads(stat.shot_type_details)
        assert len(data) == 2
        classes = {d['shot_class'] for d in data}
        assert {'atr', 'fg3'} <= classes
