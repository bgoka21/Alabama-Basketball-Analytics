import pytest
from datetime import date
from flask import Flask

from models.database import db, Season, Practice, Roster, Possession, PlayerPossession
from parse_practice_csv import parse_practice_csv


@pytest.fixture
def app(tmp_path):
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['TESTING'] = True
    db.init_app(app)
    with app.app_context():
        db.create_all()
    yield app
    with app.app_context():
        db.drop_all()


def test_practice_possession_records(app, tmp_path):
    csv_path = tmp_path / "practice.csv"
    practice_date = date(2024, 1, 6)

    with app.app_context():
        season = Season(id=1, season_name='2024', start_date=practice_date)
        db.session.add(season)
        r1 = Roster(season_id=1, player_name='#1 A')
        r2 = Roster(season_id=1, player_name='#2 B')
        db.session.add_all([r1, r2])
        practice = Practice(id=1, season_id=1, date=practice_date, category='Official Practice')
        db.session.add(practice)
        db.session.commit()
        r1_id = r1.id
        r2_id = r2.id

    csv_content = "Row,CRIMSON PLAYER POSSESSIONS,WHITE PLAYER POSSESSIONS,#1 A,#2 B\n"
    csv_content += "Crimson,\"#1 A\",\"#2 B\",2FG+,\n"
    csv_path.write_text(csv_content)

    with app.app_context():
        parse_practice_csv(str(csv_path), season_id=1, category='Official Practice', file_date=practice_date)
        poss = Possession.query.order_by(Possession.id).all()
        assert len(poss) == 2
        sides = {p.possession_side for p in poss}
        assert sides == {'Offense', 'Defense'}
        pts = {p.possession_side: p.points_scored for p in poss}
        assert pts['Offense'] == 2
        assert pts['Defense'] == 2
        pp_counts = PlayerPossession.query.count()
        assert pp_counts == 2
        crimson_poss = next(p for p in poss if p.possession_side == 'Offense')
        white_poss = next(p for p in poss if p.possession_side == 'Defense')
        crimson_pp = PlayerPossession.query.filter_by(possession_id=crimson_poss.id).first()
        white_pp = PlayerPossession.query.filter_by(possession_id=white_poss.id).first()
        assert crimson_pp.player_id == r1_id
        assert white_pp.player_id == r2_id
