import json
import re
from datetime import date
from functools import cmp_to_key
from pathlib import Path

import pytest
from bs4 import BeautifulSoup
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import (
    db, Season, Practice, PlayerStats, Roster,
    Possession, PlayerPossession, ShotDetail
)
from utils.shottype import persist_player_shot_details
from models.user import User
from admin.routes import admin_bp
from utils.leaderboard_helpers import (
    get_bulk_on_off_summaries,
    get_bulk_turnover_rates_onfloor,
    get_bulk_rebound_rates_onfloor,
)


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
        practice = Practice(id=1, season_id=1, date=date(2024, 1, 2), category='Official')
        db.session.add(practice)
        roster = Roster(id=1, season_id=1, player_name='#1 Test')
        other = Roster(id=2, season_id=1, player_name='#2 Other')
        db.session.add_all([roster, other])
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        shots = [
            {"shot_class": "atr", "result": "made", "possession_type": "total", "drill_labels": ["4V4 DRILLS"]},
            {"shot_class": "atr", "result": "miss", "possession_type": "total", "drill_labels": ["4V4 DRILLS"]},
            {"shot_class": "2fg", "result": "made", "possession_type": "total", "drill_labels": ["4V4 DRILLS"]},
            {"shot_class": "2fg", "result": "made", "possession_type": "total", "drill_labels": ["4V4 DRILLS"]},
            {"shot_class": "3fg", "result": "made", "possession_type": "total", "drill_labels": ["4V4 DRILLS"]},
        ]
        player_stat = PlayerStats(
            practice_id=1,
            season_id=1,
            player_name='#1 Test',
            atr_attempts=2,
            atr_makes=1,
            fg2_attempts=2,
            fg2_makes=2,
            fg3_attempts=1,
            fg3_makes=1,
            turnovers=1,
            assists=3,
            pot_assists=2,
            bump_positive=3,
            bump_missed=1,
            shot_type_details=json.dumps(shots),
        )
        db.session.add(player_stat)
        persist_player_shot_details(player_stat, shots, replace=True)

        poss1 = Possession(id=1, practice_id=1, season_id=1, game_id=None,
                           possession_side='Offense', points_scored=3,
                           drill_labels='4V4 DRILLS')
        db.session.add(poss1)
        db.session.add(PlayerPossession(possession_id=1, player_id=1))
        db.session.add(ShotDetail(possession_id=1, event_type='3FG+'))
        db.session.add(ShotDetail(possession_id=1, event_type='Off Rebound'))

        poss2 = Possession(id=2, practice_id=1, season_id=1, game_id=None,
                           possession_side='Offense', points_scored=2,
                           drill_labels='4V4 DRILLS')
        db.session.add(poss2)
        db.session.add(PlayerPossession(possession_id=2, player_id=2))
        db.session.add(ShotDetail(possession_id=2, event_type='2FG+'))

        poss3 = Possession(id=3, practice_id=1, season_id=1, game_id=None,
                           possession_side='Offense', points_scored=2)
        db.session.add(poss3)
        db.session.add(PlayerPossession(possession_id=3, player_id=1))
        db.session.add(ShotDetail(possession_id=3, event_type='2FG+'))
        db.session.add(ShotDetail(possession_id=3, event_type='Turnover'))

        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_leaderboard_shows_frequency_stats(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'atr_freq_pct'})
    html = resp.data.decode('utf-8')
    assert 'ATR Frequency' in html
    assert '40.0%' in html

    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'fg3_freq_pct'})
    html = resp.data.decode('utf-8')
    assert '3FG Frequency' in html
    assert '20.0%' in html


def test_fg_pct_table_includes_frequency(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'atr_fg_pct'})
    html = resp.data.decode('utf-8')
    assert 'Freq' in html
    assert '40.0%' in html


def test_assist_summary_table(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'assist_summary'})
    html = resp.data.decode('utf-8')
    assert 'Assist/TO Stats' in html
    assert 'Adj AST/TO' in html


def test_on_court_offensive_metrics(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'ppp_on'})
    html = resp.data.decode('utf-8')
    assert 'PPP On' in html
    assert '2.5' in html

    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'efg_on'})
    html = resp.data.decode('utf-8')
    assert 'EFG%' in html
    assert '125.0%' in html


def test_offense_summary_table(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'offense_summary'})
    html = resp.data.decode('utf-8')
    assert 'Offense Stats' in html
    assert 'PPP On' in html
    assert 'Ind TO Rate (Poss.)' in html
    assert 'Team TO Rate' in html
    assert 'TO % (Bamalytics)' in html
    assert "% of TO's (NBA.com)" in html
    # turnovers=1, total_fga=5, pot_assists=2, assists=3 => 1/11*100=9.1
    assert '9.1' in html
    assert '100.0' in html


def test_offensive_metrics_filter(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'ppp_on'})
    html = resp.data.decode('utf-8')
    assert '2.5' in html  # total PPP On without filter

    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'ppp_on', 'label': '4V4 DRILLS'})
    html = resp.data.decode('utf-8')
    assert '3.0' in html  # filtered PPP On only scrimmage possessions


def test_defense_leaderboard(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'defense'})
    html = resp.data.decode('utf-8')
    assert 'Collisions' in html
    assert 'Bump +' in html
    assert 'Bump Opps' in html
    assert '75.0%' in html


def test_defense_leaderboard_sorts_by_pct_and_opps(client, app):
    """Players with equal percentages should be sorted by opportunities."""
    with app.app_context():
        # Add two players with identical bump percentages (50%) but different total opportunities
        db.session.add(PlayerStats(practice_id=1, season_id=1, player_name='#2 Other', bump_positive=1, bump_missed=1))
        db.session.add(Roster(id=3, season_id=1, player_name='#3 Tie'))
        db.session.add(PlayerStats(practice_id=1, season_id=1, player_name='#3 Tie', bump_positive=2, bump_missed=2))
        db.session.commit()

    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'defense'})
    html = resp.data.decode('utf-8')

    # '#3 Tie' has more opportunities than '#2 Other' but the same percentage.
    assert html.index('#1 Test') < html.index('#3 Tie') < html.index('#2 Other')


def test_defense_leaderboard_headers_have_data_col(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'defense'})
    soup = BeautifulSoup(resp.data, 'html.parser')

    header_keys = {th.get('data-key'): th for th in soup.select('th[data-key]')}
    assert 'totals_bump_plus' in header_keys
    assert 'totals_bump_opps' in header_keys
    assert 'totals_bump_pct' in header_keys
    assert 'last_bump_plus' in header_keys
    assert 'last_bump_opps' in header_keys
    assert 'last_bump_pct' in header_keys


def test_defense_leaderboard_team_totals_row(client, app):
    with app.app_context():
        db.session.add(PlayerStats(practice_id=1, season_id=1, player_name='#2 Other', bump_positive=2, bump_missed=2))
        db.session.commit()

    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'defense'})
    soup = BeautifulSoup(resp.data, 'html.parser')
    table = soup.find('table')
    assert table is not None
    default_sort = table.get('data-default-sort')
    assert default_sort == 'totals_bump_pct:desc;totals_bump_opps:desc;totals_bump_plus:desc;player:asc'

    tfoot = soup.find('tfoot')
    assert tfoot is not None
    total_row = tfoot.find('tr')
    assert total_row is not None
    total_cells = [c.get_text(strip=True) for c in total_row.find_all(['th', 'td'])]
    assert total_cells[1] == 'Team Totals'
    assert total_cells[2] == '5'  # bump_positive total
    assert total_cells[3] == '8'  # total opportunities
    assert total_cells[4] == '62.5%'


def test_bulk_leaderboard_helpers_return_consistent_values(app):
    with app.app_context():
        summaries = get_bulk_on_off_summaries([1, 2])
        assert summaries[1].offensive_possessions_on == 2
        assert summaries[1].ppp_on_offense == pytest.approx(2.5)
        assert summaries[2].offensive_possessions_on == 1
        assert summaries[2].ppp_on_offense == pytest.approx(2.0)

        turnovers = get_bulk_turnover_rates_onfloor([1, 2])
        assert turnovers[1]['team_turnover_rate_on'] == pytest.approx(50.0)
        assert turnovers[1]['indiv_turnover_rate'] == pytest.approx(50.0)
        assert turnovers[1]['bamalytics_turnover_rate'] == pytest.approx(9.1, rel=1e-2)
        assert turnovers[1]['individual_team_turnover_pct'] == pytest.approx(100.0)
        assert turnovers[2]['team_turnover_rate_on'] == pytest.approx(0.0)
        assert turnovers[2]['indiv_turnover_rate'] == pytest.approx(0.0)

        rebounds = get_bulk_rebound_rates_onfloor([1, 2])
        assert rebounds[1]['off_reb_rate_on'] == pytest.approx(0.0)
        assert rebounds[1]['def_reb_rate_on'] == pytest.approx(0.0)
        assert rebounds[2]['off_reb_rate_on'] == pytest.approx(0.0)
        assert rebounds[2]['def_reb_rate_on'] == pytest.approx(0.0)
