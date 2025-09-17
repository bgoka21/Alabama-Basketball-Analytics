from datetime import date
from pathlib import Path

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from admin.routes import admin_bp
from models.database import (
    db,
    Season,
    Roster,
    PlayerStats,
    PlayerDevelopmentPlan,
    SkillEntry,
)
from models.user import User


@pytest.fixture
def app():
    template_root = Path(__file__).resolve().parents[1] / 'templates'
    app = Flask(__name__, template_folder=str(template_root))
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SECRET_KEY'] = 'test'
    app.config['TESTING'] = True

    db.init_app(app)
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'admin.login'

    @login_manager.user_loader
    def load_user(uid):
        return db.session.get(User, int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')

    with app.app_context():
        db.create_all()

        primary_season = Season(id=1, season_name='2023-24', start_date=date(2023, 11, 1))
        other_season = Season(id=2, season_name='2024-25', start_date=date(2024, 11, 1))
        db.session.add_all([primary_season, other_season])

        primary_roster = Roster(id=1, season_id=primary_season.id, player_name='Old Name')
        other_roster = Roster(id=2, season_id=other_season.id, player_name='Old Name')
        db.session.add_all([primary_roster, other_roster])

        db.session.add(PlayerStats(season_id=primary_season.id, player_name='Old Name', points=5))
        db.session.add(PlayerStats(season_id=other_season.id, player_name='Old Name', points=7))

        db.session.add(
            PlayerDevelopmentPlan(season_id=primary_season.id, player_name='Old Name', stat_1_name='PTS')
        )
        db.session.add(
            PlayerDevelopmentPlan(season_id=other_season.id, player_name='Old Name', stat_1_name='REB')
        )

        db.session.add(
            SkillEntry(player_id=primary_roster.id, date=date(2023, 11, 2), skill_name='NBA 100', value=75)
        )

        admin_user = User(
            username='admin',
            password_hash=generate_password_hash('pw'),
            is_admin=True,
        )
        linked_user = User(
            username='player',
            password_hash=generate_password_hash('pw'),
            is_player=True,
            player_name='Old Name',
        )
        db.session.add_all([admin_user, linked_user])

        db.session.commit()

    yield app

    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_roster_rename_propagates_to_related_tables(client, app):
    response = client.post('/admin/roster/1/rename', data={'new_name': 'New Name'})
    assert response.status_code == 302

    with app.app_context():
        updated_roster = db.session.get(Roster, 1)
        assert updated_roster.player_name == 'New Name'

        secondary_roster = db.session.get(Roster, 2)
        assert secondary_roster.player_name == 'Old Name'

        season_one_stats = PlayerStats.query.filter_by(season_id=1, player_name='New Name').all()
        assert len(season_one_stats) == 1
        assert PlayerStats.query.filter_by(season_id=1, player_name='Old Name').count() == 0

        season_two_stats = PlayerStats.query.filter_by(season_id=2, player_name='Old Name').all()
        assert len(season_two_stats) == 1

        plan = PlayerDevelopmentPlan.query.filter_by(season_id=1, player_name='New Name').one()
        assert plan.stat_1_name == 'PTS'
        assert (
            PlayerDevelopmentPlan.query.filter_by(season_id=2, player_name='Old Name').count() == 1
        )

        user = User.query.filter_by(username='player').one()
        assert user.player_name == 'New Name'

        skill_entry = SkillEntry.query.filter_by(player_id=1).one()
        assert skill_entry.player_id == 1
        assert skill_entry.player.player_name == 'New Name'
