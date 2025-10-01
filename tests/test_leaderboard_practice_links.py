from datetime import date

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash
from pathlib import Path

from admin.routes import admin_bp
from models.database import db, Season, Practice, PlayerStats, Roster
from models.user import User


@pytest.fixture
def app():
    template_root = Path(__file__).resolve().parents[1] / "templates"
    app = Flask(__name__, template_folder=str(template_root))
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["SECRET_KEY"] = "test"
    app.config["TESTING"] = True

    db.init_app(app)

    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = "admin.login"

    @login_manager.user_loader
    def load_user(user_id):  # pragma: no cover - simple test helper
        return db.session.get(User, int(user_id))

    app.register_blueprint(admin_bp, url_prefix="/admin")

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name="2024", start_date=date(2024, 1, 1))
        db.session.add(season)
        practice = Practice(id=1, season_id=1, date=date(2024, 1, 2), category="Official")
        db.session.add(practice)
        roster = Roster(id=1, season_id=1, player_name="#1 Test")
        db.session.add(roster)
        stats = PlayerStats(
            practice_id=1,
            season_id=1,
            player_name="#1 Test",
            points=5,
        )
        db.session.add(stats)
        admin_user = User(
            username="admin",
            password_hash=generate_password_hash("pw"),
            is_admin=True,
        )
        db.session.add(admin_user)
        db.session.commit()

        # Remove one optional practice endpoint to ensure the template skips it.
        app.view_functions.pop("admin.leaderboard_pass_contests", None)

    yield app

    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post("/admin/login", data={"username": "admin", "password": "pw"})
        yield client


def test_leaderboard_renders_without_optional_practice_endpoint(client):
    response = client.get(
        "/admin/leaderboard",
        query_string={"season_id": 1, "stat": "points"},
    )

    assert response.status_code == 200
