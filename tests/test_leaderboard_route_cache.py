from datetime import date

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from admin.routes import admin_bp
from models.database import CachedLeaderboard, Season, UploadedFile, db
from models.user import User
from services.cache_leaderboard import cache_build_one


def _dummy_builder(stat_key: str, season_id: int):
    return {
        "config": {"key": stat_key, "label": stat_key.title(), "format": "int"},
        "rows": [("#1 Example", 12)],
        "team_totals": {stat_key: 12},
    }


@pytest.fixture
def app(tmp_path):
    app = Flask(__name__)
    app.config.update(
        SQLALCHEMY_DATABASE_URI="sqlite:///:memory:",
        SECRET_KEY="test",
        TESTING=True,
    )
    upload_folder = tmp_path / "uploads"
    upload_folder.mkdir()
    app.config["UPLOAD_FOLDER"] = str(upload_folder)

    db.init_app(app)
    login_manager = LoginManager()
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):  # pragma: no cover - required by flask-login
        return db.session.get(User, int(user_id))

    app.register_blueprint(admin_bp, url_prefix="/admin")

    with app.app_context():
        db.create_all()
        db.session.add(Season(id=1, season_name="2024", start_date=date(2024, 1, 1)))
        db.session.add(
            User(
                username="admin",
                password_hash=generate_password_hash("pw"),
                is_admin=True,
            )
        )
        db.session.add(
            UploadedFile(
                id=1,
                season_id=1,
                filename="practice.csv",
                category="Official Practice",
                file_date=date(2024, 1, 1),
            )
        )
        db.session.commit()

    yield app

    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post("/admin/login", data={"username": "admin", "password": "pw"})
        yield client


def test_leaderboard_route_uses_cache_when_available(app, client, monkeypatch):
    with app.app_context():
        cache_build_one("points", 1, _dummy_builder)

    def fail(*args, **kwargs):
        raise AssertionError("cache_build_one should not be invoked for cache hits")

    monkeypatch.setattr("admin.routes.cache_build_one", fail)

    resp = client.get("/admin/leaderboard?season_id=1&stat=points")
    assert resp.status_code == 200
    assert "#1 Example" in resp.get_data(as_text=True)


def test_leaderboard_route_computes_when_cache_missing(app, client, monkeypatch):
    with app.app_context():
        db.session.query(CachedLeaderboard).delete()
        db.session.commit()

    monkeypatch.setattr("admin.routes.build_leaderboard_cache_payload", _dummy_builder)

    calls = []

    from services import cache_leaderboard as cache_module

    def wrapper(stat_key, season_id, compute_fn):
        calls.append((stat_key, season_id))
        return cache_module.cache_build_one(stat_key, season_id, compute_fn)

    monkeypatch.setattr("admin.routes.cache_build_one", wrapper)

    def fake_compute(stat_key, season_id, *args, **kwargs):
        result = _dummy_builder(stat_key, season_id)
        return result["config"], result["rows"], result["team_totals"]

    monkeypatch.setattr("admin.routes.compute_leaderboard", fake_compute)

    resp = client.get("/admin/leaderboard?season_id=1&stat=points")
    assert resp.status_code == 200
    assert calls == []

    with app.app_context():
        cached = CachedLeaderboard.query.filter_by(season_id=1, stat_key="points").first()
        assert cached is None
