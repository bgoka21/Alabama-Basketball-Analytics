import importlib
import json

from werkzeug.security import generate_password_hash

import app as app_module
import routes
from models.database import db, Season, Roster, PlayerStats
from models.user import User


def test_player_shot_chart_aggregates_and_normalizes():
    app = app_module.create_app()
    app_module.app = app
    importlib.reload(routes)
    app.config["TESTING"] = True

    with app.app_context():
        db.drop_all()
        db.create_all()
        season = Season(id=1, season_name="2024")
        roster = Roster(id=1, season_id=1, player_name="#1 A")
        admin = User(
            username="admin",
            password_hash=generate_password_hash("pw"),
            is_admin=True,
        )
        shots = [
            {"shot_location": "Rim", "result": "made"},
            {"shot_location": "Right Wing", "result": "missed"},
            {"shot_location": "Right Wing", "result": "made"},
            {"shot_location": "Mystery"},
        ]
        stats = PlayerStats(
            season_id=1,
            player_name="#1 A",
            shot_type_details=json.dumps(shots),
        )
        db.session.add_all([season, roster, admin, stats])
        db.session.commit()

    client = app.test_client()
    client.post("/admin/login", data={"username": "admin", "password": "pw"})

    resp = client.get("/api/players/1/shot-chart?season=1&raw=1")
    assert resp.status_code == 200
    payload = resp.get_json()

    assert payload["zones"] == {
        "rim": 1,
        "wing_right": 2,
        "unknown": 1,
    }
    assert payload["raw"][0]["normalized_location"] == "rim"
    assert payload["raw"][1]["normalized_location"] == "wing_right"
    assert payload["raw"][3]["normalized_location"] == "unknown"


def test_player_shot_chart_filters_by_context_and_shot_fields():
    app = app_module.create_app()
    app_module.app = app
    importlib.reload(routes)
    app.config["TESTING"] = True

    with app.app_context():
        db.drop_all()
        db.create_all()
        season = Season(id=1, season_name="2024")
        roster = Roster(id=1, season_id=1, player_name="#1 A")
        admin = User(
            username="admin",
            password_hash=generate_password_hash("pw"),
            is_admin=True,
        )
        game_stats = PlayerStats(
            season_id=1,
            game_id=10,
            player_name="#1 A",
            shot_type_details=json.dumps(
                [
                    {"shot_location": "Rim", "shot_class": "2fg", "possession_type": "Halfcourt"},
                    {"shot_location": "Wing", "shot_class": "3fg", "possession_type": "Transition"},
                ]
            ),
        )
        practice_stats = PlayerStats(
            season_id=1,
            practice_id=22,
            player_name="#1 A",
            shot_type_details=json.dumps(
                [
                    {"shot_location": "Rim", "shot_class": "2fg", "possession_type": "Halfcourt"},
                    {"shot_location": "Corner", "shot_class": "3fg", "possession_type": "Halfcourt"},
                ]
            ),
        )
        db.session.add_all([season, roster, admin, game_stats, practice_stats])
        db.session.commit()

    client = app.test_client()
    client.post("/admin/login", data={"username": "admin", "password": "pw"})

    resp = client.get("/api/players/1/shot-chart?season=1&game=10&shot_class=2fg")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["zones"] == {"rim": 1}
    assert "raw" not in payload

    resp = client.get(
        "/api/players/1/shot-chart?season=1&practice=22&shot_class=3fg&possession_type=halfcourt"
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["zones"] == {"corner": 1}
