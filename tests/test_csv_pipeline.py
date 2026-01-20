from io import BytesIO

import pandas as pd
import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from admin.routes import admin_bp
from app.csv_pipeline.routes import csv_pipeline_bp
from models.database import db
from models.user import User


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SECRET_KEY="test-secret",
        SQLALCHEMY_DATABASE_URI="sqlite:///:memory:",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        WTF_CSRF_ENABLED=False,
    )

    db.init_app(app)

    login_manager = LoginManager()
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(csv_pipeline_bp, url_prefix="/management")

    with app.app_context():
        db.create_all()
        if not db.session.get(User, 1):
            user = User(
                id=1,
                username="coach",
                password_hash=generate_password_hash("pw"),
                is_admin=True,
            )
            db.session.add(user)
            db.session.commit()

    yield app

    with app.app_context():
        db.session.remove()
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        with client.session_transaction() as session:
            session["_user_id"] = "1"
            session["_fresh"] = True
        yield client


def _csv_file(df: pd.DataFrame, name: str):
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return BytesIO(csv_bytes), name


def _base_pre_combined():
    return pd.DataFrame(
        {
            "Row": [
                "Player",
                "Offense",
                "Defense",
                "PnR",
                "Offense Rebound Opportunities",
                "Defense Rebound Opportunities",
            ],
            "Timeline": ["T1", "T2", "T3", "T4", "T5", "T6"],
            "#1": ["keep-player", "", "", "", "", ""],
            "Misc": ["keep", "", "", "", "", ""],
        }
    )


def _offense_inputs():
    shot_type = pd.DataFrame(
        {
            "Row": ["Offense", "Offense"],
            "Timeline": ["O1", "O2"],
            "#1": ["A", "B"],
            "Shot Type": ["Type1", "Type2"],
        }
    )
    shot_creation = pd.DataFrame(
        {
            "Row": ["Offense", "Offense"],
            "Timeline": ["O1", "O2"],
            "Shot Creation": ["Create1", "Create2"],
        }
    )
    turnover_type = pd.DataFrame(
        {
            "Row": ["Offense", "Offense"],
            "Timeline": ["O1", "O2"],
            "TO Type": ["TO1", "TO2"],
        }
    )
    return shot_type, shot_creation, turnover_type


def _defense_inputs():
    defensive_possessions = pd.DataFrame(
        {
            "Row": ["Defense", "Defense"],
            "Timeline": ["D1", "D2"],
            "#1": ["Base", ""],
            "Def Poss": ["DP1", "DP2"],
        }
    )
    gap_help = pd.DataFrame(
        {
            "Row": ["Defense", "Defense"],
            "Timeline": ["D1", "D2"],
            "#1": ["Gap", "Help"],
            "Gap Help": ["drop", "over"],
        }
    )
    shot_contest = pd.DataFrame(
        {
            "Row": ["Defense", "Defense"],
            "Timeline": ["D1", "D2"],
            "#1": ["Contest", ""],
        }
    )
    pass_contest = pd.DataFrame(
        {
            "Row": ["Defense", "Defense"],
            "Timeline": ["D1", "D2"],
            "#1": ["Pass", ""],
        }
    )
    return defensive_possessions, gap_help, shot_contest, pass_contest


def _pnr_inputs():
    gap_help = pd.DataFrame(
        {
            "Row": ["PnR", "PnR"],
            "Timeline": ["P1", "P2"],
            "#1": ["Gap", ""],
            "Ignore": ["X", "Y"],
        }
    )
    grade = pd.DataFrame(
        {
            "Row": ["PnR", "PnR"],
            "Timeline": ["P1", "P2"],
            "#1": ["Grade", ""],
        }
    )
    return gap_help, grade


def _rebound_inputs():
    offense = pd.DataFrame(
        {
            "Row": ["Offense Rebound Opportunities"],
            "Timeline": ["R1"],
            "#1": ["ORB"],
        }
    )
    defense = pd.DataFrame(
        {
            "Row": ["Defense Rebound Opportunities"],
            "Timeline": ["R2"],
            "#1": ["DRB"],
        }
    )
    return offense, defense


def _post_payload(pre_combined):
    shot_type, shot_creation, turnover_type = _offense_inputs()
    defense_possessions, gap_help, shot_contest, pass_contest = _defense_inputs()
    pnr_gap_help, pnr_grade = _pnr_inputs()
    off_reb, def_reb = _rebound_inputs()

    return {
        "pre_combined": _csv_file(pre_combined, "pre.csv"),
        "offense_shot_type": _csv_file(shot_type, "offense_shot_type.csv"),
        "offense_shot_creation": _csv_file(shot_creation, "offense_shot_creation.csv"),
        "offense_turnover_type": _csv_file(turnover_type, "offense_turnover.csv"),
        "defense_possessions": _csv_file(defense_possessions, "defense_possessions.csv"),
        "defense_gap_help": _csv_file(gap_help, "defense_gap_help.csv"),
        "defense_shot_contest": _csv_file(shot_contest, "defense_shot_contest.csv"),
        "defense_pass_contest": _csv_file(pass_contest, "defense_pass_contest.csv"),
        "pnr_gap_help": _csv_file(pnr_gap_help, "pnr_gap_help.csv"),
        "pnr_grade": _csv_file(pnr_grade, "pnr_grade.csv"),
        "offense_rebound": _csv_file(off_reb, "offense_rebound.csv"),
        "defense_rebound": _csv_file(def_reb, "defense_rebound.csv"),
    }


def test_csv_pipeline_happy_path(client):
    pre_combined = _base_pre_combined()
    data = _post_payload(pre_combined)

    resp = client.post(
        "/management/csv-pipeline",
        data=data,
        content_type="multipart/form-data",
    )

    assert resp.status_code == 200
    assert resp.mimetype == "text/csv"

    output = pd.read_csv(BytesIO(resp.data))
    offense_rows = output[output["Row"] == "Offense"]
    assert "Shot Type" in offense_rows.columns
    assert "Shot Creation" in offense_rows.columns
    assert "TO Type" in offense_rows.columns


def test_csv_pipeline_missing_row_column(client):
    pre_combined = pd.DataFrame({"Timeline": ["T1"], "#1": ["keep"]})
    data = _post_payload(_base_pre_combined())
    data["pre_combined"] = _csv_file(pre_combined, "pre.csv")

    resp = client.post(
        "/management/csv-pipeline",
        data=data,
        content_type="multipart/form-data",
    )

    assert resp.status_code == 400
    assert "missing required" in resp.data.decode("utf-8")


def test_csv_pipeline_row_count_mismatch(client):
    pre_combined = _base_pre_combined()
    shot_type, shot_creation, turnover_type = _offense_inputs()
    shot_creation = shot_creation.iloc[:1].copy()

    data = _post_payload(pre_combined)
    data["offense_shot_creation"] = _csv_file(shot_creation, "offense_shot_creation.csv")

    resp = client.post(
        "/management/csv-pipeline",
        data=data,
        content_type="multipart/form-data",
    )

    assert resp.status_code == 400
    assert "row count mismatch" in resp.data.decode("utf-8")


def test_csv_pipeline_preserves_player_rows(client):
    pre_combined = _base_pre_combined()
    data = _post_payload(pre_combined)

    resp = client.post(
        "/management/csv-pipeline",
        data=data,
        content_type="multipart/form-data",
    )

    assert resp.status_code == 200
    output = pd.read_csv(BytesIO(resp.data))
    player_row = output[output["Row"] == "Player"].iloc[0]
    assert player_row["#1"] == "keep-player"
    assert player_row["Misc"] == "keep"
