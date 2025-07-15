import json
from io import BytesIO
import os

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db
from models.user import User

from merge_app import app as merge_module
from admin.routes import admin_bp


@pytest.fixture
def client(tmp_path):
    merge_module.MERGE_CONFIG_PATH = os.path.join(tmp_path, "merge_config.json")
    with open(merge_module.MERGE_CONFIG_PATH, "w") as f:
        json.dump({"files": {}, "on": [], "how": "inner"}, f)

    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["TESTING"] = True
    app.secret_key = "test"
    db.init_app(app)
    lm = LoginManager(); lm.init_app(app)
    lm.login_view = 'admin.login'

    @lm.user_loader
    def load_user(uid):
        return db.session.get(User, int(uid))

    app.register_blueprint(merge_module.merge_bp, url_prefix="/merge")
    app.register_blueprint(admin_bp, url_prefix="/admin")

    with app.app_context():
        db.create_all()
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        db.session.commit()

    with app.test_client() as client:
        client.post('/admin/login', data={'username':'admin','password':'pw'})
        yield client
    with app.app_context():
        db.drop_all()


def test_multi_file_merge(client):
    config = {
        "files": {
            "file1": ["A", "B"],
            "file2": ["B", "C"],
            "file3": ["B", "D"],
        },
        "on": ["B"],
        "how": "inner",
    }
    with open(merge_module.MERGE_CONFIG_PATH, "w") as f:
        json.dump(config, f)

    data = {
        "files": [
            (BytesIO(b"A,B,X\n1,10,99\n"), "f1.csv"),
            (BytesIO(b"B,C\n10,20\n"), "f2.csv"),
            (BytesIO(b"B,D\n10,30\n"), "f3.csv"),
        ]
    }
    resp = client.post("/merge/merge_csv", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
    csv = resp.data.decode()
    lines = csv.strip().splitlines()
    assert lines[0] == "A,B,C,D"
    assert lines[1] == "1,10,20,30"


def test_concat_column_filter(client):
    config = {
        "files": {"file1": ["A"], "file2": ["B"]},
        "on": [],
        "how": "inner",
    }
    with open(merge_module.MERGE_CONFIG_PATH, "w") as f:
        json.dump(config, f)

    data = {
        "files": [
            (BytesIO(b"A,B\n1,2\n"), "f1.csv"),
            (BytesIO(b"B,C\n3,4\n"), "f2.csv"),
        ]
    }
    resp = client.post("/merge/merge_csv", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
    csv = resp.data.decode().strip().splitlines()
    assert csv[0] == "A,B"
    assert csv[1] == "1,"  # from first file
    assert csv[2] == ",3"  # from second file

