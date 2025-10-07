import hashlib
import json

from models.database import CachedLeaderboard, Season, db


SEASON_ID = 2030


def _store_snapshot(app, stat_key: str, payload: dict | None = None) -> None:
    payload = payload or {
        "schema_version": 1,
        "formatter_version": 1,
        "rows": [{"value": 42}],
        "columns_manifest": [],
    }
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    etag = hashlib.sha256(body.encode("utf-8")).hexdigest()

    with app.app_context():
        if db.session.get(Season, SEASON_ID) is None:
            db.session.add(Season(id=SEASON_ID, season_name="2030 Season"))
        db.session.add(
            CachedLeaderboard(
                season_id=SEASON_ID,
                stat_key=stat_key,
                schema_version=payload.get("schema_version", 1),
                formatter_version=payload.get("formatter_version", 1),
                etag=etag,
                payload_json=body,
            )
        )
        db.session.commit()


def test_all_leaderboards_returns_cached_payload(app, client):
    _store_snapshot(app, "points", payload={
        "schema_version": 2,
        "formatter_version": 3,
        "rows": [{"value": 99}],
        "columns_manifest": [],
    })

    resp = client.get(f"/admin/api/leaderboards/{SEASON_ID}/all")
    assert resp.status_code == 200
    assert resp.is_json
    data = resp.get_json()
    assert "leaderboards" in data
    assert "points" in data["leaderboards"]
    assert data["leaderboards"]["points"]["rows"][0]["value"] == 99
    assert data["leaderboards"]["points"]["schema_version"] == 2
    assert resp.headers.get("ETag")


def test_all_leaderboards_honours_if_none_match(app, client):
    _store_snapshot(app, "assists")

    url = f"/admin/api/leaderboards/{SEASON_ID}/all"
    first = client.get(url)
    etag = first.headers.get("ETag")
    assert etag

    second = client.get(url, headers={"If-None-Match": etag})
    assert second.status_code == 304
    assert second.get_data() == b""


def test_all_leaderboards_does_not_compute(monkeypatch, app, client):
    _store_snapshot(app, "rebounds")

    def fail(*args, **kwargs):  # pragma: no cover - defensive guard
        raise AssertionError("build_leaderboard_payload should not be called")

    monkeypatch.setattr("services.leaderboard_cache.build_leaderboard_payload", fail)

    resp = client.get(f"/admin/api/leaderboards/{SEASON_ID}/all")
    assert resp.status_code == 200


def test_all_leaderboards_reports_missing_keys(monkeypatch, app, client):
    _store_snapshot(app, "points")

    monkeypatch.setattr("admin.routes.LEADERBOARD_STAT_KEYS", ["points", "steals"])
    monkeypatch.setattr("constants.LEADERBOARD_STAT_KEYS", ["points", "steals"], raising=False)

    resp = client.get(f"/admin/api/leaderboards/{SEASON_ID}/all")
    assert resp.status_code == 200

    data = resp.get_json()
    assert "steals" in data["missing"]
    assert "steals" not in data["leaderboards"]
