from flask import current_app
from pathlib import Path
import json
import os
import time


def _base_dir():
    base = Path(current_app.instance_path) / "progress"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _path(key: str) -> Path:
    return _base_dir() / f"{key}.json"


def set_progress(key: str, percent: int, message: str, done: bool = False, error: str | None = None):
    data = {
        "percent": int(max(0, min(100, percent))),
        "message": str(message),
        "done": bool(done),
        "error": (str(error) if error else None),
        "updated_at": int(time.time()),
    }
    p = _path(key)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(data), encoding="utf-8")
    os.replace(tmp, p)


def get_progress(key: str) -> dict:
    p = _path(key)
    if not p.exists():
        return {"percent": 0, "message": "Idle", "done": False, "error": None, "updated_at": 0}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"percent": 0, "message": "Idle", "done": False, "error": "corrupt", "updated_at": 0}


def clear_progress(key: str):
    p = _path(key)
    if p.exists():
        try:
            p.unlink()
        except Exception:
            pass
