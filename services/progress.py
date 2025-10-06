"""Helpers for tracking long-running background job progress.

Progress is stored in Flask-Caching when available so the front end can poll a
JSON endpoint and render a progress bar. When the cache backend is not
configured, progress falls back to a JSON file within the application's
instance folder so status survives across polling requests.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from flask import current_app

try:  # Reuse the leaderboard cache backend when possible.
    from services.leaderboard_cache import cache as _leaderboard_cache_backend  # type: ignore
except Exception:  # pragma: no cover - import-time failure only occurs in unusual setups
    _leaderboard_cache_backend = None

_LOGGER = logging.getLogger(__name__)
_TTL_SECONDS = 60 * 60  # 1 hour


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _is_real_cache(obj: Any) -> bool:
    if obj is None:
        return False
    name = obj.__class__.__name__
    # The in-repo fallback cache is named ``_InMemoryCache``. When that is in
    # use we treat it as "no cache" so we can persist progress to disk.
    return name != "_InMemoryCache"


def _get_cache_backend() -> Any:
    """Return the active cache backend, if Flask-Caching is configured."""

    try:
        app = current_app._get_current_object()
    except RuntimeError:  # pragma: no cover - only triggered outside app context
        app = None

    if app is not None:
        cache_ext = None
        with suppress(AttributeError):
            cache_ext = app.extensions.get("cache")  # type: ignore[assignment]
        if _is_real_cache(cache_ext):
            return cache_ext

    if _is_real_cache(_leaderboard_cache_backend):
        return _leaderboard_cache_backend

    return None


def _progress_store_path() -> Optional[str]:
    try:
        app = current_app._get_current_object()
    except RuntimeError:  # pragma: no cover - only triggered outside app context
        return None

    instance_path = getattr(app, "instance_path", None)
    if not instance_path:
        return None

    os.makedirs(instance_path, exist_ok=True)
    return os.path.join(instance_path, "progress_store.json")


def _read_file_store() -> Dict[str, Any]:
    path = _progress_store_path()
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
            if isinstance(data, dict):
                return data
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        _LOGGER.warning("Progress store JSON file was corrupt; resetting.")
    except OSError:
        _LOGGER.exception("Unable to read progress store JSON file.")
    return {}


def _write_file_store(data: Dict[str, Any]) -> None:
    path = _progress_store_path()
    if not path:
        return
    tmp_path = f"{path}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(data, handle)
        os.replace(tmp_path, path)
    except OSError:
        _LOGGER.exception("Unable to persist progress store JSON file.")
        with suppress(FileNotFoundError):
            os.remove(tmp_path)


def clear_progress(key: str) -> None:
    """Remove ``key`` from both the cache backend and file store."""

    cache_backend = _get_cache_backend()
    if cache_backend is not None:
        with suppress(Exception):
            cache_backend.delete(key)

    if _is_real_cache(_get_cache_backend()):
        # When a real cache is present we rely on it for storage, so no need to
        # keep stale values on disk.
        return

    store = _read_file_store()
    if key in store:
        store.pop(key, None)
        _write_file_store(store)


def set_progress(
    key: str,
    percent: int,
    message: str,
    *,
    done: bool = False,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist progress information for a background job."""

    pct = max(0, min(int(percent), 100))
    payload: Dict[str, Any] = {
        "percent": pct,
        "message": message,
        "done": bool(done),
        "error": error,
        "updated_at": _utc_now_iso(),
    }

    cache_backend = _get_cache_backend()
    if cache_backend is not None:
        try:
            cache_backend.set(key, payload, timeout=_TTL_SECONDS)
        except Exception:  # pragma: no cover - backend errors are logged but ignored
            _LOGGER.exception("Failed to store progress in cache backend.")
    else:
        store = _read_file_store()
        store[key] = payload
        _write_file_store(store)

    return payload


def get_progress(key: str) -> Optional[Dict[str, Any]]:
    """Return the stored progress for ``key`` or ``None`` when unavailable."""

    cache_backend = _get_cache_backend()
    if cache_backend is not None:
        try:
            value = cache_backend.get(key)
        except Exception:  # pragma: no cover - backend errors are logged but ignored
            _LOGGER.exception("Failed to read progress from cache backend.")
        else:
            if isinstance(value, dict):
                return value

    if _is_real_cache(cache_backend):
        return None

    store = _read_file_store()
    value = store.get(key)
    if isinstance(value, dict):
        return value
    return None
