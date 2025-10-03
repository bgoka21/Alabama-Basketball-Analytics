"""Utility helpers for application-level caching."""
from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import Iterable, Optional, Sequence, Tuple

from flask import current_app

LEADERBOARD_CACHE_PREFIX = "leaderboard"
LEADERBOARD_REGISTRY_KEY = "leaderboard:registry"


def get_cache():
    """Return the configured cache instance, if available."""
    try:
        app = current_app._get_current_object()
    except RuntimeError:
        return None

    extension = app.extensions.get("cache")
    if extension is None:
        return None

    if hasattr(extension, "set") and hasattr(extension, "get"):
        return extension

    if isinstance(extension, dict):
        for maybe_cache in extension.keys():
            if hasattr(maybe_cache, "set") and hasattr(maybe_cache, "get"):
                return maybe_cache
        for maybe_cache in extension.values():
            if hasattr(maybe_cache, "set") and hasattr(maybe_cache, "get"):
                return maybe_cache

    return None


def normalize_label_set(label_set: Optional[Iterable[str]]) -> Tuple[str, ...]:
    """Return a normalized, sorted tuple of labels."""
    if not label_set:
        return tuple()

    normalized = {
        str(label).strip().lower()
        for label in label_set
        if label is not None and str(label).strip()
    }
    return tuple(sorted(normalized))


def _normalize_boundary(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def build_leaderboard_cache_key(
    stat_key: str,
    season_id: int,
    start_dt=None,
    end_dt=None,
    normalized_labels: Sequence[str] = (),
):
    """Return a cache key and metadata for the leaderboard payload."""
    start_token = _normalize_boundary(start_dt)
    end_token = _normalize_boundary(end_dt)
    labels_string = "|".join(normalized_labels)
    label_digest = (
        hashlib.sha1(labels_string.encode("utf-8")).hexdigest()
        if labels_string
        else "none"
    )
    cache_key = (
        f"{LEADERBOARD_CACHE_PREFIX}:{season_id}:{stat_key}"
        f":{start_token}:{end_token}:{label_digest}"
    )
    metadata = {
        "season_id": season_id,
        "stat_key": stat_key,
        "start": start_token,
        "end": end_token,
        "labels": tuple(normalized_labels),
    }
    return cache_key, metadata


def register_leaderboard_cache_entry(cache, cache_key: str, metadata: dict) -> None:
    """Store the cache metadata so it can be invalidated later."""
    if cache is None:
        return

    registry = cache.get(LEADERBOARD_REGISTRY_KEY)
    if not registry:
        registry = {}
    else:
        registry = dict(registry)

    registry[cache_key] = metadata
    cache.set(LEADERBOARD_REGISTRY_KEY, registry)


def invalidate_leaderboard_cache(
    season_id: int,
    stat_key: Optional[str] = None,
    start_dt=None,
    end_dt=None,
    label_set: Optional[Iterable[str]] = None,
) -> int:
    """Invalidate cached leaderboard entries and return the number removed."""
    cache = get_cache()
    if cache is None:
        return 0

    registry = cache.get(LEADERBOARD_REGISTRY_KEY) or {}
    if not registry:
        return 0

    normalized_labels = (
        normalize_label_set(label_set) if label_set is not None else None
    )
    start_token = _normalize_boundary(start_dt) if start_dt is not None else None
    end_token = _normalize_boundary(end_dt) if end_dt is not None else None

    removed = 0
    updated_registry = dict(registry)
    for cache_key, metadata in list(registry.items()):
        if metadata.get("season_id") != season_id:
            continue
        if stat_key is not None and metadata.get("stat_key") != stat_key:
            continue
        if start_token is not None and metadata.get("start") != start_token:
            continue
        if end_token is not None and metadata.get("end") != end_token:
            continue
        if normalized_labels is not None and tuple(metadata.get("labels", ())) != normalized_labels:
            continue

        cache.delete(cache_key)
        updated_registry.pop(cache_key, None)
        removed += 1

    if removed:
        cache.set(LEADERBOARD_REGISTRY_KEY, updated_registry)

    return removed


def invalidate_season_leaderboard_cache(season_id: int) -> int:
    """Invalidate all cached leaderboard data for the given season."""
    return invalidate_leaderboard_cache(season_id)
