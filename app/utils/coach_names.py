"""Utilities for normalizing coach names and applying alias mappings."""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, Set, Tuple

# Path to alias map shipped with the app.  If missing, we fall back to an empty map.
_ALIAS_PATH = Path(__file__).resolve().parents[1] / "data" / "coach_aliases.json"

# Cache for alias map and inverse index
_alias_map: Dict[str, Dict[str, str]] | None = None
_inverse_map: Dict[str, Set[str]] | None = None

_space_re = re.compile(r"\s+")
_punct_key_re = re.compile(r"[^a-z0-9'\- ]+")


def _load_aliases() -> tuple[Dict[str, Dict[str, str]], Dict[str, Set[str]]]:
    """Load alias map and build inverse index (cached)."""
    global _alias_map, _inverse_map
    if _alias_map is None:
        try:
            with open(_ALIAS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
        # normalize keys to lower-case for matching
        _alias_map = {k.lower(): v for k, v in data.items()}
        _inverse_map = {}
        for raw, info in _alias_map.items():
            canon = info.get("canonical", "").strip()
            key = info.get("key")
            if not key:
                key, _ = canonicalize(canon)
                info["key"] = key
            _inverse_map.setdefault(key, set()).add(raw)
            if canon:
                _inverse_map[key].add(canon.lower())
    return _alias_map, _inverse_map or {}


def canonicalize(raw: str) -> Tuple[str, str]:
    """
    Normalize a raw coach string into (canonical_key, display_name).
    The key is lowercased and stripped of most punctuation for matching.
    The display name is nicely cased for UI.
    """
    if not raw:
        return "", ""
    # Unicode normalization and whitespace condense
    s = unicodedata.normalize("NFKD", str(raw))
    s = _space_re.sub(" ", s).strip()

    # Comma swap: "Last, First" -> "First Last"
    if "," in s:
        parts = [p.strip() for p in s.split(",", 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            s = f"{parts[1]} {parts[0]}"

    # Normalize spacing around periods (initials)
    s = re.sub(r"\s*\.\s*", ". ", s)
    s = _space_re.sub(" ", s).strip()

    # Proper-case for display: keep initials (e.g., "B. Self")
    tokens = []
    for token in s.split(" "):
        if len(token) == 2 and token.endswith("."):
            tokens.append(token[0].upper() + ".")
        elif any(c.isupper() for c in token[1:]):
            tokens.append(token)
        else:
            tokens.append(token.capitalize())
    display = " ".join(tokens)

    # Canonical key: lower-case, strip most punctuation except hyphen/apostrophe
    key = display.lower()
    key = _punct_key_re.sub("", key)
    key = _space_re.sub(" ", key).strip()
    return key, display


def normalize_coach_name(raw: str) -> Tuple[str, str]:
    """Return (canonical_key, display_name) with alias resolution."""
    raw = raw or ""
    alias_map, _ = _load_aliases()
    raw_key = raw.strip().lower()
    entry = alias_map.get(raw_key)
    if not entry:
        # try by canonicalized key as well
        cand_key, cand_display = canonicalize(raw)
        entry = alias_map.get(cand_key)
        if entry:
            return entry.get("key", cand_key), entry.get("canonical", cand_display)
        return cand_key, cand_display
    return entry.get("key", ""), entry.get("canonical", raw.strip())


def get_alias_variants(display_name: str) -> Set[str]:
    """Return all known raw name variants (lowercased) for a canonical display name."""
    key, canon_display = normalize_coach_name(display_name)
    _, inverse = _load_aliases()
    variants = {canon_display.lower()}
    variants.update(v.lower() for v in inverse.get(key, set()))
    return variants
