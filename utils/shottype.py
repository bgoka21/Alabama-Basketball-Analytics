"""Utilities for computing player shot-type breakdowns.

These helpers centralize logic shared by the player detail view and
leaderboard tables so 3FG Shrink/Non-Shrink numbers always match.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from typing import Iterable, List, Mapping, MutableMapping

from sqlalchemy import and_, or_

from models.database import Game, Practice, PlayerStats, Roster, db


def _normalize_iterable(value):
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return [value]


def gather_labels_for_shot(shot: Mapping) -> set[str]:
    """Return a set of normalized labels for ``shot``.

    This mirrors the logic used on the player detail view so any consumer can
    reason about tags (e.g. Shrink/Non-Shrink) in a consistent way.
    """

    labels: list[str] = []

    if shot.get("Assisted"):
        labels.append("Assisted")
    else:
        labels.append("Non-Assisted")

    raw_sc = (shot.get("shot_class") or "").lower()

    suffix_map = {
        "atr": ["Type", "Defenders", "Dribble", "Feet", "Hands", "Other", "PA", "RA"],
        "2fg": ["Type", "Defenders", "Dribble", "Feet", "Hands", "Other", "PA", "RA"],
        "3fg": ["Contest", "Footwork", "Good/Bad", "Line", "Move", "Pocket", "Shrink", "Type"],
    }

    def _extend_from_value(val):
        for item in _normalize_iterable(val):
            if item is None:
                continue
            for lbl in re.split(r",", str(item)):
                cleaned = lbl.strip()
                if cleaned:
                    labels.append(cleaned)

    for suffix in suffix_map.get(raw_sc, []):
        key = f"{raw_sc}_{suffix.lower().replace('/', '_').replace(' ', '_')}"
        _extend_from_value(shot.get(key, ""))

    for scheme in ("scheme_attack", "scheme_drive", "scheme_pass"):
        key = f"{raw_sc}_{scheme}"
        _extend_from_value(shot.get(key, ""))

    return set(labels)


def compute_3fg_breakdown_from_shots(shot_list: Iterable[Mapping]) -> MutableMapping[str, float]:
    """Return shrink/non-shrink totals for a list of shot detail dicts."""

    total_att = total_makes = 0
    shrink_att = shrink_makes = 0
    non_att = non_makes = 0

    for shot in shot_list:
        if (shot.get("shot_class") or "").lower() != "3fg":
            continue

        total_att += 1
        made = shot.get("result") == "made"
        if made:
            total_makes += 1

        has_shrink = False
        has_nonshrink = False
        for cand in gather_labels_for_shot(shot):
            norm = str(cand).strip().lower()
            plain = norm.replace("-", "").replace(" ", "")
            if plain == "shrink":
                has_shrink = True
                break
            if plain == "nonshrink":
                has_nonshrink = True

        if has_shrink:
            shrink_att += 1
            if made:
                shrink_makes += 1
        elif has_nonshrink:
            non_att += 1
            if made:
                non_makes += 1

    def pct(makes: int, attempts: int) -> float:
        return (makes / attempts * 100.0) if attempts else 0.0

    return {
        "fg3_makes": total_makes,
        "fg3_att": total_att,
        "fg3_pct": pct(total_makes, total_att),
        "fg3_shrink_makes": shrink_makes,
        "fg3_shrink_att": shrink_att,
        "fg3_shrink_pct": pct(shrink_makes, shrink_att),
        "fg3_nonshrink_makes": non_makes,
        "fg3_nonshrink_att": non_att,
        "fg3_nonshrink_pct": pct(non_makes, non_att),
    }


def _collect_shots_from_query(rows: Iterable) -> List[Mapping]:
    """Expand serialized ``shot_type_details`` blobs into shot dicts."""

    shots: list[Mapping] = []
    for row in rows:
        blob = row[0] if isinstance(row, (list, tuple)) else getattr(row, "shot_type_details", None)
        if not blob:
            continue
        data = json.loads(blob) if isinstance(blob, str) else blob
        if isinstance(data, list):
            shots.extend(data)
        else:
            shots.append(data)
    return shots


def _apply_common_filters(query, practice, start_date, end_date):
    if practice is True:
        query = query.filter(PlayerStats.practice_id != None)  # noqa: E711
    elif practice is False:
        query = query.filter(PlayerStats.game_id != None)  # noqa: E711

    if start_date or end_date:
        query = query.outerjoin(Game, PlayerStats.game_id == Game.id).outerjoin(Practice, PlayerStats.practice_id == Practice.id)
        if start_date:
            query = query.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date >= start_date),  # noqa: E711
                    and_(PlayerStats.practice_id != None, Practice.date >= start_date),  # noqa: E711
                )
            )
        if end_date:
            query = query.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date <= end_date),  # noqa: E711
                    and_(PlayerStats.practice_id != None, Practice.date <= end_date),  # noqa: E711
                )
            )
    return query


def get_player_shottype_3fg_breakdown(
    player_id: int,
    season_id: int | None = None,
    practice: bool | None = False,
    *,
    start_date=None,
    end_date=None,
    label_set: set[str] | None = None,
) -> MutableMapping[str, float]:
    """Return Shrink/Non-Shrink 3FG totals for a player.

    ``practice`` may be ``True`` (practice only), ``False`` (games only), or
    ``None`` to include both. ``label_set`` should contain uppercase label
    tokens to match the player detail filters.
    """

    roster_entry = db.session.get(Roster, player_id)
    if not roster_entry:
        return {k: 0 for k in (
            "fg3_makes",
            "fg3_att",
            "fg3_pct",
            "fg3_shrink_makes",
            "fg3_shrink_att",
            "fg3_shrink_pct",
            "fg3_nonshrink_makes",
            "fg3_nonshrink_att",
            "fg3_nonshrink_pct",
        )}

    season = season_id or roster_entry.season_id
    q = PlayerStats.query.filter(PlayerStats.player_name == roster_entry.player_name)
    if season:
        q = q.filter(PlayerStats.season_id == season)

    q = _apply_common_filters(q, practice, start_date, end_date)

    if label_set:
        clauses = []
        for lbl in label_set:
            pattern = f"%{lbl}%"
            clauses.append(PlayerStats.shot_type_details.ilike(pattern))
            clauses.append(PlayerStats.stat_details.ilike(pattern))
        q = q.filter(or_(*clauses))

    rows = q.with_entities(PlayerStats.shot_type_details).all()
    shots = _collect_shots_from_query(rows)
    return compute_3fg_breakdown_from_shots(shots)
