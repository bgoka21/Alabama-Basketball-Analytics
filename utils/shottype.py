"""Utilities for computing player shot-type breakdowns.

These helpers centralize logic shared by the player detail view and
leaderboard tables so 3FG Shrink/Non-Shrink numbers always match.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Sequence
from typing import Iterable, List, Mapping, MutableMapping

from sqlalchemy import and_, or_, case, func, literal

from models.database import (
    Game,
    Practice,
    PlayerShotDetail,
    PlayerShotDetailLabel,
    PlayerStats,
    Roster,
    db,
)
from utils.label_filters import apply_player_label_filter


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


def _normalize_shot_class(value) -> str | None:
    if value is None:
        return None
    token = str(value).strip().lower()
    if not token:
        return None
    if token == "atr":
        return "atr"
    if token in {"2fg", "fg2"}:
        return "fg2"
    if token in {"3fg", "fg3"}:
        return "fg3"
    return None


def _normalize_shot_result(value) -> str:
    token = str(value or "").strip().lower()
    if token in {"made", "make", "makes", "hit", "1", "yes", "y"}:
        return "made"
    return "missed"


def _extract_assisted_flag(shot: Mapping) -> bool:
    if not isinstance(shot, Mapping):
        return False

    if "assisted_flag" in shot:
        return bool(shot.get("assisted_flag"))

    assisted_val = shot.get("Assisted")
    if isinstance(assisted_val, str) and assisted_val.strip():
        return True
    if isinstance(assisted_val, bool):
        return assisted_val

    assisted_generic = shot.get("assisted")
    if isinstance(assisted_generic, str) and assisted_generic.strip():
        lowered = assisted_generic.strip().lower()
        if lowered in {"yes", "true", "1", "assisted"}:
            return True
        if lowered in {"no", "false", "0", "non-assisted", "nonassisted"}:
            return False
    elif isinstance(assisted_generic, bool):
        return assisted_generic

    non_assisted_val = shot.get("Non-Assisted") or shot.get("non_assisted")
    if isinstance(non_assisted_val, str) and non_assisted_val.strip():
        return False

    return False


def _extract_drill_labels(shot: Mapping) -> list[str]:
    raw = shot.get("drill_labels")
    if not raw:
        return []
    if isinstance(raw, str):
        candidates = re.split(r",", raw)
    elif isinstance(raw, Iterable):
        candidates = raw
    else:
        candidates = [raw]

    labels: list[str] = []
    for item in candidates:
        if not isinstance(item, str):
            continue
        cleaned = item.strip()
        if cleaned:
            labels.append(cleaned.upper())
    return labels


def _normalized_labels_for_shot(shot: Mapping) -> set[str]:
    labels: set[str] = {
        str(lbl).strip().upper()
        for lbl in gather_labels_for_shot(shot)
        if str(lbl).strip()
    }

    poss_val = shot.get("possession_type")
    if isinstance(poss_val, str):
        tokens = re.split(r",", poss_val)
    elif isinstance(poss_val, Iterable):
        tokens = poss_val
    else:
        tokens = ()
    for token in tokens:
        if not isinstance(token, str):
            continue
        cleaned = token.strip()
        if cleaned:
            labels.add(cleaned.upper())

    labels.update(_extract_drill_labels(shot))

    extras = set()
    for lbl in labels:
        collapsed = re.sub(r"[\s-]+", "", lbl)
        if collapsed and collapsed != lbl:
            extras.add(collapsed)
    labels.update(extras)
    return labels


def persist_player_shot_details(
    player_stat: PlayerStats,
    shots: Iterable[Mapping],
    *,
    replace: bool = False,
) -> None:
    """Persist ``shots`` for ``player_stat`` into ``PlayerShotDetail`` rows."""

    if player_stat is None:
        return

    if replace and player_stat.player_shot_details:
        for existing in list(player_stat.player_shot_details):
            db.session.delete(existing)

    for shot in shots or []:
        if not isinstance(shot, Mapping):
            continue
        shot_class = _normalize_shot_class(shot.get("shot_class"))
        if not shot_class:
            continue

        result = _normalize_shot_result(shot.get("result"))
        is_assisted = _extract_assisted_flag(shot)
        raw_poss = shot.get("possession_type")
        possession = None
        if isinstance(raw_poss, str):
            possession = raw_poss.strip() or None
        elif raw_poss is not None:
            possession = str(raw_poss).strip() or None

        raw_location = shot.get("shot_location")
        if isinstance(raw_location, str):
            shot_location = raw_location.strip() or None
        elif raw_location is None:
            shot_location = None
        else:
            shot_location = str(raw_location).strip() or None

        drill_labels = _extract_drill_labels(shot)
        drill_csv = ",".join(drill_labels) if drill_labels else None

        detail = PlayerShotDetail(
            player_stats=player_stat,
            shot_class=shot_class,
            result=result,
            possession_type=possession,
            is_assisted=is_assisted,
            shot_location=shot_location,
            drill_labels=drill_csv,
        )

        for lbl in _normalized_labels_for_shot(shot):
            detail.label_entries.append(PlayerShotDetailLabel(label=lbl))

        db.session.add(detail)


def _normalize_label_filter_set(label_set: Iterable[str] | None) -> set[str]:
    if not label_set:
        return set()
    normalized: set[str] = set()
    for raw in label_set:
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        upper = text.upper()
        normalized.add(upper)
        collapsed = re.sub(r"[\s-]+", "", upper)
        if collapsed:
            normalized.add(collapsed)
    return normalized


def compute_leaderboard_shot_details(
    session,
    season_id: int,
    *,
    start_date=None,
    end_date=None,
    label_set: Iterable[str] | None = None,
) -> dict[str, dict[str, float]]:
    """Return per-player shot detail aggregates for leaderboard metrics."""

    if season_id is None:
        return {}

    normalized_label_set = _normalize_label_filter_set(label_set)

    base_query = (
        session.query(PlayerStats)
        .join(
            Roster,
            and_(
                Roster.player_name == PlayerStats.player_name,
                Roster.season_id == PlayerStats.season_id,
            ),
        )
        .join(PlayerShotDetail, PlayerShotDetail.player_stats_id == PlayerStats.id)
        .filter(PlayerStats.season_id == season_id)
    )

    base_query = apply_player_label_filter(base_query, label_set)

    if start_date or end_date:
        base_query = (
            base_query.outerjoin(Game, PlayerStats.game_id == Game.id)
            .outerjoin(Practice, PlayerStats.practice_id == Practice.id)
        )
        if start_date:
            base_query = base_query.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date >= start_date),  # noqa: E711
                    and_(PlayerStats.practice_id != None, Practice.date >= start_date),  # noqa: E711
                )
            )
        if end_date:
            base_query = base_query.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date <= end_date),  # noqa: E711
                    and_(PlayerStats.practice_id != None, Practice.date <= end_date),  # noqa: E711
                )
            )

    base_query = base_query.filter(
        func.lower(PlayerShotDetail.shot_class).in_(("atr", "2fg", "fg2", "3fg", "fg3"))
    )

    if normalized_label_set:
        base_query = base_query.filter(
            PlayerShotDetail.label_entries.any(
                PlayerShotDetailLabel.label.in_(tuple(normalized_label_set))
            )
        )

    context_expr = case(
        (func.lower(PlayerShotDetail.possession_type).like("%trans%"), literal("transition")),
        (func.lower(PlayerShotDetail.possession_type).like("%half%"), literal("halfcourt")),
        else_=literal("total"),
    )
    shot_class_expr = case(
        (func.lower(PlayerShotDetail.shot_class).in_(("2fg", "fg2")), literal("fg2")),
        (func.lower(PlayerShotDetail.shot_class).in_(("3fg", "fg3")), literal("fg3")),
        else_=literal("atr"),
    )

    filtered_query = base_query

    aggregates = (
        filtered_query.with_entities(
            Roster.player_name.label("player"),
            shot_class_expr.label("shot_class"),
            PlayerShotDetail.is_assisted.label("is_assisted"),
            context_expr.label("context"),
            func.count().label("attempts"),
            func.sum(
                case(
                    (func.lower(PlayerShotDetail.result) == "made", 1),
                    else_=0,
                )
            ).label("makes"),
        )
        .group_by(Roster.player_name, shot_class_expr, PlayerShotDetail.is_assisted, context_expr)
        .all()
    )

    if not aggregates:
        return {}

    def _rows_to_map(query):
        return {
            row.player: (int(row.attempts or 0), int(row.makes or 0))
            for row in query
        }

    shrink_rows = (
        filtered_query.filter(
            PlayerShotDetail.label_entries.any(PlayerShotDetailLabel.label == "SHRINK")
        )
        .with_entities(
            Roster.player_name.label("player"),
            func.count().label("attempts"),
            func.sum(
                case(
                    (func.lower(PlayerShotDetail.result) == "made", 1),
                    else_=0,
                )
            ).label("makes"),
        )
        .group_by(Roster.player_name)
        .all()
    )
    non_rows = (
        filtered_query.filter(
            PlayerShotDetail.label_entries.any(
                PlayerShotDetailLabel.label.in_(("NON-SHRINK", "NONSHRINK"))
            )
        )
        .filter(
            ~PlayerShotDetail.label_entries.any(PlayerShotDetailLabel.label == "SHRINK")
        )
        .with_entities(
            Roster.player_name.label("player"),
            func.count().label("attempts"),
            func.sum(
                case(
                    (func.lower(PlayerShotDetail.result) == "made", 1),
                    else_=0,
                )
            ).label("makes"),
        )
        .group_by(Roster.player_name)
        .all()
    )

    shrink_map = _rows_to_map(shrink_rows)
    non_map = _rows_to_map(non_rows)

    per_player_entries: dict[str, list[tuple[str, str, str, int, int]]] = defaultdict(list)
    for row in aggregates:
        sc = row.shot_class
        if sc not in {"atr", "fg2", "fg3"}:
            continue
        label = "Assisted" if row.is_assisted else "Non-Assisted"
        context = (row.context or "total").strip().lower() or "total"
        attempts = int(row.attempts or 0)
        makes = int(row.makes or 0)
        per_player_entries[row.player].append((sc, label, context, attempts, makes))

    shot_details: dict[str, dict[str, float]] = {}

    def _pct(makes: int, attempts: int) -> float:
        return (makes / attempts * 100.0) if attempts else 0.0

    for player, entries in per_player_entries.items():
        flat: dict[str, float] = {}
        totals_by_sc: dict[str, dict[str, int]] = defaultdict(lambda: {"attempts": 0, "makes": 0})
        per_sc_entries: dict[str, list[tuple[str, str, int, int]]] = defaultdict(list)

        for sc, label, context, attempts, makes in entries:
            totals_by_sc[sc]["attempts"] += attempts
            totals_by_sc[sc]["makes"] += makes
            per_sc_entries[sc].append((label, context, attempts, makes))

        for sc, items in per_sc_entries.items():
            sc_total_attempts = totals_by_sc[sc]["attempts"] or 0
            pts = 2 if sc in {"atr", "fg2"} else 3
            for label, context, attempts, makes in items:
                key = f"{sc}_{label}_{context}"
                flat[f"{key}_attempts"] = attempts
                flat[f"{key}_makes"] = makes
                flat[f"{key}_fg_pct"] = _pct(makes, attempts)
                flat[f"{key}_pps"] = (pts * makes / attempts) if attempts else 0.0
                flat[f"{key}_freq_pct"] = (
                    (attempts / sc_total_attempts * 100.0)
                    if sc_total_attempts
                    else 0.0
                )

        total_attempts = sum(t["attempts"] for t in totals_by_sc.values())
        for sc, totals in totals_by_sc.items():
            attempts = totals["attempts"]
            makes = totals["makes"]
            pts = 2 if sc in {"atr", "fg2"} else 3
            flat[f"{sc}_attempts"] = attempts
            flat[f"{sc}_makes"] = makes
            flat[f"{sc}_fg_pct"] = _pct(makes, attempts)
            flat[f"{sc}_pps"] = (pts * makes / attempts) if attempts else 0.0
            flat[f"{sc}_freq_pct"] = (
                (attempts / total_attempts * 100.0)
                if total_attempts
                else 0.0
            )

        fg3_attempts = totals_by_sc.get("fg3", {}).get("attempts", 0)
        fg3_makes = totals_by_sc.get("fg3", {}).get("makes", 0)
        shrink_attempts, shrink_makes = shrink_map.get(player, (0, 0))
        non_attempts, non_makes = non_map.get(player, (0, 0))

        flat.update(
            {
                "fg3_att": fg3_attempts,
                "fg3_makes": fg3_makes,
                "fg3_pct": _pct(fg3_makes, fg3_attempts),
                "fg3_shrink_att": shrink_attempts,
                "fg3_shrink_makes": shrink_makes,
                "fg3_shrink_pct": _pct(shrink_makes, shrink_attempts),
                "fg3_nonshrink_att": non_attempts,
                "fg3_nonshrink_makes": non_makes,
                "fg3_nonshrink_pct": _pct(non_makes, non_attempts),
            }
        )

        shot_details[player] = flat

    return shot_details


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
        q = apply_player_label_filter(q, label_set)

    rows = q.with_entities(PlayerStats.shot_type_details).all()
    shots = _collect_shots_from_query(rows)
    return compute_3fg_breakdown_from_shots(shots)
