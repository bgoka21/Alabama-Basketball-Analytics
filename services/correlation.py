"""Correlation analytics service layer.

This module provides helpers for normalising practice/game metrics into
``pandas`` Series so we can compute correlation studies for the analytics
workspace.  The implementation purposefully keeps the public surface small so
the forthcoming admin API can delegate without duplicating any of the
transformation logic.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Callable, Dict, FrozenSet, Iterable, Mapping, Optional, Sequence

import pandas as pd
from sqlalchemy import and_, func

from app.stats.field_catalog_practice import PRACTICE_FIELD_GROUPS
from models.database import (
    BlueCollarStats,
    Game,
    PlayerStats,
    Practice,
    Roster,
    db,
)
from stats_config import LEADERBOARD_STATS


class MetricSource(str, Enum):
    """Identify where a metric is sourced from."""

    PRACTICE = "practice"
    GAME = "game"


@dataclass(frozen=True)
class MetricDefinition:
    """A metric that will be plotted on one study axis."""

    source: MetricSource
    key: str
    label: Optional[str] = None


@dataclass(frozen=True)
class StudyDefinition:
    """Describe a single correlation study request."""

    x: MetricDefinition
    y: MetricDefinition
    identifier: Optional[str] = None
    label: Optional[str] = None


@dataclass(frozen=True)
class StudyScope:
    """Filters shared by every study in a request."""

    season_id: int
    roster_ids: Optional[Sequence[int]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


# -- Metric catalog helpers --------------------------------------------------


def _flatten_practice_catalog() -> Dict[str, Mapping[str, Any]]:
    catalog: Dict[str, Mapping[str, Any]] = {}
    for group, entries in PRACTICE_FIELD_GROUPS.items():
        for entry in entries:
            key = entry.get("key")
            if key:
                catalog[key] = {**entry, "group": group}
    return catalog


_PRACTICE_CATALOG: Dict[str, Mapping[str, Any]] = _flatten_practice_catalog()
_LEADERBOARD_CATALOG: Dict[str, Mapping[str, Any]] = {
    entry.get("key"): entry for entry in LEADERBOARD_STATS if entry.get("key")
}


# -- Shared numeric helpers --------------------------------------------------


def _as_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_div(numer: Any, denom: Any) -> Optional[float]:
    try:
        if numer in (None, "") or denom in (None, ""):
            return None
        denom_f = float(denom)
        if math.isclose(denom_f, 0.0):
            return None
        return float(numer) / denom_f
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _pct(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value * 100.0


def _total_fga(row: Mapping[str, Any]) -> float:
    return (
        _as_float(row.get("atr_attempts"))
        + _as_float(row.get("fg2_attempts"))
        + _as_float(row.get("fg3_attempts"))
    )


def _efg_pct(row: Mapping[str, Any]) -> Optional[float]:
    total_fga = _total_fga(row)
    if math.isclose(total_fga, 0.0):
        return None
    makes = _as_float(row.get("atr_makes")) + _as_float(row.get("fg2_makes"))
    threes = _as_float(row.get("fg3_makes"))
    return _pct((makes + 1.5 * threes) / total_fga)


def _spearman_from_dataframe(df: pd.DataFrame) -> Optional[float]:
    ranked = df.rank(method="average")
    value = ranked["x"].corr(ranked["y"], method="pearson")
    return None if pd.isna(value) else float(value)


# -- Practice metric computation --------------------------------------------


@dataclass(frozen=True)
class _MetricSpec:
    required_fields: Sequence[str]
    compute: Callable[[Mapping[str, Any]], Optional[float]]


_PRACTICE_PLAYER_FIELDS: Sequence[str] = (
    "points",
    "assists",
    "turnovers",
    "foul_by",
    "atr_makes",
    "atr_attempts",
    "fg2_makes",
    "fg2_attempts",
    "fg3_makes",
    "fg3_attempts",
    "ftm",
    "fta",
    "second_assists",
    "pot_assists",
    "crash_positive",
    "crash_missed",
    "back_man_positive",
    "back_man_missed",
    "box_out_positive",
    "box_out_missed",
    "off_reb_given_up",
)

_PRACTICE_BLUE_FIELDS: Sequence[str] = (
    "total_blue_collar",
    "deflection",
    "charge_taken",
    "floor_dive",
    "reb_tip",
    "misc",
    "steal",
    "block",
    "off_reb",
    "def_reb",
)


def _practice_metric_specs() -> Dict[str, _MetricSpec]:
    def direct(field: str) -> _MetricSpec:
        return _MetricSpec((field,), lambda row: row.get(field))

    def blue(field: str) -> _MetricSpec:
        return direct(field)

    def freq(field: str) -> _MetricSpec:
        return _MetricSpec(
            (field, "atr_attempts", "fg2_attempts", "fg3_attempts"),
            lambda row: _pct(_safe_div(row.get(field), _total_fga(row))),
        )

    def pct(numer: str, denom: str) -> _MetricSpec:
        return _MetricSpec((numer, denom), lambda row: _pct(_safe_div(row.get(numer), row.get(denom))))

    specs: Dict[str, _MetricSpec] = {
        "shooting_atr_makes": direct("atr_makes"),
        "shooting_atr_attempts": direct("atr_attempts"),
        "shooting_atr_pct": pct("atr_makes", "atr_attempts"),
        "shooting_atr_freq_pct": freq("atr_attempts"),
        "shooting_fg2_makes": direct("fg2_makes"),
        "shooting_fg2_attempts": direct("fg2_attempts"),
        "shooting_fg2_pct": pct("fg2_makes", "fg2_attempts"),
        "shooting_fg2_freq_pct": freq("fg2_attempts"),
        "shooting_fg3_pct": pct("fg3_makes", "fg3_attempts"),
        "shooting_fg3_attempts": direct("fg3_attempts"),
        "shooting_fg3_makes": direct("fg3_makes"),
        "shooting_fg3_freq_pct": freq("fg3_attempts"),
        "shooting_ft_makes": direct("ftm"),
        "shooting_ft_attempts": direct("fta"),
        "shooting_ft_pct": pct("ftm", "fta"),
        "shooting_pps": _MetricSpec(
            ("atr_attempts", "fg2_attempts", "fg3_attempts", "fg3_makes", "fg2_makes", "atr_makes"),
            lambda row: (
                None
                if math.isclose(_total_fga(row), 0.0)
                else (_efg_pct(row) or 0.0) * 0.02
            ),
        ),
        "shooting_efg_pct": _MetricSpec(
            ("atr_attempts", "fg2_attempts", "fg3_attempts", "fg3_makes", "fg2_makes", "atr_makes"),
            _efg_pct,
        ),
        "play_ast": direct("assists"),
        "play_to": direct("turnovers"),
        "play_potential_ast": direct("pot_assists"),
        "play_second_ast": direct("second_assists"),
        "play_ast_to_ratio": _MetricSpec(
            ("assists", "turnovers"),
            lambda row: _safe_div(row.get("assists"), row.get("turnovers")),
        ),
        "play_adj_ast_to_ratio": _MetricSpec(
            ("assists", "pot_assists", "second_assists", "turnovers"),
            lambda row: _safe_div(
                _as_float(row.get("assists"))
                + _as_float(row.get("pot_assists"))
                + _as_float(row.get("second_assists")),
                row.get("turnovers"),
            ),
        ),
        "reb": _MetricSpec(("off_reb", "def_reb"), lambda row: _as_float(row.get("off_reb")) + _as_float(row.get("def_reb"))),
        "oreb": blue("off_reb"),
        "dreb": blue("def_reb"),
        "pts": direct("points"),
        "ast": direct("assists"),
        "to": direct("turnovers"),
        "stl": blue("steal"),
        "blk": blue("block"),
        "pf": direct("foul_by"),
        "ppp": _MetricSpec(
            ("points", "turnovers", "atr_attempts", "fg2_attempts", "fg3_attempts"),
            lambda row: _safe_div(
                row.get("points"),
                _total_fga(row) + _as_float(row.get("turnovers")),
            ),
        ),
        "atr": _MetricSpec(
            ("assists", "turnovers"),
            lambda row: _safe_div(row.get("assists"), row.get("turnovers")),
        ),
        "ft_rate": _MetricSpec(
            ("fta", "atr_attempts", "fg2_attempts", "fg3_attempts"),
            lambda row: _safe_div(row.get("fta"), _total_fga(row)),
        ),
        "pps": _MetricSpec(
            ("points", "atr_attempts", "fg2_attempts", "fg3_attempts"),
            lambda row: _safe_div(row.get("points"), _total_fga(row)),
        ),
        "rd_crash_plus": direct("crash_positive"),
        "rd_crash_att": _MetricSpec(
            ("crash_positive", "crash_missed"),
            lambda row: _as_float(row.get("crash_positive")) + _as_float(row.get("crash_missed")),
        ),
        "rd_crash_pct": _MetricSpec(
            ("crash_positive", "crash_missed"),
            lambda row: _pct(
                _safe_div(
                    row.get("crash_positive"),
                    _as_float(row.get("crash_positive")) + _as_float(row.get("crash_missed")),
                )
            ),
        ),
        "rd_back_plus": direct("back_man_positive"),
        "rd_back_att": _MetricSpec(
            ("back_man_positive", "back_man_missed"),
            lambda row: _as_float(row.get("back_man_positive")) + _as_float(row.get("back_man_missed")),
        ),
        "rd_back_pct": _MetricSpec(
            ("back_man_positive", "back_man_missed"),
            lambda row: _pct(
                _safe_div(
                    row.get("back_man_positive"),
                    _as_float(row.get("back_man_positive")) + _as_float(row.get("back_man_missed")),
                )
            ),
        ),
        "rd_box_plus": direct("box_out_positive"),
        "rd_box_att": _MetricSpec(
            ("box_out_positive", "box_out_missed"),
            lambda row: _as_float(row.get("box_out_positive")) + _as_float(row.get("box_out_missed")),
        ),
        "rd_box_pct": _MetricSpec(
            ("box_out_positive", "box_out_missed"),
            lambda row: _pct(
                _safe_div(
                    row.get("box_out_positive"),
                    _as_float(row.get("box_out_positive")) + _as_float(row.get("box_out_missed")),
                )
            ),
        ),
        "rd_given_up": direct("off_reb_given_up"),
        "bcp_total": blue("total_blue_collar"),
        "deflections": blue("deflection"),
        "charges": blue("charge_taken"),
        "floor_dives": blue("floor_dive"),
        "loose_balls_won": blue("misc"),
        "tips": blue("reb_tip"),
        "steals_bc": blue("steal"),
        "blocks_bc": blue("block"),
    }

    return specs


_PRACTICE_METRICS = _practice_metric_specs()
SUPPORTED_PRACTICE_METRICS: FrozenSet[str] = frozenset(_PRACTICE_METRICS)


def _load_practice_rows(scope: StudyScope) -> Dict[str, Dict[str, Any]]:
    roster_query = Roster.query.filter(Roster.season_id == scope.season_id)
    if scope.roster_ids:
        roster_query = roster_query.filter(Roster.id.in_(scope.roster_ids))

    roster_entries = roster_query.all()
    if not roster_entries:
        return {}

    rows: Dict[int, Dict[str, Any]] = {
        entry.id: {"player": entry.player_name, "roster_id": entry.id}
        for entry in roster_entries
    }

    base_query = (
        db.session.query(
            Roster.id.label("roster_id"),
            func.count(func.distinct(PlayerStats.practice_id)).label("practice_count"),
            *[
                func.coalesce(func.sum(getattr(PlayerStats, field)), 0).label(field)
                for field in _PRACTICE_PLAYER_FIELDS
            ],
        )
        .join(
            PlayerStats,
            and_(
                PlayerStats.player_name == Roster.player_name,
                PlayerStats.season_id == Roster.season_id,
            ),
        )
        .filter(PlayerStats.practice_id.isnot(None))
        .filter(PlayerStats.season_id == scope.season_id)
    )

    if scope.roster_ids:
        base_query = base_query.filter(Roster.id.in_(scope.roster_ids))

    if scope.start_date or scope.end_date:
        base_query = base_query.join(Practice, PlayerStats.practice_id == Practice.id)
        if scope.start_date:
            base_query = base_query.filter(Practice.date >= scope.start_date)
        if scope.end_date:
            base_query = base_query.filter(Practice.date <= scope.end_date)

    base_query = base_query.group_by(Roster.id)

    for result in base_query.all():
        row = rows.get(result.roster_id)
        if not row:
            continue
        row["practice_count"] = int(result.practice_count or 0)
        for field in _PRACTICE_PLAYER_FIELDS:
            row[field] = _as_float(getattr(result, field))

    blue_query = (
        db.session.query(
            Roster.id.label("roster_id"),
            *[
                func.coalesce(func.sum(getattr(BlueCollarStats, field)), 0).label(field)
                for field in _PRACTICE_BLUE_FIELDS
            ],
        )
        .join(BlueCollarStats, BlueCollarStats.player_id == Roster.id)
        .filter(Roster.season_id == scope.season_id)
        .filter(BlueCollarStats.practice_id.isnot(None))
    )

    if scope.roster_ids:
        blue_query = blue_query.filter(Roster.id.in_(scope.roster_ids))

    if scope.start_date or scope.end_date:
        blue_query = blue_query.join(Practice, BlueCollarStats.practice_id == Practice.id)
        if scope.start_date:
            blue_query = blue_query.filter(Practice.date >= scope.start_date)
        if scope.end_date:
            blue_query = blue_query.filter(Practice.date <= scope.end_date)

    blue_query = blue_query.group_by(Roster.id)

    for result in blue_query.all():
        row = rows.get(result.roster_id)
        if not row:
            continue
        for field in _PRACTICE_BLUE_FIELDS:
            row[field] = _as_float(getattr(result, field))

    final: Dict[str, Dict[str, Any]] = {}
    for row in rows.values():
        numeric_values = [row.get(field, 0.0) for field in _PRACTICE_PLAYER_FIELDS]
        blue_values = [row.get(field, 0.0) for field in _PRACTICE_BLUE_FIELDS]
        if not any(value not in (0.0, None) for value in numeric_values + blue_values):
            continue
        final[row["player"]] = row
    return final


# -- Game metric computation -------------------------------------------------


_GAME_PLAYER_FIELDS: Sequence[str] = (
    "points",
    "assists",
    "turnovers",
    "atr_makes",
    "atr_attempts",
    "fg2_makes",
    "fg2_attempts",
    "fg3_makes",
    "fg3_attempts",
    "ftm",
    "fta",
)


def _game_metric_specs() -> Dict[str, _MetricSpec]:
    def direct(field: str) -> _MetricSpec:
        return _MetricSpec((field,), lambda row: row.get(field))

    def pct(numer: str, denom: str) -> _MetricSpec:
        return _MetricSpec((numer, denom), lambda row: _pct(_safe_div(row.get(numer), row.get(denom))))

    specs: Dict[str, _MetricSpec] = {
        "points": direct("points"),
        "assists": direct("assists"),
        "turnovers": direct("turnovers"),
        "fta": direct("fta"),
        "ftm": direct("ftm"),
        "fg3_pct": pct("fg3_makes", "fg3_attempts"),
        "three_fg_pct": pct("fg3_makes", "fg3_attempts"),
        "two_fg_pct": pct("fg2_makes", "fg2_attempts"),
        "assist_turnover_ratio": _MetricSpec(
            ("assists", "turnovers"),
            lambda row: _safe_div(row.get("assists"), row.get("turnovers")),
        ),
    }

    return specs


_GAME_METRICS = _game_metric_specs()
SUPPORTED_GAME_METRICS: FrozenSet[str] = frozenset(_GAME_METRICS)


def _load_game_rows(scope: StudyScope) -> Dict[str, Dict[str, Any]]:
    roster_query = Roster.query.filter(Roster.season_id == scope.season_id)
    if scope.roster_ids:
        roster_query = roster_query.filter(Roster.id.in_(scope.roster_ids))

    roster_entries = roster_query.all()
    if not roster_entries:
        return {}

    rows: Dict[int, Dict[str, Any]] = {
        entry.id: {"player": entry.player_name, "roster_id": entry.id}
        for entry in roster_entries
    }

    game_query = (
        db.session.query(
            Roster.id.label("roster_id"),
            func.count(func.distinct(PlayerStats.game_id)).label("game_count"),
            *[
                func.coalesce(func.sum(getattr(PlayerStats, field)), 0).label(field)
                for field in _GAME_PLAYER_FIELDS
            ],
        )
        .join(
            PlayerStats,
            and_(
                PlayerStats.player_name == Roster.player_name,
                PlayerStats.season_id == Roster.season_id,
            ),
        )
        .join(Game, PlayerStats.game_id == Game.id)
        .filter(PlayerStats.game_id.isnot(None))
        .filter(PlayerStats.season_id == scope.season_id)
    )

    if scope.roster_ids:
        game_query = game_query.filter(Roster.id.in_(scope.roster_ids))

    if scope.start_date:
        game_query = game_query.filter(Game.game_date >= scope.start_date)
    if scope.end_date:
        game_query = game_query.filter(Game.game_date <= scope.end_date)

    game_query = game_query.group_by(Roster.id)

    for result in game_query.all():
        row = rows.get(result.roster_id)
        if not row:
            continue
        row["game_count"] = int(result.game_count or 0)
        for field in _GAME_PLAYER_FIELDS:
            row[field] = _as_float(getattr(result, field))

    final: Dict[str, Dict[str, Any]] = {}
    for row in rows.values():
        numeric_values = [row.get(field, 0.0) for field in _GAME_PLAYER_FIELDS]
        if not any(value not in (0.0, None) for value in numeric_values):
            continue
        final[row["player"]] = row
    return final


# -- Metric extraction -------------------------------------------------------


def _metric_series(
    metric: MetricDefinition,
    practice_rows: Mapping[str, Mapping[str, Any]],
    game_rows: Mapping[str, Mapping[str, Any]],
) -> pd.Series:
    if metric.source is MetricSource.PRACTICE:
        spec = _PRACTICE_METRICS.get(metric.key)
        if spec is None:
            raise ValueError(f"Unsupported practice metric '{metric.key}'")
        return _series_from_rows(practice_rows, spec)

    if metric.source is MetricSource.GAME:
        spec = _GAME_METRICS.get(metric.key)
        if spec is None:
            raise ValueError(f"Unsupported game metric '{metric.key}'")
        return _series_from_rows(game_rows, spec)

    raise ValueError(f"Unsupported metric source '{metric.source}'")


def _series_from_rows(rows: Mapping[str, Mapping[str, Any]], spec: _MetricSpec) -> pd.Series:
    if not rows:
        return pd.Series(dtype=float)

    values: Dict[str, float] = {}
    for player, data in rows.items():
        value = spec.compute(data)
        if value is None:
            values[player] = float("nan")
        else:
            values[player] = float(value)
    return pd.Series(values, dtype=float)


def _coerce_metric(defn: Mapping[str, Any] | MetricDefinition) -> MetricDefinition:
    if isinstance(defn, MetricDefinition):
        return defn

    if not isinstance(defn, Mapping):
        raise TypeError("Metric definitions must be mapping objects")

    source = defn.get("source")
    if isinstance(source, MetricSource):
        metric_source = source
    else:
        try:
            metric_source = MetricSource(str(source).lower())
        except ValueError as exc:  # pragma: no cover - defensive branch
            raise ValueError(f"Unknown metric source '{source}'") from exc

    key = str(defn.get("key"))
    label = defn.get("label")

    if metric_source is MetricSource.PRACTICE and key in _PRACTICE_CATALOG:
        label = label or _PRACTICE_CATALOG[key].get("label")
    elif metric_source is MetricSource.GAME and key in _LEADERBOARD_CATALOG:
        label = label or _LEADERBOARD_CATALOG[key].get("label")

    return MetricDefinition(source=metric_source, key=key, label=label)


def _coerce_study(defn: StudyDefinition | Mapping[str, Any]) -> StudyDefinition:
    if isinstance(defn, StudyDefinition):
        return defn

    if not isinstance(defn, Mapping):
        raise TypeError("Study definitions must be mapping objects")

    x_metric = _coerce_metric(defn.get("x"))
    y_metric = _coerce_metric(defn.get("y"))
    identifier = defn.get("identifier") or defn.get("id")
    label = defn.get("label")
    return StudyDefinition(x=x_metric, y=y_metric, identifier=identifier, label=label)


def _coerce_scope(scope: StudyScope | Mapping[str, Any]) -> StudyScope:
    if isinstance(scope, StudyScope):
        return scope

    if not isinstance(scope, Mapping):
        raise TypeError("Study scope must be a mapping object")

    season_id = scope.get("season_id")
    if season_id is None:
        raise ValueError("season_id is required for correlation studies")

    roster_ids = scope.get("roster_ids")
    if roster_ids is not None:
        roster_ids = [int(rid) for rid in roster_ids]

    start_date = scope.get("start_date")
    end_date = scope.get("end_date")

    return StudyScope(
        season_id=int(season_id),
        roster_ids=roster_ids,
        start_date=start_date,
        end_date=end_date,
    )


# -- Public entry point ------------------------------------------------------


def run_studies(
    studies: Sequence[StudyDefinition | Mapping[str, Any]],
    scope: StudyScope | Mapping[str, Any],
) -> Dict[str, Any]:
    """Execute correlation studies for the supplied scope."""

    normalized_scope = _coerce_scope(scope)
    practice_rows = _load_practice_rows(normalized_scope)
    game_rows = _load_game_rows(normalized_scope)

    player_meta: Dict[str, Dict[str, Any]] = {}
    for data in practice_rows.values():
        player_meta[data["player"]] = {"roster_id": data.get("roster_id")}
    for data in game_rows.values():
        meta = player_meta.setdefault(data["player"], {})
        if meta.get("roster_id") is None and data.get("roster_id") is not None:
            meta["roster_id"] = data.get("roster_id")

    results = []

    for index, study in enumerate(studies):
        study_def = _coerce_study(study)
        x_series = _metric_series(study_def.x, practice_rows, game_rows)
        y_series = _metric_series(study_def.y, practice_rows, game_rows)

        combined = pd.concat([x_series.rename("x"), y_series.rename("y")], axis=1, join="inner")
        combined = combined.dropna()

        samples = int(len(combined.index))
        pearson: Optional[float] = None
        spearman: Optional[float] = None

        if samples >= 2:
            pearson_val = combined["x"].corr(combined["y"], method="pearson")
            pearson = None if pd.isna(pearson_val) else float(pearson_val)
            spearman = _spearman_from_dataframe(combined)

        scatter: Iterable[Dict[str, Any]] = []
        if samples:
            scatter = []
            for player, row in combined.iterrows():
                meta = player_meta.get(player, {})
                point = {
                    "player": player,
                    "x": float(row["x"]),
                    "y": float(row["y"]),
                }
                roster_id = meta.get("roster_id")
                if roster_id is not None:
                    point["roster_id"] = roster_id
                scatter.append(point)

        results.append(
            {
                "id": study_def.identifier or f"study-{index}",
                "label": study_def.label,
                "x_metric": {
                    "key": study_def.x.key,
                    "label": study_def.x.label,
                    "source": study_def.x.source.value,
                },
                "y_metric": {
                    "key": study_def.y.key,
                    "label": study_def.y.label,
                    "source": study_def.y.source.value,
                },
                "samples": samples,
                "pearson": pearson,
                "spearman": spearman,
                "scatter": list(scatter),
            }
        )

    return {"studies": results}


__all__ = [
    "MetricDefinition",
    "MetricSource",
    "StudyDefinition",
    "StudyScope",
    "run_studies",
]
