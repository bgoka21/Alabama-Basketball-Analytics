from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Mapping


DualContextResult = Dict[str, Any]


def _default_context() -> DualContextResult:
    return {
        "season_rows": [],
        "season_team_totals": None,
        "last_rows": None,
        "last_team_totals": None,
        "last_practice_date": None,
    }


def _normalize_compute_result(result: Any) -> Tuple[Any, Any]:
    """Return ``(team_totals, rows)`` from a compute function response."""

    if isinstance(result, dict):
        return result.get("team_totals"), result.get("rows")

    if not isinstance(result, tuple):
        raise TypeError(f"Unexpected compute_fn return type: {type(result)!r}")

    if len(result) == 3:
        first, second, third = result
        if isinstance(second, list):
            return third, second
        if isinstance(first, list):
            return third, first
        if isinstance(third, list):
            return second, third
        return third, second

    if len(result) == 2:
        first, second = result
        if isinstance(second, list) or second is None:
            return first, second
        if isinstance(first, list) or first is None:
            return second, first
        return first, second

    raise ValueError(
        "compute_fn is expected to return a tuple of length 2 or 3, "
        f"got length {len(result)}"
    )


def build_dual_context(
    *,
    season_id: Optional[int],
    compute_fn: Callable[..., Any],
    stat_key: Optional[str] = None,
    label_set: Optional[Any] = None,
    extra_kwargs: Optional[Dict[str, Any]] = None,
) -> DualContextResult:
    """Return combined season and last-practice leaderboard contexts."""

    context = _default_context()

    if season_id is None:
        return context

    extra_kwargs = extra_kwargs or {}

    season_result = compute_fn(
        stat_key=stat_key,
        season_id=season_id,
        start_dt=None,
        end_dt=None,
        label_set=label_set,
        **extra_kwargs,
    )
    season_team_totals, season_rows = _normalize_compute_result(season_result)
    context.update(
        {
            "season_rows": season_rows,
            "season_team_totals": season_team_totals,
        }
    )

    from app.services.last_practice import get_last_practice  # inline to avoid circular import

    last_practice = get_last_practice(season_id)
    if last_practice and getattr(last_practice, "date", None):
        last_result = compute_fn(
            stat_key=stat_key,
            season_id=season_id,
            start_dt=last_practice.date,
            end_dt=last_practice.date,
            label_set=label_set,
            **extra_kwargs,
        )
        last_team_totals, last_rows = _normalize_compute_result(last_result)
        context.update(
            {
                "last_rows": last_rows,
                "last_team_totals": last_team_totals,
                "last_practice_date": last_practice.date,
            }
        )

    return context


# --- Helpers for preparing template-friendly dual leaderboard data ---

_PLAYER_KEYS = ("player_name", "player", "name")
_PLUS_KEYS = (
    "plus",
    "bump_positive",
    "crash_plus",
    "back_plus",
    "box_plus",
    "gap_plus",
    "cw_plus",
    "sd_plus",
    "close_window_plus",
    "shut_door_plus",
    "low_plus",
)
_OPPS_KEYS = (
    "opps",
    "attempts",
    "total_opps",
    "crash_opp",
    "crash_opps",
    "back_opp",
    "back_opps",
    "box_opp",
    "gap_opp",
    "cw_opp",
    "sd_opp",
    "close_window_opp",
    "shut_door_opp",
    "low_opp",
)
_PCT_KEYS = (
    "pct",
    "percentage",
    "crash_pct",
    "back_pct",
    "box_pct",
    "gap_pct",
    "cw_pct",
    "sd_pct",
    "close_window_pct",
    "shut_door_pct",
    "low_pct",
)


def _resolve_value(source: Any, keys: Tuple[str, ...], *, index: Optional[int] = None) -> Any:
    if source is None:
        return None

    if isinstance(keys, str):  # type: ignore[arg-type]
        keys = (keys,)  # pragma: no cover - defensive

    if isinstance(source, Mapping):
        for key in keys:
            if key in source and source[key] is not None:
                return source[key]

    for key in keys:
        if hasattr(source, key):
            value = getattr(source, key)
            if value is not None:
                return value

    if index is not None and isinstance(source, Sequence):
        try:
            return source[index]
        except IndexError:
            return None

    return None


def _to_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):  # treat bools as ints
        return int(value)
    if isinstance(value, (int,)):
        return int(value)
    if isinstance(value, float):
        return int(round(value))
    try:
        text = str(value).strip()
        if not text:
            return 0
        if "." in text:
            return int(round(float(text)))
        return int(text)
    except (TypeError, ValueError):
        return 0


def _to_pct(value: Any, plus: Optional[int] = None, opps: Optional[int] = None) -> Optional[float]:
    if value is None:
        if plus is not None and opps:
            try:
                return (float(plus) / float(opps)) * 100.0
            except (TypeError, ValueError, ZeroDivisionError):
                return None
        return None

    if isinstance(value, (int, float)):
        return float(value)

    try:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("%"):
            text = text[:-1]
        return float(text)
    except (TypeError, ValueError):
        return None


def _build_stats_entry(
    *,
    player: Optional[str],
    plus: Any,
    opps: Any,
    pct: Any,
    subtype: Optional[str] = None,
    extra_name: Optional[str] = None,
    extra_value: Any = None,
) -> Dict[str, Any]:
    plus_val = _to_int(plus)
    opps_val = _to_int(opps)
    entry: Dict[str, Any] = {
        "plus": plus_val,
        "opps": opps_val,
        "pct": _to_pct(pct, plus_val, opps_val),
    }

    if player is not None:
        entry["player_name"] = player
    if subtype is not None:
        entry["subtype"] = subtype
    if extra_name:
        entry[extra_name] = _to_int(extra_value)

    return entry


def _normalize_simple_rows(
    rows: Any,
    *,
    indexes: Optional[Dict[str, int]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> list[Dict[str, Any]]:
    indexes = indexes or {}
    normalized: list[Dict[str, Any]] = []

    for row in rows or []:
        player = _resolve_value(row, _PLAYER_KEYS, index=indexes.get("player", 0))
        if player is None:
            continue

        plus = _resolve_value(row, _PLUS_KEYS, index=indexes.get("plus"))
        opps = _resolve_value(row, _OPPS_KEYS, index=indexes.get("opps"))
        pct = _resolve_value(row, _PCT_KEYS, index=indexes.get("pct"))

        extra_value = None
        if extra:
            aliases = tuple(extra.get("aliases", (extra["key"],)))
            extra_value = _resolve_value(row, aliases, index=extra.get("index"))

        normalized.append(
            _build_stats_entry(
                player=player,
                plus=plus,
                opps=opps,
                pct=pct,
                extra_name=extra["key"] if extra else None,
                extra_value=extra_value,
            )
        )

    return normalized


def _normalize_simple_totals(
    totals: Any,
    *,
    indexes: Optional[Dict[str, int]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if totals is None:
        return None

    indexes = indexes or {}

    plus = _resolve_value(totals, _PLUS_KEYS, index=indexes.get("plus"))
    opps = _resolve_value(totals, _OPPS_KEYS, index=indexes.get("opps"))
    pct = _resolve_value(totals, _PCT_KEYS, index=indexes.get("pct"))

    extra_value = None
    if extra:
        aliases = tuple(extra.get("aliases", (extra["key"],)))
        extra_value = _resolve_value(totals, aliases, index=extra.get("index"))

    return _build_stats_entry(
        player=None,
        plus=plus,
        opps=opps,
        pct=pct,
        extra_name=extra["key"] if extra else None,
        extra_value=extra_value,
    )


def _group_by_subtype(rows: list[Dict[str, Any]]) -> Dict[str, list[Dict[str, Any]]]:
    grouped: Dict[str, list[Dict[str, Any]]] = {}
    for row in rows or []:
        subtype = row.get("subtype")
        if not subtype:
            continue
        grouped.setdefault(subtype, []).append(row)
    return grouped


def _normalize_split_rows(rows: Any, specs: Tuple[Dict[str, Any], ...]) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []

    for row in rows or []:
        if isinstance(row, Mapping) and row.get("subtype"):
            subtype = row.get("subtype")
            player = _resolve_value(row, _PLAYER_KEYS, index=None)
            plus = _resolve_value(row, _PLUS_KEYS, index=None)
            opps = _resolve_value(row, _OPPS_KEYS, index=None)
            pct = _resolve_value(row, _PCT_KEYS, index=None)
            normalized.append(
                _build_stats_entry(
                    player=player,
                    plus=plus,
                    opps=opps,
                    pct=pct,
                    subtype=subtype if isinstance(subtype, str) else None,
                )
            )
            continue

        player = _resolve_value(row, _PLAYER_KEYS, index=0)
        if player is None:
            continue

        for spec in specs:
            plus = _resolve_value(row, tuple(spec.get("plus_keys", ())), index=spec.get("plus_index"))
            if plus is None:
                plus = _resolve_value(row, _PLUS_KEYS, index=spec.get("plus_index"))

            opps = _resolve_value(row, tuple(spec.get("opps_keys", ())), index=spec.get("opps_index"))
            if opps is None:
                opps = _resolve_value(row, _OPPS_KEYS, index=spec.get("opps_index"))

            pct = _resolve_value(row, tuple(spec.get("pct_keys", ())), index=spec.get("pct_index"))
            if pct is None:
                pct = _resolve_value(row, _PCT_KEYS, index=spec.get("pct_index"))

            normalized.append(
                _build_stats_entry(
                    player=player,
                    plus=plus,
                    opps=opps,
                    pct=pct,
                    subtype=spec.get("subtype"),
                )
            )

    return normalized


def _value_from_seq(seq: Any, index: Optional[int]) -> Any:
    if index is None:
        return None
    if isinstance(seq, Sequence):
        try:
            return seq[index]
        except IndexError:
            return None
    return None


def _normalize_split_totals(totals: Any, specs: Tuple[Dict[str, Any], ...]) -> Dict[str, Optional[Dict[str, Any]]]:
    result: Dict[str, Optional[Dict[str, Any]]] = {spec["subtype"]: None for spec in specs}

    if totals is None:
        return result

    if isinstance(totals, Mapping):
        for spec in specs:
            entry = None
            keys = (spec["subtype"],) + tuple(spec.get("aliases", ()))
            for key in keys:
                if key in totals:
                    entry = totals[key]
                    break
            result[spec["subtype"]] = _normalize_simple_totals(
                entry,
                indexes=spec.get("indexes"),
            )
        return result

    if isinstance(totals, Sequence):
        for spec in specs:
            idxs = spec.get("total_indexes") or spec.get("indexes") or {}
            plus = _value_from_seq(totals, idxs.get("plus"))
            opps = _value_from_seq(totals, idxs.get("opps"))
            pct = _value_from_seq(totals, idxs.get("pct"))
            result[spec["subtype"]] = _build_stats_entry(
                player=None,
                plus=plus,
                opps=opps,
                pct=pct,
            )
        return result

    for spec in specs:
        result[spec["subtype"]] = _normalize_simple_totals(
            totals,
            indexes=spec.get("indexes"),
        )

    return result


def prepare_dual_context(context: DualContextResult, stat_key: Optional[str]) -> DualContextResult:
    """Normalize ``context`` so templates receive predictable structures."""

    ctx = dict(context)

    if stat_key in {"defense", "collision_gap_help"}:
        row_indexes = {"player": 0, "plus": 1, "opps": 2, "pct": 3}
        total_indexes = {"plus": 0, "opps": 1, "pct": 2}
        ctx["season_rows"] = _normalize_simple_rows(ctx.get("season_rows"), indexes=row_indexes)
        ctx["last_rows"] = _normalize_simple_rows(ctx.get("last_rows"), indexes=row_indexes)
        ctx["season_team_totals"] = _normalize_simple_totals(ctx.get("season_team_totals"), indexes=total_indexes)
        ctx["last_team_totals"] = _normalize_simple_totals(ctx.get("last_team_totals"), indexes=total_indexes)
        return ctx

    if stat_key == "def_rebounding":
        row_indexes = {"player": 0, "plus": 1, "opps": 2, "pct": 3}
        total_indexes = {"plus": 0, "opps": 1, "pct": 2}
        extra = {"key": "off_reb_given_up", "index": 4, "aliases": ("off_reb_given_up", "given_up")}
        total_extra = {"key": "off_reb_given_up", "index": 3, "aliases": ("off_reb_given_up", "given_up")}
        ctx["season_rows"] = _normalize_simple_rows(ctx.get("season_rows"), indexes=row_indexes, extra=extra)
        ctx["last_rows"] = _normalize_simple_rows(ctx.get("last_rows"), indexes=row_indexes, extra=extra)
        ctx["season_team_totals"] = _normalize_simple_totals(ctx.get("season_team_totals"), indexes=total_indexes, extra=total_extra)
        ctx["last_team_totals"] = _normalize_simple_totals(ctx.get("last_team_totals"), indexes=total_indexes, extra=total_extra)
        return ctx

    if stat_key == "off_rebounding":
        specs = (
            {
                "subtype": "crash",
                "plus_index": 1,
                "opps_index": 2,
                "pct_index": 3,
                "aliases": ("crash",),
                "indexes": {"plus": 0, "opps": 1, "pct": 2},
                "plus_keys": ("plus", "crash_plus"),
                "opps_keys": ("opps", "crash_opp", "crash_opps"),
                "pct_keys": ("pct", "crash_pct"),
            },
            {
                "subtype": "back_man",
                "plus_index": 4,
                "opps_index": 5,
                "pct_index": 6,
                "aliases": ("back_man", "back"),
                "indexes": {"plus": 3, "opps": 4, "pct": 5},
                "plus_keys": ("plus", "back_plus"),
                "opps_keys": ("opps", "back_opp", "back_opps"),
                "pct_keys": ("pct", "back_pct"),
            },
        )
        ctx["season_rows"] = _normalize_split_rows(ctx.get("season_rows"), specs)
        ctx["last_rows"] = _normalize_split_rows(ctx.get("last_rows"), specs)
        ctx["season_team_totals"] = _normalize_split_totals(ctx.get("season_team_totals"), specs)
        ctx["last_team_totals"] = _normalize_split_totals(ctx.get("last_team_totals"), specs)
        ctx["season_rows_by_subtype"] = _group_by_subtype(ctx["season_rows"])
        ctx["last_rows_by_subtype"] = _group_by_subtype(ctx["last_rows"])
        return ctx

    if stat_key == "pnr_gap_help":
        specs = (
            {
                "subtype": "gap_help",
                "plus_index": 1,
                "opps_index": 2,
                "pct_index": 3,
                "aliases": ("gap_help", "gap"),
                "indexes": {"plus": 0, "opps": 1, "pct": 2},
                "plus_keys": ("plus", "gap_plus"),
                "opps_keys": ("opps", "gap_opp"),
                "pct_keys": ("pct", "gap_pct"),
            },
            {
                "subtype": "low_help",
                "plus_index": 4,
                "opps_index": 5,
                "pct_index": 6,
                "aliases": ("low_help", "low"),
                "indexes": {"plus": 3, "opps": 4, "pct": 5},
                "plus_keys": ("plus", "low_plus"),
                "opps_keys": ("opps", "low_opp"),
                "pct_keys": ("pct", "low_pct"),
            },
        )
        ctx["season_rows"] = _normalize_split_rows(ctx.get("season_rows"), specs)
        ctx["last_rows"] = _normalize_split_rows(ctx.get("last_rows"), specs)
        ctx["season_team_totals"] = _normalize_split_totals(ctx.get("season_team_totals"), specs)
        ctx["last_team_totals"] = _normalize_split_totals(ctx.get("last_team_totals"), specs)
        ctx["season_rows_by_subtype"] = _group_by_subtype(ctx["season_rows"])
        ctx["last_rows_by_subtype"] = _group_by_subtype(ctx["last_rows"])
        return ctx

    if stat_key == "pnr_grade":
        specs = (
            {
                "subtype": "close_window",
                "plus_index": 1,
                "opps_index": 2,
                "pct_index": 3,
                "aliases": ("close_window", "cw"),
                "indexes": {"plus": 0, "opps": 1, "pct": 2},
                "plus_keys": ("plus", "cw_plus", "close_window_plus"),
                "opps_keys": ("opps", "cw_opp", "close_window_opp"),
                "pct_keys": ("pct", "cw_pct", "close_window_pct"),
            },
            {
                "subtype": "shut_door",
                "plus_index": 4,
                "opps_index": 5,
                "pct_index": 6,
                "aliases": ("shut_door", "sd"),
                "indexes": {"plus": 3, "opps": 4, "pct": 5},
                "plus_keys": ("plus", "sd_plus", "shut_door_plus"),
                "opps_keys": ("opps", "sd_opp", "shut_door_opp"),
                "pct_keys": ("pct", "sd_pct", "shut_door_pct"),
            },
        )
        ctx["season_rows"] = _normalize_split_rows(ctx.get("season_rows"), specs)
        ctx["last_rows"] = _normalize_split_rows(ctx.get("last_rows"), specs)
        ctx["season_team_totals"] = _normalize_split_totals(ctx.get("season_team_totals"), specs)
        ctx["last_team_totals"] = _normalize_split_totals(ctx.get("last_team_totals"), specs)
        ctx["season_rows_by_subtype"] = _group_by_subtype(ctx["season_rows"])
        ctx["last_rows_by_subtype"] = _group_by_subtype(ctx["last_rows"])
        return ctx

    return ctx
