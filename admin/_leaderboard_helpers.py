from __future__ import annotations

from datetime import date
import inspect
import functools
import re
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Mapping

from sqlalchemy import func
from sqlalchemy.orm import Session

from models.database import Practice, db
from markupsafe import Markup


DualContextResult = Dict[str, Any]


_GRADE_BUCKET_RE = re.compile(r"grade-token--(\d+)")


def _default_context() -> DualContextResult:
    return {
        "season_rows": [],
        "season_team_totals": None,
        "last_rows": [],
        "last_team_totals": None,
        "last_practice_date": None,
    }


def _supports_date_window(fn: Callable[..., Any]) -> bool:
    """Return ``True`` if ``fn`` accepts date window keyword arguments."""

    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):  # pragma: no cover - builtins without signatures
        return True

    has_var_kw = any(
        param.kind == inspect.Parameter.VAR_KEYWORD for param in sig.parameters.values()
    )
    if has_var_kw:
        return True

    return all(name in sig.parameters for name in ("start_dt", "end_dt"))


def get_last_practice(
    session: Session,
    season_id: Optional[int],
    *,
    start_dt: Optional[date] = None,
    end_dt: Optional[date] = None,
):
    """Return the latest :class:`Practice` for ``season_id``."""

    if season_id is None:
        return None

    query = session.query(Practice).filter(Practice.season_id == season_id)

    date_column = getattr(Practice, "date", None)
    created_at_column = getattr(Practice, "created_at", None)

    normalized_date_expr = None
    if date_column is not None and created_at_column is not None:
        normalized_date_expr = func.coalesce(date_column, func.date(created_at_column))
    elif date_column is not None:
        normalized_date_expr = date_column
    elif created_at_column is not None:
        normalized_date_expr = func.date(created_at_column)

    if date_column is not None:
        query = query.filter(date_column.isnot(None))

    if normalized_date_expr is not None:
        if start_dt is not None:
            query = query.filter(normalized_date_expr >= start_dt)
        if end_dt is not None:
            query = query.filter(normalized_date_expr <= end_dt)

    order_clauses = []

    if created_at_column is not None or date_column is not None:
        if created_at_column is not None and date_column is not None:
            order_clauses.append(func.coalesce(created_at_column, date_column).desc())
        else:
            column = created_at_column or date_column
            if column is not None:
                order_clauses.append(column.desc())

    if date_column is not None:
        order_clauses.append(date_column.desc())

    order_clauses.append(Practice.id.desc())

    return query.order_by(*order_clauses).first()


def with_last_practice(
    session: Session,
    season_id: Optional[int],
    compute_fn: Callable[..., Any],
    **kwargs: Any,
) -> DualContextResult:
    """Return dual compute results including the most recent practice slice."""

    context = _default_context()

    if season_id is None:
        return context

    compute_kwargs = dict(kwargs)
    start_dt: Optional[date] = compute_kwargs.pop("start_dt", None)
    end_dt: Optional[date] = compute_kwargs.pop("end_dt", None)

    season_result = compute_fn(
        session=session,
        season_id=season_id,
        start_dt=start_dt,
        end_dt=end_dt,
        **compute_kwargs,
    )
    season_team_totals, season_rows = _normalize_compute_result(season_result)
    context.update(
        {
            "season_rows": season_rows,
            "season_team_totals": season_team_totals,
        }
    )

    last_practice_kwargs = {}
    if _supports_date_window(get_last_practice):
        if start_dt is not None:
            last_practice_kwargs["start_dt"] = start_dt
        if end_dt is not None:
            last_practice_kwargs["end_dt"] = end_dt

    last_practice = get_last_practice(
        session,
        season_id,
        **last_practice_kwargs,
    )
    if not last_practice:
        return context

    last_practice_date: Optional[date] = getattr(last_practice, "date", None)
    if last_practice_date is None:
        created_at = getattr(last_practice, "created_at", None)
        if created_at is not None:
            last_practice_date = created_at.date()

    if last_practice_date is None:
        return context

    last_result = compute_fn(
        session=session,
        season_id=season_id,
        start_dt=last_practice_date,
        end_dt=last_practice_date,
        **compute_kwargs,
    )
    last_team_totals, last_rows = _normalize_compute_result(last_result)
    context.update(
        {
            "last_rows": last_rows,
            "last_team_totals": last_team_totals,
            "last_practice_date": last_practice_date,
        }
    )

    return context


def build_pnr_gap_help_context(session: Session, season_id: Optional[int], **kwargs: Any) -> DualContextResult:
    """Return combined context for PnR Gap Help including the Low-Man slice."""

    compute_fn: Optional[Callable[..., Any]] = kwargs.pop("compute_fn", None)
    if compute_fn is None:
        from admin.routes import compute_pnr_gap_help as _compute

        compute_fn = _compute

    extra_kwargs = dict(kwargs.pop("extra_kwargs", {}) or {})

    label_set = kwargs.pop("label_set", None)
    if label_set is not None and "label_set" not in extra_kwargs:
        extra_kwargs["label_set"] = label_set

    stat_key = kwargs.pop("stat_key", None)
    if stat_key is not None and "stat_key" not in extra_kwargs:
        extra_kwargs["stat_key"] = stat_key

    start_dt = kwargs.pop("start_dt", None)
    if start_dt is not None:
        extra_kwargs["start_dt"] = start_dt

    end_dt = kwargs.pop("end_dt", None)
    if end_dt is not None:
        extra_kwargs["end_dt"] = end_dt

    if kwargs:
        extra_kwargs.update(kwargs)

    primary_ctx = with_last_practice(
        session,
        season_id,
        compute_fn=compute_fn,
        **extra_kwargs,
    )

    lowman_ctx = with_last_practice(
        session,
        season_id,
        compute_fn=functools.partial(compute_fn, role="low_man"),
        **extra_kwargs,
    )

    context = dict(primary_ctx)
    context.update(
        {
            "pnr_rows": primary_ctx.get("season_rows") or [],
            "pnr_totals": primary_ctx.get("season_team_totals"),
            "pnr_last_rows": primary_ctx.get("last_rows") or [],
            "pnr_last_totals": primary_ctx.get("last_team_totals"),
            "low_rows": lowman_ctx.get("season_rows") or [],
            "low_totals": lowman_ctx.get("season_team_totals"),
            "low_last_rows": lowman_ctx.get("last_rows") or [],
            "low_last_totals": lowman_ctx.get("last_team_totals"),
            "last_practice_date": primary_ctx.get("last_practice_date"),
        }
    )

    return context


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
    session: Optional[Session] = None,
) -> DualContextResult:
    """Return combined season and last-practice leaderboard contexts."""

    if season_id is None:
        return _default_context()

    active_session = session or db.session
    compute_kwargs: Dict[str, Any] = dict(extra_kwargs or {})
    if stat_key is not None:
        compute_kwargs.setdefault("stat_key", stat_key)
    if "label_set" not in compute_kwargs:
        compute_kwargs["label_set"] = label_set

    return with_last_practice(
        active_session,
        season_id,
        compute_fn,
        **compute_kwargs,
    )


# --- Helpers for preparing template-friendly dual leaderboard data ---

_PLAYER_KEYS = ("player_name", "player", "name")
_JERSEY_KEYS = (
    "jersey",
    "jersey_number",
    "uniform_number",
    "number",
    "num",
    "player_number",
)
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


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip()
    except Exception:  # pragma: no cover - defensive
        return None

    if not text:
        return None

    sanitized = text.replace(",", "")
    sanitized = re.sub(r"[^0-9.+-]", "", sanitized)
    if not sanitized or sanitized in {".", "+", "-", "+.", "-."}:
        return None

    try:
        return float(sanitized)
    except (TypeError, ValueError):
        return None


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


def _format_fg_summary(makes: int, attempts: int) -> str:
    return f"{makes}\u2013{attempts}"


def _normalize_fg_pct_rows(rows: Any, *, stat_key: str) -> list[Dict[str, Any]]:
    base = stat_key[:-len("_fg_pct")] if stat_key.endswith("_fg_pct") else stat_key
    make_keys = (f"{base}_makes", "makes", "fgm", "plus")
    attempt_keys = (f"{base}_attempts", "attempts", "fga", "opps")
    pct_keys = (stat_key, "pct", "percentage", f"{base}_pct")
    freq_keys = (f"{base}_freq_pct", "freq_pct", "frequency", "freq")

    normalized: list[Dict[str, Any]] = []

    for row in rows or []:
        player = _resolve_value(row, _PLAYER_KEYS, index=0)
        if player is None:
            continue

        jersey = _resolve_value(row, _JERSEY_KEYS, index=1)
        makes_raw = _resolve_value(row, make_keys, index=1)
        attempts_raw = _resolve_value(row, attempt_keys, index=2)
        pct_raw = _resolve_value(row, pct_keys, index=3)
        freq_raw = _resolve_value(row, freq_keys, index=4)

        makes = _to_int(makes_raw)
        attempts = _to_int(attempts_raw)
        pct = _to_pct(pct_raw, makes, attempts)
        freq = _to_pct(freq_raw)

        normalized.append(
            {
                "player_name": player,
                "jersey": jersey,
                "makes": makes,
                "attempts": attempts,
                "fg_pct": pct,
                "freq": freq,
                "fg_summary": _format_fg_summary(makes, attempts),
            }
        )

    return normalized


def _normalize_fg_pct_totals(totals: Any, *, stat_key: str) -> Optional[Dict[str, Any]]:
    if totals is None:
        return None

    base = stat_key[:-len("_fg_pct")] if stat_key.endswith("_fg_pct") else stat_key
    make_keys = (f"{base}_makes", "makes", "fgm", "plus")
    attempt_keys = (f"{base}_attempts", "attempts", "fga", "opps")
    pct_keys = (stat_key, "pct", "percentage", f"{base}_pct")
    freq_keys = (f"{base}_freq_pct", "freq_pct", "frequency", "freq")

    makes_raw = _resolve_value(totals, make_keys, index=0)
    attempts_raw = _resolve_value(totals, attempt_keys, index=1)
    pct_raw = _resolve_value(totals, pct_keys, index=2)
    freq_raw = _resolve_value(totals, freq_keys, index=3)

    makes = _to_int(makes_raw)
    attempts = _to_int(attempts_raw)
    pct = _to_pct(pct_raw, makes, attempts)
    freq = _to_pct(freq_raw)

    return {
        "makes": makes,
        "attempts": attempts,
        "fg_pct": pct,
        "freq": freq,
        "fg_summary": _format_fg_summary(makes, attempts),
    }


def prepare_dual_context(context: DualContextResult, stat_key: Optional[str]) -> DualContextResult:
    """Normalize ``context`` so templates receive predictable structures."""

    ctx = dict(context)

    if stat_key in {"defense", "collision_gap_help", "overall_gap_help", "overall_low_man", "pass_contest"}:
        row_indexes = {"player": 0, "plus": 1, "opps": 2, "pct": 3}
        total_indexes = {"plus": 0, "opps": 1, "pct": 2}
        ctx["season_rows"] = _normalize_simple_rows(ctx.get("season_rows"), indexes=row_indexes)
        ctx["last_rows"] = _normalize_simple_rows(ctx.get("last_rows"), indexes=row_indexes)
        ctx["season_team_totals"] = _normalize_simple_totals(ctx.get("season_team_totals"), indexes=total_indexes)
        ctx["last_team_totals"] = _normalize_simple_totals(ctx.get("last_team_totals"), indexes=total_indexes)
        return ctx

    contest_keys = {
        "atr_contest_breakdown": "atr",
        "fg2_contest_breakdown": "fg2",
        "fg3_contest_breakdown": "fg3",
    }

    contest_aliases = {
        "fg3_fg_pct": "fg3_contest_breakdown",
    }
    contest_stat_key = contest_aliases.get(stat_key, stat_key)

    if contest_stat_key in contest_keys:
        sc = contest_keys[contest_stat_key]
        specs = (
            {
                "subtype": "contest",
                "plus_keys": (
                    "contest_makes",
                    f"{sc}_contest_makes",
                    f"{sc}_contest_plus",
                ),
                "opps_keys": (
                    "contest_attempts",
                    f"{sc}_contest_attempts",
                    f"{sc}_contest_opps",
                ),
                "pct_keys": (
                    "contest_pct",
                    f"{sc}_contest_pct",
                ),
            },
            {
                "subtype": "late",
                "plus_keys": (
                    "late_makes",
                    f"{sc}_late_makes",
                    f"{sc}_late_plus",
                ),
                "opps_keys": (
                    "late_attempts",
                    f"{sc}_late_attempts",
                    f"{sc}_late_opps",
                ),
                "pct_keys": (
                    "late_pct",
                    f"{sc}_late_pct",
                ),
            },
            {
                "subtype": "no_contest",
                "plus_keys": (
                    "no_contest_makes",
                    f"{sc}_no_contest_makes",
                    f"{sc}_no_contest_plus",
                ),
                "opps_keys": (
                    "no_contest_attempts",
                    f"{sc}_no_contest_attempts",
                    f"{sc}_no_contest_opps",
                ),
                "pct_keys": (
                    "no_contest_pct",
                    f"{sc}_no_contest_pct",
                ),
            },
        )
        ctx["season_rows"] = _normalize_split_rows(ctx.get("season_rows"), specs)
        ctx["last_rows"] = _normalize_split_rows(ctx.get("last_rows"), specs)
        ctx["season_team_totals"] = _normalize_split_totals(ctx.get("season_team_totals"), specs)
        ctx["last_team_totals"] = _normalize_split_totals(ctx.get("last_team_totals"), specs)
        ctx["season_rows_by_subtype"] = _group_by_subtype(ctx.get("season_rows"))
        ctx["last_rows_by_subtype"] = _group_by_subtype(ctx.get("last_rows"))
        return ctx

    if stat_key and stat_key.endswith("_fg_pct"):
        ctx["season_rows"] = _normalize_fg_pct_rows(ctx.get("season_rows"), stat_key=stat_key)
        ctx["last_rows"] = _normalize_fg_pct_rows(ctx.get("last_rows"), stat_key=stat_key)
        ctx["season_team_totals"] = _normalize_fg_pct_totals(ctx.get("season_team_totals"), stat_key=stat_key)
        ctx["last_team_totals"] = _normalize_fg_pct_totals(ctx.get("last_team_totals"), stat_key=stat_key)
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


def _clean_display_value(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def _is_placeholder_value(value: Any, placeholder: str) -> bool:
    cleaned = _clean_display_value(value)
    if cleaned is None:
        return True
    if isinstance(cleaned, str):
        if cleaned == placeholder:
            return True
        normalized = cleaned.lower()
        if normalized in {"na", "n/a", "n.a.", "nan"}:
            return True
        if cleaned in {"-", "--", "—", "–"}:
            return True
    return False


def _format_pct_value(value: Any, placeholder: str = "—") -> str:
    cleaned = _clean_display_value(value)
    if cleaned is None:
        return placeholder

    if isinstance(cleaned, str):
        text = cleaned.strip()
        if not text:
            return placeholder
        if text.endswith("%"):
            text = text[:-1]
        try:
            numeric = float(text)
        except (TypeError, ValueError):
            return placeholder
    else:
        try:
            numeric = float(cleaned)
        except (TypeError, ValueError):
            return placeholder

    return f"{numeric:.1f}%"


def _resolve_column_value(row: Any, keys: Tuple[str, ...]) -> Any:
    for key in keys:
        if not key:
            continue
        value = _resolve_value(row, (key,))
        cleaned = _clean_display_value(value)
        if cleaned is not None:
            return cleaned
    return None


def _parse_column_spec(
    column: str, spec: Any
) -> Tuple[Tuple[str, ...], Optional[str], Any, Optional[str], Optional[str], Optional[str]]:
    formatter: Optional[str] = None
    default_value: Any = None
    grade_metric: Optional[str] = None
    label_override: Optional[str] = None
    subgroup: Optional[str] = None

    if spec is None:
        keys: Tuple[str, ...] = (column,)
    elif isinstance(spec, Mapping):
        raw_keys = spec.get("keys") or spec.get("key") or ()
        if isinstance(raw_keys, str):
            keys = (raw_keys,)
        else:
            keys = tuple(raw_keys)
        formatter = spec.get("format")
        default_value = spec.get("default")
        label_override = spec.get("label") or spec.get("display")
        subgroup_value = spec.get("subgroup")
        if subgroup_value is not None:
            subgroup = str(subgroup_value)
        metric_override = (
            spec.get("metric")
            or spec.get("grade_metric")
            or spec.get("grade_key")
        )
        if metric_override:
            grade_metric = str(metric_override)
    else:
        if isinstance(spec, str):
            keys = (spec,)
        else:
            keys = tuple(spec)

    if not keys:
        keys = (column,)

    return keys, formatter, default_value, grade_metric, label_override, subgroup


def _format_columns_for_source(
    source: Any,
    columns: Sequence[str],
    mapping: Mapping[str, Any],
    pct_set: set[str],
    default_placeholder: str,
    *,
    jersey: Any,
    player: Any,
) -> Dict[str, Any]:
    entry: Dict[str, Any] = {"jersey": jersey, "player": player}

    def _normalize_keys(value: Any) -> Tuple[str, ...]:
        if not value:
            return tuple()
        if isinstance(value, (str, bytes)):
            return (str(value),)
        try:
            return tuple(str(part) for part in value if part not in {None, ""})
        except TypeError:
            return (str(value),)

    for column in columns:
        spec = mapping.get(column)
        keys, formatter, default_value, _, _, _ = _parse_column_spec(column, spec)
        compose_mode: Optional[str] = None
        make_keys: Tuple[str, ...] = tuple()
        attempt_keys: Tuple[str, ...] = tuple()
        make_index: Optional[int] = None
        attempt_index: Optional[int] = None
        if isinstance(spec, Mapping):
            raw_compose = spec.get("compose")
            if raw_compose is not None:
                compose_mode = str(raw_compose)
            if compose_mode:
                make_keys = _normalize_keys(spec.get("make_keys")) or _normalize_keys(
                    spec.get("make_key")
                )
                attempt_keys = _normalize_keys(spec.get("attempt_keys")) or _normalize_keys(
                    spec.get("attempt_key")
                )
                make_index = spec.get("make_index")
                attempt_index = spec.get("attempt_index")

        if compose_mode == "makes_attempts":
            make_value = _resolve_value(source, make_keys or keys, index=make_index)
            attempt_value = _resolve_value(
                source, attempt_keys or keys, index=attempt_index
            )
            if make_value is None and attempt_value is None:
                value = default_value if default_value is not None else default_placeholder
            else:
                value = f"{_to_int(make_value)}-{_to_int(attempt_value)}"
        else:
            value = _resolve_column_value(source, keys)
        if value is None and default_value is not None:
            value = default_value

        if (column in pct_set) or (formatter == "pct"):
            value = _format_pct_value(value, default_placeholder)
        elif value is None:
            value = default_placeholder

        entry[column] = value

    return entry


def format_dual_rows(
    rows: Any,
    columns: Sequence[str],
    column_map: Optional[Mapping[str, Sequence[str]]] = None,
    pct_columns: Optional[Sequence[str]] = None,
    *,
    default_placeholder: str = "—",
) -> list[Dict[str, Any]]:
    """Return display-friendly rows for the dual leaderboard tables."""

    pct_set = {col for col in (pct_columns or [])}
    mapping: Mapping[str, Any] = column_map or {}
    formatted: list[Dict[str, Any]] = []

    for index, row in enumerate(rows or [], start=1):
        if row is None:
            continue

        jersey = _resolve_column_value(row, _JERSEY_KEYS)
        player_name = _resolve_column_value(row, _PLAYER_KEYS)
        entry = _format_columns_for_source(
            row,
            columns,
            mapping,
            pct_set,
            default_placeholder,
            jersey=jersey if jersey is not None else index,
            player=player_name or "",
        )

        formatted.append(entry)

    return formatted


def format_dual_totals(
    totals: Any,
    columns: Sequence[str],
    column_map: Optional[Mapping[str, Sequence[str]]] = None,
    pct_columns: Optional[Sequence[str]] = None,
    *,
    label: str = "Team Totals",
    default_placeholder: str = "—",
) -> Optional[Dict[str, Any]]:
    """Return a formatted totals row for dual leaderboard tables."""

    if not totals:
        return None

    pct_set = {col for col in (pct_columns or [])}
    mapping: Mapping[str, Any] = column_map or {}

    return _format_columns_for_source(
        totals,
        columns,
        mapping,
        pct_set,
        default_placeholder,
        jersey="",
        player=label,
    )


def combine_dual_rows(
    season_rows: Optional[Sequence[Mapping[str, Any]]],
    last_rows: Optional[Sequence[Mapping[str, Any]]],
) -> list[Dict[str, Any]]:
    """Merge season and last-practice rows into paired entries for templates."""

    combined: list[Dict[str, Any]] = []
    last_lookup: dict[tuple[Any, Any], Mapping[str, Any]] = {}

    for entry in last_rows or []:
        if not entry:
            continue
        name = entry.get("player") or entry.get("player_name") or entry.get("name")
        jersey = entry.get("jersey")
        key = (jersey, name)
        if name is None and jersey is None:
            continue
        last_lookup[key] = entry
        if name is not None:
            last_lookup.setdefault((None, name), entry)

    for index, row in enumerate(season_rows or [], start=1):
        if not row:
            continue
        name = row.get("player") or row.get("player_name") or row.get("name") or ""
        jersey = row.get("jersey")
        key = (jersey, name if name else None)
        fallback_key = (None, name if name else None)
        combined.append(
            {
                "jersey": jersey if jersey not in (None, "") else index,
                "player": name,
                "totals": row,
                "last": last_lookup.get(key) or last_lookup.get(fallback_key),
            }
        )
        last_lookup.pop(key, None)
        if fallback_key != key:
            last_lookup.pop(fallback_key, None)

    for key, entry in list(last_lookup.items()):
        if key[0] is not None:
            continue
        name = entry.get("player") or entry.get("player_name") or entry.get("name") or ""
        combined.append(
            {
                "jersey": entry.get("jersey") or "",
                "player": name,
                "totals": None,
                "last": entry,
            }
        )
        last_lookup.pop(key, None)

    return combined


def combine_dual_totals(
    season_totals: Optional[Mapping[str, Any]],
    last_totals: Optional[Mapping[str, Any]],
    *,
    label: str = "Team Totals",
) -> Optional[Dict[str, Any]]:
    """Return a combined totals payload for the dual leaderboard macro."""

    if not season_totals and not last_totals:
        return None

    totals_label = label
    if season_totals and season_totals.get("player"):
        totals_label = season_totals.get("player")
    elif last_totals and last_totals.get("player"):
        totals_label = last_totals.get("player")

    return {
        "label": totals_label,
        "totals": season_totals,
        "last": last_totals,
    }


_SLUG_SANITIZE_RE = re.compile(r"[^a-z0-9]+")


def _slugify_label(label: Any) -> str:
    text = str(label or "").strip().lower()
    if not text:
        return "col"
    replacements = {
        "%": " pct ",
        "+": " plus ",
        "#": " number ",
        "/": " ",
    }
    for target, replacement in replacements.items():
        text = text.replace(target, replacement)
    slug = _SLUG_SANITIZE_RE.sub("_", text).strip("_")
    return slug or "col"


def _extract_numeric_for_column(
    source: Any,
    column: str,
    mapping: Mapping[str, Any],
    pct_set: set[str],
) -> Optional[float]:
    if source is None:
        return None

    spec = mapping.get(column)
    keys, formatter, default_value, _, _, _ = _parse_column_spec(column, spec)
    value = _resolve_column_value(source, keys)
    if value is None and default_value is not None:
        value = default_value

    numeric_value = None
    if isinstance(spec, Mapping):
        raw_numeric = spec.get("numeric_keys") or spec.get("numeric_key")
        numeric_keys: Tuple[str, ...] = ()
        if isinstance(raw_numeric, (str, bytes)):
            numeric_keys = (str(raw_numeric),)
        elif isinstance(raw_numeric, Sequence):
            numeric_keys = tuple(str(key) for key in raw_numeric if key)
        if numeric_keys:
            numeric_value = _resolve_column_value(source, numeric_keys)
            if numeric_value is None and default_value is not None:
                numeric_value = default_value

    if numeric_value is None:
        numeric_value = value

    if (column in pct_set) or (formatter == "pct"):
        return _to_pct(numeric_value)

    return _to_float(numeric_value)


def _build_header_rows_for_columns(
    table_columns: list[Dict[str, Any]],
) -> list[list[Dict[str, Any]]]:
    if not table_columns:
        return []

    has_groups = any(col.get("group") for col in table_columns)
    has_subgroups = any(col.get("subgroup") for col in table_columns)
    header_rows: list[list[Dict[str, Any]]] = []

    if has_groups:
        group_row: list[Dict[str, Any]] = []
        group_segments: list[Dict[str, Any]] = []
        index = 0
        while index < len(table_columns):
            column_entry = table_columns[index]
            group_label = column_entry.get("group")
            if not group_label:
                column_entry["render_header"] = False
                group_row.append(
                    {
                        "type": "column",
                        "column": column_entry,
                        "rowspan": 2 + (1 if has_subgroups else 0),
                        "colspan": 1,
                    }
                )
                index += 1
                continue
            span = 1
            while (
                index + span < len(table_columns)
                and table_columns[index + span].get("group") == group_label
            ):
                span += 1
            group_row.append(
                {
                    "type": "group",
                    "label": group_label,
                    "colspan": span,
                    "rowspan": 1,
                }
            )
            group_segments.append(
                {
                    "label": group_label,
                    "columns": table_columns[index : index + span],
                }
            )
            index += span

        header_rows.append(group_row)

        if has_subgroups:
            subgroup_row: list[Dict[str, Any]] = []
            for segment in group_segments:
                segment_columns = segment.get("columns") or []
                sub_index = 0
                while sub_index < len(segment_columns):
                    seg_column = segment_columns[sub_index]
                    subgroup_label = seg_column.get("subgroup")
                    subgroup_header = seg_column.get("subgroup_header")
                    span = 1
                    while (
                        sub_index + span < len(segment_columns)
                        and segment_columns[sub_index + span].get("subgroup")
                        == subgroup_label
                    ):
                        span += 1
                    label_source = subgroup_header or subgroup_label
                    label_text = str(label_source) if label_source else "\u00A0"
                    subgroup_row.append(
                        {
                            "type": "group",
                            "label": label_text,
                            "colspan": span,
                            "rowspan": 1,
                        }
                    )
                    sub_index += span
            header_rows.append(subgroup_row)
        return header_rows

    if has_subgroups:
        subgroup_row: list[Dict[str, Any]] = []
        index = 0
        while index < len(table_columns):
            column_entry = table_columns[index]
            subgroup_label = column_entry.get("subgroup")
            subgroup_header = column_entry.get("subgroup_header")
            span = 1
            while (
                index + span < len(table_columns)
                and table_columns[index + span].get("subgroup") == subgroup_label
            ):
                span += 1
            label_source = subgroup_header or subgroup_label
            label_text = str(label_source) if label_source else "\u00A0"
            subgroup_row.append(
                {
                    "type": "group",
                    "label": label_text,
                    "colspan": span,
                    "rowspan": 1,
                }
            )
            index += span
        header_rows.append(subgroup_row)

    return header_rows


def build_dual_table(
    *,
    base_columns: Sequence[str],
    season_rows: Optional[Sequence[Mapping[str, Any]]],
    last_rows: Optional[Sequence[Mapping[str, Any]]],
    season_totals: Optional[Mapping[str, Any]],
    last_totals: Optional[Mapping[str, Any]],
    column_map: Optional[Mapping[str, Any]] = None,
    pct_columns: Optional[Sequence[str]] = None,
    left_label: str = "Season Totals",
    right_label: str = "Last Practice",
    totals_label: str = "Team Totals",
    table_id: Optional[str] = None,
    default_sort: Optional[Sequence[Any]] = None,
    default_placeholder: str = "—",
    grade_metrics: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Return render_table-ready payload for dual leaderboard tables."""

    mapping: Mapping[str, Any] = column_map or {}
    pct_set = {col for col in (pct_columns or [])}

    formatted_season = format_dual_rows(
        season_rows,
        base_columns,
        column_map=mapping,
        pct_columns=pct_columns,
        default_placeholder=default_placeholder,
    )
    formatted_last = format_dual_rows(
        last_rows,
        base_columns,
        column_map=mapping,
        pct_columns=pct_columns,
        default_placeholder=default_placeholder,
    )
    formatted_season_totals = format_dual_totals(
        season_totals,
        base_columns,
        column_map=mapping,
        pct_columns=pct_columns,
        default_placeholder=default_placeholder,
        label=totals_label,
    )
    formatted_last_totals = format_dual_totals(
        last_totals,
        base_columns,
        column_map=mapping,
        pct_columns=pct_columns,
        default_placeholder=default_placeholder,
        label=totals_label,
    )

    display_rows = combine_dual_rows(formatted_season, formatted_last)
    display_totals = combine_dual_totals(
        formatted_season_totals,
        formatted_last_totals,
        label=totals_label,
    )

    def _numeric_rows(
        source_rows: Optional[Sequence[Mapping[str, Any]]],
        formatted_rows: Sequence[Mapping[str, Any]],
    ) -> list[Dict[str, Any]]:
        numeric: list[Dict[str, Any]] = []
        for index, formatted in enumerate(formatted_rows):
            raw = None
            if source_rows is not None and index < len(source_rows):
                raw = source_rows[index]
            entry: Dict[str, Any] = {
                "jersey": formatted.get("jersey"),
                "player": formatted.get("player"),
            }
            for column in base_columns:
                display_value = formatted.get(column)
                if _is_placeholder_value(display_value, default_placeholder):
                    entry[column] = None
                    continue
                entry[column] = _extract_numeric_for_column(raw, column, mapping, pct_set)
            numeric.append(entry)
        return numeric

    numeric_season_rows = _numeric_rows(season_rows, formatted_season)
    numeric_last_rows = _numeric_rows(last_rows, formatted_last)

    numeric_season_totals = None
    if season_totals:
        numeric_season_totals = {"player": totals_label}
        formatted_totals = formatted_season_totals or {}
        for column in base_columns:
            display_value = formatted_totals.get(column)
            if _is_placeholder_value(display_value, default_placeholder):
                numeric_season_totals[column] = None
                continue
            numeric_season_totals[column] = _extract_numeric_for_column(
                season_totals, column, mapping, pct_set
            )

    numeric_last_totals = None
    if last_totals:
        numeric_last_totals = {"player": totals_label}
        formatted_last_display = formatted_last_totals or {}
        for column in base_columns:
            display_value = formatted_last_display.get(column)
            if _is_placeholder_value(display_value, default_placeholder):
                numeric_last_totals[column] = None
                continue
            numeric_last_totals[column] = _extract_numeric_for_column(
                last_totals, column, mapping, pct_set
            )

    numeric_rows = combine_dual_rows(numeric_season_rows, numeric_last_rows)
    numeric_totals = combine_dual_totals(
        numeric_season_totals,
        numeric_last_totals,
        label=totals_label,
    )

    column_specs: list[Dict[str, Any]] = []
    canonical_map: Dict[str, Dict[str, Any]] = {}

    for column in base_columns:
        slug = _slugify_label(column)
        (
            spec_keys,
            formatter,
            _,
            grade_metric,
            label_override,
            subgroup,
        ) = _parse_column_spec(column, mapping.get(column))
        canonical_keys = [
            str(key).lower()
            for key in spec_keys
            if isinstance(key, str)
        ]
        is_pct = column in pct_set or formatter == "pct"
        display_label = label_override or column
        column_info = {
            "label": display_label,
            "source_label": column,
            "slug": slug,
            "keys": canonical_keys,
            "formatter": formatter,
            "is_pct": is_pct,
        }
        if grade_metric:
            column_info["grade_metric"] = str(grade_metric)
        if subgroup:
            column_info["subgroup"] = subgroup
        column_specs.append(column_info)
        for key in canonical_keys:
            canonical_map.setdefault(key, column_info)

        lowered_label = str(column).lower()
        if "%" in str(column) or "pct" in lowered_label:
            canonical_map.setdefault("pct", column_info)
        if "opp" in lowered_label or "att" in lowered_label:
            canonical_map.setdefault("opps", column_info)
        if "+" in str(column) or "plus" in lowered_label:
            canonical_map.setdefault("plus", column_info)

    explicit_grade_map: Dict[str, str] = {}
    if grade_metrics:
        for slug, metric in grade_metrics.items():
            if metric:
                explicit_grade_map[str(slug)] = str(metric)

    for spec in column_specs:
        slug = spec["slug"]
        if slug in explicit_grade_map:
            continue
        grade_metric = spec.get("grade_metric")
        if grade_metric:
            explicit_grade_map[slug] = grade_metric
        elif spec.get("is_pct"):
            explicit_grade_map[slug] = slug

    def _resolve_grade_token(metric_key: Optional[str], numeric_value: Any) -> str:
        if not metric_key:
            return ""
        if numeric_value is None or numeric_value == "":
            return ""
        try:
            numeric_float = float(numeric_value)
        except (TypeError, ValueError):
            return ""
        from app.grades import grade_token as _grade_token

        token = _grade_token(metric_key, numeric_float)
        return token or ""

    columns: list[Dict[str, Any]] = [
        {
            "key": "rank",
            "label": "#",
            "align": "right",
            "sortable": True,
            "width": "w-14",
            "value_key": "rank_value",
        },
        {
            "key": "player",
            "label": "Player",
            "align": "left",
            "sortable": True,
            "cell_class": "font-semibold",
        },
    ]

    for spec in column_specs:
        cell_class = "font-semibold" if spec.get("is_pct") else None
        column_entry = {
            "key": f"totals_{spec['slug']}",
            "label": spec["label"],
            "align": "right",
            "sortable": True,
            "value_key": f"totals_{spec['slug']}_value",
            "group": left_label,
        }
        if cell_class:
            column_entry["cell_class"] = cell_class
        if spec.get("subgroup"):
            column_entry["subgroup"] = spec["subgroup"]
        if spec.get("subgroup_header"):
            column_entry["subgroup_header"] = spec["subgroup_header"]
        columns.append(column_entry)

    for spec in column_specs:
        cell_class = "font-semibold" if spec.get("is_pct") else None
        column_entry = {
            "key": f"last_{spec['slug']}",
            "label": spec["label"],
            "align": "right",
            "sortable": True,
            "value_key": f"last_{spec['slug']}_value",
            "group": right_label,
        }
        if cell_class:
            column_entry["cell_class"] = cell_class
        if spec.get("subgroup"):
            column_entry["subgroup"] = spec["subgroup"]
        if spec.get("subgroup_header"):
            column_entry["subgroup_header"] = spec["subgroup_header"]
        columns.append(column_entry)

    header_rows = _build_header_rows_for_columns(columns)

    rows: list[Dict[str, Any]] = []
    for index, display in enumerate(display_rows):
        numeric = numeric_rows[index] if index < len(numeric_rows) else {}
        totals_display = display.get("totals") or {}
        last_display = display.get("last") or {}
        totals_numeric = (numeric.get("totals") or {}) if numeric else {}
        last_numeric = (numeric.get("last") or {}) if numeric else {}

        jersey = display.get("jersey")
        if jersey in (None, ""):
            jersey_display = index + 1
        else:
            jersey_display = jersey

        rank_value = _to_float(jersey_display)
        if rank_value is None:
            rank_value = float(index + 1)

        row_entry: Dict[str, Any] = {
            "rank": jersey_display,
            "rank_value": rank_value,
            "player": display.get("player") or "",
        }

        for spec in column_specs:
            source_label = spec.get("source_label") or spec["label"]
            slug = spec["slug"]
            total_value = totals_display.get(source_label, default_placeholder)
            last_value = last_display.get(source_label, default_placeholder)
            row_entry[f"totals_{slug}"] = total_value
            value_numeric = (
                totals_numeric.get(source_label) if totals_numeric else None
            )
            if value_numeric is not None:
                row_entry[f"totals_{slug}_value"] = value_numeric
            else:
                row_entry[f"totals_{slug}_value"] = ""
            metric_key = explicit_grade_map.get(slug)
            row_entry[f"totals_{slug}_token"] = _resolve_grade_token(
                metric_key,
                row_entry.get(f"totals_{slug}_value") or value_numeric,
            )
            row_entry[f"last_{slug}"] = last_value
            value_numeric = (
                last_numeric.get(source_label) if last_numeric else None
            )
            if value_numeric is not None:
                row_entry[f"last_{slug}_value"] = value_numeric
            else:
                row_entry[f"last_{slug}_value"] = ""
            row_entry[f"last_{slug}_token"] = _resolve_grade_token(
                metric_key,
                row_entry.get(f"last_{slug}_value") or value_numeric,
            )

        rows.append(row_entry)

    totals_row: Optional[Dict[str, Any]] = None
    if display_totals:
        totals_display_data = display_totals.get("totals") or {}
        last_totals_display = display_totals.get("last") or {}
        totals_label_value = display_totals.get("label") or totals_label
        totals_numeric_data = (numeric_totals.get("totals") or {}) if numeric_totals else {}
        last_numeric_data = (numeric_totals.get("last") or {}) if numeric_totals else {}

        totals_row = {
            "rank": "",
            "player": totals_label_value,
        }

        for spec in column_specs:
            source_label = spec.get("source_label") or spec["label"]
            slug = spec["slug"]
            totals_row[f"totals_{slug}"] = totals_display_data.get(
                source_label, default_placeholder
            )
            totals_row[f"last_{slug}"] = last_totals_display.get(
                source_label, default_placeholder
            )
            totals_numeric_value = totals_numeric_data.get(source_label)
            last_numeric_value = last_numeric_data.get(source_label)
            if totals_numeric_value is not None:
                totals_row[f"totals_{slug}_value"] = totals_numeric_value
            if last_numeric_value is not None:
                totals_row[f"last_{slug}_value"] = last_numeric_value
            metric_key = explicit_grade_map.get(slug)
            totals_row[f"totals_{slug}_token"] = _resolve_grade_token(
                metric_key,
                totals_row.get(f"totals_{slug}_value") or totals_numeric_value,
            )
            totals_row[f"last_{slug}_token"] = _resolve_grade_token(
                metric_key,
                totals_row.get(f"last_{slug}_value") or last_numeric_value,
            )

    def _resolve_sort_key(key: str) -> Optional[str]:
        lowered = key.lower()
        if lowered in {"player", "name"}:
            return "player"
        if lowered in {"rank", "jersey", "#"}:
            return "rank"
        if lowered.startswith("totals_") or lowered.startswith("last_"):
            return lowered
        for spec in column_specs:
            if lowered == spec["slug"] or lowered == str(spec["label"]).lower():
                return f"totals_{spec['slug']}"
        mapped = canonical_map.get(lowered)
        if mapped:
            return f"totals_{mapped['slug']}"
        return None

    default_sort_sequence: list[Any] = []
    if default_sort:
        default_sort_sequence.extend(default_sort)
    else:
        default_sort_sequence.extend([
            ("pct", "desc"),
            ("opps", "desc"),
            ("plus", "desc"),
        ])

    resolved_sort: list[Tuple[str, str]] = []
    for item in default_sort_sequence:
        if isinstance(item, (list, tuple)):
            key = item[0]
            direction = item[1] if len(item) > 1 else "desc"
        else:
            key = item
            direction = "desc"
        key_str = str(key).strip()
        if not key_str:
            continue
        direction_str = str(direction).strip().lower()
        if direction_str not in {"asc", "desc"}:
            direction_str = "desc"
        resolved_key = _resolve_sort_key(key_str)
        if not resolved_key:
            continue
        resolved_sort.append((resolved_key, direction_str))

    # Ensure Player ascending is always a final tie-breaker
    if not any(key == "player" for key, _ in resolved_sort):
        resolved_sort.append(("player", "asc"))

    # Remove duplicates while preserving order
    seen_sort_keys: set[str] = set()
    sort_parts: list[str] = []
    for key, direction in resolved_sort:
        if key in seen_sort_keys:
            continue
        seen_sort_keys.add(key)
        sort_parts.append(f"{key}:{direction}")

    default_sort_value = ";".join(sort_parts)

    return {
        "id": table_id,
        "columns": columns,
        "rows": rows,
        "totals": totals_row,
        "default_sort": default_sort_value,
        "has_data": bool(rows) or bool(totals_row),
        "header_rows": header_rows,
    }


def split_dual_table(
    table_data: Optional[Mapping[str, Any]],
    *,
    prefix: str,
    table_id_suffix: Optional[str] = None,
) -> Dict[str, Any]:
    """Return a single-group table derived from a dual leaderboard table."""

    if not table_data:
        return {
            "id": None,
            "columns": [],
            "rows": [],
            "totals": None,
            "default_sort": "",
            "has_data": False,
        }

    normalized_prefix = str(prefix or "").strip()
    if normalized_prefix not in {"totals_", "last_"}:
        raise ValueError("prefix must be 'totals_' or 'last_'")

    columns = list(table_data.get("columns") or [])
    shared_columns: list[Dict[str, Any]] = []
    prefixed_columns: list[Dict[str, Any]] = []

    for column in columns:
        key = str(column.get("key") or "")
        column_copy = dict(column)
        # ``render_header`` is injected when building the combined dual table so
        # that certain columns (like rank/player) can span multiple header
        # rows. Once we split the table into a standalone view, those helper
        # flags would suppress the header cells entirely, which causes the
        # column groups to shift left. Reset the flag so the headers render as
        # normal in the split table.
        column_copy.pop("render_header", None)
        if key.startswith(normalized_prefix):
            column_copy.pop("group", None)
            prefixed_columns.append(column_copy)
        elif key.startswith("totals_") or key.startswith("last_"):
            continue
        else:
            shared_columns.append(column_copy)

    selected_columns = shared_columns + prefixed_columns
    header_rows = _build_header_rows_for_columns(selected_columns)

    allowed_keys: set[str] = {"rank", "rank_value", "player"}
    for column in selected_columns:
        key = column.get("key")
        if key:
            allowed_keys.add(str(key))
            allowed_keys.add(f"{key}_token")
        value_key = column.get("value_key")
        if value_key:
            allowed_keys.add(str(value_key))

    rows: list[Dict[str, Any]] = []
    for row in table_data.get("rows") or []:
        new_row: Dict[str, Any] = {}
        for key in allowed_keys:
            if key in row:
                new_row[key] = row[key]
        rows.append(new_row)

    totals: Optional[Dict[str, Any]] = None
    base_totals = table_data.get("totals")
    if base_totals:
        totals = {}
        for key in allowed_keys:
            if key in base_totals:
                totals[key] = base_totals[key]
        if not totals:
            totals = None

    default_sort = str(table_data.get("default_sort") or "")
    if normalized_prefix == "last_" and default_sort:
        default_sort = default_sort.replace("totals_", "last_")

    base_id = table_data.get("id")
    resolved_id = None
    if base_id:
        suffix = table_id_suffix or normalized_prefix.rstrip("_")
        resolved_id = f"{base_id}-{suffix}"

    return {
        "id": resolved_id,
        "columns": selected_columns,
        "rows": rows,
        "totals": totals,
        "default_sort": default_sort,
        "has_data": bool(rows) or bool(totals),
        "header_rows": header_rows,
    }


def build_leaderboard_table(
    *,
    config: Optional[Mapping[str, Any]],
    rows: Optional[Sequence[Any]],
    team_totals: Optional[Any] = None,
    table_id: Optional[str] = None,
    default_sort: Optional[Sequence[Any]] = None,
    default_placeholder: str = "—",
) -> Dict[str, Any]:
    """Return ``render_table`` payload for standard (non-dual) leaderboards."""

    cfg = dict(config or {})
    stat_key = str(cfg.get("key") or "").strip()
    label = cfg.get("label") or stat_key.title() or "Stat"

    grade_metric_map: Dict[str, str] = {}
    shrink_gradient_slugs: set[str] = set()
    if stat_key == "fg3_fg_pct":
        grade_metric_map.update(
            {
                "fg_pct": "fg3_pct",
                "shrink_3fg_pct": "fg3_pct",
                "non_shrink_3fg_pct": "fg3_pct",
            }
        )
        shrink_gradient_slugs.update({"shrink_3fg_pct", "non_shrink_3fg_pct"})

    def _spec(
        column_label: str,
        *,
        keys: Optional[Sequence[str]] = None,
        index: Optional[int] = None,
        fmt: str = "float",
        precision: Optional[int] = None,
        strip_trailing: bool = True,
        align: str = "right",
        slug: Optional[str] = None,
        default: bool = False,
        default_order: int = 0,
        default_direction: str = "desc",
        compose: Optional[str] = None,
        make_keys: Optional[Sequence[str]] = None,
        make_index: Optional[int] = None,
        attempt_keys: Optional[Sequence[str]] = None,
        attempt_index: Optional[int] = None,
        sort_source: str = "value",
    ) -> Dict[str, Any]:
        return {
            "label": column_label,
            "keys": tuple(keys or ()),
            "index": index,
            "format": fmt,
            "precision": precision,
            "strip_trailing": strip_trailing,
            "align": align,
            "slug": slug or _slugify_label(column_label),
            "default_sort": default,
            "default_order": default_order,
            "default_direction": default_direction,
            "compose": compose,
            "make_keys": tuple(make_keys or ()),
            "make_index": make_index,
            "attempt_keys": tuple(attempt_keys or ()),
            "attempt_index": attempt_index,
            "sort_source": sort_source,
        }

    column_specs: list[Dict[str, Any]] = []

    if stat_key == "assist_summary":
        column_specs.extend(
            [
                _spec("Ast", keys=("assists", "ast"), index=1, fmt="int", default=True),
                _spec("Pot Ast", keys=("pot_assists", "pot_ast"), index=2, fmt="int"),
                _spec("2nd Ast", keys=("second_assists", "sec_ast"), index=3, fmt="int"),
                _spec("TO", keys=("turnovers", "tos"), index=4, fmt="int"),
                _spec(
                    "AST/TO",
                    keys=("assist_turnover_ratio", "ast_to"),
                    index=5,
                    fmt="float",
                    precision=2,
                ),
                _spec(
                    "Adj AST/TO",
                    keys=("adj_assist_turnover_ratio", "adj_ast_to"),
                    index=6,
                    fmt="float",
                    precision=2,
                ),
            ]
        )
    elif stat_key == "offense_summary":
        column_specs.extend(
            [
                _spec(
                    "Off Poss",
                    keys=("offensive_possessions", "poss"),
                    index=1,
                    fmt="int",
                    default=True,
                    default_order=2,
                ),
                _spec(
                    "PPP On",
                    keys=("ppp_on",),
                    index=2,
                    fmt="float",
                    default=True,
                ),
                _spec(
                    "PPP Off",
                    keys=("ppp_off",),
                    index=3,
                    fmt="float",
                ),
                _spec(
                    "Ind TO Rate (Poss.)",
                    keys=("individual_turnover_rate", "ind_to_rate"),
                    index=4,
                    fmt="float",
                    precision=1,
                ),
                _spec(
                    "TO % (Bamalytics)",
                    keys=("bamalytics_turnover_rate", "bama_to_rate"),
                    index=5,
                    fmt="float",
                    precision=1,
                    strip_trailing=False,
                ),
                _spec(
                    "% of TO's (NBA.com)",
                    keys=("individual_team_turnover_pct", "team_to_pct"),
                    index=6,
                    fmt="float",
                    precision=1,
                    strip_trailing=False,
                ),
                _spec(
                    "Team TO Rate",
                    keys=("turnover_rate", "team_to_rate"),
                    index=7,
                    fmt="float",
                    precision=1,
                ),
                _spec(
                    "Ind Off Reb%",
                    keys=("individual_off_reb_rate", "ind_oreb_pct"),
                    index=8,
                    fmt="float",
                    precision=1,
                ),
                _spec(
                    "Off Reb Rate",
                    keys=("off_reb_rate",),
                    index=9,
                    fmt="float",
                    precision=1,
                ),
                _spec(
                    "Ind Fouls Drawn%",
                    keys=("individual_foul_rate", "ind_foul_rate"),
                    index=10,
                    fmt="float",
                    precision=1,
                ),
                _spec(
                    "Fouls Rate",
                    keys=("fouls_drawn_rate", "foul_rate"),
                    index=11,
                    fmt="float",
                    precision=1,
                ),
            ]
        )
    elif stat_key.endswith("_fg_pct"):
        base = stat_key.replace("_fg_pct", "")
        make_keys = (f"{base}_makes", "makes", "fgm")
        attempt_keys = (f"{base}_attempts", "attempts", "fga")

        column_specs.append(
            _spec(
                "FG",
                compose="makes_attempts",
                make_keys=make_keys,
                make_index=1,
                attempt_keys=attempt_keys,
                attempt_index=2,
                align="center",
                sort_source="makes",
                default=True,
                default_order=2,
            )
        )
        column_specs.append(
            _spec(
                "FG%",
                keys=(stat_key, "pct"),
                index=3,
                fmt="pct",
                default=True,
            )
        )
        column_specs.append(
            _spec(
                "Freq",
                keys=(f"{base}_freq_pct", "freq"),
                index=4,
                fmt="pct",
            )
        )

        if stat_key == "atr_fg_pct":
            grade_metric_map["fg_pct"] = "atr2fg_pct"

        if stat_key == "fg3_fg_pct":
            column_specs.append(
                _spec(
                    "Shrink 3FG",
                    compose="makes_attempts",
                    make_keys=("fg3_shrink_makes", "shrink_makes"),
                    make_index=5,
                    attempt_keys=("fg3_shrink_att", "shrink_attempts"),
                    attempt_index=6,
                    align="center",
                    sort_source="makes",
                )
            )
            column_specs.append(
                _spec(
                    "Shrink 3FG %",
                    keys=("fg3_shrink_pct", "shrink_pct"),
                    index=7,
                    fmt="pct",
                )
            )
            column_specs.append(
                _spec(
                    "Shrink 3FG Freq",
                    keys=("fg3_shrink_freq_pct", "shrink_freq"),
                    index=8,
                    fmt="pct",
                )
            )
            column_specs.append(
                _spec(
                    "Non-Shrink 3FG",
                    compose="makes_attempts",
                    make_keys=("fg3_nonshrink_makes", "nonshrink_makes"),
                    make_index=9,
                    attempt_keys=("fg3_nonshrink_att", "nonshrink_attempts"),
                    attempt_index=10,
                    align="center",
                    sort_source="makes",
                )
            )
            column_specs.append(
                _spec(
                    "Non-Shrink 3FG %",
                    keys=("fg3_nonshrink_pct", "nonshrink_pct"),
                    index=11,
                    fmt="pct",
                )
            )
            column_specs.append(
                _spec(
                    "Non-Shrink 3FG Freq",
                    keys=("fg3_nonshrink_freq_pct", "nonshrink_freq"),
                    index=12,
                    fmt="pct",
                )
            )
    else:
        fmt = cfg.get("format") or "float"
        column_specs.append(
            _spec(
                label,
                keys=(stat_key,),
                index=1,
                fmt="pct" if fmt == "pct" else ("int" if fmt == "int" else "float"),
                precision=1 if fmt == "pct" else None,
                default=True,
                strip_trailing=fmt != "pct",
            )
        )

    if grade_metric_map:
        for spec in column_specs:
            slug = spec.get("slug")
            if not slug:
                continue
            metric = grade_metric_map.get(slug)
            if metric:
                spec["grade_metric"] = metric

    columns: list[Dict[str, Any]] = [
        {
            "key": "rank",
            "label": "#",
            "align": "right",
            "sortable": True,
            "width": "w-14",
            "value_key": "rank_value",
        },
        {
            "key": "player",
            "label": "Player",
            "align": "left",
            "sortable": True,
            "cell_class": "font-semibold",
        },
    ]

    for spec in column_specs:
        key = spec["slug"]
        spec["key"] = key
        spec["value_key"] = f"{key}_value"
        cell_class = "font-semibold" if spec.get("format") == "pct" else None
        column_entry = {
            "key": key,
            "label": spec["label"],
            "align": spec["align"],
            "sortable": True,
            "value_key": spec["value_key"],
        }
        if cell_class:
            column_entry["cell_class"] = cell_class
        columns.append(column_entry)

    def _format_composed(
        source: Any,
        spec: Dict[str, Any],
    ) -> tuple[str, Optional[float]]:
        make = _resolve_value(source, spec.get("make_keys", ()), index=spec.get("make_index"))
        attempt = _resolve_value(
            source,
            spec.get("attempt_keys", ()),
            index=spec.get("attempt_index"),
        )
        make_int = _to_int(make)
        attempt_int = _to_int(attempt)
        display = f"{make_int}-{attempt_int}"
        if spec.get("sort_source") == "attempts":
            numeric = _to_float(attempt)
        else:
            numeric = _to_float(make)
        return display, numeric

    def _format_value(
        source: Any,
        spec: Dict[str, Any],
    ) -> tuple[str, Optional[float]]:
        if spec.get("compose") == "makes_attempts":
            return _format_composed(source, spec)

        value = _resolve_value(source, spec.get("keys", ()), index=spec.get("index"))

        fmt = spec.get("format")
        if fmt == "int":
            numeric = _to_float(value)
            if numeric is None:
                return default_placeholder, None
            return str(int(round(numeric))), numeric
        if fmt == "float":
            numeric = _to_float(value)
            if numeric is None:
                return default_placeholder, None
            precision = spec.get("precision")
            if precision is not None:
                formatted = f"{numeric:.{precision}f}"
                if spec.get("strip_trailing", True) and precision > 0:
                    formatted = formatted.rstrip("0").rstrip(".")
                    if not formatted:
                        formatted = "0"
            else:
                formatted = str(numeric)
            return formatted, numeric
        if fmt == "pct":
            pct_value = _to_pct(value)
            if pct_value is None:
                return default_placeholder, None
            display = _format_pct_value(pct_value, default_placeholder)
            return display, pct_value

        text = _clean_display_value(value)
        if text is None:
            return default_placeholder, None
        return str(text), None

    def _resolve_grade_token(metric_key: Optional[str], numeric_value: Any) -> str:
        if not metric_key:
            return ""
        if numeric_value is None or numeric_value == "":
            return ""
        try:
            numeric_float = float(numeric_value)
        except (TypeError, ValueError):
            return ""
        from app.grades import grade_token as _grade_token

        token = _grade_token(metric_key, numeric_float)
        return token or ""

    def _wrap_percent_display(display_text: str, slug: str, grade_token: str) -> Markup:
        classes = ["percent-box"]
        bucket: Optional[str] = None
        if grade_token:
            classes.extend(part for part in grade_token.split() if part)
            match = _GRADE_BUCKET_RE.search(grade_token)
            if match:
                bucket = match.group(1)
        if slug in shrink_gradient_slugs and bucket is not None:
            classes.append(f"shrink-3fg--{bucket}")
        class_attr = " ".join(classes)
        return Markup(f'<span class="{class_attr}">{Markup.escape(display_text)}</span>')

    row_entries: list[Dict[str, Any]] = []
    for index, row in enumerate(rows or [], start=1):
        player = _resolve_value(row, _PLAYER_KEYS, index=0) or ""
        rank_value = float(index)
        entry: Dict[str, Any] = {
            "rank": index,
            "rank_value": rank_value,
            "player": player,
        }

        for spec in column_specs:
            display, numeric = _format_value(row, spec)
            metric_key = spec.get("grade_metric")
            grade_token = _resolve_grade_token(metric_key, numeric) if metric_key else ""
            display_text = display if display is not None else default_placeholder
            if spec["slug"] in grade_metric_map:
                entry[spec["key"]] = _wrap_percent_display(
                    str(display_text), spec["slug"], grade_token
                )
            else:
                entry[spec["key"]] = display_text
            if numeric is not None:
                entry[spec["value_key"]] = numeric

        row_entries.append(entry)

    totals_entry: Optional[Dict[str, Any]] = None
    if team_totals is not None:
        totals_entry = {"rank": "", "player": "Team Totals"}
        for spec in column_specs:
            display, numeric = _format_value(team_totals, spec)
            metric_key = spec.get("grade_metric")
            grade_token = _resolve_grade_token(metric_key, numeric) if metric_key else ""
            display_text = display if display is not None else default_placeholder
            if spec["slug"] in grade_metric_map:
                totals_entry[spec["key"]] = _wrap_percent_display(
                    str(display_text), spec["slug"], grade_token
                )
            else:
                totals_entry[spec["key"]] = display_text
            if numeric is not None:
                totals_entry[spec["value_key"]] = numeric

    sort_sequence: list[Tuple[str, str]] = []
    if default_sort:
        for item in default_sort:
            if isinstance(item, (list, tuple)):
                key = str(item[0]).strip()
                direction = str(item[1] if len(item) > 1 else "desc").lower()
            else:
                key = str(item).strip()
                direction = "desc"
            if not key:
                continue
            if direction not in {"asc", "desc"}:
                direction = "desc"
            sort_sequence.append((key, direction))
    else:
        ordered = sorted(
            (spec for spec in column_specs if spec.get("default_sort")),
            key=lambda s: s.get("default_order", 0),
        )
        for spec in ordered:
            sort_sequence.append((spec["key"], spec.get("default_direction", "desc")))
        if not sort_sequence and column_specs:
            sort_sequence.append((column_specs[0]["key"], "desc"))

    if not any(key == "player" for key, _ in sort_sequence):
        sort_sequence.append(("player", "asc"))

    seen_sort: set[str] = set()
    sort_parts: list[str] = []
    for key, direction in sort_sequence:
        if key in seen_sort:
            continue
        seen_sort.add(key)
        sort_parts.append(f"{key}:{direction}")

    resolved_table_id = table_id or (f"leaderboard-{stat_key}" if stat_key else None)

    return {
        "id": resolved_table_id,
        "columns": columns,
        "rows": row_entries,
        "totals": totals_entry,
        "default_sort": ";".join(sort_parts),
        "has_data": bool(row_entries) or bool(totals_entry),
    }
