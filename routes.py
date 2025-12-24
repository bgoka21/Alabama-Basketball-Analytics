import os
import csv
import re
from io import StringIO
from typing import Dict, Iterable, Mapping, Optional, List
import pandas as pd
from flask import render_template, jsonify, request, current_app, make_response, abort, redirect, url_for, flash
from werkzeug.utils import secure_filename
from app import app, db, PDFKIT_CONFIG, PDF_OPTIONS
from sqlalchemy import func, or_
from models import Possession, PossessionPlayer, ShotDetail
from models.database import PlayerDraftStock
from admin.routes import (
    collect_practice_labels,
    compute_filtered_totals,
    compute_filtered_blue,
    aggregate_stats,
    compute_team_shot_details,
)
from models.database import (
    PlayerStats,
    Practice,
    BlueCollarStats,
    Game,
    Season,
    RecordDefinition,
    RecordEntry,
    Roster,
)
from datetime import date
from types import SimpleNamespace
from flask_login import login_required
from utils.auth import admin_required
from utils.leaderboard_helpers import (
    get_on_off_summary,
    get_turnover_rates_onfloor,
    get_rebound_rates_onfloor,
)
import pdfkit
from public.routes import game_homepage, season_leaderboard
from admin.routes import player_detail
from clients.synergy_client import SynergyDataCoreClient, SynergyAPI
from app.utils.table_cells import num, pct
from utils.records.qualifications import get_threshold
from utils.records.stat_keys import get_label_for_key
# BEGIN Advanced Possession
from services.reports.advanced_possession import (
    cache_get_or_compute_adv_poss_game,
    cache_get_or_compute_adv_poss_practice,
)
# END Advanced Possession
# BEGIN Playcall Report
from services.reports.playcall import (
    aggregate_playcall_reports,
    cache_get_or_compute_playcall_report,
)
# END Playcall Report


_FLOW_PREFIX_PATTERN = re.compile(r"^\s*flow\s*[–-]\s*", flags=re.IGNORECASE)


def _normalize_flow_label(label: object) -> str:
    """Normalize a FLOW playcall label for display or export."""
    if not isinstance(label, str):
        if label is None:
            return ""
        label = str(label)
    trimmed = label.strip()
    normalized = _FLOW_PREFIX_PATTERN.sub("", trimmed)
    return normalized.strip()



def _flatten_playcall_series(series_payload: Mapping[str, object]) -> Dict[str, object]:
    """Flatten a playcall series payload into row-level entries with totals."""

    rows: List[Dict[str, object]] = []
    totals = {
        "ran": 0,
        "off_set": {"pts": 0, "chances": 0, "ppc": 0.0},
        "in_flow": {"pts": 0, "chances": 0, "ppc": 0.0},
    }

    if not isinstance(series_payload, Mapping):
        return {"rows": rows, "totals": totals}

    seen_playcalls: set[str] = set()
    flow_payload: Mapping[str, object] = {}

    for family_name, payload in series_payload.items():
        if not isinstance(payload, Mapping):
            continue
        if isinstance(family_name, str) and family_name.upper() == "FLOW":
            flow_payload = payload
            continue

        if not isinstance(family_name, str):
            continue

        family_upper = family_name.upper()
        treat_as_misc = family_upper in ("UKNOWN", "UNKNOWN", "MISC")
        family_label = "MISC" if treat_as_misc else family_name

        plays_map = payload.get("plays") if isinstance(payload.get("plays"), Mapping) else {}
        if not isinstance(plays_map, Mapping):
            continue

        for playcall, entry in plays_map.items():
            if not isinstance(entry, Mapping):
                continue

            playcall_label = playcall if isinstance(playcall, str) else str(playcall)
            normalized_playcall = playcall_label.strip()
            display_playcall = normalized_playcall or playcall_label

            if treat_as_misc and (not normalized_playcall or normalized_playcall.upper() == "UNKNOWN"):
                continue

            ran_val = int(entry.get("ran", 0) or 0)
            off_set = entry.get("off_set", {}) if isinstance(entry.get("off_set"), Mapping) else {}
            in_flow = entry.get("in_flow", {}) if isinstance(entry.get("in_flow"), Mapping) else {}

            off_pts = int((off_set or {}).get("pts", 0) or 0)
            off_chances = int((off_set or {}).get("chances", 0) or 0)
            off_ppc = float((off_set or {}).get("ppc", 0.0) or 0.0)

            in_pts = int((in_flow or {}).get("pts", 0) or 0)
            in_chances = int((in_flow or {}).get("chances", 0) or 0)
            in_ppc = float((in_flow or {}).get("ppc", 0.0) or 0.0)

            rows.append(
                {
                    "series": family_label,
                    "playcall": display_playcall,
                    "ran": ran_val,
                    "off_set": {"pts": off_pts, "chances": off_chances, "ppc": off_ppc},
                    "in_flow": {"pts": in_pts, "chances": in_chances, "ppc": in_ppc},
                }
            )

            totals["ran"] += ran_val
            totals["off_set"]["pts"] += off_pts
            totals["off_set"]["chances"] += off_chances
            totals["in_flow"]["pts"] += in_pts
            totals["in_flow"]["chances"] += in_chances

            seen_playcalls.add(display_playcall)

    if isinstance(flow_payload, Mapping):
        plays_payload = flow_payload.get("plays")
        if isinstance(plays_payload, Iterable):
            for entry in plays_payload:
                if not isinstance(entry, Mapping):
                    continue

                playcall = _normalize_flow_label(entry.get("playcall", ""))
                if not playcall or playcall.upper() == "UNKNOWN":
                    continue

                if playcall in seen_playcalls:
                    continue

                ran_in_flow = int(entry.get("ran_in_flow", 0) or 0)
                in_flow = entry.get("in_flow", {}) if isinstance(entry.get("in_flow"), Mapping) else {}

                in_pts = int((in_flow or {}).get("pts", 0) or 0)
                in_chances = int((in_flow or {}).get("chances", 0) or 0)
                in_ppc = float((in_flow or {}).get("ppc", 0.0) or 0.0)

                rows.append(
                    {
                        "series": "FLOW",
                        "playcall": playcall,
                        "ran": ran_in_flow,
                        "off_set": {"pts": 0, "chances": 0, "ppc": 0.0},
                        "in_flow": {"pts": in_pts, "chances": in_chances, "ppc": in_ppc},
                    }
                )

                totals["ran"] += ran_in_flow
                totals["in_flow"]["pts"] += in_pts
                totals["in_flow"]["chances"] += in_chances

    off_total_chances = totals["off_set"]["chances"] or 0
    in_total_chances = totals["in_flow"]["chances"] or 0

    totals["off_set"]["ppc"] = (
        totals["off_set"]["pts"] / off_total_chances if off_total_chances else 0.0
    )
    totals["in_flow"]["ppc"] = (
        totals["in_flow"]["pts"] / in_total_chances if in_total_chances else 0.0
    )

    return {"rows": rows, "totals": totals}


RECORD_TAB_LABELS = {
    "team": "Team",
    "player": "Player",
    "opponent": "Opponent",
    "blue_collar": "Blue Collar",
}
RECORD_SCOPE_LABELS = {
    "GAME": "Single Game",
    "SEASON": "Single Season",
    "CAREER": "Career",
}


def _parse_record_tab(raw_tab: str | None) -> str:
    if not raw_tab:
        return "team"
    normalized = raw_tab.strip().lower()
    return normalized if normalized in RECORD_TAB_LABELS else "team"


def _parse_record_scope(raw_scope: str | None) -> str:
    if not raw_scope:
        return "GAME"
    normalized = raw_scope.strip().upper()
    return normalized if normalized in RECORD_SCOPE_LABELS else "GAME"


def _format_record_value(value: float | None) -> str:
    if value is None:
        return "—"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return f"{value:.3f}".rstrip("0").rstrip(".")


def _format_record_date(value: date | None) -> str:
    if not value:
        return "Date TBD"
    return value.strftime("%b %d, %Y")


def _build_record_entry_payload(
    entry: RecordEntry,
    *,
    scope: str,
    roster_lookup: dict[int, str],
    game_lookup: dict[int, Game],
) -> dict[str, object]:
    game = game_lookup.get(entry.game_id) if entry.game_id else None
    if entry.holder_entity_type == "PLAYER":
        holder = roster_lookup.get(entry.holder_player_id) or entry.holder_player_name or "—"
    elif entry.holder_entity_type == "OPPONENT":
        holder = entry.holder_opponent_name or (game.opponent_name if game else "Unknown Opponent")
    else:
        holder = "Alabama"

    occurred_on = entry.occurred_on or (game.game_date if game else None)
    opponent_name = entry.holder_opponent_name or (game.opponent_name if game else None)
    game_url = url_for("admin.game_stats", game_id=entry.game_id) if game else None

    return {
        "id": entry.id,
        "holder": holder,
        "value_display": _format_record_value(entry.value),
        "scope": scope,
        "season_year": entry.season_year,
        "occurred_on": occurred_on,
        "occurred_on_display": _format_record_date(occurred_on),
        "opponent": opponent_name,
        "game_url": game_url,
        "is_current": bool(entry.is_current),
        "is_forced": bool(entry.is_forced_current),
        "source_type": entry.source_type,
    }


def _load_record_book(category: str, scope: str) -> dict[str, object]:
    definitions = (
        RecordDefinition.query.filter_by(
            is_active=True,
            category=category,
            scope=scope,
        )
        .order_by(RecordDefinition.name.asc())
        .all()
    )
    if not definitions:
        return {"sections": []}

    definition_ids = [definition.id for definition in definitions]
    entries = (
        RecordEntry.query.filter(
            RecordEntry.record_definition_id.in_(definition_ids),
            RecordEntry.is_active.is_(True),
        )
        .order_by(
            RecordEntry.record_definition_id.asc(),
            RecordEntry.value.desc(),
            RecordEntry.occurred_on.desc(),
        )
        .all()
    )

    entries_by_definition: dict[int, list[RecordEntry]] = {}
    player_ids = {entry.holder_player_id for entry in entries if entry.holder_player_id}
    game_ids = {entry.game_id for entry in entries if entry.game_id}

    roster_lookup = {
        player.id: player.player_name
        for player in (
            Roster.query.filter(Roster.id.in_(player_ids)).all() if player_ids else []
        )
    }
    game_lookup = {
        game.id: game for game in (Game.query.filter(Game.id.in_(game_ids)).all() if game_ids else [])
    }

    for entry in entries:
        entries_by_definition.setdefault(entry.record_definition_id, []).append(entry)

    sections = [
        {
            "title": "Records",
            "definitions": [],
        }
    ]

    for definition in definitions:
        definition_entries = entries_by_definition.get(definition.id, [])
        forced_entries = [entry for entry in definition_entries if entry.is_forced_current]
        if forced_entries:
            current_entries = forced_entries
        else:
            current_entries = [entry for entry in definition_entries if entry.is_current]

        current_payloads = [
            _build_record_entry_payload(
                entry,
                scope=scope,
                roster_lookup=roster_lookup,
                game_lookup=game_lookup,
            )
            for entry in sorted(
                current_entries,
                key=lambda record: (
                    record.value if record.value is not None else float("-inf"),
                    record.occurred_on or date.min,
                ),
                reverse=True,
            )
        ]

        history_entries = sorted(
            definition_entries,
            key=lambda record: (
                record.value if record.value is not None else float("-inf"),
                record.occurred_on or date.min,
            ),
            reverse=True,
        )[:10]
        history_payloads = [
            _build_record_entry_payload(
                entry,
                scope=scope,
                roster_lookup=roster_lookup,
                game_lookup=game_lookup,
            )
            for entry in history_entries
        ]

        qualifier_tooltip = None
        if definition.qualifier_stat_key:
            qualifier_threshold = get_threshold(definition)
            qualifier_label = get_label_for_key(definition.qualifier_stat_key)
            threshold_text = "N/A" if qualifier_threshold is None else f"{qualifier_threshold:g}"
            qualifier_tooltip = f"Min: {threshold_text} ({qualifier_label})"

        sections[0]["definitions"].append(
            {
                "id": definition.id,
                "name": definition.name,
                "scope": definition.scope,
                "stat_key": definition.stat_key,
                "qualifier_tooltip": qualifier_tooltip,
                "current_entries": current_payloads,
                "history_entries": history_payloads,
            }
        )

    return {"sections": sections}



@app.route('/draft-impact')
def draft_impact_page():
    """Render the page showing draft stock visuals."""
    return render_template('draft_impact.html')


def _get_synergy_client() -> SynergyDataCoreClient:
    """Instantiate a DataCore client using app config."""
    return SynergyDataCoreClient(
        current_app.config['SYNERGY_CLIENT_ID'],
        current_app.config['SYNERGY_CLIENT_SECRET'],
    )


def render_pdf_from_html(html, name):
    pdf = pdfkit.from_string(html, False, options=PDF_OPTIONS, configuration=PDFKIT_CONFIG)
    resp = make_response(pdf)
    resp.headers['Content-Type'] = 'application/pdf'
    resp.headers['Content-Disposition'] = f'attachment; filename="{name}.pdf"'
    return resp


def get_shot_data(shot_type):
    """Aggregate makes and attempts for the given shot type."""
    if shot_type == 'atr':
        makes_col = PlayerStats.atr_makes
        att_col = PlayerStats.atr_attempts
    elif shot_type == '2fg':
        makes_col = PlayerStats.fg2_makes
        att_col = PlayerStats.fg2_attempts
    else:  # 3fg
        makes_col = PlayerStats.fg3_makes
        att_col = PlayerStats.fg3_attempts

    makes, attempts = db.session.query(
        func.coalesce(func.sum(makes_col), 0),
        func.coalesce(func.sum(att_col), 0)
    ).one()
    pct = (makes / attempts * 100) if attempts else 0
    return SimpleNamespace(makes=makes, attempts=attempts, pct=pct)


@app.template_global()
def render_shot_section(shot_type, data):
    """Render a single shot-type section."""
    return render_template('_shot_section.html', shot_type=shot_type, data=data)


# BEGIN Advanced Possession
def _format_adv_table_for_csv(
    rows: Iterable[Mapping[str, object]],
    totals: Mapping[str, object],
) -> str:
    rows = list(rows or [])
    totals = totals or {}
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["LABEL", "PTS", "CHANCES", "PPC", "FREQ"])
    for row in rows:
        pts = int(row.get("pts", 0) or 0)
        chances = int(row.get("chances", 0) or 0)
        ppc_val = float(row.get("ppc", 0.0) or 0.0)
        freq_val = float(row.get("freq", 0.0) or 0.0)
        writer.writerow(
            [
                row.get("label", ""),
                pts,
                chances,
                f"{ppc_val:.2f}",
                f"{freq_val:.1f}%",
            ]
        )
    if totals:
        totals_pts = int(totals.get("pts", 0) or 0)
        totals_chances = int(totals.get("chances", 0) or 0)
        totals_ppc = float(totals.get("ppc", 0.0) or 0.0)
        totals_freq = float(totals.get("freq", 0.0) or 0.0)
        writer.writerow(
            [
                totals.get("label", "Total"),
                totals_pts,
                totals_chances,
                f"{totals_ppc:.2f}",
                f"{totals_freq:.1f}%",
            ]
        )
    buffer.seek(0)
    return buffer.getvalue()


def _format_adv_table_for_view(
    row_entries: Iterable[Mapping[str, object]],
    totals: Mapping[str, object],
) -> Dict[str, object]:
    formatted_rows = []
    for entry in row_entries or []:
        pts = int(entry.get("pts", 0) or 0)
        chances = int(entry.get("chances", 0) or 0)
        ppc_value = float(entry.get("ppc", 0.0) or 0.0)
        freq_value = float(entry.get("freq", 0.0) or 0.0)
        formatted_rows.append(
            {
                "label": entry.get("label", ""),
                "pts": pts,
                "chances": chances,
                "ppc": {"display": f"{ppc_value:.2f}", "data_value": ppc_value},
                "freq": {"display": f"{freq_value:.1f}%", "data_value": freq_value},
            }
        )
    totals = totals or {}
    totals_pts = int(totals.get("pts", 0) or 0)
    totals_chances = int(totals.get("chances", 0) or 0)
    totals_ppc = float(totals.get("ppc", 0.0) or 0.0)
    totals_freq = float(totals.get("freq", 0.0) or 0.0)
    return {
        "rows": formatted_rows,
        "totals": {
            "label": totals.get("label", "Totals"),
            "pts": totals_pts,
            "chances": totals_chances,
            "ppc": f"{totals_ppc:.2f}",
            "freq": f"{totals_freq:.1f}%",
        },
    }


# BEGIN Playcall Report
def _format_playcall_family_csv(family_payload: Mapping[str, object]) -> str:
    plays_payload = family_payload.get("plays") if isinstance(family_payload, Mapping) else None
    totals_payload = family_payload.get("totals") if isinstance(family_payload, Mapping) else None
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "PLAYCALL",
        "RAN",
        "OFF SET PTS",
        "OFF SET CHANCES",
        "OFF SET PPC",
        "IN FLOW PTS",
        "IN FLOW CHANCES",
        "IN FLOW PPC",
    ])
    total_ran = 0
    if isinstance(plays_payload, Mapping):
        for playcall, entry in plays_payload.items():
            ran = int(entry.get("ran", 0) or 0)
            off_set = entry.get("off_set", {}) if isinstance(entry, Mapping) else {}
            in_flow = entry.get("in_flow", {}) if isinstance(entry, Mapping) else {}
            off_pts = int(off_set.get("pts", 0) or 0)
            off_chances = int(off_set.get("chances", 0) or 0)
            off_ppc = float(off_set.get("ppc", 0.0) or 0.0)
            in_pts = int(in_flow.get("pts", 0) or 0)
            in_chances = int(in_flow.get("chances", 0) or 0)
            in_ppc = float(in_flow.get("ppc", 0.0) or 0.0)
            writer.writerow([
                playcall,
                ran,
                off_pts,
                off_chances,
                f"{off_ppc:.2f}",
                in_pts,
                in_chances,
                f"{in_ppc:.2f}",
            ])
            total_ran += ran
    off_totals = {}
    in_totals = {}
    if isinstance(totals_payload, Mapping):
        off_totals = totals_payload.get("off_set", {}) if isinstance(totals_payload.get("off_set"), Mapping) else {}
        in_totals = totals_payload.get("in_flow", {}) if isinstance(totals_payload.get("in_flow"), Mapping) else {}
    if off_totals or in_totals:
        off_pts_total = int((off_totals or {}).get("pts", 0) or 0)
        off_ch_total = int((off_totals or {}).get("chances", 0) or 0)
        off_ppc_total = float((off_totals or {}).get("ppc", 0.0) or 0.0)
        in_pts_total = int((in_totals or {}).get("pts", 0) or 0)
        in_ch_total = int((in_totals or {}).get("chances", 0) or 0)
        in_ppc_total = float((in_totals or {}).get("ppc", 0.0) or 0.0)
        writer.writerow([
            "Totals",
            total_ran,
            off_pts_total,
            off_ch_total,
            f"{off_ppc_total:.2f}",
            in_pts_total,
            in_ch_total,
            f"{in_ppc_total:.2f}",
        ])
    buffer.seek(0)
    return buffer.getvalue()


def _format_playcall_flow_csv(flow_payload: Mapping[str, object]) -> str:
    plays_payload = flow_payload.get("plays") if isinstance(flow_payload, Mapping) else None
    totals_payload = flow_payload.get("totals") if isinstance(flow_payload, Mapping) else None
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "PLAYCALL",
        "RAN (IN FLOW)",
        "IN FLOW PTS",
        "IN FLOW CHANCES",
        "IN FLOW PPC",
    ])
    total_ran = 0
    if isinstance(plays_payload, Iterable):
        for entry in plays_payload:
            if not isinstance(entry, Mapping):
                continue
            playcall = _normalize_flow_label(entry.get("playcall", ""))
            ran = int(entry.get("ran_in_flow", 0) or 0)
            in_flow = entry.get("in_flow", {}) if isinstance(entry.get("in_flow"), Mapping) else {}
            in_pts = int(in_flow.get("pts", 0) or 0)
            in_chances = int(in_flow.get("chances", 0) or 0)
            in_ppc = float(in_flow.get("ppc", 0.0) or 0.0)
            writer.writerow([
                playcall,
                ran,
                in_pts,
                in_chances,
                f"{in_ppc:.2f}",
            ])
            total_ran += ran
    totals_in_flow = {}
    if isinstance(totals_payload, Mapping):
        totals_in_flow = totals_payload.get("in_flow", {}) if isinstance(totals_payload.get("in_flow"), Mapping) else {}
    in_pts_total = int((totals_in_flow or {}).get("pts", 0) or 0)
    in_ch_total = int((totals_in_flow or {}).get("chances", 0) or 0)
    in_ppc_total = float((totals_in_flow or {}).get("ppc", 0.0) or 0.0)
    writer.writerow([
        "Totals",
        total_ran,
        in_pts_total,
        in_ch_total,
        f"{in_ppc_total:.2f}",
    ])
    buffer.seek(0)
    return buffer.getvalue()


def _format_playcall_all_csv(flat_payload: Mapping[str, object]) -> str:
    rows_payload = flat_payload.get("rows") if isinstance(flat_payload, Mapping) else None
    totals_payload = flat_payload.get("totals") if isinstance(flat_payload, Mapping) else {}
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        [
            "SERIES",
            "PLAYCALL",
            "RAN",
            "OFF SET PTS",
            "OFF SET CHANCES",
            "OFF SET PPC",
            "IN FLOW PTS",
            "IN FLOW CHANCES",
            "IN FLOW PPC",
        ]
    )
    total_ran = 0
    if isinstance(rows_payload, Iterable):
        for entry in rows_payload:
            if not isinstance(entry, Mapping):
                continue
            off_set = entry.get("off_set", {}) if isinstance(entry.get("off_set"), Mapping) else {}
            in_flow = entry.get("in_flow", {}) if isinstance(entry.get("in_flow"), Mapping) else {}
            ran_val = int(entry.get("ran", 0) or 0)
            off_ppc = float((off_set or {}).get("ppc", 0.0) or 0.0)
            in_ppc = float((in_flow or {}).get("ppc", 0.0) or 0.0)
            writer.writerow(
                [
                    entry.get("series", ""),
                    entry.get("playcall", ""),
                    ran_val,
                    int((off_set or {}).get("pts", 0) or 0),
                    int((off_set or {}).get("chances", 0) or 0),
                    f"{off_ppc:.2f}",
                    int((in_flow or {}).get("pts", 0) or 0),
                    int((in_flow or {}).get("chances", 0) or 0),
                    f"{in_ppc:.2f}",
                ]
            )
            total_ran += ran_val

    off_totals = totals_payload.get("off_set") if isinstance(totals_payload, Mapping) else {}
    in_totals = totals_payload.get("in_flow") if isinstance(totals_payload, Mapping) else {}
    off_ppc_total = float((off_totals or {}).get("ppc", 0.0) or 0.0)
    in_ppc_total = float((in_totals or {}).get("ppc", 0.0) or 0.0)

    writer.writerow(
        [
            "Totals",
            "Totals",
            total_ran,
            int((off_totals or {}).get("pts", 0) or 0),
            int((off_totals or {}).get("chances", 0) or 0),
            f"{off_ppc_total:.2f}",
            int((in_totals or {}).get("pts", 0) or 0),
            int((in_totals or {}).get("chances", 0) or 0),
            f"{in_ppc_total:.2f}",
        ]
    )

    buffer.seek(0)
    return buffer.getvalue()
# END Playcall Report


@app.route("/api/reports/advanced_offense")
@login_required
def api_advanced_offense():
    if not current_app.config.get("ADVANCED_POSSESSION_ENABLED", True):
        abort(404)
    practice_id = request.args.get("practice_id", type=int)
    if not practice_id:
        return jsonify({"error": "practice_id required"}), 400

    data, meta = cache_get_or_compute_adv_poss_practice(practice_id)

    if request.args.get("format") == "csv":
        table_key = (request.args.get("table") or "").strip()
        team_key = (request.args.get("team") or "crimson").strip().lower()
        team_payload = data.get(team_key) if isinstance(data, Mapping) else None
        if not team_payload:
            return jsonify({"error": "invalid team"}), 400
        rows_payload = []
        totals_payload: Mapping[str, object] = {}
        if isinstance(team_payload, Mapping):
            rows_payload = team_payload.get(table_key) or []
            totals_payload = (team_payload.get("totals") or {}).get(table_key, {})
        if not rows_payload and not totals_payload:
            return jsonify({"error": "invalid table"}), 400
        csv_body = _format_adv_table_for_csv(rows_payload, totals_payload)
        response = make_response(csv_body)
        response.headers["Content-Type"] = "text/csv"
        filename = f"practice_{practice_id}_{team_key}_{table_key}.csv"
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    return jsonify({"data": data, "meta": meta})


@app.route("/api/reports/advanced_offense_game")
@login_required
def api_advanced_offense_game():
    if not current_app.config.get("ADVANCED_POSSESSION_ENABLED", True):
        abort(404)
    game_id = request.args.get("game_id", type=int)
    if not game_id:
        return jsonify({"error": "game_id required"}), 400

    data, meta = cache_get_or_compute_adv_poss_game(game_id)

    if request.args.get("format") == "csv":
        table_key = (request.args.get("table") or "").strip()
        offense_payload = data.get("offense") if isinstance(data, Mapping) else None
        if not offense_payload:
            return jsonify({"error": "invalid table"}), 400
        rows_payload = []
        totals_payload: Mapping[str, object] = {}
        if isinstance(offense_payload, Mapping):
            rows_payload = offense_payload.get(table_key) or []
            totals_payload = (offense_payload.get("totals") or {}).get(table_key, {})
        if not rows_payload and not totals_payload:
            return jsonify({"error": "invalid table"}), 400
        csv_body = _format_adv_table_for_csv(rows_payload, totals_payload)
        response = make_response(csv_body)
        response.headers["Content-Type"] = "text/csv"
        filename = f"game_{game_id}_{table_key}.csv"
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    return jsonify({"data": data, "meta": meta})


# BEGIN Playcall Report
@app.route("/api/reports/playcall")
@login_required
def api_playcall_report():
    if not current_app.config.get("PLAYCALL_REPORT_ENABLED", True):
        abort(404)

    view_scope = (request.args.get("view") or "game").lower()
    if view_scope not in {"game", "season"}:
        view_scope = "game"

    if view_scope == "season":
        season_id = request.args.get("season_id", type=int)
        if not season_id:
            return jsonify({"error": "season_id required"}), 400
        season = Season.query.get(season_id)
        if not season:
            return jsonify({"error": "season not found"}), 404
        games = (
            Game.query.filter(Game.season_id == season_id)
            .order_by(Game.game_date.asc(), Game.id.asc())
            .all()
        )
        game_ids = [game.id for game in games]
        data, meta = aggregate_playcall_reports(game_ids)
        meta = dict(meta or {})
        meta.update(
            {
                "view": "season",
                "season_id": season_id,
                "season_name": season.season_name,
            }
        )

        if request.args.get("format") == "csv":
            family_key = (request.args.get("family") or "").strip()
            if not family_key:
                return jsonify({"error": "family required"}), 400
            series_payload = data.get("series") if isinstance(data, Mapping) else None
            label = season.season_name or f"season_{season_id}"
            safe_label = "".join(
                ch.lower() if ch.isalnum() else "_" for ch in label
            ).strip("_")
            if not safe_label:
                safe_label = f"season_{season_id}"
            if family_key == "ALL":
                flat_payload = _flatten_playcall_series(
                    series_payload if isinstance(series_payload, Mapping) else {}
                )
                csv_body = _format_playcall_all_csv(flat_payload)
                filename = f"season_{safe_label}_all.csv"
            else:
                if not isinstance(series_payload, Mapping) or family_key not in series_payload:
                    return jsonify({"error": "invalid family"}), 400
                family_payload = series_payload[family_key]
                if family_key == "FLOW":
                    csv_body = _format_playcall_flow_csv(family_payload)
                    filename = f"season_{safe_label}_flow.csv"
                else:
                    csv_body = _format_playcall_family_csv(family_payload)
                    safe_family = family_key.lower().replace(" ", "_")
                    filename = f"season_{safe_label}_{safe_family}.csv"
            response = make_response(csv_body)
            response.headers["Content-Type"] = "text/csv"
            response.headers["Content-Disposition"] = f"attachment; filename={filename}"
            return response

        return jsonify({"data": data, "meta": meta})

    game_id = request.args.get("game_id", type=int)
    if not game_id:
        return jsonify({"error": "game_id required"}), 400

    data, meta = cache_get_or_compute_playcall_report(game_id)
    meta = dict(meta or {})
    meta.update({"view": "game", "game_id": game_id})

    if request.args.get("format") == "csv":
        family_key = (request.args.get("family") or "").strip()
        if not family_key:
            return jsonify({"error": "family required"}), 400
        series_payload = data.get("series") if isinstance(data, Mapping) else None
        if family_key == "ALL":
            flat_payload = _flatten_playcall_series(
                series_payload if isinstance(series_payload, Mapping) else {}
            )
            csv_body = _format_playcall_all_csv(flat_payload)
            filename = f"game_{game_id}_all.csv"
        else:
            if not isinstance(series_payload, Mapping) or family_key not in series_payload:
                return jsonify({"error": "invalid family"}), 400
            family_payload = series_payload[family_key]
            if family_key == "FLOW":
                csv_body = _format_playcall_flow_csv(family_payload)
                filename = f"game_{game_id}_flow.csv"
            else:
                csv_body = _format_playcall_family_csv(family_payload)
                safe_family = family_key.lower().replace(" ", "_")
                filename = f"game_{game_id}_{safe_family}.csv"
        response = make_response(csv_body)
        response.headers["Content-Type"] = "text/csv"
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    return jsonify({"data": data, "meta": meta})
# END Playcall Report


@app.route("/reports/advanced_offense")
@login_required
def advanced_offense_report():
    if not current_app.config.get("ADVANCED_POSSESSION_ENABLED", True):
        abort(404)

    active_tab = request.args.get("tab", "practice")
    if active_tab not in {"practice", "game"}:
        active_tab = "practice"

    practices = Practice.query.order_by(Practice.date.desc()).all()
    games = Game.query.order_by(Game.game_date.desc()).all()

    practice_id = request.args.get("practice_id", type=int)
    if not practice_id and practices:
        practice_id = practices[0].id

    game_id = request.args.get("game_id", type=int)
    if not game_id and games:
        game_id = games[0].id

    practice_tables: Dict[str, Dict[str, object]] = {}
    practice_meta = None
    if practice_id:
        try:
            practice_raw, practice_meta = cache_get_or_compute_adv_poss_practice(practice_id)
        except Exception:
            practice_raw, practice_meta = {}, None
        for team_key in ("crimson", "white"):
            team_payload = practice_raw.get(team_key) if isinstance(practice_raw, Mapping) else {}
            team_totals = team_payload.get("totals") if isinstance(team_payload, Mapping) else {}
            practice_tables[team_key] = {
                "paint_touches": _format_adv_table_for_view(
                    team_payload.get("paint_touches") if isinstance(team_payload, Mapping) else [],
                    team_totals.get("paint_touches", {}) if isinstance(team_totals, Mapping) else {},
                ),
                "shot_clock": _format_adv_table_for_view(
                    team_payload.get("shot_clock") if isinstance(team_payload, Mapping) else [],
                    team_totals.get("shot_clock", {}) if isinstance(team_totals, Mapping) else {},
                ),
                "possession_type": _format_adv_table_for_view(
                    team_payload.get("possession_type") if isinstance(team_payload, Mapping) else [],
                    team_totals.get("possession_type", {}) if isinstance(team_totals, Mapping) else {},
                ),
                "meta": team_payload.get("meta") or {"total_pts": 0, "total_chances": 0},
            }

    game_tables: Dict[str, Dict[str, object]] = {}
    game_meta = None
    if game_id:
        try:
            game_raw, game_meta = cache_get_or_compute_adv_poss_game(game_id)
        except Exception:
            game_raw, game_meta = {}, None
        offense_payload = game_raw.get("offense") if isinstance(game_raw, Mapping) else {}
        offense_totals = offense_payload.get("totals") if isinstance(offense_payload, Mapping) else {}
        game_tables["offense"] = {
            "paint_touches": _format_adv_table_for_view(
                offense_payload.get("paint_touches") if isinstance(offense_payload, Mapping) else [],
                offense_totals.get("paint_touches", {}) if isinstance(offense_totals, Mapping) else {},
            ),
            "shot_clock": _format_adv_table_for_view(
                offense_payload.get("shot_clock") if isinstance(offense_payload, Mapping) else [],
                offense_totals.get("shot_clock", {}) if isinstance(offense_totals, Mapping) else {},
            ),
            "possession_type": _format_adv_table_for_view(
                offense_payload.get("possession_type") if isinstance(offense_payload, Mapping) else [],
                offense_totals.get("possession_type", {}) if isinstance(offense_totals, Mapping) else {},
            ),
            "meta": offense_payload.get("meta") or {"total_pts": 0, "total_chances": 0},
        }

    return render_template(
        "reports/advanced_offense.html",
        practices=practices,
        games=games,
        selected_practice=practice_id,
        selected_game=game_id,
        practice_tables=practice_tables,
        game_tables=game_tables,
        practice_meta=practice_meta,
        game_meta=game_meta,
        active_tab=active_tab,
    )
# END Advanced Possession


# BEGIN Playcall Report
@app.route("/reports/playcall")
@login_required
def playcall_report():
    if not current_app.config.get("PLAYCALL_REPORT_ENABLED", True):
        abort(404)

    view_scope = (request.args.get("view") or "game").lower()
    if view_scope not in {"game", "season"}:
        view_scope = "game"

    seasons = (
        Season.query.order_by(Season.start_date.desc(), Season.id.desc()).all()
    )
    season_id = request.args.get("season_id", type=int)
    game_id = request.args.get("game_id", type=int)

    selected_game = Game.query.get(game_id) if game_id else None
    if selected_game:
        season_id = selected_game.season_id

    if season_id is None and seasons:
        season_id = seasons[0].id

    selected_season = next((season for season in seasons if season.id == season_id), None)
    if season_id and selected_season is None:
        selected_season = Season.query.get(season_id)

    def _games_for_season(season_value: Optional[int], ascending: bool = False):
        query = Game.query
        if season_value:
            query = query.filter(Game.season_id == season_value)
        if ascending:
            query = query.order_by(Game.game_date.asc(), Game.id.asc())
        else:
            query = query.order_by(Game.game_date.desc(), Game.id.desc())
        return query.all()

    games = _games_for_season(season_id)

    if view_scope == "game":
        if not selected_game and game_id:
            selected_game = next((game for game in games if game.id == game_id), None)
        if not selected_game and games:
            selected_game = games[0]
            game_id = selected_game.id
        if selected_game:
            season_id = selected_game.season_id
            if not selected_season or selected_season.id != season_id:
                selected_season = next(
                    (season for season in seasons if season.id == season_id),
                    None,
                )
                if not selected_season:
                    selected_season = Season.query.get(season_id)
            games = _games_for_season(season_id)

    def _build_playcall_views(
        raw_payload: Mapping[str, object], csv_kwargs: Mapping[str, object]
    ):
        options: list[str] = []
        family_sections: list[Dict[str, object]] = []
        flow_section: Optional[Dict[str, object]] = None
        all_section: Optional[Dict[str, object]] = None

        series_payload = raw_payload.get("series") if isinstance(raw_payload, Mapping) else {}
        flow_payload: Mapping[str, object] = {}
        if isinstance(series_payload, Mapping):
            for family_name, payload in series_payload.items():
                if not isinstance(payload, Mapping):
                    continue
                if family_name == "FLOW":
                    flow_payload = payload
                    continue
                plays_map = payload.get("plays") if isinstance(payload.get("plays"), Mapping) else {}
                rows: list[Dict[str, object]] = []
                total_ran = 0
                if isinstance(plays_map, Mapping):
                    for playcall, entry in plays_map.items():
                        if not isinstance(entry, Mapping):
                            continue
                        ran_val = int(entry.get("ran", 0) or 0)
                        off_set = entry.get("off_set", {}) if isinstance(entry.get("off_set"), Mapping) else {}
                        in_flow = entry.get("in_flow", {}) if isinstance(entry.get("in_flow"), Mapping) else {}
                        off_ppc_val = float((off_set or {}).get("ppc", 0.0) or 0.0)
                        in_ppc_val = float((in_flow or {}).get("ppc", 0.0) or 0.0)
                        rows.append(
                            {
                                "series": family_name,
                                "playcall": playcall,
                                "ran": ran_val,
                                "off_set_pts": int((off_set or {}).get("pts", 0) or 0),
                                "off_set_chances": int((off_set or {}).get("chances", 0) or 0),
                                "off_set_ppc": {
                                    "display": f"{off_ppc_val:.2f}",
                                    "data_value": off_ppc_val,
                                },
                                "in_flow_pts": int((in_flow or {}).get("pts", 0) or 0),
                                "in_flow_chances": int((in_flow or {}).get("chances", 0) or 0),
                                "in_flow_ppc": {
                                    "display": f"{in_ppc_val:.2f}",
                                    "data_value": in_ppc_val,
                                },
                            }
                        )
                        total_ran += ran_val
                totals_payload = payload.get("totals") if isinstance(payload.get("totals"), Mapping) else {}
                off_totals = totals_payload.get("off_set") if isinstance(totals_payload, Mapping) else {}
                in_totals = totals_payload.get("in_flow") if isinstance(totals_payload, Mapping) else {}
                off_totals_ppc = float((off_totals or {}).get("ppc", 0.0) or 0.0)
                in_totals_ppc = float((in_totals or {}).get("ppc", 0.0) or 0.0)
                totals_row = {
                    "series": family_name,
                    "playcall": "Totals",
                    "ran": total_ran,
                    "off_set_pts": int((off_totals or {}).get("pts", 0) or 0),
                    "off_set_chances": int((off_totals or {}).get("chances", 0) or 0),
                    "off_set_ppc": f"{off_totals_ppc:.2f}",
                    "in_flow_pts": int((in_totals or {}).get("pts", 0) or 0),
                    "in_flow_chances": int((in_totals or {}).get("chances", 0) or 0),
                    "in_flow_ppc": f"{in_totals_ppc:.2f}",
                }
                csv_url = url_for(
                    "api_playcall_report",
                    **{**csv_kwargs, "family": family_name, "format": "csv"},
                )
                if family_name not in options:
                    options.append(family_name)
                family_sections.append(
                    {
                        "name": family_name,
                        "rows": rows,
                        "totals": totals_row,
                        "csv_url": csv_url,
                    }
                )

        if isinstance(flow_payload, Mapping) and flow_payload:
            flow_rows: list[Dict[str, object]] = []
            total_ran_flow = 0
            plays_payload = flow_payload.get("plays") if isinstance(flow_payload.get("plays"), Iterable) else []
            if isinstance(plays_payload, Iterable):
                for entry in plays_payload:
                    if not isinstance(entry, Mapping):
                        continue
                    ran_val = int(entry.get("ran_in_flow", 0) or 0)
                    in_flow = entry.get("in_flow", {}) if isinstance(entry.get("in_flow"), Mapping) else {}
                    in_ppc_val = float((in_flow or {}).get("ppc", 0.0) or 0.0)
                    flow_rows.append(
                        {
                            "playcall": _normalize_flow_label(entry.get("playcall", "")),
                            "ran_in_flow": ran_val,
                            "in_flow_pts": int((in_flow or {}).get("pts", 0) or 0),
                            "in_flow_chances": int((in_flow or {}).get("chances", 0) or 0),
                            "in_flow_ppc": {
                                "display": f"{in_ppc_val:.2f}",
                                "data_value": in_ppc_val,
                            },
                        }
                    )
                    total_ran_flow += ran_val
            totals_payload = flow_payload.get("totals") if isinstance(flow_payload.get("totals"), Mapping) else {}
            totals_in_flow = totals_payload.get("in_flow") if isinstance(totals_payload, Mapping) else {}
            flow_ppc_val = float((totals_in_flow or {}).get("ppc", 0.0) or 0.0)
            flow_totals = {
                "playcall": "Totals",
                "ran_in_flow": total_ran_flow,
                "in_flow_pts": int((totals_in_flow or {}).get("pts", 0) or 0),
                "in_flow_chances": int((totals_in_flow or {}).get("chances", 0) or 0),
                "in_flow_ppc": f"{flow_ppc_val:.2f}",
            }
            flow_csv_url = url_for(
                "api_playcall_report",
                **{**csv_kwargs, "family": "FLOW", "format": "csv"},
            )
            flow_section = {
                "rows": flow_rows,
                "totals": flow_totals,
                "csv_url": flow_csv_url,
            }
            if "FLOW" not in options:
                options.append("FLOW")

        flat_payload = _flatten_playcall_series(
            series_payload if isinstance(series_payload, Mapping) else {}
        )
        flat_rows = flat_payload.get("rows") if isinstance(flat_payload, Mapping) else []
        if flat_rows:
            formatted_rows: list[Dict[str, object]] = []
            for entry in sorted(
                flat_rows,
                key=lambda item: (
                    (item.get("series") or "") if isinstance(item, Mapping) else "",
                    (item.get("playcall") or "") if isinstance(item, Mapping) else "",
                ),
            ):
                if not isinstance(entry, Mapping):
                    continue
                off_set = entry.get("off_set", {}) if isinstance(entry.get("off_set"), Mapping) else {}
                in_flow = entry.get("in_flow", {}) if isinstance(entry.get("in_flow"), Mapping) else {}
                off_ppc_val = float((off_set or {}).get("ppc", 0.0) or 0.0)
                in_ppc_val = float((in_flow or {}).get("ppc", 0.0) or 0.0)
                formatted_rows.append(
                    {
                        "series": entry.get("series", ""),
                        "playcall": entry.get("playcall", ""),
                        "ran": int(entry.get("ran", 0) or 0),
                        "off_set_pts": int((off_set or {}).get("pts", 0) or 0),
                        "off_set_chances": int((off_set or {}).get("chances", 0) or 0),
                        "off_set_ppc": {
                            "display": f"{off_ppc_val:.2f}",
                            "data_value": off_ppc_val,
                        },
                        "in_flow_pts": int((in_flow or {}).get("pts", 0) or 0),
                        "in_flow_chances": int((in_flow or {}).get("chances", 0) or 0),
                        "in_flow_ppc": {
                            "display": f"{in_ppc_val:.2f}",
                            "data_value": in_ppc_val,
                        },
                    }
                )

            totals_payload = flat_payload.get("totals") if isinstance(flat_payload, Mapping) else {}
            off_totals = totals_payload.get("off_set") if isinstance(totals_payload, Mapping) else {}
            in_totals = totals_payload.get("in_flow") if isinstance(totals_payload, Mapping) else {}
            off_ppc_total = float((off_totals or {}).get("ppc", 0.0) or 0.0)
            in_ppc_total = float((in_totals or {}).get("ppc", 0.0) or 0.0)
            totals_row_all = {
                "series": "Totals",
                "playcall": "Totals",
                "ran": int((totals_payload or {}).get("ran", 0) or 0),
                "off_set_pts": int((off_totals or {}).get("pts", 0) or 0),
                "off_set_chances": int((off_totals or {}).get("chances", 0) or 0),
                "off_set_ppc": f"{off_ppc_total:.2f}",
                "in_flow_pts": int((in_totals or {}).get("pts", 0) or 0),
                "in_flow_chances": int((in_totals or {}).get("chances", 0) or 0),
                "in_flow_ppc": f"{in_ppc_total:.2f}",
            }

            all_section = {
                "name": "ALL",
                "rows": formatted_rows,
                "totals": totals_row_all,
                "csv_url": url_for(
                    "api_playcall_report",
                    **{**csv_kwargs, "family": "ALL", "format": "csv"},
                ),
            }
            if "ALL" not in options:
                options.insert(0, "ALL")

        return options, family_sections, flow_section, all_section

    series_options: list[str] = []
    families_view: list[Dict[str, object]] = []
    flow_view: Optional[Dict[str, object]] = None
    all_view: Optional[Dict[str, object]] = None
    raw_data: Mapping[str, object] = {}
    playcall_meta: Mapping[str, object] = {}

    try:
        if view_scope == "season" and season_id:
            game_ids = [game.id for game in _games_for_season(season_id, ascending=True)]
            raw_data, playcall_meta = aggregate_playcall_reports(game_ids)
        elif view_scope == "game" and selected_game:
            raw_data, playcall_meta = cache_get_or_compute_playcall_report(selected_game.id)
    except Exception:
        raw_data, playcall_meta = {}, {}

    csv_kwargs: Dict[str, object] = {"view": view_scope}
    if view_scope == "game" and selected_game:
        csv_kwargs["game_id"] = selected_game.id
    elif view_scope == "season" and season_id:
        csv_kwargs["season_id"] = season_id

    if isinstance(raw_data, Mapping) and raw_data:
        built = _build_playcall_views(raw_data, csv_kwargs)
        if built:
            series_options, families_view, flow_view, all_view = built

    selected_series = [
        value for value in request.args.getlist("series") if value in series_options
    ]
    if not selected_series and series_options:
        selected_series = list(series_options)

    search_query = request.args.get("search", "")

    data_meta = raw_data.get("meta") if isinstance(raw_data, Mapping) else {}
    display_meta: Dict[str, object] = {}
    if isinstance(playcall_meta, Mapping):
        display_meta.update({k: v for k, v in playcall_meta.items() if v is not None})
    if isinstance(data_meta, Mapping):
        display_meta.update({k: v for k, v in data_meta.items() if v is not None})
    display_meta.setdefault("view", view_scope)
    if selected_season:
        display_meta.setdefault("season_id", selected_season.id)
        display_meta.setdefault("season_name", selected_season.season_name)

    return render_template(
        "reports/playcall.html",
        games=games,
        seasons=seasons,
        selected_game=selected_game.id if selected_game else None,
        selected_season=season_id,
        selected_view=view_scope,
        families=families_view,
        all_section=all_view,
        flow=flow_view,
        series_options=series_options,
        selected_series=selected_series,
        search_query=search_query,
        meta=display_meta,
        selected_game_obj=selected_game,
    )
# END Playcall Report


@app.route('/pdf/home')
def pdf_home():
    html = game_homepage()
    return render_pdf_from_html(html, 'home')


@app.route('/pdf/leaderboard')
def pdf_leaderboard():
    html = season_leaderboard()
    return render_pdf_from_html(html, 'leaderboard')


@app.route('/pdf/player/<int:player_id>')
def pdf_player(player_id):
    from models.database import Roster
    player = Roster.query.get_or_404(player_id)
    html = player_detail(player.player_name)
    return render_pdf_from_html(html, f'player_{player_id}')


@app.route('/api/competitions', methods=['GET'])
def api_competitions():
    """Return the list of available competitions from Synergy."""
    client = _get_synergy_client()
    competitions = client.get_competitions()
    return jsonify(competitions)


@app.route('/api/stats', methods=['GET'])
def api_stats():
    """Return recent games with player stats for a competition."""
    competition_id = request.args.get('competition_id')
    if not competition_id:
        return jsonify({'error': 'competition_id required'}), 400

    client = _get_synergy_client()
    games = client.get_recent_games_with_stats(competition_id)
    return jsonify(games)


@app.route('/api/player_stats', methods=['GET'])
def api_player_stats():
    """Return cumulative Synergy stats for a given player name."""
    player_name = request.args.get('player_name')
    if not player_name:
        return jsonify({'error': 'player_name required'}), 400

    synergy_api = SynergyAPI()
    player_id = synergy_api.find_player_id(player_name)
    if not player_id:
        return jsonify({'error': 'player not found'}), 404

    stats = synergy_api.get_player_stats(player_id)
    return jsonify(stats)



@app.route('/practice/team_totals')
@login_required
def practice_team_totals():
    """Show aggregated practice totals with date and drill label filters."""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    start_dt = end_dt = None
    if start_date:
        try:
            start_dt = date.fromisoformat(start_date)
        except ValueError:
            start_date = ''
    if end_date:
        try:
            end_dt = date.fromisoformat(end_date)
        except ValueError:
            end_date = ''

    # >>> SESSION RANGE INTEGRATION START
    from utils.filters import apply_session_range

    start_dt, end_dt, selected_session = apply_session_range(request.args, start_dt, end_dt)
    # If a session is active, it should override manual dates in the actual query constraints below.
    # >>> SESSION RANGE INTEGRATION END

    q = PlayerStats.query.filter(PlayerStats.practice_id != None)
    if start_dt or end_dt:
        q = q.join(Practice, PlayerStats.practice_id == Practice.id)
        if start_dt:
            q = q.filter(Practice.date >= start_dt)
        if end_dt:
            q = q.filter(Practice.date <= end_dt)

    stats = q.all()

    label_options = collect_practice_labels(stats)
    selected_labels = [
        lbl for lbl in request.args.getlist('label') if lbl.upper() in label_options
    ]
    label_set = {lbl.upper() for lbl in selected_labels}

    if label_set:
        totals = compute_filtered_totals(stats, label_set)
        blue_totals = compute_filtered_blue(stats, label_set)
    else:
        totals = aggregate_stats(stats)
        bc_query = db.session.query(
            func.coalesce(func.sum(BlueCollarStats.def_reb), 0).label('def_reb'),
            func.coalesce(func.sum(BlueCollarStats.off_reb), 0).label('off_reb'),
            func.coalesce(func.sum(BlueCollarStats.misc), 0).label('misc'),
            func.coalesce(func.sum(BlueCollarStats.deflection), 0).label('deflection'),
            func.coalesce(func.sum(BlueCollarStats.steal), 0).label('steal'),
            func.coalesce(func.sum(BlueCollarStats.block), 0).label('block'),
            func.coalesce(func.sum(BlueCollarStats.floor_dive), 0).label('floor_dive'),
            func.coalesce(func.sum(BlueCollarStats.charge_taken), 0).label('charge_taken'),
            func.coalesce(func.sum(BlueCollarStats.reb_tip), 0).label('reb_tip'),
            func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0).label('total_blue_collar'),
        ).filter(BlueCollarStats.practice_id != None)
        if start_dt or end_dt:
            bc_query = bc_query.join(Practice, BlueCollarStats.practice_id == Practice.id)
            if start_dt:
                bc_query = bc_query.filter(Practice.date >= start_dt)
            if end_dt:
                bc_query = bc_query.filter(Practice.date <= end_dt)
        bc = bc_query.one()
        blue_totals = SimpleNamespace(
            def_reb=bc.def_reb,
            off_reb=bc.off_reb,
            misc=bc.misc,
            deflection=bc.deflection,
            steal=bc.steal,
            block=bc.block,
            floor_dive=bc.floor_dive,
            charge_taken=bc.charge_taken,
            reb_tip=bc.reb_tip,
            total_blue_collar=bc.total_blue_collar,
        )

    pt_query = db.session.query(
        func.coalesce(Possession.paint_touches, '').label('pt'),
        func.coalesce(func.sum(Possession.points_scored), 0).label('points'),
        func.count(Possession.id).label('poss'),
    ).filter(Possession.practice_id != None)
    if start_dt or end_dt:
        pt_query = pt_query.join(Practice, Possession.practice_id == Practice.id)
        if start_dt:
            pt_query = pt_query.filter(Practice.date >= start_dt)
        if end_dt:
            pt_query = pt_query.filter(Practice.date <= end_dt)
    pt_rows = pt_query.group_by(Possession.paint_touches).all()
    buckets = {0: {'pts': 0, 'poss': 0}, 1: {'pts': 0, 'poss': 0}, 2: {'pts': 0, 'poss': 0}, 3: {'pts': 0, 'poss': 0}}
    for r in pt_rows:
        try:
            val = int(float(str(r.pt).strip() or '0'))
        except ValueError:
            continue
        key = 3 if val >= 3 else val
        buckets[key]['pts'] += r.points
        buckets[key]['poss'] += r.poss
    paint_ppp = SimpleNamespace(
        zero=round(buckets[0]['pts'] / buckets[0]['poss'], 2) if buckets[0]['poss'] else 0.0,
        one=round(buckets[1]['pts'] / buckets[1]['poss'], 2) if buckets[1]['poss'] else 0.0,
        two=round(buckets[2]['pts'] / buckets[2]['poss'], 2) if buckets[2]['poss'] else 0.0,
        three=round(buckets[3]['pts'] / buckets[3]['poss'], 2) if buckets[3]['poss'] else 0.0,
    )

    return render_template(
        'admin/team_totals.html',
        totals=totals,
        blue_totals=blue_totals,
        paint_ppp=paint_ppp,
        label_options=label_options,
        selected_labels=selected_labels,
        start_date=start_date or '',
        end_date=end_date or '',
        seasons=[],
        selected_season=None,
        active_page='team_totals',
        # >>> TEMPLATE CONTEXT SESSION START
        selected_session=selected_session if 'selected_session' in locals() else request.args.get('session') or 'All',
        sessions=['Summer 1','Summer 2','Fall','Official Practice','All'],
        # <<< TEMPLATE CONTEXT SESSION END
    )


@app.route('/shot-type/<string:shot_type>')
def shot_type_report(shot_type):
    """Printable report for a single shot type."""
    valid = {'atr': 'ATR', '2fg': '2FG', '3fg': '3FG'}
    if shot_type not in valid:
        abort(404)
    data = get_shot_data(shot_type)
    title = valid[shot_type] + ' Shot Type Report'
    return render_template('shot_type.html', shot_type=shot_type, title=title, data=data)


@app.route('/player/<player_name>')
def player_view(player_name):
    """Public-facing player page with on-court offensive metrics."""
    from models.database import Roster
    player = Roster.query.filter_by(player_name=player_name).first_or_404()

    label_options = collect_practice_labels([])
    selected_labels = [
        lbl for lbl in request.args.getlist('label') if lbl.upper() in label_options
    ]
    label_set = {lbl.upper() for lbl in selected_labels}

    # 1. On-court offensive possessions & points
    helper_labels = list(label_set) if label_set else None
    summary = get_on_off_summary(
        player_id=player.id,
        labels=helper_labels,
    )
    turnover_rates = get_turnover_rates_onfloor(
        player_id=player.id,
        labels=helper_labels,
    )
    rebound_rates = get_rebound_rates_onfloor(
        player_id=player.id,
        labels=helper_labels,
    )

    ON_poss = summary.offensive_possessions_on
    PPP_ON = summary.ppp_on_offense or 0.0
    PPP_OFF = summary.ppp_off_offense or 0.0

    # helper to count shot/event details on-court
    def count_event(ev_type):
        q = (
            db.session.query(func.count(ShotDetail.id))
            .join(Possession, ShotDetail.possession_id == Possession.id)
            .join(PossessionPlayer, Possession.id == PossessionPlayer.possession_id)
            .filter(
                PossessionPlayer.player_id == player.id,
                func.lower(Possession.time_segment) == 'offense',
                ShotDetail.event_type == ev_type,
            )
        )
        if label_set:
            clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
            q = q.filter(or_(*clauses))
        return q.scalar() or 0

    # 5. Shooting splits
    FGM2_ON = count_event('ATR+') + count_event('2FG+')
    FGM3_ON = count_event('3FG+')
    FGA_ON  = sum(count_event(e) for e in ['ATR+','ATR-','2FG+','2FG-','3FG+','3FG-'])
    EFG_ON  = (FGM2_ON + 1.5 * FGM3_ON) / FGA_ON if FGA_ON else 0
    ATR_pct = count_event('ATR+') / (count_event('ATR+') + count_event('ATR-')) if (count_event('ATR+') + count_event('ATR-')) else 0
    FG2_pct = count_event('2FG+') / (count_event('2FG+') + count_event('2FG-')) if (count_event('2FG+') + count_event('2FG-')) else 0
    FG3_pct = count_event('3FG+') / (count_event('3FG+') + count_event('3FG-')) if (count_event('3FG+') + count_event('3FG-')) else 0

    # 6. Rate metrics
    turnover_pct = turnover_rates.get('team_turnover_rate_on') or 0.0
    turnover_rate     = (turnover_pct / 100) if ON_poss else 0
    off_reb_pct = rebound_rates.get('off_reb_rate_on') or 0.0
    off_reb_rate      = (off_reb_pct / 100) if ON_poss else 0
    fouls_drawn_rate  = count_event('Fouled') / ON_poss if ON_poss else 0

    player_summary_rows = [
        {"stat": "Possessions", "value": num(ON_poss)},
        {"stat": "PPP (On-court)", "value": num(round(PPP_ON, 2))},
        {"stat": "PPP (Off-court)", "value": num(round(PPP_OFF, 2))},
        {"stat": "eFG%", "value": pct(EFG_ON)},
        {"stat": "ATR%", "value": pct(ATR_pct)},
        {"stat": "2FG%", "value": pct(FG2_pct)},
        {"stat": "3FG%", "value": pct(FG3_pct)},
        {"stat": "Turnover Rate", "value": pct(turnover_rate)},
        {"stat": "Off-Reb Rate", "value": pct(off_reb_rate)},
        {"stat": "Fouls Drawn Rate", "value": pct(fouls_drawn_rate)},
    ]

    # 7. Shot type breakdown for mobile tables
    stats_records = PlayerStats.query.filter_by(player_name=player.player_name).all()
    raw_totals, shot_summaries = compute_team_shot_details(stats_records, label_set)
    shot_type_categories = []
    for key, label in [('atr', 'ATR'), ('fg2', '2FG'), ('fg3', '3FG')]:
        summary = shot_summaries.get(key)
        if not summary:
            continue
        tot_att = summary.total.attempts or 0
        def fmt(ctx):
            return SimpleNamespace(
                fga=ctx.attempts,
                fg_pct=f"{(ctx.fg_pct*100 if ctx.fg_pct <= 1 else ctx.fg_pct):.1f}%",
                pps=f"{ctx.pps:.2f}",
                freq_pct=f"{(ctx.attempts / tot_att * 100) if tot_att else 0:.1f}%",
            )
        shot_type_categories.append(
            SimpleNamespace(
                name=label,
                total=fmt(summary.total),
                transition=fmt(summary.transition),
                half_court=fmt(summary.halfcourt),
            )
        )

    # 8. Pass into template context
    return render_template(
        'player_view.html',
        player=player,
        offensive_possessions = ON_poss,
        ppp_on               = round(PPP_ON,2),
        ppp_off              = round(PPP_OFF,2),
        efg_on               = round(EFG_ON*100,1),
        atr_pct              = round(ATR_pct*100,1),
        two_fg_pct           = round(FG2_pct*100,1),
        three_fg_pct         = round(FG3_pct*100,1),
        turnover_rate        = round(turnover_rate*100,1),
        off_reb_rate         = round(off_reb_rate*100,1),
        fouls_drawn_rate     = round(fouls_drawn_rate*100,1),
        shot_type_categories = shot_type_categories,
        shot_type_totals     = raw_totals,
        shot_summaries       = shot_summaries,
        player_summary_rows  = player_summary_rows,
    )




# —– 2. Head-to-Head NET API —–
@app.route('/api/draft/net')
def draft_net():
    rival = request.args.get('school')
    al_net = db.session.query(func.sum(PlayerDraftStock.net)).filter_by(team='Alabama').scalar() or 0
    rival_net = 0
    if rival:
        rival_net = (
            db.session.query(func.sum(PlayerDraftStock.net)).filter_by(team=rival).scalar() or 0
        )
    return jsonify({'alabama_net': int(al_net), 'rival_net': int(rival_net)})


@app.get('/records')
def records_page():
    selected_tab = _parse_record_tab(request.args.get("tab"))
    selected_scope = _parse_record_scope(request.args.get("scope"))
    record_data = _load_record_book(selected_tab, selected_scope)

    return render_template(
        "records.html",
        tab_options=RECORD_TAB_LABELS,
        scope_options=RECORD_SCOPE_LABELS,
        selected_tab=selected_tab,
        selected_scope=selected_scope,
        sections=record_data["sections"],
    )


def _dev_tables_enabled() -> bool:
    """Return True when dev-only routes should be registered."""
    return app.debug or os.environ.get('FLASK_ENV') == 'development'


if _dev_tables_enabled():

    @app.route('/dev/tables-smoke')
    def dev_tables_smoke():
        """Render the unified tables smoke test sandbox."""
        return render_template('dev/tables_smoketest.html')
