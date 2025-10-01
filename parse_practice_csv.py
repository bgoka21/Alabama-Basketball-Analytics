import json
import math
import os
import re
from datetime import date
from collections import defaultdict

import pandas as pd
from flask import current_app
from utils.lineup import compute_lineup_efficiencies, compute_player_on_off_by_team
from models.database import (
    db,
    Roster,
    PlayerStats,
    BlueCollarStats,
    Practice,
)
from models import Possession, PossessionPlayer, ShotDetail


def safe_str(value):
    """Safely convert a value to a string, returning an empty string for None."""
    return "" if value is None else str(value)

# ── COPY OF blue_collar_values FROM test_parse.py ───────────────────
blue_collar_values = {
    "reb_tip":      0.5,
    "def_reb":      1.0,
    "misc":         1.0,
    "deflection":   1.0,
    "steal":        1.0,
    "block":        1.0,
    "off_reb":      1.5,
    "floor_dive":   2.0,
    "charge_taken": 4.0
}
# ─────────────────────────────────────────────────────────────────────

# Defensive token mapping (mirrors game parsing)
defense_mapping = {
    "Foul By": "foul_by",
    "Front": "contest_front",
    "Side": "contest_side",
    "Behind": "contest_behind",
    "Late": "contest_late",
    "Contest": "contest_early",
    "No Contest": "contest_no",
    "None": "contest_no",
    "Bump +": "bump_positive",
    "Bump -": "bump_missed",
    "Blowby": "blowby_total",
    "Triple Threat": "blowby_triple_threat",
    "Closeout": "blowby_closeout",
    "Isolation": "blowby_isolation",
}

SHOT_TOKEN_MAP = {
    "ATR+": ("atr", "made"),
    "ATR-": ("atr", "miss"),
    "2FG+": ("2fg", "made"),
    "2FG-": ("2fg", "miss"),
    "3FG+": ("3fg", "made"),
    "3FG-": ("3fg", "miss"),
}

CONTEST_LEVEL_MAP = {
    "Contest": ("contest_early", "contest"),
    "Late": ("contest_late", "late"),
    "No Contest": ("contest_no", "no_contest"),
    "None": ("contest_no", "no_contest"),
}

SHOT_PREFIX_LOOKUP = {
    "atr": "atr",
    "2fg": "fg2",
    "3fg": "fg3",
}




def get_roster_id(name, season_id):
    """Lookup Roster.id by exact player_name & season."""
    clean_name = name.strip() if isinstance(name, str) else name
    row = Roster.query.filter_by(player_name=clean_name, season_id=season_id).first()
    return row.id if row else None


def extract_tokens(val):
    """Return list of comma-separated tokens from the cell value."""
    if pd.isna(val) or not isinstance(val, str):
        return []
    return [t.strip() for t in val.split(',') if t.strip()]




# === Helpers for new practice stats (idempotent) ===
PLAYER_COL_RE = re.compile(r"^#\d+\s+\S+", re.UNICODE)  # e.g., "#12 John Doe"


def _is_player_column(col_name: str) -> bool:
    try:
        return bool(col_name) and bool(PLAYER_COL_RE.match(str(col_name)))
    except Exception:
        return False


def _date_from_filename(fname: str) -> date | None:
    if not fname:
        return None
    basename = os.path.basename(str(fname))
    match = re.match(r"^(\d{2})_(\d{2})_(\d{2})\b", basename)
    if not match:
        return None
    yy, mm, dd = map(int, match.groups())
    try:
        return date(2000 + yy, mm, dd)
    except ValueError:
        return None


def split_tokens(cell) -> list[str]:
    """
    Robustly split a CSV cell that may contain multiple labels like:
    "Def -, Given Up" or "Gap +; SD -". Handles commas/semicolons/newlines.
    """
    if cell is None:
        return []
    try:
        if math.isnan(cell):
            return []
    except Exception:
        pass
    s = str(cell).strip()
    if not s:
        return []
    # Normalize separators to comma, then split
    for sep in [";", "\n", "\r", "\t"]:
        s = s.replace(sep, ",")
    return [t.strip() for t in s.split(",") if t.strip()]


def bump(d: dict, key: str, amount: int = 1):
    d[key] = d.get(key, 0) + amount


def _determine_row_shot_context(row, player_columns):
    """Return the first ATR/2FG/3FG attempt found in the row, if any."""
    for col in player_columns:
        cell = row.get(col, "")
        tokens = split_tokens(cell)
        for token in tokens:
            if token in SHOT_TOKEN_MAP:
                shot_class, shot_result = SHOT_TOKEN_MAP[token]
                return {
                    "shot_class": shot_class,
                    "shot_result": shot_result,
                    "shot_token": token,
                }
    return None


def ensure_player_defaults(slot: dict):
    """
    Ensure all new counter fields exist with default 0.
    Safe to call for any player row before bumping.
    """
    defaults = {
        # Offensive Rebounding Opportunities
        "crash_positive": 0,     # Off +
        "crash_missed": 0,       # Off -
        "back_man_positive": 0,  # BM +
        "back_man_missed": 0,    # BM -
        # Defensive Rebounding Opportunities
        "box_out_positive": 0,   # Def +
        "box_out_missed": 0,     # Def -
        "off_reb_given_up": 0,   # Given Up
        # Collision Gap (Crimson/White)
        "collision_gap_positive": 0,  # Gap +
        "collision_gap_missed": 0,    # Gap -
        "pass_contest_positive": 0,   # Contest Pass +
        "pass_contest_missed": 0,     # Contest Pass -
        # PnR Gap Help / Low
        "pnr_gap_positive": 0,   # Gap +
        "pnr_gap_missed": 0,     # Gap -
        "low_help_positive": 0,  # Low +
        "low_help_missed": 0,    # Low -
        # PnR Grade
        "close_window_positive": 0,  # CW +
        "close_window_missed": 0,    # CW -
        "shut_door_positive": 0,     # SD +
        "shut_door_missed": 0,       # SD -
        # Contest by shot type
        "atr_contest_attempts": 0,
        "atr_contest_makes": 0,
        "atr_late_attempts": 0,
        "atr_late_makes": 0,
        "atr_no_contest_attempts": 0,
        "atr_no_contest_makes": 0,
        "fg2_contest_attempts": 0,
        "fg2_contest_makes": 0,
        "fg2_late_attempts": 0,
        "fg2_late_makes": 0,
        "fg2_no_contest_attempts": 0,
        "fg2_no_contest_makes": 0,
        "fg3_contest_attempts": 0,
        "fg3_contest_makes": 0,
        "fg3_late_attempts": 0,
        "fg3_late_makes": 0,
        "fg3_no_contest_attempts": 0,
        "fg3_no_contest_makes": 0,
    }
    for k, v in defaults.items():
        if k not in slot:
            slot[k] = v


def parse_practice_csv(practice_csv_path, season_id=None, category=None, file_date=None):
    """
    1) Read CSV into DataFrame.
    2) Build player_stats_dict and player_blue_dict.
    3) Find existing Practice (routes.py created it).
    4) Insert PlayerStats and BlueCollarStats for that practice.
    """
    from app.services.csv_tokens import (
        count_bump_tokens_in_cells,
        count_low_man_tokens_in_cells,
    )
    from app.utils.category_normalization import normalize_category

    # Use utf-8-sig to seamlessly strip any UTF-8 BOM that may be present in
    # practice CSV files exported from Excel. Without this, the first column
    # name becomes '\ufeffRow' and row types are not recognized.
    try:
        df = pd.read_csv(
            practice_csv_path,
            encoding="utf-8-sig",
            engine="python",
            dtype=str,
        )
    except Exception:
        if current_app:
            current_app.logger.exception("Failed to read practice CSV")
        return {
            "error": "Unable to parse practice CSV. Please check that the file is formatted correctly."
        }
    # Normalize column headers to avoid mismatches caused by stray whitespace
    df.columns = [str(c).strip() for c in df.columns]

    category = normalize_category(category)
    practice_date = file_date or _date_from_filename(practice_csv_path)

    # ─── Locate the Practice row ───────────────────────────────────
    current_practice = (
        Practice.query
        .filter_by(season_id=season_id, category=category, date=practice_date)
        .first()
    )
    if current_practice is None:
        raise RuntimeError(
            f"Could not find existing Practice row for season={season_id}, "
            f"category='{category}', date={practice_date}"
        )
    practice_id = current_practice.id

    # ─── Step A: Initialize accumulators ─────────────────────────────
    player_stats_dict   = defaultdict(lambda: defaultdict(int))
    player_blue_dict    = defaultdict(lambda: defaultdict(int))
    player_shot_list    = defaultdict(list)
    player_detail_list  = defaultdict(list)
    possession_data     = []
    events              = defaultdict(lambda: defaultdict(int))
    last_offense_possession = {}  # map team name → (Possession, [player names])
    # ── Find all columns beginning with "#" to use for player tokens
    player_columns = [c for c in df.columns if _is_player_column(c)]
    # ─────────────────────────────────────────────────────────────────────

    # ─── Step B: Loop through each row in the CSV ────────────────────
    for _, row in df.iterrows():
        row_type = str(row.get("Row", "")).strip()
        row_type_clean = row_type.strip().lower()
        drill_val = row.get("DRILL TYPE")
        if pd.isna(drill_val):
            drill_str = ""
        else:
            drill_str = str(drill_val)


        labels = [t.strip().upper() for t in drill_str.split(",") if t.strip()]

        if row_type in ("Crimson", "White", "Alabama", "Blue"):
            row_shot_context = _determine_row_shot_context(row, player_columns)
        else:
            row_shot_context = None

        # ─── Bump parsing (scan all player columns regardless of row type) ──
        for col in player_columns:
            cell_value = row.get(col, "")
            plus, minus = count_bump_tokens_in_cells([cell_value])
            low_plus, low_minus = count_low_man_tokens_in_cells([cell_value])
            if not (plus or minus or low_plus or low_minus):
                continue
            roster_id = get_roster_id(col, season_id)
            if roster_id is None:
                continue
            stats = player_stats_dict[roster_id]
            details = player_detail_list[roster_id]
            if plus:
                stats["bump_positive"] += plus
                for _ in range(plus):
                    details.append({"event": "bump_positive", "drill_labels": labels})
            if minus:
                stats["bump_missed"] += minus
                for _ in range(minus):
                    details.append({"event": "bump_missed", "drill_labels": labels})
            if low_plus:
                stats["low_help_positive"] += low_plus
                for _ in range(low_plus):
                    details.append({"event": "low_help_positive", "drill_labels": labels})
            if low_minus:
                stats["low_help_missed"] += low_minus
                for _ in range(low_minus):
                    details.append({"event": "low_help_missed", "drill_labels": labels})

        # ─── Possession parsing ─────────────────────────────────────────
        team = row['Row']  # 'Crimson' or 'White'
        if team in ("Crimson", "White"):
            # === Capture only true team Off Reb without creating a possession ===
            team_cell = str(row.get('TEAM', '') or '')
            off_reb_row = 'Off Reb' in [t.strip() for t in team_cell.split(',')]
            if off_reb_row:
                off_players = [
                    name.strip()
                    for name in str(row.get(f"{team.upper()} PLAYER POSSESSIONS", '')).split(',')
                    if name.strip()
                ]
                for player in off_players:
                    events[player]['team_off_reb_on'] = (
                        events[player].get('team_off_reb_on', 0) + 1
                    )
                # skip normal possession parsing for this rebound row
                # (but still allow token parsing below to record shot attempts)
            # ================================================================
            offense_label = 'Offense'
            defense_label = 'Defense'
            offense_team = team
            defense_team = 'White' if team == 'Crimson' else 'Crimson'

            ps_col  = 'POSSESSION START'
            pt_col  = 'POSSESSION TYPE'
            pc_col  = 'PAINT TOUCHES'
            sc_col  = 'SHOT CLOCK'
            scp_col = 'SHOT CLOCK PT'
            off_col = f"{offense_team.upper()} PLAYER POSSESSIONS"
            def_col = f"{defense_team.upper()} PLAYER POSSESSIONS"

            p_start    = row.get(ps_col, '')
            p_type     = row.get(pt_col, '')
            p_paint    = row.get(pc_col, '')
            p_clock    = row.get(sc_col, '')
            p_clock_pt = row.get(scp_col, '')

            # build full row text for shot/event parsing
            row_text = ' '.join(safe_str(val) for val in row.to_dict().values())
            # only skip when the TEAM cell literally says "Off Reb" or "Neutral"
            team_cell = str(row.get('TEAM', '') or '').upper()
            skip_possession = ('OFF REB' in team_cell) or ('NEUTRAL' in team_cell) or off_reb_row

            def compute_points(text, team_name):
                # count only actual made-shot points; ignore any "Fouled +1" credit
                pts = text.count('ATR+')*2 + text.count('2FG+')*2
                pts += text.count('3FG+')*3 + text.count('FT+')*1
                return pts

            points_scored = compute_points(row_text, offense_team)

            off_players = []
            def_players = []
            off_events = []
            def_events = []

            if not skip_possession:
                poss_off = Possession(
                    practice_id     = current_practice.id,
                    season_id       = season_id,
                    game_id         = 0,
                    possession_side = offense_label,
                    time_segment    = offense_label,
                    possession_start= p_start,
                    possession_type = p_type,
                    paint_touches   = p_paint,
                    shot_clock      = p_clock,
                    shot_clock_pt   = p_clock_pt,
                    points_scored   = points_scored,
                    drill_labels    = ",".join(labels) if labels else None,
                )
                db.session.add(poss_off)
                db.session.flush()

                for cell in str(row.get(off_col, '') or '').split(','):
                    name = cell.strip()
                    if not name:
                        continue
                    pid = get_roster_id(name, season_id)
                    if pid is not None:
                        db.session.add(PossessionPlayer(possession_id=poss_off.id, player_id=pid))
                        off_players.append(name)

                last_offense_possession[offense_team] = (poss_off, off_players.copy())

                def persist_events(poss_id, text):
                    hudl_labels = [
                        'ATR+', 'ATR-', '2FG+', '2FG-', '3FG+', '3FG-',
                        'FT+', 'Turnover', 'Foul'
                    ]

                    for label in hudl_labels:
                        count = text.count(label)
                        for _ in range(count):
                            db.session.add(ShotDetail(possession_id=poss_id, event_type=label))

                    fp1 = f"{offense_team} Fouled +1"
                    if fp1 in text:
                        db.session.add(ShotDetail(possession_id=poss_id, event_type='FT+'))

                # capture team offensive rebounds from the TEAM column
                team_cell = row.get('TEAM', '')
                for token in extract_tokens(team_cell):
                    if token == 'Off Reb':
                        db.session.add(
                            ShotDetail(
                                possession_id=poss_off.id,
                                event_type='TEAM Off Reb'
                            )
                        )
                        for player in off_players:
                            events[player]['team_off_reb_on'] = (
                                events[player].get('team_off_reb_on', 0) + 1
                            )

                # Capture team missed-shot events for OREB% denominator
                label_val = str(row.get('Label', '') or '').strip()
                if label_val in ('ATR-', '2FG-', '3FG-'):
                    for p in off_players:
                        events[p]['team_misses_on'] = (
                            events[p].get('team_misses_on', 0) + 1
                        )

                persist_events(poss_off.id, row_text)

                poss_def = Possession(
                    practice_id     = current_practice.id,
                    season_id       = season_id,
                    game_id         = 0,
                    possession_side = defense_label,
                    time_segment    = defense_label,
                    possession_start= p_start,
                    possession_type = p_type,
                    paint_touches   = p_paint,
                    shot_clock      = p_clock,
                    shot_clock_pt   = p_clock_pt,
                    points_scored   = points_scored,
                    drill_labels    = ",".join(labels) if labels else None,
                )
                db.session.add(poss_def)
                db.session.flush()

                for cell in str(row.get(def_col, '') or '').split(','):
                    name = cell.strip()
                    if not name:
                        continue
                    pid = get_roster_id(name, season_id)
                    if pid is not None:
                        db.session.add(PossessionPlayer(possession_id=poss_def.id, player_id=pid))
                        def_players.append(name)

                persist_events(poss_def.id, row_text)

                base = {
                    'possession_start': safe_str(p_start),
                    'possession_type':  safe_str(p_type),
                    'paint_touches':   safe_str(p_paint),
                    'shot_clock':      safe_str(p_clock),
                    'shot_clock_pt':   safe_str(p_clock_pt),
                    'points_scored':   points_scored,
                }

                # 1) Offense possession with its events
                poss_data_off = dict(base)
                poss_data_off.update({
                    'side': offense_team,
                    'players_on_floor': off_players,
                    'events': off_events.copy(),
                })
                possession_data.append(poss_data_off)

                # 2) Defense possession with its events
                poss_data_def = dict(base)
                poss_data_def.update({
                    'side': defense_team,
                    'players_on_floor': def_players,
                    'events': def_events.copy(),
                })
                possession_data.append(poss_data_def)

                # 3) Reset event lists for the next row
                off_events.clear()
                def_events.clear()
            else:
                poss_off, prev_off_players = last_offense_possession.get(offense_team, (None, []))
                if poss_off is not None:
                    poss_off.points_scored = (poss_off.points_scored or 0) + points_scored
                    for label in ['ATR+','ATR-','2FG+','2FG-','3FG+','3FG-','FT+','Turnover','Foul']:
                        count = row_text.count(label)
                        for _ in range(count):
                            db.session.add(ShotDetail(possession_id=poss_off.id, event_type=label))
                    if f"{offense_team} Fouled +1" in row_text:
                        db.session.add(ShotDetail(possession_id=poss_off.id, event_type='FT+'))
                    team_cell = row.get('TEAM', '')
                    for token in extract_tokens(team_cell):
                        if token == 'Off Reb':
                            db.session.add(ShotDetail(possession_id=poss_off.id, event_type='TEAM Off Reb'))
                            if not off_reb_row:
                                for player in prev_off_players:
                                    events[player]['team_off_reb_on'] += 1
                    label_val = str(row.get('Label','')).strip()
                    if label_val in ('ATR-','2FG-','3FG-'):
                        for player in prev_off_players:
                            events[player]['team_misses_on'] += 1

        # ─── Practice rebounding & gap metrics ───────────────────────────
        if row_type_clean in (
            "offense rebounding opportunities",
            "offense rebound opportunities",
        ):
            player_cols = [col for col in row.index if _is_player_column(str(col))]
            for col in player_cols:
                tokens = split_tokens(row.get(col, ""))
                if not tokens:
                    continue
                roster_id = get_roster_id(col, season_id)
                if roster_id is None:
                    continue
                slot = player_stats_dict[roster_id]
                ensure_player_defaults(slot)
                details = player_detail_list[roster_id]
                for t in tokens:
                    if t == "Off +":
                        bump(slot, "crash_positive", 1)
                        details.append({"event": "crash_positive"})
                    elif t == "Off -":
                        bump(slot, "crash_missed", 1)
                        details.append({"event": "crash_missed"})
                    elif t == "BM +":
                        bump(slot, "back_man_positive", 1)
                        details.append({"event": "back_man_positive"})
                    elif t == "BM -":
                        bump(slot, "back_man_missed", 1)
                        details.append({"event": "back_man_missed"})
            continue

        if row_type_clean in (
            "defense rebounding opportunities",
            "defense rebound opportunities",
        ):
            player_cols = [col for col in row.index if _is_player_column(str(col))]
            for col in player_cols:
                tokens = split_tokens(row.get(col, ""))
                if not tokens:
                    continue
                roster_id = get_roster_id(col, season_id)
                if roster_id is None:
                    continue
                slot = player_stats_dict[roster_id]
                ensure_player_defaults(slot)
                details = player_detail_list[roster_id]
                for t in tokens:
                    if t == "Def +":
                        bump(slot, "box_out_positive", 1)
                        details.append({"event": "box_out_positive"})
                    elif t == "Def -":
                        bump(slot, "box_out_missed", 1)
                        details.append({"event": "box_out_missed"})
                    elif t == "Given Up":
                        bump(slot, "off_reb_given_up", 1)
                        details.append({"event": "off_reb_given_up"})
            continue

        if row_type in ("Crimson", "White"):
            player_cols = [col for col in row.index if _is_player_column(str(col))]
            handled = False
            for col in player_cols:
                tokens = split_tokens(row.get(col, ""))
                if not tokens:
                    continue
                roster_id = get_roster_id(col, season_id)
                if roster_id is None:
                    continue
                slot = player_stats_dict[roster_id]
                ensure_player_defaults(slot)
                details = player_detail_list[roster_id]
                for t in tokens:
                    if t == "Gap +":
                        bump(slot, "collision_gap_positive", 1)
                        details.append({"event": "collision_gap_positive", "context": row_type})
                        handled = True
                    elif t == "Gap -":
                        bump(slot, "collision_gap_missed", 1)
                        details.append({"event": "collision_gap_missed", "context": row_type})
                        handled = True
                    elif t == "Contest Pass +":
                        bump(slot, "pass_contest_positive", 1)
                        details.append({"event": "pass_contest_positive", "context": row_type})
                        handled = True
                    elif t == "Contest Pass -":
                        bump(slot, "pass_contest_missed", 1)
                        details.append({"event": "pass_contest_missed", "context": row_type})
                        handled = True
            if handled:
                continue

        if row_type == "PnR":
            player_cols = [col for col in row.index if _is_player_column(str(col))]
            for col in player_cols:
                tokens = split_tokens(row.get(col, ""))
                if not tokens:
                    continue
                roster_id = get_roster_id(col, season_id)
                if roster_id is None:
                    continue
                slot = player_stats_dict[roster_id]
                ensure_player_defaults(slot)
                details = player_detail_list[roster_id]
                for t in tokens:
                    if t == "Gap +":
                        bump(slot, "pnr_gap_positive", 1)
                        details.append({"event": "pnr_gap_positive"})
                    elif t == "Gap -":
                        bump(slot, "pnr_gap_missed", 1)
                        details.append({"event": "pnr_gap_missed"})
                    elif t == "Low +":
                        bump(slot, "low_help_positive", 1)
                        details.append({"event": "low_help_positive"})
                    elif t == "Low -":
                        bump(slot, "low_help_missed", 1)
                        details.append({"event": "low_help_missed"})
                    elif t == "CW +":
                        bump(slot, "close_window_positive", 1)
                        details.append({"event": "close_window_positive"})
                    elif t == "CW -":
                        bump(slot, "close_window_missed", 1)
                        details.append({"event": "close_window_missed"})
                    elif t == "SD +":
                        bump(slot, "shut_door_positive", 1)
                        details.append({"event": "shut_door_positive"})
                    elif t == "SD -":
                        bump(slot, "shut_door_missed", 1)
                        details.append({"event": "shut_door_missed"})
            continue

        # ─── 1) FREE THROW row: capture FT+ / FT- ─────────────────────────
        if row_type == "FREE THROW":
            for col in player_columns:
                cell = str(row.get(col, "") or "").strip()
                if not cell:
                    continue
                tokens = [t.strip() for t in cell.split(",") if t.strip()]
                if not tokens:
                    continue

                roster_id = get_roster_id(col, season_id)
                if roster_id is None:
                    continue

                for token in tokens:
                    if token == "FT+":
                        player_stats_dict[roster_id]["fta"] += 1
                        player_stats_dict[roster_id]["ftm"] += 1
                        player_stats_dict[roster_id]["points"] += 1
                        shot_obj = {
                            "event":       "shot_attempt",
                            "shot_class":   "ft",
                            "result":       "made",
                            "drill_labels": labels,
                        }
                        shot_location = safe_str(row.get("Shot Location", ""))
                        shot_obj["shot_location"] = shot_location
                        player_shot_list[roster_id].append(shot_obj)
                        player_detail_list[roster_id].append(dict(shot_obj))
                    elif token == "FT-":
                        player_stats_dict[roster_id]["fta"] += 1
                        shot_obj = {
                            "event":       "shot_attempt",
                            "shot_class":   "ft",
                            "result":       "miss",
                            "drill_labels": labels,
                        }
                        shot_location = safe_str(row.get("Shot Location", ""))
                        shot_obj["shot_location"] = shot_location
                        player_shot_list[roster_id].append(shot_obj)
                        player_detail_list[roster_id].append(dict(shot_obj))
            continue
        # ───────────────────────────────────────────────────────────────────


        # ─── 2) Win / Loss parsing ─────────────────────────────────────────
        if row_type == "Win / Loss":
            for team_col in ("CRIMSON", "WHITE", "ALABAMA", "BLUE"):
                cell = str(row.get(team_col, "") or "").strip()
                if not cell:
                    continue

                lower_cell = cell.lower()
                if "win" in lower_cell:
                    is_win = True
                elif "loss" in lower_cell:
                    is_win = False
                else:
                    continue

                tokens = [t.strip() for t in cell.split(",") if t.strip()]

                for token in tokens:
                    if "#" not in token:
                        continue

                    player_token = token[token.index("#") :]
                    clean_name = player_token.split("#")[-1].strip()
                    clean_name = "#" + clean_name
                    roster_id = get_roster_id(clean_name, season_id)
                    if roster_id is None:
                        continue

                    if is_win:
                        player_stats_dict[roster_id]["practice_wins"] += 1
                        event_type = "win"
                    else:
                        player_stats_dict[roster_id]["practice_losses"] += 1
                        event_type = "loss"

                    player_detail_list[roster_id].append({
                        "event": event_type,
                        "team": team_col,
                        "drill_labels": labels,
                    })
            continue
        # ───────────────────────────────────────────────────────────────────

        # ─── Defensive metrics parsing ─────────────────────────────────────
        # Defensive labels like "Bump +" can appear under the same
        # team rows that contain offensive stats. Parse them here so that
        # we capture these metrics alongside the offensive ones.
        if row_type in ("Crimson", "White", "Alabama", "Blue"):
            for col in player_columns:
                cell = str(row.get(col, "") or "").strip()
                if not cell:
                    continue
                tokens = [t.strip() for t in cell.split(",") if t.strip()]
                if not tokens:
                    continue

                roster_id = get_roster_id(col, season_id)
                if roster_id is None:
                    continue

                slot = player_stats_dict[roster_id]
                details = player_detail_list[roster_id]

                for token in tokens:
                    if token in ("Bump +", "Bump -", "Low Man +", "Low Man -"):
                        continue
                    if token in defense_mapping:
                        key = defense_mapping[token]
                        slot[key] += 1
                        detail_entry = {
                            "event": key,
                            "drill_labels": labels,
                        }

                        if token in CONTEST_LEVEL_MAP and row_shot_context:
                            _, level_name = CONTEST_LEVEL_MAP[token]
                            shot_class = row_shot_context["shot_class"]
                            shot_result = row_shot_context["shot_result"]
                            prefix = SHOT_PREFIX_LOOKUP.get(shot_class)
                            if prefix:
                                ensure_player_defaults(slot)
                                attempt_field = f"{prefix}_{level_name}_attempts"
                                make_field = f"{prefix}_{level_name}_makes"
                                bump(slot, attempt_field, 1)
                                if shot_result == "made":
                                    bump(slot, make_field, 1)
                            detail_entry.update({
                                "contest_level": level_name,
                                "shot_class": shot_class,
                                "shot_result": shot_result,
                            })

                        details.append(detail_entry)
                        if row_type in ("Crimson", "White"):
                            if col in off_players:
                                off_events.append(token)
                            elif col in def_players:
                                def_events.append(token)
        # allow offensive parsing to run as well for the same row
        # ───────────────────────────────────────────────────────────────────

        # ─── 3) Offense‐equivalent rows: Shot Type, Mapped Stats ──────────
        if row_type in ("Crimson", "White", "Alabama", "Blue"):
            for col in player_columns:
                cell = str(row.get(col, "") or "").strip()
                if not cell:
                    continue
                tokens = [t.strip() for t in cell.split(",") if t.strip()]
                if not tokens:
                    continue

                roster_id = get_roster_id(col, season_id)
                if roster_id is None:
                    continue

                # ─── Corrected Assisted‐shot logic ────────────────────────────────
                assisted_flag = False
                for other_col in player_columns:
                    assist_cell = str(row.get(other_col, "") or "").strip()
                    if not assist_cell:
                        continue
                    assist_tokens = [t.strip() for t in assist_cell.split(",") if t.strip()]
                    if any(t == "Assist" or t == "Pot. Assist" for t in assist_tokens):
                        assisted_flag = True
                        break
                # ───────────────────────────────────────────────────────────────────

                for token in tokens:
                    if row_type in ("Crimson", "White"):
                        if col in off_players:
                            off_events.append(token)
                        elif col in def_players:
                            def_events.append(token)
                    cls = None
                    result = None

                    # Only count ATR+ / ATR- as actual ATR attempts
                    if token in ("ATR+", "ATR-"):
                        cls = "atr"
                        result = "made" if token == "ATR+" else "miss"
                        player_stats_dict[roster_id]["atr_attempts"] += 1
                        if token == "ATR+":
                            player_stats_dict[roster_id]["atr_makes"] += 1
                            player_stats_dict[roster_id]["points"] += 2

                    elif token in ("2FG+", "2FG-"):
                        cls = "2fg"
                        result = "made" if token == "2FG+" else "miss"
                        player_stats_dict[roster_id]["fg2_attempts"] += 1
                        if token == "2FG+":
                            player_stats_dict[roster_id]["fg2_makes"] += 1
                            player_stats_dict[roster_id]["points"] += 2

                    elif token in ("3FG+", "3FG-"):
                        cls = "3fg"
                        result = "made" if token == "3FG+" else "miss"
                        player_stats_dict[roster_id]["fg3_attempts"] += 1
                        if token == "3FG+":
                            player_stats_dict[roster_id]["fg3_makes"] += 1
                            player_stats_dict[roster_id]["points"] += 3

                    if cls:
                        # Safely coerce “POSSESSION TYPE” to a string
                        poss_val = row.get("POSSESSION TYPE", "")
                        if pd.isna(poss_val):
                            possession_str = ""
                        else:
                            possession_str = str(poss_val).strip()

                        shot_obj = {
                            "event":          "shot_attempt",
                            "shot_class":      cls,
                            "result":          result,
                            "possession_type": possession_str,
                            "Assisted":        "Assisted"     if assisted_flag else "",
                            "Non-Assisted":    "" if assisted_flag else "Non-Assisted",
                            "drill_labels":    labels,
                        }
                        shot_location = safe_str(row.get("Shot Location", ""))
                        shot_obj["shot_location"] = shot_location

                        # For both ATR and 2FG shot types, capture 2FG‐subcategory columns:
                        if cls in ("2fg", "atr"):
                            for detail_col in df.columns:
                                # this catches columns like "2FG (Dribble)", "2FG (Feet)", etc.
                                if detail_col.startswith("2FG (") and detail_col.endswith(")"):
                                    suffix = detail_col[len("2FG ("):-1].lower().replace(' ', '_').replace('/', '_')
                                    # 1) store under "2fg_<suffix>"
                                    key_2fg = f"2fg_{suffix}"
                                    shot_obj[key_2fg] = row.get(detail_col, "")
                                    # 2) if this was an ATR shot, also duplicate into "atr_<suffix>"
                                    if cls == "atr":
                                        key_atr = f"atr_{suffix}"
                                        shot_obj[key_atr] = row.get(detail_col, "")

                            # capture any 2FG scheme fields (Attack/Drive/Pass)
                            for scheme_col in ("2FG Scheme (Attack)", "2FG Scheme (Drive)", "2FG Scheme (Pass)"):
                                if scheme_col in df.columns:
                                    val = str(row.get(scheme_col, "") or "").strip()
                                    if val:
                                        # original "2fg_scheme_attack" key
                                        shot_obj_key_2fg = scheme_col.lower().replace(" ", "_").replace("(", "").replace(")", "")
                                        shot_obj[shot_obj_key_2fg] = val
                                        # if ATR, also duplicate it with "atr_scheme_attack" key
                                        if cls == "atr":
                                            # e.g. schema "2fg_scheme_attack" → "atr_scheme_attack"
                                            key_atr_scheme = shot_obj_key_2fg.replace("2fg", "atr")
                                            shot_obj[key_atr_scheme] = val


                        # For 3FG shot types, capture 3FG‐subcategory columns:
                        elif cls == "3fg":
                            for suffix in ("Contest", "Footwork", "Good/Bad", "Line", "Move", "Pocket", "Shrink", "Type"):
                                col_name = f"3FG ({suffix})"
                                if col_name in df.columns:
                                    json_key = f"3fg_{suffix.lower().replace('/', '_').replace(' ', '_')}"
                                    shot_obj[json_key] = row.get(col_name, "")
                            for scheme_col in ("3FG Scheme (Attack)", "3FG Scheme (Drive)", "3FG Scheme (Pass)"):
                                if scheme_col in df.columns:
                                    val = str(row.get(scheme_col, "") or "").strip()
                                    if val:
                                        shot_obj_key = scheme_col.lower().replace(" ", "_").replace("(", "").replace(")", "")
                                        shot_obj[shot_obj_key] = val

                        player_shot_list[roster_id].append(shot_obj)
                        player_detail_list[roster_id].append(dict(shot_obj))

                    # ─── Free Throws fallback (just in case) ─────────────────————
                    if token == "FT+":
                        player_stats_dict[roster_id]["fta"] += 1
                        player_stats_dict[roster_id]["ftm"] += 1
                        player_stats_dict[roster_id]["points"] += 1
                        ft_obj = {
                            "event":       "shot_attempt",
                            "shot_class":   "ft",
                            "result":       "made",
                            "drill_labels": labels,
                        }
                        shot_location = safe_str(row.get("Shot Location", ""))
                        ft_obj["shot_location"] = shot_location
                        player_shot_list[roster_id].append(ft_obj)
                        player_detail_list[roster_id].append(dict(ft_obj))
                        continue
                    elif token == "FT-":
                        player_stats_dict[roster_id]["fta"] += 1
                        ft_obj = {
                            "event":       "shot_attempt",
                            "shot_class":   "ft",
                            "result":       "miss",
                            "drill_labels": labels,
                        }
                        shot_location = safe_str(row.get("Shot Location", ""))
                        ft_obj["shot_location"] = shot_location
                        player_shot_list[roster_id].append(ft_obj)
                        player_detail_list[roster_id].append(dict(ft_obj))
                        continue

                    # ─── Mapped basic stats (Assist, Turnover, etc.) ─────────————
                    stat_mapping = {
                        "Assist":      "assists",
                        "Turnover":    "turnovers",
                        "Pot. Assist": "pot_assists",
                        "2nd Assist":  "second_assists",
                        "Fouled":      "foul_by",
                        "Foul":        "foul_by",
                    }
                    if token in stat_mapping:
                        key = stat_mapping[token]
                        player_stats_dict[roster_id][key] += 1
                        player_detail_list[roster_id].append({
                            "event": key,
                            "drill_labels": labels,
                        })
                        continue

                # done processing tokens for this player in this offense row
            continue
        # ───────────────────────────────────────────────────────────────────────────


        # ─── 4) Blue Collar parsing ──────────────────────────────────────────
        if row_type.startswith("#"):
            cell = str(row.get(row_type.strip(), "") or "").strip()
            if cell:
                tokens = [t.strip() for t in cell.split(",") if t.strip()]
                roster_id = get_roster_id(row_type, season_id)
                if roster_id is not None:
                    blue_collar_mapping = {
                        "Reb Tip":      "reb_tip",
                        "Def Reb":      "def_reb",
                        "Misc":         "misc",
                        "Deflection":   "deflection",
                        "LB / Steal":   "steal",
                        "Block":        "block",
                        "Off Reb":      "off_reb",
                        "Floor Dive":   "floor_dive",
                        "Charge Taken": "charge_taken"
                    }
                    sprint_mapping = {
                        "Sprint Win":  "sprint_wins",
                        "Sprint Loss": "sprint_losses",
                    }
                    for token in tokens:
                        if token in blue_collar_mapping:
                            key = blue_collar_mapping[token]
                            player_blue_dict[roster_id][key] += 1
                            event_name = key
                        elif token in sprint_mapping:
                            key = sprint_mapping[token]
                            player_stats_dict[roster_id][key] += 1
                            event_name = key
                        else:
                            continue

                        player_detail_list[roster_id].append({
                            "event": event_name,
                            "drill_labels": labels,
                        })
            continue
        # ────────────────────────────────────────────────────────────────────

        # (Any rows we don’t explicitly parse just fall through.)

    # ─── Step C: After looping all rows, write aggregated stats ───────

    # Now insert each player’s aggregated stats + blue collar
    for roster_id in set(player_stats_dict) | set(player_blue_dict):
        stats = player_stats_dict.get(roster_id, {})
        blues = player_blue_dict.get(roster_id, {})
        shots = player_shot_list.get(roster_id, [])
        details = player_detail_list.get(roster_id, [])
        
        # 1) Insert PlayerStats
        db.session.add(
            PlayerStats(
                player_name       = db.session.get(Roster, roster_id).player_name,
                season_id         = season_id,
                practice_id       = practice_id,
                game_id           = None,
                
                points            = stats.get("points", 0),
                assists           = stats.get("assists", 0),
                pot_assists       = stats.get("pot_assists", 0),
                second_assists    = stats.get("second_assists", 0),
                turnovers         = stats.get("turnovers", 0),
                
                atr_makes         = stats.get("atr_makes", 0),
                atr_attempts      = stats.get("atr_attempts", 0),
                fg2_makes         = stats.get("fg2_makes", 0),
                fg2_attempts      = stats.get("fg2_attempts", 0),
                fg3_makes         = stats.get("fg3_makes", 0),
                fg3_attempts      = stats.get("fg3_attempts", 0),

                ftm               = stats.get("ftm", 0),
                fta               = stats.get("fta", 0),
                foul_by           = stats.get("foul_by", 0),
                contest_front     = stats.get("contest_front", 0),
                contest_side      = stats.get("contest_side", 0),
                contest_behind    = stats.get("contest_behind", 0),
                contest_late      = stats.get("contest_late", 0),
                contest_no        = stats.get("contest_no", 0),
                contest_early     = stats.get("contest_early", 0),
                atr_contest_attempts    = stats.get("atr_contest_attempts", 0),
                atr_contest_makes       = stats.get("atr_contest_makes", 0),
                atr_late_attempts       = stats.get("atr_late_attempts", 0),
                atr_late_makes          = stats.get("atr_late_makes", 0),
                atr_no_contest_attempts = stats.get("atr_no_contest_attempts", 0),
                atr_no_contest_makes    = stats.get("atr_no_contest_makes", 0),
                fg2_contest_attempts    = stats.get("fg2_contest_attempts", 0),
                fg2_contest_makes       = stats.get("fg2_contest_makes", 0),
                fg2_late_attempts       = stats.get("fg2_late_attempts", 0),
                fg2_late_makes          = stats.get("fg2_late_makes", 0),
                fg2_no_contest_attempts = stats.get("fg2_no_contest_attempts", 0),
                fg2_no_contest_makes    = stats.get("fg2_no_contest_makes", 0),
                fg3_contest_attempts    = stats.get("fg3_contest_attempts", 0),
                fg3_contest_makes       = stats.get("fg3_contest_makes", 0),
                fg3_late_attempts       = stats.get("fg3_late_attempts", 0),
                fg3_late_makes          = stats.get("fg3_late_makes", 0),
                fg3_no_contest_attempts = stats.get("fg3_no_contest_attempts", 0),
                fg3_no_contest_makes    = stats.get("fg3_no_contest_makes", 0),
                pass_contest_positive = stats.get("pass_contest_positive", 0),
                pass_contest_missed   = stats.get("pass_contest_missed", 0),
                bump_positive     = stats.get("bump_positive", 0),
                bump_missed       = stats.get("bump_missed", 0),
                blowby_total      = stats.get("blowby_total", 0),
                blowby_triple_threat = stats.get("blowby_triple_threat", 0),
                blowby_closeout    = stats.get("blowby_closeout", 0),
                blowby_isolation   = stats.get("blowby_isolation", 0),

                # --- Practice rebounding & gap metrics ---
                crash_positive        = stats.get("crash_positive", 0),
                crash_missed          = stats.get("crash_missed", 0),
                back_man_positive     = stats.get("back_man_positive", 0),
                back_man_missed       = stats.get("back_man_missed", 0),
                box_out_positive      = stats.get("box_out_positive", 0),
                box_out_missed        = stats.get("box_out_missed", 0),
                off_reb_given_up      = stats.get("off_reb_given_up", 0),
                collision_gap_positive = stats.get("collision_gap_positive", 0),
                collision_gap_missed   = stats.get("collision_gap_missed", 0),
                pnr_gap_positive      = stats.get("pnr_gap_positive", 0),
                pnr_gap_missed        = stats.get("pnr_gap_missed", 0),
                low_help_positive     = stats.get("low_help_positive", 0),
                low_help_missed       = stats.get("low_help_missed", 0),
                close_window_positive = stats.get("close_window_positive", 0),
                close_window_missed   = stats.get("close_window_missed", 0),
                shut_door_positive    = stats.get("shut_door_positive", 0),
                shut_door_missed      = stats.get("shut_door_missed", 0),

                practice_wins     = stats.get("practice_wins", 0),
                practice_losses   = stats.get("practice_losses", 0),
                sprint_wins       = stats.get("sprint_wins", 0),
                sprint_losses     = stats.get("sprint_losses", 0),

                shot_type_details = json.dumps(shots) if shots else None,
                stat_details      = json.dumps(details) if details else None
            )
        )
        
        # 2) Insert BlueCollarStats
        total_bcp = sum(blues.get(k, 0) * blue_collar_values[k] for k in blues)
        db.session.add(
            BlueCollarStats(
                season_id         = season_id,
                practice_id       = practice_id,
                player_id         = roster_id,
                total_blue_collar = total_bcp,
                
                reb_tip       = blues.get("reb_tip", 0),
                def_reb       = blues.get("def_reb", 0),
                misc          = blues.get("misc", 0),
                deflection    = blues.get("deflection", 0),
                steal         = blues.get("steal", 0),
                block         = blues.get("block", 0),
                off_reb       = blues.get("off_reb", 0),
                floor_dive    = blues.get("floor_dive", 0),
                charge_taken  = blues.get("charge_taken", 0),
            )
        )
    db.session.commit()

    # ─── Compute lineup and on/off metrics ───────────────────────────
    lineup_efficiencies = compute_lineup_efficiencies(
        possession_data,
        group_sizes=(2, 3, 4, 5),
        min_poss=1,
    )
    player_on_off = compute_player_on_off_by_team(possession_data)

    # ─────────────────────────────────────────────────────────────────

    return {
        "lineup_efficiencies": lineup_efficiencies,
        "player_on_off": player_on_off,
    }


if __name__ == "__main__":
    import sys

    # The `routes.py` call passes: parse_practice_csv(path, season_id, category, file_date)
    _, csv_path, season_id_str, category, file_date = sys.argv
    season_id = int(season_id_str)

    # ── Import `app` only now, to avoid circular imports ────────────────
    from app import app
    # ─────────────────────────────────────────────────────────────────

    # Push the Flask context so `db.session.add(...)` works
    with app.app_context():
        parse_practice_csv(csv_path, season_id, category, file_date)
