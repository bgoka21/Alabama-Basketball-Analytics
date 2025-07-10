import json
import pandas as pd
from collections import defaultdict
from models.database import (
    db,
    Roster,
    PlayerStats,
    BlueCollarStats,
    Practice,
    Possession,
    PlayerPossession,
)


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


def process_practice_possessions(df):
    """Parse possession information from practice DataFrame."""
    possession_data = []
    opponent_map = {
        "Crimson": "White",
        "White": "Crimson",
        "Alabama": "Blue",
        "Blue": "Alabama",
    }

    for _, row in df.iterrows():
        row_type = str(row.get("Row", "")).strip()
        if row_type not in opponent_map:
            continue

        offense_color = row_type
        defense_color = opponent_map[row_type]

        off_col = f"{offense_color.upper()} PLAYER POSSESSIONS"
        def_col = f"{defense_color.upper()} PLAYER POSSESSIONS"

        offense_players = extract_tokens(row.get(off_col, ""))
        defense_players = extract_tokens(row.get(def_col, ""))

        points_scored = 0
        for col in df.columns:
            if str(col).startswith("#"):
                for token in extract_tokens(row.get(col, "")):
                    tok = token.upper()
                    if tok == "ATR+" or tok == "2FG+":
                        points_scored += 2
                    elif tok == "3FG+":
                        points_scored += 3
                    elif tok == "FT+":
                        points_scored += 1

        base = {
            "possession_start": safe_str(row.get("POSSESSION START", "")),
            "possession_type": safe_str(row.get("POSSESSION TYPE", "")),
            "paint_touches": safe_str(row.get("PAINT TOUCHES", "")),
            "shot_clock": safe_str(row.get("SHOT CLOCK", "")),
            "shot_clock_pt": safe_str(row.get("SHOT CLOCK PT", "")),
            "points_scored": points_scored,
        }

        poss_off = dict(base)
        poss_off.update({"side": offense_color, "players_on_floor": offense_players})
        possession_data.append(poss_off)

        poss_def = dict(base)
        poss_def.update({"side": defense_color, "players_on_floor": defense_players})
        possession_data.append(poss_def)

    return possession_data


def parse_practice_csv(practice_csv_path, season_id=None, category=None, file_date=None):
    """
    1) Read CSV into DataFrame.
    2) Build player_stats_dict and player_blue_dict.
    3) Find existing Practice (routes.py created it).
    4) Insert PlayerStats and BlueCollarStats for that practice.
    """
    # Use utf-8-sig to seamlessly strip any UTF-8 BOM that may be present in
    # practice CSV files exported from Excel. Without this, the first column
    # name becomes '\ufeffRow' and row types are not recognized.
    df = pd.read_csv(practice_csv_path, encoding="utf-8-sig")
    # Normalize column headers to avoid mismatches caused by stray whitespace
    df.columns = [str(c).strip() for c in df.columns]
    
    # ─── Step A: Initialize accumulators ─────────────────────────────
    player_stats_dict   = defaultdict(lambda: defaultdict(int))
    player_blue_dict    = defaultdict(lambda: defaultdict(int))
    player_shot_list    = defaultdict(list)
    player_detail_list  = defaultdict(list)
    # ── Find all columns beginning with "#" to use for player tokens
    player_columns = [c for c in df.columns if str(c).strip().startswith("#")]
    # ─────────────────────────────────────────────────────────────────────

    # ─── Step B: Loop through each row in the CSV ────────────────────
    for _, row in df.iterrows():
        row_type = str(row.get("Row", "")).strip()
        drill_val = row.get("DRILL TYPE")
        if pd.isna(drill_val):
            drill_str = ""
        else:
            drill_str = str(drill_val)

        labels = [t.strip().upper() for t in drill_str.split(",") if t.strip()]

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

                for token in tokens:
                    if token in defense_mapping:
                        key = defense_mapping[token]
                        player_stats_dict[roster_id][key] += 1
                        player_detail_list[roster_id].append({
                            "event": key,
                            "drill_labels": labels,
                        })
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
                        "Fouled":      "foul_by"
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

    # ─── Step C: After looping all rows, find existing Practice and write to DB ───────
    practice = (
        Practice.query
        .filter_by(season_id=season_id, category=category, date=file_date)
        .first()
    )
    if practice is None:
        raise RuntimeError(
            f"Could not find existing Practice row for season={season_id}, "
            f"category='{category}', date={file_date}"
        )
    practice_id = practice.id

    # Now insert each player’s aggregated stats + blue collar
    for roster_id in set(player_stats_dict) | set(player_blue_dict):
        stats = player_stats_dict.get(roster_id, {})
        blues = player_blue_dict.get(roster_id, {})
        shots = player_shot_list.get(roster_id, [])
        details = player_detail_list.get(roster_id, [])
        
        # 1) Insert PlayerStats
        db.session.add(
            PlayerStats(
                player_name       = Roster.query.get(roster_id).player_name,
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
                bump_positive     = stats.get("bump_positive", 0),
                bump_missed       = stats.get("bump_missed", 0),
                blowby_total      = stats.get("blowby_total", 0),
                blowby_triple_threat = stats.get("blowby_triple_threat", 0),
                blowby_closeout    = stats.get("blowby_closeout", 0),
                blowby_isolation   = stats.get("blowby_isolation", 0),

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

    # ─── Process possession data for scrimmage rows ───────────────────
    possession_data = process_practice_possessions(df)
    for poss in possession_data:
        new_poss = Possession(
            game_id=0,
            practice_id=practice_id,
            season_id=season_id,
            possession_side=poss.get("side", ""),
            possession_start=poss.get("possession_start", ""),
            possession_type=poss.get("possession_type", ""),
            paint_touches=poss.get("paint_touches", ""),
            shot_clock=poss.get("shot_clock", ""),
            shot_clock_pt=poss.get("shot_clock_pt", ""),
            points_scored=poss.get("points_scored", 0),
        )
        db.session.add(new_poss)
        db.session.flush()
        for jersey in poss.get("players_on_floor", []):
            roster_id = get_roster_id(jersey, season_id)
            if roster_id is not None:
                db.session.add(
                    PlayerPossession(possession_id=new_poss.id, player_id=roster_id)
                )
    db.session.commit()
    # ─────────────────────────────────────────────────────────────────


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
