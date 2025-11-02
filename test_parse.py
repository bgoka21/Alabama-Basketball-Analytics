import os
import json
try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - allow tests without pandas
    class _DummyPD:
        Series = type('Series', (), {})
        def isna(self, val):
            return val is None
    pd = _DummyPD()

try:
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover
    class _DummyNP:
        ndarray = type('ndarray', (), {})
    np = _DummyNP()
import sqlite3
from utils.lineup import compute_lineup_efficiencies
from utils.shottype import persist_player_shot_details
# BEGIN Advanced Possession
from services.reports.advanced_possession import (
    cache_get_or_compute_adv_poss_game,
    invalidate_adv_poss_game,
)
# END Advanced Possession
# BEGIN Playcall Report
from services.reports.playcall import (
    compute_playcall_report_from_dataframe,
    invalidate_playcall_report,
)
# END Playcall Report
from models.database import db, Game, PlayerStats, Possession, TeamStats, BlueCollarStats, OpponentBlueCollarStats, PlayerPossession, Roster, ShotDetail

#print("🔥 parse_csv() function has started executing!")

# --- Helper Functions ---

def safe_value(val, default=0):
    # anything array‑ or series‑like gets treated as “no data”
    if isinstance(val, (list, dict, tuple, np.ndarray, pd.Series)):
        return default
    return default if pd.isna(val) else val



def safe_str(val, default=""):
    """Return the string representation of val if not NaN; otherwise, return default (empty string)."""
    return default if pd.isna(val) else str(val)

# --- Period Normalization Helper ---
def normalize_period_label(value):
    """Return a canonical period label from assorted CSV variations."""
    if pd.isna(value):
        return ""

    raw_text = str(value)
    normalized = raw_text.replace("\xa0", " ")
    for dash in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2015"):
        normalized = normalized.replace(dash, "-")

    # Treat any dash as a space to handle values like "2nd-half".
    normalized = normalized.replace("-", " ")
    normalized = " ".join(normalized.strip().split())

    lowered = normalized.lower()
    if not lowered:
        return ""

    if "1st half" in lowered or "first half" in lowered:
        return "1st Half"

    if "2nd half" in lowered or "second half" in lowered:
        return "2nd Half"

    if "overtime" in lowered:
        return "Overtime"

    # Handle short "OT" tokens that may appear alone or before/after other words.
    tokens = lowered.split()
    if any(tok == "ot" for tok in tokens):
        return "Overtime"

    return normalized

# BEGIN safe_increment_helper
def inc_stat(bucket: dict, key: str, by: int = 1):
    """
    Increment bucket[key] by `by`, creating the key at 0 if missing.
    Keeps buckets as plain dicts; no defaultdict required.
    """
    if key is None or key == "":
        return  # ignore empty keys defensively
    bucket[key] = bucket.get(key, 0) + by
# END safe_increment_helper

def extract_tokens(cell_value):
    """
    Splits a cell’s string into normalized tokens, handling commas, semicolons and newlines.
    """
    if pd.isna(cell_value):
        return []
    if not isinstance(cell_value, str):
        cell_value = str(cell_value)
    normalized = cell_value.replace("–", "-")
    for sep in (";", "\n", "\r", "\t"):
        normalized = normalized.replace(sep, ",")
    return [token.strip() for token in normalized.split(",") if token.strip()]

def initialize_player_stats(player_name, game_id, season_id, stat_mapping, blue_collar_values):
    """
    Build a dict of zeros for every base stat plus ​every subcategory under ATR, 2FG, and 3FG.
    Also includes an empty shot_type_details list for storing per-shot JSON objects.
    """
    base_stats = {
        "game_id":          game_id,
        "season_id":        season_id,
        "player_name":      player_name,
        "points":          0,
        "assists":         0,
        "pot_assists":     0,
        "second_assists":  0,
        "turnovers":       0,
        "atr_makes":       0,
        "atr_attempts":    0,
        "fg2_makes":       0,
        "fg2_attempts":    0,
        "fg3_makes":       0,
        "fg3_attempts":    0,
        "ftm":             0,
        "fta":             0,
        "foul_by":         0,
        "atr_fouled":      0,
        "fg2_fouled":      0,
        "fg3_fouled":      0,
        # Rebounding duties & practice-only metrics (mirrored for games)
        "crash_positive":        0,
        "crash_missed":         0,
        "back_man_positive":    0,
        "back_man_missed":      0,
        "box_out_positive":     0,
        "box_out_missed":       0,
        "off_reb_given_up":     0,
        "collision_gap_positive": 0,
        "collision_gap_missed":   0,
        "pass_contest_positive":  0,
        "pass_contest_missed":    0,
        "pnr_gap_positive":       0,
        "pnr_gap_missed":         0,
        "low_help_positive":      0,
        "low_help_missed":        0,
        "close_window_positive":  0,
        "close_window_missed":    0,
        "shut_door_positive":     0,
        "shut_door_missed":       0,
        "shot_type_details": [],
        "blue_collar_accum": {key: 0 for key in blue_collar_values.keys()}
    }
    # Copy in any other mapped stats (e.g. "blocks", "steals") you already had:
    base_stats.update({key: 0 for key in stat_mapping.values()})

    # ─── Build out subcategory keys for ATR, 2FG, and 3FG ───────────────────
    def slug(s):
        return (
            s.lower()
             .replace("(", "").replace(")", "")
             .replace("/", "_")
             .replace("-", "_")
             .replace(" ", "_")
        )

    detail_columns = {
        # ATR & 2FG shared columns
        "2FG (Feet)":           ["Off 1 Foot", "Off 2 Feet"],
        "2FG (Hands)":          ["Left Hand Finish", "Right Hand Finish", "Hands To Rim", "Hands Away From Rim"],
        "2FG (Defenders)":      ["Primary Defender", "Secondary Defender", "Multiple Defenders", "Unguarded"],
        "2FG (Type)":           ["Dunk", "Catch", "Floater", "Layup", "Pull Up", "Step Back"],
        "2FG (Dribble)":        ["Dribble", "No Dribble"],
        "2FG (Other)":          ["Blocked"],
        "2FG (PA)":             ["Play Action", "No Play Action"],
        "2FG (RA)":             ["Restricted Area", "Non Restricted Area"],
        "2FG Scheme (Attack)":  ["Beast / Post", "DHO / Get", "Iso", "PnR Handler", "Off Closeout", "PnR Sneak", "Transition Push", "OREB Putback"],
        "2FG Scheme (Drive)":   ["Middle Drive", "Baseline Drive", "Slot Drive", "Drive Right", "Drive Left"],
        "2FG Scheme (Pass)":    [
            "Swing", "Check Down", "Off Screen", "1 More", "Lift", "PnR Pocket", "Post Entry",
            "Drift", "PnR Lob", "Post Pass Out", "Kickdown", "PnR Late Roll", "Dump Off",
            "Slot Skip", "Pocket Extra", "Lob", "Nail Pitch", "DHO / Get", "PnR Pop", "Slash / Cut",
            "Reshape", "Shake", "Skip", "Pull Behind", "Outlet", "Press Break", "Cross Court",
            "Under OB", "Kick Ahead", "Dagger", "Side / Press OB"
        ],

        # 3FG‐specific columns
        "3FG (Contest)":        ["Contested", "Uncontested", "Late Contest", "Blocked"],
        "3FG (Footwork)":       ["WTN Left-Right", "WTN Right-Left", "Left-Right", "Right-Left", "Hop"],
        "3FG (Good/Bad)":       ["Good", "Bad", "Neutral Three"],
        "3FG (Line)":           ["On The Line", "Off The Line"],
        "3FG (Move)":           ["Stationary", "On Move"],
        "3FG (Pocket)":         ["Shot Pocket", "Non-Shot Pocket"],
        "3FG (Shrink)":         ["Shrink", "Non-Shrink"],
        "3FG (Type)":           ["Catch and Shoot", "Pull Up", "Step Back", "Catch and Hold", "Slide Dribble"],
        "3FG Scheme (Attack)":  ["Beast / Post", "DHO / Get", "PnR Handler", "Iso", "Off Closeout", "PnR Sneak", "Transition Push", "OREB Putback"],
        "3FG Scheme (Drive)":   ["Drive Right", "Drive Left", "Dip"],
        "3FG Scheme (Pass)":    [
            "Swing", "Checkdown", "Off Screen", "1 More", "Lift", "Drift", "Post Entry", "Post Pass Out",
            "Kickdown", "Slot Skip", "Pocket Extra (In)", "Pocket Extra (Out)", "Nail Pitch", "DHO / Get",
            "PnR Pop", "Reshape", "Shake", "Skip", "Pull Behind", "Outlet", "Press Break", "Cross Court",
            "Under OB", "Kick Ahead", "Dagger", "Side / Press OB"
        ]
    }

    # Now zero-out every possible "<shot>_<col>_<label>" key for shot in ("atr","fg2","fg3")
    for shot in ("atr", "fg2", "fg3"):
        for col_name, labels in detail_columns.items():
            for lab in labels:
                key = f"{shot}_{slug(col_name)}_{slug(lab)}"
                base_stats[key] = 0

    return base_stats



def process_offense_row(row, df_columns, player_stats_dict, game_id, season_id, stat_mapping, blue_collar_values):
    """
    Parses one “Offense” row (game), updates player_stats_dict, and records detailed shot info
    including Assisted vs Non-Assisted flags, plus every subcategory under ATR, 2FG, and 3FG.
    """
    shooter_col   = None
    shooter_type  = None
    shot_result   = None
    assisted_flag = False

    # 1) First pass: identify shooter_col & shooter_type (exact match on ATR/2FG/3FG tokens)
    for col in df_columns:
        if not col.startswith("#"):
            continue

        tokens = extract_tokens(row.get(col, ""))
        if not tokens:
            continue

        # Initialize this player if we haven't seen him/her yet
        if col not in player_stats_dict:
            player_stats_dict[col] = initialize_player_stats(col, game_id, season_id, stat_mapping, blue_collar_values)

        # Check for ATR
        if "ATR+" in tokens or "ATR-" in tokens:
            shooter_col   = col
            shooter_type  = "ATR"
            was_made      = ("ATR+" in tokens)
            shot_result   = "made" if was_made else "missed"

            # Increment counters once
            if was_made:
                player_stats_dict[col]["atr_makes"] += 1
                player_stats_dict[col]["points"]   += 2
            player_stats_dict[col]["atr_attempts"] += 1

            break  # stop scanning other columns for shooter

        # Otherwise, check for 2FG
        elif "2FG+" in tokens or "2FG-" in tokens:
            shooter_col   = col
            shooter_type  = "2FG"
            was_made      = ("2FG+" in tokens)
            shot_result   = "made" if was_made else "missed"

            if was_made:
                player_stats_dict[col]["fg2_makes"] += 1
                player_stats_dict[col]["points"]   += 2
            player_stats_dict[col]["fg2_attempts"] += 1

            break

        # Otherwise, check for 3FG
        elif "3FG+" in tokens or "3FG-" in tokens:
            shooter_col   = col
            shooter_type  = "3FG"
            was_made      = ("3FG+" in tokens)
            shot_result   = "made" if was_made else "missed"

            if was_made:
                player_stats_dict[col]["fg3_makes"] += 1
                player_stats_dict[col]["points"]   += 3
            player_stats_dict[col]["fg3_attempts"] += 1

            break

    # 2) Scan entire row for EXACT "Assist" or "Pot. Assist" once a shooter is known
    if shooter_col and shooter_type:
        for other_col in df_columns:
            other_tokens = extract_tokens(row.get(other_col, ""))
            if "Assist" in other_tokens:
                assisted_flag = True
                player_stats_dict[shooter_col]["assists"] += 1
                break
            elif "Pot. Assist" in other_tokens:
                assisted_flag = True
                player_stats_dict[shooter_col]["pot_assists"] += 1
                break

    # 3) Free Throws (FT+ / FT-) always counted, even if no shooter_col
    for col in df_columns:
        if not col.startswith("#"):
            continue
        tokens = extract_tokens(row.get(col, ""))
        if not tokens:
            continue

        ft_makes = tokens.count("FT+")
        ft_misses = tokens.count("FT-")
        if not ft_makes and not ft_misses:
            continue

        if col not in player_stats_dict:
            player_stats_dict[col] = initialize_player_stats(col, game_id, season_id, stat_mapping, blue_collar_values)

        if ft_makes:
            player_stats_dict[col]["ftm"] += ft_makes
            player_stats_dict[col]["points"] += ft_makes

        attempts = ft_makes + ft_misses
        if attempts:
            player_stats_dict[col]["fta"] += attempts

    # 4) Miscellaneous mapped stats (Turnover, 2nd Assist, Fouled), excluding "Assist"/"Pot. Assist"
    for col in df_columns:
        if not col.startswith("#"):
            continue
        tokens = extract_tokens(row.get(col, ""))
        if col not in player_stats_dict:
            player_stats_dict[col] = initialize_player_stats(col, game_id, season_id, stat_mapping, blue_collar_values)

        for token in tokens:
            if token in stat_mapping and token not in ("Assist", "Pot. Assist"):
                mapped_key = stat_mapping[token]
                player_stats_dict[col][mapped_key] = player_stats_dict[col].get(mapped_key, 0) + 1

    # 5) Build and append one shot_detail object if we found shooter_type
    if shooter_col and shooter_type:
        # Safely coerce possession type
        poss_val = row.get("POSSESSION TYPE", "")
        possession_str = "" if pd.isna(poss_val) else str(poss_val).strip()

        shot_detail = {
            "shot_class":      shooter_type,
            "result":          shot_result,
            "possession_type": possession_str,
            "Assisted":        "Assisted"     if assisted_flag else "",
            "Non-Assisted":    "" if assisted_flag else "Non-Assisted"
        }
        shot_location = safe_str(row.get("Shot Location", ""))
        shot_detail["shot_location"] = shot_location
        key_prefix = shooter_type.lower()  # → "atr" or "2fg" or "3fg"

        # ─── Shared ATR & 2FG subcategories (pull from "2FG (…)” columns) ────────────
        if shooter_type in ("ATR", "2FG"):
            for suffix in ["Type", "Defenders", "Dribble", "Feet", "Hands", "Other", "PA", "RA"]:
                col_name = f"2FG ({suffix})"
                shot_detail[f"{key_prefix}_{suffix.lower().replace(' ', '_')}"] = safe_str(row.get(col_name, ""))

            # 2FG Scheme (Attack) & (Pass)
            for token2 in extract_tokens(row.get("2FG Scheme (Attack)", "")):
                shot_detail[f"{key_prefix}_scheme_attack"] = token2

            for token2 in extract_tokens(row.get("2FG Scheme (Drive)", "")):
                shot_detail[f"{key_prefix}_scheme_drive"] = token2
                
            for token2 in extract_tokens(row.get("2FG Scheme (Pass)", "")):
                shot_detail[f"{key_prefix}_scheme_pass"] = token2

        # ─── 3FG-only subcategories (pull from "3FG (…)” columns) ───────────────
        else:  # shooter_type == "3FG"
            for suffix in ["Contest", "Footwork", "Good/Bad", "Line", "Move", "Pocket", "Shrink", "Type"]:
                col_name = f"3FG ({suffix})"
                json_key = f"{key_prefix}_{suffix.lower().replace('/', '_').replace(' ', '_')}"
                shot_detail[json_key] = safe_str(row.get(col_name, ""))

            # 3FG Scheme (Attack), (Drive), (Pass)
            for token3 in extract_tokens(row.get("3FG Scheme (Attack)", "")):
                shot_detail[f"{key_prefix}_scheme_attack"] = token3
            for token3 in extract_tokens(row.get("3FG Scheme (Drive)", "")):
                shot_detail[f"{key_prefix}_scheme_drive"] = token3
            for token3 in extract_tokens(row.get("3FG Scheme (Pass)", "")):
                shot_detail[f"{key_prefix}_scheme_pass"] = token3

        # 6) Append this single shot_detail to the shooter’s shot_type_details list
        player_stats_dict[shooter_col]["shot_type_details"].append(shot_detail)




def process_defense_row(row, opponent_totals, stat_mapping):
    tokens = extract_tokens(row.get("OPP STATS", ""))
    for token in tokens:
        if token == "ATR+":
            opponent_totals["atr_makes"] += 1
            opponent_totals["atr_attempts"] += 1
            opponent_totals["total_points"] += 2
        elif token == "ATR-":
            opponent_totals["atr_attempts"] += 1
        elif token == "2FG+":
            opponent_totals["fg2_makes"] += 1
            opponent_totals["fg2_attempts"] += 1
            opponent_totals["total_points"] += 2
        elif token == "2FG-":
            opponent_totals["fg2_attempts"] += 1
        elif token == "3FG+":
            opponent_totals["fg3_makes"] += 1
            opponent_totals["fg3_attempts"] += 1
            opponent_totals["total_points"] += 3
        elif token == "3FG-":
            opponent_totals["fg3_attempts"] += 1
        elif token == "FT+":
            opponent_totals["ftm"] += 1
            opponent_totals["fta"] += 1
            opponent_totals["total_points"] += 1
        elif token == "FT-":
            opponent_totals["fta"] += 1
        elif token in stat_mapping:
            opponent_totals[stat_mapping[token]] += 1

blue_collar_mapping = {
    "Reb Tip": "reb_tip",
    "Def Reb": "def_reb",
    "Misc": "misc",
    "Deflection": "deflection",
    "LB / Steal": "steal",
    "Block": "block",
    "Off Reb": "off_reb",
    "Floor Dive": "floor_dive",
    "Charge Taken": "charge_taken"
}

def process_def_note_row(row, df_columns, player_stats_dict, game_id, season_id, stat_mapping, blue_collar_values, team_totals):
    for col in df_columns:
        if col.startswith("#"):
            tokens = extract_tokens(row.get(col, ""))
            if tokens:
                if col not in player_stats_dict:
                    player_stats_dict[col] = initialize_player_stats(col, game_id, season_id, stat_mapping, blue_collar_values)
                for token in tokens:
                    if token in blue_collar_mapping:
                        key = blue_collar_mapping[token]
                        player_stats_dict[col]["blue_collar_accum"][key] += 1
                        team_totals[key] = team_totals.get(key, 0) + 1
                        team_totals["total_blue_collar"] += blue_collar_values[key]
                blue_total = sum(
                    player_stats_dict[col]["blue_collar_accum"].get(stat, 0) * blue_collar_values.get(stat, 0)
                    for stat in blue_collar_values
                )
                player_stats_dict[col]["_blue_collar_total"] = blue_total

def process_player_row(row, player_stats_dict, game_id, season_id, stat_mapping, blue_collar_values, team_totals):
    player_name = str(row.get("Row", "")).strip()
    if player_name not in player_stats_dict:
        player_stats_dict[player_name] = initialize_player_stats(player_name, game_id, season_id, stat_mapping, blue_collar_values)
    else:
        for key in blue_collar_values:
            if key not in player_stats_dict[player_name]["blue_collar_accum"]:
                player_stats_dict[player_name]["blue_collar_accum"][key] = 0
    tokens = extract_tokens(row.get(player_name, ""))
    for token in tokens:
        if token in blue_collar_mapping:
            key = blue_collar_mapping[token]
            player_stats_dict[player_name]["blue_collar_accum"][key] += 1
            team_totals[key] = team_totals.get(key, 0) + 1
            team_totals["total_blue_collar"] += blue_collar_values[key]
    blue_total = sum(
        player_stats_dict[player_name]["blue_collar_accum"].get(stat, 0) * blue_collar_values.get(stat, 0)
        for stat in blue_collar_values
    )
    player_stats_dict[player_name]["_blue_collar_total"] = blue_total

# BEGIN contest_side_keyerror_fix
def process_defense_player_row(row, df_columns, player_stats_dict, game_id, season_id):
    defense_mapping = {
        "Foul By": "foul_by",
        "Front": "contest_front",
        "Side": "contest_side",
        "Behind": "contest_behind",
        "Late": "contest_late",
        "Contest": "contest_early",
        "No Contest": "contest_no",
        "None": "contest_no",
        "Low Man +": "low_help_positive",
        "Low Man -": "low_help_missed",
        "Contest Pass +": "pass_contest_positive",
        "Contest Pass -": "pass_contest_missed",
        "Blowby": "blowby_total",
        "Triple Threat": "blowby_triple_threat",
        "Closeout": "blowby_closeout",
        "Isolation": "blowby_isolation"
    }
    for col in df_columns:
        if not col.startswith("#"):
            continue

        tokens = extract_tokens(row.get(col, ""))
        if not tokens:
            continue

        if col not in player_stats_dict or not isinstance(player_stats_dict.get(col), dict):
            player_stats_dict[col] = initialize_player_stats(col, game_id, season_id, defense_mapping, {"dummy": 0})

        slot = player_stats_dict[col]
        for token in tokens:
            if token == "Bump +":
                inc_stat(slot, "bump_positive")
                inc_stat(slot, "collision_gap_positive")
                continue
            if token == "Bump -":
                inc_stat(slot, "bump_missed")
                inc_stat(slot, "collision_gap_missed")
                continue

            if token == "Gap +":
                inc_stat(slot, "pnr_gap_positive")
                continue
            if token == "Gap -":
                inc_stat(slot, "pnr_gap_missed")
                continue
            if token == "Low +":
                inc_stat(slot, "low_help_positive")
                continue
            if token == "Low -":
                inc_stat(slot, "low_help_missed")
                continue

            if token in defense_mapping:
                stat_key = defense_mapping[token]
                inc_stat(slot, stat_key)
# END contest_side_keyerror_fix

# --- New get_possession_breakdown_detailed() ---
def get_possession_breakdown_detailed(df):
    """
    Returns four dicts:
      1) breakdown_offense[token]  = {'count':…, 'points':…}
      2) breakdown_defense[token]  = {'count':…, 'points':…}
      3) periodic_offense[split]   = {'count':…, 'points':…}
      4) periodic_defense[split]   = {'count':…, 'points':…}
    Splits come from the "GAME SPLITS" column: "1st Half", "2nd Half", "Overtime".
    Now excludes both Neutral and Off Reb rows for the ‘count’ buckets.
    """
    desired_tokens = ["Transition","Man","Zone","Press","UOB","SLOB","Garbage","OREB Putback"]

    # possession‐type buckets
    breakdown_offense = {t:{'count':0,'points':0} for t in desired_tokens}
    breakdown_defense = {t:{'count':0,'points':0} for t in desired_tokens}

    # game‐split buckets
    periodic_offense = {
        "1st Half": {'count':0,'points':0},
        "2nd Half": {'count':0,'points':0},
        "Overtime": {'count':0,'points':0},
    }
    periodic_defense = {k:v.copy() for k,v in periodic_offense.items()}

    for _, row in df.iterrows():
        row_type = str(row.get("Row","")).strip()
        if row_type not in ("Offense","Defense"):
            continue

        # identify Neutral vs Off-Reb rows
        team_val     = str(row.get("TEAM",""))
        is_neutral   = "Neutral" in team_val
        is_off_reb   = "Off Reb" in team_val

        opp_stats_val  = str(row.get("OPP STATS",""))
        is_opp_off_reb = "Off Reb" in opp_stats_val

        # 1) possession‐type tokens
        poss_types = [t.strip() for t in str(row.get("POSSESSION TYPE","")).split(",") if t.strip()]

        # 2) compute this row’s points
        pts = 0
        if row_type == "Offense":
            for col in df.columns:
                if col.startswith("#"):
                    for tok in extract_tokens(row.get(col,"")):
                        u = tok.upper()
                        if u in ("ATR+","2FG+"):
                            pts += 2
                        elif u=="3FG+":
                            pts += 3
                        elif u=="FT+":
                            pts += 1
        else:
            for tok in extract_tokens(opp_stats_val):
                u = tok.upper()
                if u in ("ATR+","2FG+"):
                    pts += 2
                elif u=="3FG+":
                    pts += 3
                elif u=="FT+":
                    pts += 1

        # 3) apply to possession‐type buckets (unchanged)
        for tkn in poss_types:
            if tkn not in desired_tokens:
                continue
            if row_type == "Offense":
                if not is_neutral:
                    breakdown_offense[tkn]['count'] += 1
                breakdown_offense[tkn]['points'] += pts
            else:
                if not is_neutral:
                    breakdown_defense[tkn]['count'] += 1
                breakdown_defense[tkn]['points'] += pts

        # 4) apply to split buckets, now excluding Off-Reb too
        split = normalize_period_label(row.get("Period", ""))
        if split in periodic_offense:
            if row_type == "Offense":
                if not is_neutral and not is_off_reb:
                    periodic_offense[split]['count'] += 1
                periodic_offense[split]['points'] += pts
            else:
                if not is_neutral and not is_opp_off_reb:
                    periodic_defense[split]['count'] += 1
                periodic_defense[split]['points'] += pts

    return breakdown_offense, breakdown_defense, periodic_offense, periodic_defense





# --- Updated process_possessions() ---
def process_possessions(df, game_id, season_id, subtract_off_reb=True):
    # Compute overall possession counts using non-neutral rows for count totals.
    offense_rows = df[df['Row'] == "Offense"]
    offense_count = len(offense_rows)
    if subtract_off_reb:
        offense_invalid = offense_rows['TEAM'].apply(
            lambda val: ("Neutral" in str(val)) or ("Off Reb" in str(val))
        ).sum()
    else:
        offense_invalid = offense_rows['TEAM'].apply(
            lambda val: ("Neutral" in str(val))
        ).sum()
    offensive_possessions = offense_count - offense_invalid

    defense_rows = df[df['Row'] == "Defense"]
    defense_count = len(defense_rows)
    if subtract_off_reb:
        defense_invalid = defense_rows.apply(
            lambda r: (("Off Reb" in str(r['OPP STATS'])) or ("Neutral" in str(r['TEAM']))),
            axis=1
        ).sum()
    else:
        defense_invalid = defense_rows.apply(
            lambda r: ("Neutral" in str(r['TEAM'])),
            axis=1
        ).sum()
    defensive_possessions = defense_count - defense_invalid

    #print("📝 Possession Summary:")
    #print(f"   Offensive Possessions: {offensive_possessions} (from {offense_count} Offense rows, {offense_invalid} disregarded)")
    #print(f"   Defensive Possessions: {defensive_possessions} (from {defense_count} Defense rows, {defense_invalid} disregarded)")

    possession_data = []
    for index, row in df.iterrows():
        row_type = str(row.get("Row", "")).strip()
        if row_type not in ["Offense", "Defense"]:
            continue

        is_neutral = "Neutral" in str(row.get("TEAM", ""))
        points_scored = 0
        events = []
        if row_type == "Offense":
            for col in df.columns:
                if col.startswith("#"):
                    tokens = extract_tokens(row.get(col, ""))
                    for token in tokens:
                        events.append(token)
                        token = token.upper()
                        if token == "ATR+":
                            points_scored += 2
                        elif token == "2FG+":
                            points_scored += 2
                        elif token == "3FG+":
                            points_scored += 3
                        elif token == "FT+":
                            points_scored += 1
        elif row_type == "Defense":
            tokens = extract_tokens(row.get("OPP STATS", ""))
            for token in tokens:
                events.append(token)
                token = token.upper()
                if token == "ATR+":
                    points_scored += 2
                elif token == "2FG+":
                    points_scored += 2
                elif token == "3FG+":
                    points_scored += 3
                elif token == "FT+":
                    points_scored += 1

        poss = {
            "game_id": game_id or 0,
            "season_id": season_id,
            "side": row_type,  # "Offense" or "Defense"
            "possession_start": safe_str(row.get("POSSESSION START", "")),
            "possession_type": safe_str(row.get("POSSESSION TYPE", "")),
            "paint_touches": safe_str(row.get("PAINT TOUCHES", "")),
            "shot_clock": safe_str(row.get("SHOT CLOCK", "")),
            "shot_clock_pt": safe_str(row.get("SHOT CLOCK PT", "")),
            "players_on_floor": extract_tokens(row.get("PLAYER POSSESSIONS", "")),
            "points_scored": points_scored,
            "is_neutral": is_neutral,
            "events": events
        }
        possession_data.append(poss)
    #print("\n📝 Detailed Possession Data:")
    #for poss in possession_data:
        #print(poss)
    return possession_data, offensive_possessions, defensive_possessions



def get_player_id(player_name, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM roster WHERE player_name = ?", (player_name,))
    result = cursor.fetchone()
    return result[0] if result else None

def calculate_derived_metrics(player_stats):
    for player, stats in player_stats.items():
        stats["atr_fg_pct"] = stats["atr_makes"] / stats["atr_attempts"] if stats["atr_attempts"] > 0 else None
        stats["fg2_pct"] = stats["fg2_makes"] / stats["fg2_attempts"] if stats["fg2_attempts"] > 0 else None
        stats["fg3_pct"] = stats["fg3_makes"] / stats["fg3_attempts"] if stats["fg3_attempts"] > 0 else None
        stats["ft_pct"] = stats["ftm"] / stats["fta"] if stats["fta"] > 0 else None

        stats["assist_turnover_ratio"] = stats["assists"] / stats["turnovers"] if stats["turnovers"] > 0 else None
        total_assists = stats["assists"] + stats.get("second_assists", 0) + stats.get("pot_assists", 0)
        stats["adj_assist_turnover_ratio"] = total_assists / stats["turnovers"] if stats["turnovers"] > 0 else None

        fg_denom = stats["atr_attempts"] + stats["fg2_attempts"] + stats["fg3_attempts"]
        if fg_denom > 0:
            efg = (
                (
                    stats["atr_makes"]
                    + stats["fg2_makes"]
                    + 1.5 * stats["fg3_makes"]
                )
                / fg_denom
            )
            stats["efg_pct"] = efg
            stats["points_per_shot"] = round(efg * 2, 2)
        else:
            stats["efg_pct"] = None
            stats["points_per_shot"] = None
    return player_stats

def parse_csv(file_path, game_id, season_id):
    #print("✅ Starting CSV Processing...")
    #print(f"🔍 Checking file path: {os.path.abspath(file_path)}")
    #print(f"🔍 File exists? {os.path.exists(file_path)}")

    if not os.path.exists(file_path):
        print("❌ Error: CSV file not found!")
        return

    #print(f"📚 Reading CSV: {file_path}")
    df = pd.read_csv(file_path)

    #print("🔍 Unique Row Names in CSV:")
    #print(df['Row'].unique())
    #print("📊 CSV Columns:", df.columns.tolist())

    from app import create_app
    app_instance = create_app()
    with app_instance.app_context():
        game_entry = Game.query.filter_by(csv_filename=os.path.basename(file_path)).first()
        if not game_entry:
            game_entry = Game(
                season_id=season_id,
                game_date=pd.to_datetime("today").date(),
                opponent_name="Unknown",
                home_or_away="Home",
                result="N/A",
                csv_filename=os.path.basename(file_path)
            )
            db.session.add(game_entry)
            db.session.commit()
        game_id = game_entry.id
        # BEGIN Advanced Possession
        invalidate_adv_poss_game(game_id)
        # END Advanced Possession
        # BEGIN Playcall Report
        invalidate_playcall_report(game_id)
        # END Playcall Report
        print(f"🎯 Assigned Game ID: {game_id}")

    if df.empty:
        print("❌ CSV is empty! No data to insert.")
        return

    #print(f"✅ CSV Loaded! {len(df)} rows detected.")
    #print("⚙️ parse_csv() function is executing correctly!")

    # --- Define Mappings ---
    stat_mapping = {
        "Assist": "assists",
        "Turnover": "turnovers",
        "Pot. Assist": "pot_assists",
        "2nd Assist": "second_assists",
        "Fouled": "foul_by"
    }
    blue_collar_values = {
        "reb_tip": 0.5,
        "def_reb": 1.0,
        "misc": 1.0,
        "deflection": 1.0,
        "steal": 1.0,
        "block": 1.0,
        "off_reb": 1.5,
        "floor_dive": 2.0,
        "charge_taken": 4.0
    }
    global blue_collar_mapping
    blue_collar_mapping = {
        "Reb Tip": "reb_tip",
        "Def Reb": "def_reb",
        "Misc": "misc",
        "Deflection": "deflection",
        "LB / Steal": "steal",
        "Block": "block",
        "Off Reb": "off_reb",
        "Floor Dive": "floor_dive",
        "Charge Taken": "charge_taken"
    }

    player_stats_dict = {}
    team_totals = {
        "total_points": 0,
        "total_assists": 0,
        "total_second_assists": 0,
        "total_pot_assists": 0,
        "total_turnovers": 0,
        "total_atr_makes": 0,
        "total_atr_attempts": 0,
        "total_fg2_makes": 0,
        "total_fg2_attempts": 0,
        "total_fg3_makes": 0,
        "total_fg3_attempts": 0,
        "total_ftm": 0,
        "total_fta": 0,
        "total_blue_collar": 0,
        "foul_by": 0,
        "def_reb": 0,
        "off_reb": 0,
        "misc": 0,
        "deflection": 0,
        "steal": 0,
        "block": 0,
        "floor_dive": 0,
        "charge_taken": 0,
        "reb_tip": 0
    }
    opponent_totals = {
        "atr_makes": 0,
        "atr_attempts": 0,
        "fg2_makes": 0,
        "fg2_attempts": 0,
        "fg3_makes": 0,
        "fg3_attempts": 0,
        "ftm": 0,
        "fta": 0,
        "total_points": 0,
        "assists": 0,
        "turnovers": 0,
        "pot_assists": 0,
        "second_assists": 0,
        "foul_by": 0,
        "total_blue_collar": 0
    }

    opponent_blue_collar_accum = {
        "def_reb": 0,
        "off_reb": 0,
        "misc": 0,
        "deflection": 0,
        "steal": 0,
        "block": 0,
        "floor_dive": 0,
        "charge_taken": 0,
        "reb_tip": 0
    }

    offense_reb_rows = {
        "rebound opportunities",
        "offense rebounding opportunities",
        "offense rebound opportunities",
        "offensive rebounding opportunities",
        "offensive rebound opportunities",
    }
    defense_reb_rows = {
        "defense rebounding opportunities",
        "defense rebound opportunities",
        "defensive rebounding opportunities",
        "defensive rebound opportunities",
    }

    # --- Process Each Row ---
    for index, row in df.iterrows():
        row_type = str(row.get("Row", "")).strip()
        row_type_lower = row_type.lower()

        row_tokens_by_col = {}
        for col in df.columns:
            if not col.startswith("#"):
                continue
            tokens = extract_tokens(row.get(col, ""))
            if not tokens:
                continue
            if col not in player_stats_dict:
                player_stats_dict[col] = initialize_player_stats(col, game_id, season_id, stat_mapping, blue_collar_values)
            row_tokens_by_col[col] = tokens

        if row_type == "TEAM":
            tokens = extract_tokens(row.get("TEAM", ""))
            for token in tokens:
                if token in blue_collar_mapping:
                    key = blue_collar_mapping[token]
                    team_totals[key] = team_totals.get(key, 0) + 1
                    team_totals["total_blue_collar"] += blue_collar_values[key]
            continue

        if row_tokens_by_col and row_type_lower == "pnr":
            for col, tokens in row_tokens_by_col.items():
                slot = player_stats_dict[col]
                for token in tokens:
                    if token == "Gap +":
                        inc_stat(slot, "pnr_gap_positive")
                    elif token == "Gap -":
                        inc_stat(slot, "pnr_gap_missed")
                    elif token == "Low +":
                        inc_stat(slot, "low_help_positive")
                    elif token == "Low -":
                        inc_stat(slot, "low_help_missed")

        if row_type_lower in offense_reb_rows:
            for col, tokens in row_tokens_by_col.items():
                slot = player_stats_dict[col]
                for token in tokens:
                    if token == "Off +":
                        inc_stat(slot, "crash_positive")
                    elif token == "Off -":
                        inc_stat(slot, "crash_missed")
                    elif token == "BM +":
                        inc_stat(slot, "back_man_positive")
                    elif token == "BM -":
                        inc_stat(slot, "back_man_missed")
                    elif row_type_lower == "rebound opportunities":
                        if token == "Def +":
                            inc_stat(slot, "box_out_positive")
                        elif token == "Def -":
                            inc_stat(slot, "box_out_missed")
                        elif token == "Given Up":
                            inc_stat(slot, "off_reb_given_up")
            continue

        if row_type_lower in defense_reb_rows:
            for col, tokens in row_tokens_by_col.items():
                slot = player_stats_dict[col]
                for token in tokens:
                    if token == "Def +":
                        inc_stat(slot, "box_out_positive")
                    elif token == "Def -":
                        inc_stat(slot, "box_out_missed")
                    elif token == "Given Up":
                        inc_stat(slot, "off_reb_given_up")
            continue

        if row_type_lower in ("pnr", "pnr file"):
            for col, tokens in row_tokens_by_col.items():
                slot = player_stats_dict[col]
                for token in tokens:
                    if token == "CW +":
                        inc_stat(slot, "close_window_positive")
                    elif token == "CW -":
                        inc_stat(slot, "close_window_missed")
                    elif token == "SD +":
                        inc_stat(slot, "shut_door_positive")
                    elif token == "SD -":
                        inc_stat(slot, "shut_door_missed")
            continue

        if row_type == "Offense":
            process_offense_row(row, df.columns, player_stats_dict, game_id, season_id, stat_mapping, blue_collar_values)
        elif row_type == "Defense":
            process_defense_row(row, opponent_totals, stat_mapping)
            process_defense_player_row(row, df.columns, player_stats_dict, game_id, season_id)
        elif row_type == "Opponent Blue Collar Plays":
            tokens = extract_tokens(row.get("OPP STATS", ""))
            for token in tokens:
                if token in blue_collar_mapping:
                    key = blue_collar_mapping[token]
                    opponent_totals["total_blue_collar"] += blue_collar_values[key]
                    opponent_blue_collar_accum[key] += 1
        elif row_type == "DEF Note":
            process_def_note_row(row, df.columns, player_stats_dict, game_id, season_id, stat_mapping, blue_collar_values, team_totals)
        elif row_type.startswith("#"):
            process_player_row(row, player_stats_dict, game_id, season_id, stat_mapping, blue_collar_values, team_totals)

    for player_stats in player_stats_dict.values():
        player_stats["atr_total_attempts"] = player_stats.get("atr_attempts", 0)
        player_stats["fg2_total_attempts"] = player_stats.get("fg2_attempts", 0)
        player_stats["fg3_total_attempts"] = player_stats.get("fg3_attempts", 0)
        player_stats["ft_total_attempts"]  = player_stats.get("fta", 0)

    # --- Insert/Overwrite Player Stats into Database ---
    with app_instance.app_context():
        valid_cols = {c.name for c in PlayerStats.__table__.columns}

        for player_name, player_stats in player_stats_dict.items():
            # Remove any existing rows for this player & game to avoid duplicates
            PlayerStats.query \
                .filter_by(player_name=player_name, game_id=game_id) \
                .delete()

            # Prepare shot-detail JSON (if any)
            json_details = None
            if player_stats.get("shot_type_details"):
                json_details = json.dumps(player_stats["shot_type_details"])

            # Build a fresh dict of only valid columns (excluding array/dict fields)
            clean_stats = {
                k: safe_value(v)
                for k, v in player_stats.items()
                if k in valid_cols
                and not isinstance(v, (dict, list, tuple, np.ndarray, pd.Series))
            }

            # Ensure game_id, season_id, and player_name are set correctly:
            clean_stats["game_id"]     = game_id
            clean_stats["season_id"]   = season_id
            clean_stats["player_name"] = player_name
            # A game row should never have a practice_id
            clean_stats["practice_id"] = None

            # Attach shot_type_details JSON if present
            if json_details is not None:
                clean_stats["shot_type_details"] = json_details

            # Insert the new, non-duplicated PlayerStats row
            player_stat = PlayerStats(**clean_stats)
            db.session.add(player_stat)
            persist_player_shot_details(
                player_stat,
                player_stats.get("shot_type_details") or [],
                replace=True,
            )

        # Commit once after processing all players
        db.session.commit()

        # Accumulate team totals from all player stats
        for player_stats in player_stats_dict.values():
            team_totals["total_points"]        += safe_value(player_stats.get("points", 0))
            team_totals["total_assists"]       += safe_value(player_stats.get("assists", 0))
            team_totals["total_second_assists"]+= safe_value(player_stats.get("second_assists", 0))
            team_totals["total_pot_assists"]   += safe_value(player_stats.get("pot_assists", 0))
            team_totals["total_turnovers"]     += safe_value(player_stats.get("turnovers", 0))
            team_totals["total_atr_makes"]     += safe_value(player_stats.get("atr_makes", 0))
            team_totals["total_atr_attempts"]  += safe_value(player_stats.get("atr_attempts", 0))
            team_totals["total_fg2_makes"]     += safe_value(player_stats.get("fg2_makes", 0))
            team_totals["total_fg2_attempts"]  += safe_value(player_stats.get("fg2_attempts", 0))
            team_totals["total_fg3_makes"]     += safe_value(player_stats.get("fg3_makes", 0))
            team_totals["total_fg3_attempts"]  += safe_value(player_stats.get("fg3_attempts", 0))
            team_totals["total_ftm"]           += safe_value(player_stats.get("ftm", 0))
            team_totals["total_fta"]           += safe_value(player_stats.get("fta", 0))
            team_totals["foul_by"]             += safe_value(player_stats.get("foul_by", 0))

        team_totals["total_points"] = (
            2 * team_totals.get("total_atr_makes", 0)
            + 2 * team_totals.get("total_fg2_makes", 0)
            + 3 * team_totals.get("total_fg3_makes", 0)
            + team_totals.get("total_ftm", 0)
        )


        # Process possessions for bucket 2 (Team vs Opponent) with subtract_off_reb=True
        _, offensive_possessions, defensive_possessions = process_possessions(df, game_id, season_id, subtract_off_reb=True)
        team_totals["total_possessions"] = int(offensive_possessions)
        opponent_totals["total_possessions"] = int(defensive_possessions)


        # --- Derived metrics for TeamStats ---------------------------------------
        # helper to count any label tokens in a set of rows
        def count_tokens(rows, tokens):
            return sum(
                1
                for _, r in rows.iterrows()
                for col in df.columns if col.startswith("#")
                for tok in extract_tokens(r.get(col, ""))
                if tok in tokens
            )

        # 1) Count actual Off. Rebounds
        oreb_count = int(
            df.loc[df['Row'] == "Offense", 'TEAM']
            .fillna('')
            .str.count("Off Reb")
            .sum()
        )
        team_totals["total_off_reb"] = oreb_count

        # 2) Sportscode possessions (exclude Neutral & Off Reb)
        run = int(df['Row'].eq("Offense").sum())
        neu = int(
            df.loc[df['Row'] == "Offense", 'TEAM']
            .fillna('')
            .str.contains("Neutral")
            .sum()
        )
        poss = run - neu - oreb_count
        team_totals["poss_for_derived"] = poss

        # 3) Assist %
        fgm_total = (
            team_totals["total_atr_makes"]
        + team_totals["total_fg2_makes"]
        + team_totals["total_fg3_makes"]
        )
        team_totals["assist_pct"] = (
            round(team_totals["total_assists"] / fgm_total * 100, 1)
            if fgm_total > 0 else 0.0
        )

        # 4) OREB %
        atr_miss = team_totals["total_atr_attempts"] - team_totals["total_atr_makes"]
        fg2_miss = team_totals["total_fg2_attempts"] - team_totals["total_fg2_makes"]
        fg3_miss = team_totals["total_fg3_attempts"] - team_totals["total_fg3_makes"]
        reb_chance = atr_miss + fg2_miss + fg3_miss
        team_totals["oreb_pct"] = (
            round(oreb_count / reb_chance * 100, 0)
            if reb_chance > 0 else 0.0
        )

        # 5) FT Rate (NBA formula: FTA ⁄ FGA)
        offense_rows = df[df['Row'] == "Offense"]
        # Count free‐throw attempts (made + missed)
        fta = count_tokens(offense_rows, ("FT+", "FT-"))
        # Total field‐goal attempts = ATR + 2FG + 3FG attempts
        fga = (
            team_totals["total_atr_attempts"]
          + team_totals["total_fg2_attempts"]
          + team_totals["total_fg3_attempts"]
        )
        team_totals["ft_rate"] = (
            round(fta / fga * 100, 1)
            if fga > 0 else 0.0
        )


        # 6) Turnover %
        turns = team_totals["total_turnovers"]
        team_totals["turnover_pct"] = (
            round(turns / poss * 100, 1)
            if poss > 0 else 0.0
        )

        # 7) Good Shot %
        ftr = team_totals["total_fta"]
        good = (
            ftr
        + team_totals["total_atr_makes"] + atr_miss
        + team_totals["total_fg3_makes"] + fg3_miss
        )
        bad = team_totals["total_fg2_makes"] + fg2_miss
        den = good + bad
        team_totals["good_shot_pct"] = (
            round(good / den * 100, 2)
            if den > 0 else 0.0
        )

        # 8) TCR per Sportscode definition
        # Denominator: made + missed FG + steals, minus neutrals
        made      = count_tokens(offense_rows, ("ATR+", "2FG+", "3FG+"))
        missed    = count_tokens(offense_rows, ("ATR-", "2FG-", "3FG-"))
        steals    = count_tokens(offense_rows, ("Steal",))
        neutrals  = offense_rows[offense_rows['TEAM'].fillna('').str.contains("Neutral")]
        madeneu   = count_tokens(neutrals, ("ATR+", "2FG+", "3FG+"))
        missneu   = count_tokens(neutrals, ("ATR-", "2FG-", "3FG-"))
        stealneu  = count_tokens(neutrals, ("Steal",))
        trans_opps = (made + missed + steals) - (madeneu + missneu + stealneu)

        #    Numerator: ATR± + 2FG± + 3FG± + Fouled (in Transition rows)
        trans_rows = offense_rows[
            offense_rows['POSSESSION TYPE'].fillna('').str.contains("Transition")
        ]
        conversions = count_tokens(trans_rows, (
            "ATR+", "ATR-",
            "2FG+", "2FG-",
            "3FG+", "3FG-",
            "Fouled"
        ))
        team_totals["tcr_pct"] = (
            round(conversions / trans_opps * 100, 1)
            if trans_opps > 0 else 0.0
        )
        # -------------------------------------------------------------------------


        # Recount FT from CSV to fix missing free throws
        offense_rows = df[df['Row'] == "Offense"]
        team_totals["total_ftm"] = count_tokens(offense_rows, ("FT+",))
        team_totals["total_fta"] = count_tokens(offense_rows, ("FT+","FT-"))
    
        # Insert Team Stats for your team
        team_entry = TeamStats(
            game_id=game_id,
            season_id=season_id,
            total_points=team_totals["total_points"],
            total_assists=team_totals["total_assists"],
            total_second_assists=team_totals["total_second_assists"],
            total_pot_assists=team_totals["total_pot_assists"],
            total_turnovers=team_totals["total_turnovers"],
            total_atr_makes=team_totals["total_atr_makes"],
            total_atr_attempts=team_totals["total_atr_attempts"],
            total_fg2_makes=team_totals["total_fg2_makes"],
            total_fg2_attempts=team_totals["total_fg2_attempts"],
            total_fg3_makes=team_totals["total_fg3_makes"],
            total_fg3_attempts=team_totals["total_fg3_attempts"],
            total_ftm=team_totals["total_ftm"],
            total_fta=team_totals["total_fta"],
            total_blue_collar=team_totals["total_blue_collar"],
            total_possessions=team_totals["total_possessions"],
            assist_pct=team_totals["assist_pct"],
            turnover_pct=team_totals["turnover_pct"],
            tcr_pct=team_totals.get("tcr_pct", 0.0),
            oreb_pct=team_totals["oreb_pct"],
            ft_rate=team_totals["ft_rate"],
            good_shot_pct=team_totals["good_shot_pct"],
            is_opponent=False
        )
        db.session.add(team_entry)

        # Insert Opponent Stats as a separate record
        opponent_entry = TeamStats(
            game_id=game_id,
            season_id=season_id,
            total_points=opponent_totals["total_points"],
            total_assists=opponent_totals["assists"],
            total_second_assists=opponent_totals["second_assists"],
            total_pot_assists=opponent_totals["pot_assists"],
            total_turnovers=opponent_totals["turnovers"],
            total_atr_makes=opponent_totals["atr_makes"],
            total_atr_attempts=opponent_totals["atr_attempts"],
            total_fg2_makes=opponent_totals["fg2_makes"],
            total_fg2_attempts=opponent_totals["fg2_attempts"],
            total_fg3_makes=opponent_totals["fg3_makes"],
            total_fg3_attempts=opponent_totals["fg3_attempts"],
            total_ftm=opponent_totals["ftm"],
            total_fta=opponent_totals["fta"],
            total_blue_collar=opponent_totals["total_blue_collar"],
            total_possessions=opponent_totals["total_possessions"],
            is_opponent=True
        )

        # Insert Blue Collar Stats for our team
        blue_collar_stats_entry = BlueCollarStats(
            game_id=game_id,
            season_id=season_id,
            total_blue_collar=team_totals["total_blue_collar"],
            def_reb=team_totals.get("def_reb", 0),
            off_reb=team_totals.get("off_reb", 0),
            misc=team_totals.get("misc", 0),
            deflection=team_totals.get("deflection", 0),
            steal=team_totals.get("steal", 0),
            block=team_totals.get("block", 0),
            floor_dive=team_totals.get("floor_dive", 0),
            charge_taken=team_totals.get("charge_taken", 0),
            reb_tip=team_totals.get("reb_tip", 0)
        )
        db.session.add(blue_collar_stats_entry)
        db.session.add(opponent_entry)
        db.session.commit()

    # --- Insert Blue Collar Stats for Players (TEAM) ---
    db_path = "instance/database.db"
    conn = sqlite3.connect(db_path)
    for player_name, stats in player_stats_dict.items():
        blue_total = stats.get("_blue_collar_total", 0)
        player_id = get_player_id(player_name, conn)
        if player_id is None:
            continue
        accum = stats["blue_collar_accum"]
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO blue_collar_stats (
                game_id,
                season_id,
                player_id,
                total_blue_collar,
                reb_tip,
                def_reb,
                misc,
                deflection,
                steal,
                block,
                off_reb,
                floor_dive,
                charge_taken
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                game_id,
                season_id,
                player_id,
                blue_total,
                accum.get("reb_tip", 0),
                accum.get("def_reb", 0),
                accum.get("misc", 0),
                accum.get("deflection", 0),
                accum.get("steal", 0),
                accum.get("block", 0),
                accum.get("off_reb", 0),
                accum.get("floor_dive", 0),
                accum.get("charge_taken", 0)
            )
        )
        conn.commit()

    # --- Insert Opponent Blue Collar Stats (ONE ROW) ---
    opponent_blue_total = sum(
        opponent_blue_collar_accum[cat] * blue_collar_values[cat]
        for cat in opponent_blue_collar_accum
    )
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO opponent_blue_collar_stats (
            game_id,
            season_id,
            player_id,
            total_blue_collar,
            reb_tip,
            def_reb,
            misc,
            deflection,
            steal,
            block,
            off_reb,
            floor_dive,
            charge_taken
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            game_id,
            season_id,
            None,
            opponent_blue_total,
            opponent_blue_collar_accum["reb_tip"],
            opponent_blue_collar_accum["def_reb"],
            opponent_blue_collar_accum["misc"],
            opponent_blue_collar_accum["deflection"],
            opponent_blue_collar_accum["steal"],
            opponent_blue_collar_accum["block"],
            opponent_blue_collar_accum["off_reb"],
            opponent_blue_collar_accum["floor_dive"],
            opponent_blue_collar_accum["charge_taken"]
        )
    )
    conn.commit()

    # --- Insert Possession Records using TRUE data (subtract_off_reb=False) ---
    possession_data, _, _ = process_possessions(df, game_id, season_id, subtract_off_reb=False)
    with app_instance.app_context():
        for poss in possession_data:
            new_poss = Possession(
                game_id=game_id,
                season_id=season_id,
                possession_side=poss.get("side", ""),
                possession_type=poss.get("possession_type", ""),
                possession_start=poss.get("possession_start", ""),
                paint_touches=poss.get("paint_touches", ""),
                shot_clock=poss.get("shot_clock", ""),
                shot_clock_pt=poss.get("shot_clock_pt", ""),
                points_scored=poss.get("points_scored", 0)
            )
            db.session.add(new_poss)
            db.session.flush()
            # Insert PlayerPossession entries
            player_ids = []
            for jersey in poss.get("players_on_floor", []):
                roster_entry = Roster.query.filter_by(
                    season_id=season_id,
                    player_name=jersey,
                ).first()
                if roster_entry:
                    player_ids.append(roster_entry.id)

            for pid in player_ids:
                db.session.add(PlayerPossession(
                    possession_id=new_poss.id,
                    player_id=pid
                ))

            for tok in poss.get("events", []):
                db.session.add(ShotDetail(possession_id=new_poss.id, event_type=tok))

        db.session.commit()

    conn.close()

    #print("✅ Player Stats Successfully Inserted!")
    calculate_derived_metrics(player_stats_dict)
    
    # --- Calculate Possession Type Breakdowns using the new detailed function ---
    # --- Calculate Possession Type & Split Breakdowns ---
    offensive_breakdown, defensive_breakdown, periodic_offense, periodic_defense = \
        get_possession_breakdown_detailed(df)


    # ● compute lineup efficiencies (2-5 man units, min 10 poss)
    efficiencies = compute_lineup_efficiencies(
        possession_data,
        group_sizes=(2,3,4,5),
        min_poss=10
    )

    result = {
      "offensive_breakdown": offensive_breakdown,
      "defensive_breakdown": defensive_breakdown,
      "lineup_efficiencies": efficiencies
    }

    # BEGIN Advanced Possession
    try:
        adv_payload, _meta = cache_get_or_compute_adv_poss_game(game_id)
    except Exception:
        adv_payload = None
    if adv_payload is not None:
        result["advanced_possession_game"] = adv_payload
    # END Advanced Possession
    # BEGIN Playcall Report
    playcall_payload = None
    try:
        with app_instance.app_context():
            playcall_payload, _playcall_meta = compute_playcall_report_from_dataframe(game_id, df)
    except Exception:
        playcall_payload = None
    if playcall_payload is not None:
        result["playcall_report"] = playcall_payload
    # END Playcall Report

    return result


# --- Tests -----------------------------------------------------------------


def _build_test_row(row_type, game_split, team="Team", opp_stats="", possession_type="Man", event_token="ATR+"):
    return {
        "Row": row_type,
        "TEAM": team,
        "OPP STATS": opp_stats,
        "GAME SPLITS": game_split,
        "POSSESSION TYPE": possession_type,
        "#1": event_token,
    }


def test_period_normalization_matches_totals():
    df = pd.DataFrame(
        [
            _build_test_row("Offense", "1st\xa0Half, Segment A"),
            _build_test_row("Offense", "Transition, 1st Half, 20:00 - 16:00"),
            _build_test_row("Offense", "Second Half", event_token="3FG+"),
            _build_test_row("Offense", "OT", event_token="FT+"),
            _build_test_row("Defense", "2nd-half", opp_stats="2FG+"),
            _build_test_row("Defense", "overtime", opp_stats="3FG+"),
            _build_test_row("Offense", "1st Half \u2013 Segment 2", event_token="ATR+"),
            _build_test_row("Defense", "Second Half - Segment 4", opp_stats="FT+"),
            _build_test_row("Offense", "OT \u2013 Segment 1", event_token="ATR+"),
            _build_test_row("Defense", "OT - Segment 2", opp_stats="ATR+"),
        ]
    )

    # Match the route code by splitting but intentionally avoid normalization
    # to ensure get_possession_breakdown_detailed() now handles variants.
    def _extract_first_period(cell_value):
        if pd.isna(cell_value):
            return ""
        if not isinstance(cell_value, str):
            cell_value = str(cell_value)
        for token in cell_value.split(','):
            normalized = normalize_period_label(token.strip())
            if normalized in {"1st Half", "2nd Half", "Overtime"}:
                return normalized
        return ""

    df['Period'] = df['GAME SPLITS'].apply(_extract_first_period)

    (
        _,
        _,
        periodic_offense,
        periodic_defense,
    ) = get_possession_breakdown_detailed(df)

    expected_offense = sum(
        1
        for _, row in df.iterrows()
        if row['Row'] == "Offense"
        and "Neutral" not in str(row['TEAM'])
        and "Off Reb" not in str(row['TEAM'])
    )
    expected_defense = sum(
        1
        for _, row in df.iterrows()
        if row['Row'] == "Defense"
        and "Neutral" not in str(row['TEAM'])
        and "Off Reb" not in str(row['OPP STATS'])
    )

    assert sum(bucket['count'] for bucket in periodic_offense.values()) == expected_offense
    assert sum(bucket['count'] for bucket in periodic_defense.values()) == expected_defense

    assert periodic_offense['1st Half']['count'] == 3
    assert periodic_offense['2nd Half']['count'] == 1
    assert periodic_offense['Overtime']['count'] == 2
    assert periodic_defense['2nd Half']['count'] == 2
    assert periodic_defense['Overtime']['count'] == 2


def test_placeholder():
    """Placeholder test so pytest collects this file without executing CSV parsing."""
    assert True

#print("✅ CSV parsing script modifications completed!")
