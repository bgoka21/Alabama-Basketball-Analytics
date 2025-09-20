from itertools import product

LEADERBOARD_STATS = [
    # ─── Core PlayerStats ─────────────────────────────────────────────────────
    {"key": "points",         "label": "Points",            "format": "int"},
    {"key": "assists",        "label": "Assists",           "format": "int", "hidden": True},
    {"key": "pot_assists",    "label": "Potential Assists", "format": "int", "hidden": True},
    {"key": "second_assists", "label": "2nd Assists",       "format": "int", "hidden": True},
    {"key": "turnovers",      "label": "Turnovers",         "format": "int", "hidden": True},
    {"key": "fta",            "label": "FT Attempts",       "format": "int"},
    {"key": "ftm",            "label": "FT Makes",          "format": "int"},
]

# ─── Extra Leaderboard Metrics ──────────────────────────────────────────────
LEADERBOARD_STATS.extend([
    {"key": "atr_pct", "label": "ATR%", "format": "pct", "hidden": True},
    {"key": "fg3_pct", "label": "3FG%", "format": "pct", "hidden": True},
    {"key": "atr_freq_pct", "label": "ATR Frequency", "format": "pct", "hidden": True},
    {"key": "fg3_freq_pct", "label": "3FG Frequency", "format": "pct", "hidden": True},
    {"key": "assist_turnover_ratio", "label": "AST/TO Ratio", "hidden": True},
    {"key": "adj_assist_turnover_ratio", "label": "Adj AST/TO", "hidden": True},
    {"key": "assist_summary", "label": "Assist/TO Stats"},
    {"key": "offense_summary", "label": "Offense Stats"},
    {"key": "offensive_possessions", "label": "Off Poss", "format": "int", "hidden": True},
    {"key": "ppp_on", "label": "PPP On", "hidden": True},
    {"key": "ppp_off", "label": "PPP Off", "hidden": True},
    {"key": "efg_on", "label": "EFG%", "format": "pct", "hidden": True},
    {"key": "two_fg_pct", "label": "2FG%", "format": "pct", "hidden": True},
    {"key": "three_fg_pct", "label": "3FG%", "format": "pct", "hidden": True},
    {"key": "turnover_rate", "label": "Turnover Rate", "format": "pct", "hidden": True},
    {"key": "off_reb_rate", "label": "Off Reb Rate", "format": "pct", "hidden": True},
    {"key": "off_rebounding", "label": "Offensive Rebounding"},
    {"key": "def_rebounding", "label": "Defensive Rebounding"},
    {"key": "fouls_drawn_rate", "label": "Fouls Drawn Rate", "format": "pct", "hidden": True},
])

# ─── Defensive Leaderboard Metrics ──────────────────────────────────────────
LEADERBOARD_STATS.extend([
    {"key": "defense", "label": "Defense"},
    {"key": "collision_gap_help", "label": "Collision Gap Help"},
    {"key": "pnr_gap_help", "label": "PnR Gap Help"},
    {"key": "pnr_grade", "label": "PnR Grade"},
])

# ─── Simple Shot Type FG% Categories ─────────────────────────────────────────
for sc in ["atr", "fg2", "fg3"]:
    label_map = {
        "atr": "ATR",
        "fg2": "2FG",
        "fg3": "3FG",
    }
    LEADERBOARD_STATS.append({
        "key": f"{sc}_fg_pct",
        "label": label_map.get(sc, sc.upper()),
        "format": "pct",
    })

# ─── Detailed Shot Type FG% Categories (hidden from dropdown for now) ─────────
shot_classes = ["atr", "fg2", "fg3"]
labels = ["Assisted", "Non-Assisted"]
contexts = ["total", "transition", "halfcourt"]

for sc, lbl, ctx in product(shot_classes, labels, contexts):
    LEADERBOARD_STATS.append({
        "key": f"{sc}_{lbl}_{ctx}_fg_pct",
        "label": f"{sc.upper()} {lbl} {ctx.title()} FG%",
        "format": "pct",
        "hidden": True,
    })
