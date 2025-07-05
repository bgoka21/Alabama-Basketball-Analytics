from itertools import product

LEADERBOARD_STATS = [
    # ─── A) Core PlayerStats ─────────────────────────────────────────────────
    {"key": "points",           "label": "Points",            "format": "int"},
    {"key": "assists",          "label": "Assists",           "format": "int"},
    {"key": "pot_assists",      "label": "Potential Assists", "format": "int"},
    {"key": "second_assists",   "label": "2nd Assists",       "format": "int"},
    {"key": "turnovers",        "label": "Turnovers",         "format": "int"},
    {"key": "fta",              "label": "FT Attempts",       "format": "int"},
    {"key": "ftm",              "label": "FT Makes",          "format": "int"},
    {"key": "atr_attempts",     "label": "ATR Attempts",      "format": "int"},
    {"key": "atr_makes",        "label": "ATR Makes",         "format": "int"},
    {"key": "fg2_attempts",     "label": "2-FG Attempts",     "format": "int"},
    {"key": "fg2_makes",        "label": "2-FG Makes",        "format": "int"},
    {"key": "fg3_attempts",     "label": "3-FG Attempts",     "format": "int"},
    {"key": "fg3_makes",        "label": "3-FG Makes",        "format": "int"},

    # ─── A) Defensive Contests ────────────────────────────────────────────────
    {"key": "foul_by",          "label": "Fouled By",         "format": "int"},
    {"key": "contest_front",    "label": "Contest – Front",    "format": "int"},
    {"key": "contest_side",     "label": "Contest – Side",     "format": "int"},
    {"key": "contest_behind",   "label": "Contest – Behind",   "format": "int"},
    {"key": "contest_late",     "label": "Contest – Late",     "format": "int"},
    {"key": "contest_early",    "label": "Contest – Early",    "format": "int"},
    {"key": "contest_no",       "label": "No Contest",         "format": "int"},
    {"key": "bump_positive",    "label": "Bump + (Successful)","format": "int"},
    {"key": "bump_missed",      "label": "Bump – (Missed)",    "format": "int"},

    # ─── A) Blow-bys ───────────────────────────────────────────────────────────
    {"key": "blowby_total",          "label": "Blowby Total",          "format": "int"},
    {"key": "blowby_triple_threat",  "label": "Blowby – Triple Threat", "format": "int"},
    {"key": "blowby_closeout",       "label": "Blowby – Closeout",      "format": "int"},
    {"key": "blowby_isolation",      "label": "Blowby – Isolation",     "format": "int"},

    # ─── A) Practice & Sprints ───────────────────────────────────────────────
    {"key": "practice_wins",    "label": "Practice Wins",     "format": "int"},
    {"key": "practice_losses",  "label": "Practice Losses",   "format": "int"},
    {"key": "sprint_wins",      "label": "Sprint Wins",       "format": "int"},
    {"key": "sprint_losses",    "label": "Sprint Losses",     "format": "int"},

    # ─── B) BlueCollarStats ──────────────────────────────────────────────────
    {"key": "total_blue_collar", "label": "BCP Points",         "format": "float"},
    {"key": "reb_tip",           "label": "Rebound Tip",        "format": "int"},
    {"key": "def_reb",           "label": "Defensive Rebounds", "format": "int"},
    {"key": "misc",              "label": "Miscellaneous",     "format": "int"},
    {"key": "deflection",        "label": "Deflections",        "format": "int"},
    {"key": "steal",             "label": "Steals",            "format": "int"},
    {"key": "block",             "label": "Blocks",            "format": "int"},
    {"key": "off_reb",           "label": "Offensive Rebounds", "format": "int"},
    {"key": "floor_dive",        "label": "Floor Dive",        "format": "int"},
    {"key": "charge_taken",      "label": "Charge Taken",      "format": "int"},
]

# ─── C) Programmatically expand every shot-type sub-stat ─────────────────
shot_classes = ['atr','fg2','fg3']
labels       = ['Assisted','Non-Assisted']
contexts     = ['total','transition','halfcourt']
metrics      = [
    ('attempts','Attempts','int'),
    ('makes','Makes','int'),
    ('fg_pct','FG%','pct'),
    ('pps','PPS','float'),
    ('freq_pct','Freq%','pct'),
]

for sc, lbl, ctx, (m_key, m_label, m_fmt) in product(shot_classes, labels, contexts, metrics):
    LEADERBOARD_STATS.append({
        "key":   f"{sc}_{lbl}_{ctx}_{m_key}",
        "label": f"{sc.upper()} {lbl} {ctx.title()} {m_label}",
        "format": m_fmt
    })
