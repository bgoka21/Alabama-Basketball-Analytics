from itertools import product

LEADERBOARD_STATS = [
    # ─── Core PlayerStats ─────────────────────────────────────────────────────
    {"key": "points",         "label": "Points",            "format": "int"},
    {"key": "assists",        "label": "Assists",           "format": "int"},
    {"key": "pot_assists",    "label": "Potential Assists", "format": "int"},
    {"key": "second_assists", "label": "2nd Assists",       "format": "int"},
    {"key": "turnovers",      "label": "Turnovers",         "format": "int"},
    {"key": "fta",            "label": "FT Attempts",       "format": "int"},
    {"key": "ftm",            "label": "FT Makes",          "format": "int"},
]

# ─── Simple Shot Type FG% Categories ─────────────────────────────────────────
for sc in ["atr", "fg2", "fg3"]:
    LEADERBOARD_STATS.append({
        "key": f"{sc}_fg_pct",
        "label": f"{sc.upper()} FG%",
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
