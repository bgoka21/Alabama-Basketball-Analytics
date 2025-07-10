from collections import defaultdict
from itertools import combinations


def compute_lineup_efficiencies(possession_data, group_sizes=(2, 3, 4, 5), min_poss=5):
    """Compute PPP for each lineup size and side."""
    raw = {
        size: {
            side: defaultdict(lambda: {"poss": 0, "pts": 0})
            for side in set(p.get("side") for p in possession_data)
        }
        for size in group_sizes
    }
    for poss in possession_data:
        side = poss.get("side")
        players = poss.get("players_on_floor", [])
        pts = poss.get("points_scored", 0)
        for size in group_sizes:
            if len(players) < size:
                continue
            for combo in combinations(players, size):
                key = tuple(sorted(combo))
                raw[size][side][key]["poss"] += 1
                raw[size][side][key]["pts"] += pts
    efficiencies = {size: {side: {} for side in raw[size]} for size in group_sizes}
    for size in group_sizes:
        for side in raw[size]:
            for lineup, stats in raw[size][side].items():
                if stats["poss"] >= min_poss:
                    efficiencies[size][side][lineup] = stats["pts"] / stats["poss"]
    return efficiencies


def compute_player_on_off_by_team(possessions):
    """Return PPP for each player when on/off the floor for each team."""
    team_totals = defaultdict(lambda: {"poss": 0, "pts": 0})
    on_stats = defaultdict(lambda: defaultdict(lambda: {"poss": 0, "pts": 0}))
    for poss in possessions:
        side = poss.get("side")
        players = poss.get("players_on_floor", [])
        pts = poss.get("points_scored", 0)
        team_totals[side]["poss"] += 1
        team_totals[side]["pts"] += pts
        for p in players:
            on_stats[p][side]["poss"] += 1
            on_stats[p][side]["pts"] += pts
    result = defaultdict(dict)
    for player, sides in on_stats.items():
        for side, stats in sides.items():
            total = team_totals[side]
            off_poss = total["poss"] - stats["poss"]
            off_pts = total["pts"] - stats["pts"]
            on_ppp = stats["pts"] / stats["poss"] if stats["poss"] else None
            off_ppp = off_pts / off_poss if off_poss else None
            result[player][side] = {"on": on_ppp, "off": off_ppp}
    return result
