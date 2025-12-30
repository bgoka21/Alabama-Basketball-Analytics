from collections import defaultdict
from itertools import combinations
import math


def _split_player_tokens(cell_value):
    """Split player possession cell into normalized tokens."""
    if cell_value is None:
        return []
    if isinstance(cell_value, float) and math.isnan(cell_value):
        return []
    if not isinstance(cell_value, str):
        cell_value = str(cell_value)
    normalized = cell_value.replace("–", "-")
    for sep in (";", "\n", "\r", "\t"):
        normalized = normalized.replace(sep, ",")
    return [token.strip() for token in normalized.split(",") if token.strip()]


def normalize_lineup_side(side):
    """Normalize lineup side labels to a consistent lowercase schema."""
    if side is None:
        return ""
    if not isinstance(side, str):
        side = str(side)
    return side.strip().lower()


def format_lineup_efficiencies(raw_lineups):
    """Format lineup efficiencies for JSON serialization with normalized sides."""
    formatted = {}
    for size, sides in raw_lineups.items():
        formatted[size] = {}
        for side, side_data in sides.items():
            normalized = normalize_lineup_side(side)
            if not normalized:
                continue
            formatted[size][normalized] = {
                ",".join(combo): ppp for combo, ppp in side_data.items()
            }
    return formatted


def get_players_on_floor(row, df_columns):
    """Return player tokens using the same column leverage uses."""
    if "PLAYER POSSESSIONS" not in df_columns:
        return []
    return _split_player_tokens(row.get("PLAYER POSSESSIONS", ""))


def compute_lineup_totals(possession_data, group_sizes=(2, 3, 4, 5)):
    """Compute total points/possessions for each lineup size and side."""
    normalized_sides = {
        normalize_lineup_side(p.get("side"))
        for p in possession_data
    }
    normalized_sides.discard("")
    raw = {
        size: {
            side: defaultdict(lambda: {"poss": 0, "pts": 0})
            for side in normalized_sides
        }
        for size in group_sizes
    }
    for poss in possession_data:
        if not poss.get("is_true_possession", True):
            continue
        side = normalize_lineup_side(poss.get("side"))
        if not side:
            continue
        players = poss.get("players_on_floor", [])
        pts = poss.get("points_scored", 0)
        for size in group_sizes:
            if len(players) < size:
                continue
            for combo in combinations(players, size):
                key = tuple(sorted(combo))
                raw[size][side][key]["poss"] += 1
                raw[size][side][key]["pts"] += pts
    return raw


def compute_lineup_efficiencies(possession_data, group_sizes=(2, 3, 4, 5), min_poss=5):
    """Compute PPP for each lineup size and side."""
    raw = compute_lineup_totals(possession_data, group_sizes=group_sizes)
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
