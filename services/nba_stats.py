import logging
import requests
from datetime import date, timedelta
from typing import List, Dict
import pprint

logger = logging.getLogger(__name__)

# List of Alabama alum to track
PLAYERS = [
    "Keon Ellis",
    "Noah Clowney",
    "Herb Jones",
    "Brandon Miller",
    "Collin Sexton",
    "Alex Reese",
    "Kira Lewis Jr.",
    "Jaden Shackelford",
    "Josh Primo",
    "Aaron Estrada",
    "Mark Sears",
    "Grant Nelson",
    "Clifford Omoruyi",
    "Chris Youngblood",
]

def get_scoreboard_json(date_str: str) -> dict:
    """Fetch ESPN's Summer League scoreboard JSON for a given date string.

    Parameters
    ----------
    date_str : str
        Date in ``YYYYMMDD`` format.

    Returns
    -------
    dict
        Parsed JSON response.
    """
    url = (
        "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/"
        f"scoreboard?seasontype=50&dates={date_str}"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json() or {}
    except requests.RequestException as exc:
        logger.error("Failed to fetch scoreboard for %s: %s", date_str, exc)
        return {}

def get_game_summary(game_id: str) -> dict:
    """Fetch the detailed summary JSON for a given Summer League game."""
    url = (
        "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/"
        f"summary?event={game_id}"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json() or {}
    except requests.RequestException as exc:
        logger.error("Failed to fetch game summary %s: %s", game_id, exc)
        return {}

def _collect_athletes(team_block: dict) -> List[dict]:
    """Return a flat list of athlete dicts from a team boxscore block."""
    athletes = []
    if isinstance(team_block.get("athletes"), list):
        athletes.extend(team_block["athletes"])
    # Some responses store athletes under nested statistics groups
    if isinstance(team_block.get("players"), list):
        athletes.extend(team_block["players"])
    for stat_block in team_block.get("statistics", []):
        if isinstance(stat_block, dict) and isinstance(stat_block.get("athletes"), list):
            athletes.extend(stat_block["athletes"])
    return athletes

def parse_players_from_summary(summary_json: dict, players: List[str]) -> Dict[str, Dict]:
    """Extract desired players' box scores from the game summary JSON.

    Parameters
    ----------
    summary_json : dict
        JSON from :func:`get_game_summary`.
    players : list[str]
        Names of players to extract.

    Returns
    -------
    dict
        Mapping of player name to statistics with ``team`` and ``opponent`` keys.
    """
    results: Dict[str, Dict] = {}
    boxscore = summary_json.get("boxscore", {})
    teams = boxscore.get("players", [])
    if not isinstance(teams, list):
        return results
    team_abbrevs = [t.get("team", {}).get("abbreviation", "") for t in teams]
    for team in teams:
        team_info = team.get("team", {})
        team_abbrev = team_info.get("abbreviation", "")
        opponent_abbrev = next((a for a in team_abbrevs if a != team_abbrev), "")
        for athlete in _collect_athletes(team):
            info = athlete.get("athlete", athlete)
            name = info.get("displayName") or info.get("fullName")
            if name not in players:
                continue
            stats_block = None
            for blk in athlete.get("statistics", []):
                if blk.get("name") == "Statistics":
                    stats_block = blk
                    break
            stat_map = {}
            if stats_block:
                labels = stats_block.get("labels", [])
                totals = stats_block.get("totals", [])
                stat_map = dict(zip(labels, totals))
            stat_map["team"] = team_abbrev
            stat_map["opponent"] = opponent_abbrev
            results[name] = stat_map
    return results

def get_yesterdays_summer_stats(players: List[str]) -> Dict[str, Dict]:
    """Fetch Summer League box scores for the specified players from yesterday."""
    date_str = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    scoreboard = get_scoreboard_json(date_str)
    if not scoreboard:
        return {}

    final: Dict[str, Dict] = {}
    for event in scoreboard.get("events", []):
        game_id = event.get("id")
        if not game_id:
            continue
        summary = get_game_summary(game_id)
        if not summary:
            continue
        final.update(parse_players_from_summary(summary, players))
    return final

if __name__ == "__main__":
    date_str = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    stats = get_yesterdays_summer_stats(PLAYERS)
    print(f"Summer League box scores for {date_str}:")
    pprint.pprint(stats)
