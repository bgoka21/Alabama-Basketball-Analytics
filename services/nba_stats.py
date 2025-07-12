import requests
from datetime import date, timedelta
from typing import List, Dict
import pprint


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
    year = int(date_str[:4])
    url = (
        "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
        f"?season={year}&seasontype=50&dates={date_str}"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json() or {}
        events = data.get("events", [])
        print(
            f"DEBUG [Scoreboard]: Found {len(events)} game(s) on {date_str}: {[e.get('id') for e in events]}"
        )
        return data
    except requests.RequestException:
        return {}

def get_game_summary(game_id: str) -> dict:
    """Fetch the detailed summary JSON for a given Summer League game."""
    url = (
        "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
        f"?event={game_id}"
    )
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

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
    """Extract desired players' box scores from the game summary JSON."""

    results: Dict[str, Dict] = {}
    boxscore = summary_json.get("boxscore", {})
    teams = boxscore.get("players", [])
    # Determine all team abbreviations so we can assign opponents
    abbrevs = [t.get("team", {}).get("abbreviation") for t in teams if t.get("team")]
    for team_block in teams:
        team_abbrev = team_block.get("team", {}).get("abbreviation")
        opponent_abbrev = next((a for a in abbrevs if a != team_abbrev), "")
        for stat_block in team_block.get("statistics", []):
            if stat_block.get("name") != "Statistics":
                continue
            labels = stat_block.get("labels", [])
            for athlete in stat_block.get("athletes", []):
                name = athlete.get("athlete", {}).get("displayName")
                print(f"DEBUG [Athlete]: displayName = {name}")
                if name not in PLAYERS:
                    continue
                values = athlete.get("stats", [])
                stat_map = dict(zip(labels, values))
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
