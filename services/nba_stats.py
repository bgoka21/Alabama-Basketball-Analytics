import requests
from bs4 import BeautifulSoup
import re
from datetime import date, timedelta
import logging
from typing import List, Dict

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

PLAYERS = [
    "Keon Ellis","Noah Clowney","Herb Jones","Brandon Miller",
    "Collin Sexton","Alex Reese","Kira Lewis Jr.","Jaden Shackelford",
    "Josh Primo","Aaron Estrada","Mark Sears","Grant Nelson",
    "Clifford Omoruyi","Chris Youngblood",
]

def get_scoreboard_html(date_str: str) -> str:
    """Fetch ESPN Summer League scoreboard HTML for YYYYMMDD."""
    url = f"https://www.espn.com/nba-summer-league/scoreboard/_/date/{date_str}"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    html = resp.text
    logger.debug("Fetched scoreboard page for %s (len=%d)", date_str, len(html))
    return html

def parse_scoreboard_game_ids(html: str) -> List[str]:
    """Return all gameId values found in the scoreboard HTML."""
    soup = BeautifulSoup(html, "html.parser")
    ids = set()
    for a in soup.find_all("a", href=True):
        m = re.search(r"/game/_/gameId/(\d+)", a["href"])
        if m:
            ids.add(m.group(1))
    logger.debug("Parsed game IDs: %s", ids)
    return list(ids)

def get_game_summary(game_id: str) -> Dict:
    """Fetch ESPN JSON summary for one game."""
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={game_id}"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    logger.debug("Fetched summary for game %s", game_id)
    return data

def parse_players_from_summary(summary: Dict, players: List[str]) -> Dict[str, Dict]:
    """Extract basic stats for PLAYERS from one game summary."""
    results: Dict[str, Dict] = {}
    box = summary.get("boxscore", {})
    teams = box.get("players", [])
    abbrevs = [t["team"]["abbreviation"] for t in teams if t.get("team")]
    for t in teams:
        team_abbr = t["team"]["abbreviation"]
        opp = next(a for a in abbrevs if a != team_abbr)
        for block in t.get("statistics", []):
            if block.get("name") != "Statistics":
                continue
            labels = block.get("labels", [])
            for ath in block.get("athletes", []):
                name = ath["athlete"]["displayName"]
                logger.debug("Athlete name: %s", name)
                if name not in PLAYERS:
                    continue
                stats = dict(zip(labels, ath.get("stats", [])))
                stats["team"] = team_abbr
                stats["opponent"] = opp
                results[name] = stats
    return results

def get_yesterdays_summer_stats(players: List[str]) -> Dict[str, Dict]:
    """Scrape yesterday’s Summer League stats for PLAYERS."""
    date_str = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    html = get_scoreboard_html(date_str)
    game_ids = parse_scoreboard_game_ids(html)
    all_stats: Dict[str, Dict] = {}
    for gid in game_ids:
        summary = get_game_summary(gid)
        all_stats.update(parse_players_from_summary(summary, players))
    return all_stats

if __name__ == "__main__":
    stats = get_yesterdays_summer_stats(PLAYERS)
    print(f"Summer League box scores for {(date.today()-timedelta(days=1)).strftime('%Y-%m-%d')}:")
    for p, v in stats.items():
        print(f"{p}: PTS {v.get('PTS')} | REB {v.get('REB')} | AST {v.get('AST')}")
