import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict


def _parse_float(text: str) -> Optional[float]:
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_pct(text: str) -> Optional[float]:
    if not text:
        return None
    text = text.strip().rstrip('%')
    return _parse_float(text)


def scrape_espn_stats(espn_url: str) -> Dict[str, Optional[float]]:
    """Scrape basic stats from an ESPN player profile page."""
    stats = {
        'three_fg_pct': None,
        'assists': None,
        'turnovers': None,
        'assist_turnover_ratio': None,
        'ft_pct': None,
        'ppg': None,
        'rpg': None,
        'apg': None,
    }
    try:
        resp = requests.get(espn_url, timeout=10)
        resp.raise_for_status()
    except Exception:
        return stats

    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table')
    if not table:
        return stats

    for row in table.find_all('tr'):
        cells = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
        if len(cells) < 2:
            continue
        key = cells[0].lower()
        value = cells[-1]
        if '3pt' in key or '3fg%' in key or '3-point' in key:
            stats['three_fg_pct'] = _parse_pct(value)
        elif 'assists' in key and 'ratio' not in key:
            stats['assists'] = _parse_float(value)
        elif 'turnover' in key and 'ratio' not in key:
            stats['turnovers'] = _parse_float(value)
        elif 'assist' in key and 'ratio' in key:
            stats['assist_turnover_ratio'] = _parse_float(value)
        elif 'ft%' in key or 'free throw' in key:
            stats['ft_pct'] = _parse_pct(value)
        elif 'ppg' in key or 'points' in key:
            stats['ppg'] = _parse_float(value)
        elif 'rpg' in key or 'rebounds' in key:
            stats['rpg'] = _parse_float(value)
        elif 'apg' in key or 'assists per game' in key:
            stats['apg'] = _parse_float(value)

    return stats
