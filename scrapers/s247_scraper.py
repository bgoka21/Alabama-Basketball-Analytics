import re
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict


def _extract_int(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else None


def scrape_247_stats(s247_url: str) -> Dict[str, Optional[int]]:
    """Scrape ranking data from a 247Sports recruiting profile."""
    stats = {
        's247_overall_rank': None,
        's247_position_rank': None,
    }
    try:
        resp = requests.get(s247_url, timeout=10)
        resp.raise_for_status()
    except Exception:
        return stats

    soup = BeautifulSoup(resp.text, 'html.parser')

    overall = soup.find(string=re.compile('Overall.*Rank', re.I))
    if overall:
        stats['s247_overall_rank'] = _extract_int(overall)

    position = soup.find(string=re.compile('Position.*Rank', re.I))
    if position:
        stats['s247_position_rank'] = _extract_int(position)

    return stats
