import requests
from bs4 import BeautifulSoup


def scrape_247_stats(url: str) -> dict:
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    ovr_elem = soup.select_one('.rankings-overall .rank')
    pos_elem = soup.select_one('.rankings-position .rank')
    overall = int(ovr_elem.text.strip('#')) if ovr_elem else None
    position = int(pos_elem.text.strip('#')) if pos_elem else None
    return {'overall_rank': overall, 'position_rank': position}
