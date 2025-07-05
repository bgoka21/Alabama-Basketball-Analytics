import twofourseven
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from models.database import db
from models.recruit import Recruit


def fetch_247_recruits(year):
    players = twofourseven.getBBPlayerData(year)
    for p in players:
        rec = Recruit.query.filter_by(name=p['Name'], year=year, source='HS').first() or Recruit()
        rec.name, rec.position = p['Name'], p['Position']
        rec.height, rec.weight = p['Height'], p['Weight']
        rec.school, rec.rating = p['CommittedSchool'], p.get('Stars')
        rec.ppg = rec.rpg = rec.apg = None
        rec.year, rec.source = year, 'HS'
        rec.last_updated = datetime.utcnow()
        db.session.add(rec)
    db.session.commit()


def fetch_transfers(year):
    url = f'https://verbalcommits.com/transfers/{year}/D1'
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    table = soup.select_one('table#transfer-list')
    for row in table.tbody.find_all('tr'):
        cols = [c.text.strip() for c in row.find_all('td')]
        name, pos, htwt, prev, stats = cols[0], cols[1], cols[2], cols[3], cols[4]
        ppg, rpg, apg = map(float, stats.split('/'))
        rec = Recruit.query.filter_by(name=name, year=year, source='Transfer').first() or Recruit()
        rec.name, rec.position = name, pos
        rec.height, rec.weight = htwt.split(',')[0].strip(), htwt.split(',')[1].strip()
        rec.school, rec.rating = prev, None
        rec.ppg, rec.rpg, rec.apg = ppg, rpg, apg
        rec.year, rec.source = year, 'Transfer'
        rec.last_updated = datetime.utcnow()
        db.session.add(rec)
    db.session.commit()


def run_full_refresh(years=[2025]):
    for y in years:
        fetch_247_recruits(y)
        fetch_transfers(y)
