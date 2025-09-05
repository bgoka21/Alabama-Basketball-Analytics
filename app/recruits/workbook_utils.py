import os
import math, re
import pandas as pd
from flask import current_app


def normalize_name(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or '').strip()).lower()


def to_int_or_none(x):
    try:
        return int(float(x))
    except Exception:
        return None


def parse_pick(cell):
    # Returns (num:int|None, raw:str|None|('Undrafted'|'N/A'))
    if cell is None:
        return None, 'Undrafted'
    s = str(cell).strip()
    if s == '':
        return None, 'Undrafted'
    if s.lower() in ('n/a','na'):
        return None, 'N/A'
    if s.lower() in ('udfa','undrafted'):
        return None, 'Undrafted'
    try:
        return int(float(s)), None
    except Exception:
        m = re.search(r'(\d+)', s)
        return (int(m.group(1)), s) if m else (None, s)


def active_workbook_df():
    """
    Return a single normalized DataFrame containing at least:
    ['Coach','Player','Team','Year','Projected $','Actual $',
     'Projected Pick','Actual Pick'].
    Use your existing Workbook Manager path/logic.
    """
    path = os.path.join(current_app.instance_path, 'money_board', 'money_board.xlsx')
    if os.path.exists(path):
        df_dict = pd.read_excel(path, sheet_name=None)
        frames = []
        for sheet, frame in df_dict.items():
            f = frame.copy()
            f['__sheet'] = sheet
            frames.append(f)
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        df.columns = [str(c).strip() for c in df.columns]
        return df

    # Fallback: build DataFrame from Prospect table if no workbook is present
    try:
        from app.models.prospect import Prospect
    except Exception:
        return pd.DataFrame()

    rows = []
    for p in Prospect.query.all():
        rows.append({
            'Coach': p.coach,
            'Player': p.player,
            'Team': p.team,
            'Year': p.year,
            'Projected $': p.projected_money,
            'Actual $': p.actual_money,
            'Projected Pick': p.projected_pick_raw if p.projected_pick_raw is not None else p.projected_pick,
            'Actual Pick': p.actual_pick_raw if p.actual_pick_raw is not None else p.actual_pick,
            'Coach Team': p.coach_current_team,
            'Coach Conf': p.coach_current_conference,
            '__sheet': p.sheet,
        })
    return pd.DataFrame(rows)
