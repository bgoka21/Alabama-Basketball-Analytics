import os
import math, re
import pandas as pd
from flask import current_app


def _to_int_money(series: pd.Series) -> pd.Series:
    # Accept numeric or strings with $, commas, or blanks → int (missing → 0)
    s = series.copy()
    s = s.astype(str).str.replace(r'[\$,]', '', regex=True).str.strip()
    s = s.replace({'': None, 'nan': None, 'None': None})
    return pd.to_numeric(s, errors='coerce').fillna(0).astype(int)


def normalize_money_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize workbook monetary columns to canonical:
      'Projected $', 'Actual $', 'NET $' (all ints).
    Accepts aliases: 'Projected Money', 'Actual Money', 'NET', 'Net', etc.
    Does not mutate the original df.
    """
    out = df.copy()

    aliases = {
        'Projected $': ['Projected $', 'Projected Money', 'Proj Money', 'Projected$'],
        'Actual $'   : ['Actual $', 'Actual Money', 'Act Money', 'Actual$'],
        'NET $'      : ['NET $', 'NET', 'Net', 'Net $'],
    }

    def first_col(options):
        for c in options:
            if c in out.columns:
                return c
        return None

    # Map/copy into canonical names; create if missing
    for canon, options in aliases.items():
        src = first_col(options)
        if src and src != canon:
            out[canon] = out[src]
        elif canon not in out:
            out[canon] = 0

    # Coerce to integer dollars
    for col in ['Projected $', 'Actual $', 'NET $']:
        out[col] = _to_int_money(out[col])

    # If NET $ is zero but projected/actual exist, recompute
    recompute_mask = (out['NET $'] == 0) & (out['Actual $'].notna()) & (out['Projected $'].notna())
    out.loc[recompute_mask, 'NET $'] = out['Actual $'] - out['Projected $']

    return out


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
            'Projected Pick': p.projected_pick_text or (
                p.projected_pick_raw if p.projected_pick_raw is not None else p.projected_pick
            ),
            'Actual Pick': p.actual_pick_text or (
                p.actual_pick_raw if p.actual_pick_raw is not None else p.actual_pick
            ),
            'Coach Team': p.coach_current_team,
            'Coach Conf': p.coach_current_conference,
            '__sheet': p.sheet,
        })
    return pd.DataFrame(rows)
