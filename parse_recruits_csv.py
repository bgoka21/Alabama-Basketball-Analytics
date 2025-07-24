import csv
import json
import re
import sys

import pandas as pd

from models.recruit import Recruit, RecruitShotTypeStat
from yourapp import db


def safe_str(value):
    """Safely convert a value to a string, returning an empty string for None."""
    return "" if value is None else str(value)


def extract_tokens(val):
    """Return list of comma-separated tokens from the cell value."""
    if pd.isna(val) or not isinstance(val, str):
        return []
    return [t.strip() for t in val.split(',') if t.strip()]


def parse_recruits_csv(csv_path, recruit_id):
    recruit = db.session.get(Recruit, recruit_id)
    if not recruit:
        raise ValueError(f"No recruit with id={recruit_id}")

    shot_list = []
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        col_name = next((h for h in fieldnames if h.strip() == recruit.name), None)
        if not col_name:
            raise ValueError(f"Column for recruit '{recruit.name}' not found")

        for row in reader:
            if str(row.get('Row', '')).strip() != recruit.name:
                continue

            drill_val = row.get('DRILL TYPE')
            drill_str = '' if drill_val is None else str(drill_val)
            labels = [t.strip().upper() for t in drill_str.split(',') if t.strip()]

            cell = str(row.get(col_name, '') or '').strip()
            if not cell:
                continue
            tokens = [t.strip() for t in cell.split(',') if t.strip()]

            assisted_flag = False
            for other_col in fieldnames:
                assist_cell = str(row.get(other_col, '') or '').strip()
                if not assist_cell:
                    continue
                assist_tokens = [t.strip() for t in assist_cell.split(',') if t.strip()]
                if any(t == 'Assist' or t == 'Pot. Assist' for t in assist_tokens):
                    assisted_flag = True
                    break

            for token in tokens:
                if token in ('ATR+', 'ATR-'):
                    cls = 'atr'
                    result = 'made' if token == 'ATR+' else 'miss'
                elif token in ('2FG+', '2FG-'):
                    cls = '2fg'
                    result = 'made' if token == '2FG+' else 'miss'
                elif token in ('3FG+', '3FG-'):
                    cls = '3fg'
                    result = 'made' if token == '3FG+' else 'miss'
                else:
                    cls = None

                if cls:
                    poss_val = row.get('POSSESSION TYPE', '')
                    possession_str = '' if poss_val is None else str(poss_val).strip()

                    shot_obj = {
                        'event':          'shot_attempt',
                        'shot_class':     cls,
                        'result':         result,
                        'possession_type': possession_str,
                        'Assisted':       'Assisted' if assisted_flag else '',
                        'Non-Assisted':   '' if assisted_flag else 'Non-Assisted',
                        'drill_labels':   labels,
                    }
                    shot_location = safe_str(row.get('Shot Location', ''))
                    shot_obj['shot_location'] = shot_location

                    if cls in ('2fg', 'atr'):
                        # copy every 2FG/ATR detail column
                        for detail_col in fieldnames:
                            if detail_col.startswith('2FG (') and detail_col.endswith(')'):
                                suffix = detail_col[len('2FG ('):-1].lower().replace(' ', '_').replace('/', '_')
                                shot_obj[f'2fg_{suffix}'] = safe_str(row.get(detail_col, ''))
                                if cls == 'atr':
                                    shot_obj[f'atr_{suffix}'] = safe_str(row.get(detail_col, ''))

                        # copy every 2FG Scheme column
                        for scheme_col in ('2FG Scheme (Attack)', '2FG Scheme (Drive)', '2FG Scheme (Pass)'):
                            if scheme_col in fieldnames:
                                val = safe_str(row.get(scheme_col, ''))
                                if val:
                                    key = scheme_col.lower().replace(' ', '_').replace('(', '').replace(')', '')
                                    shot_obj[key] = val

                    else:  # cls == '3fg'
                        # copy every 3FG detail column
                        for suffix in ["Contest", "Footwork", "Good/Bad", "Line", "Move", "Pocket", "Shrink", "Type"]:
                            col_name = f"3FG ({suffix})"
                            json_key = f"3fg_{suffix.lower().replace('/', '_').replace(' ', '_')}"
                            shot_obj[json_key] = safe_str(row.get(col_name, ""))

                        # copy every 3FG Scheme column
                        for scheme_col in ("3FG Scheme (Attack)", "3FG Scheme (Drive)", "3FG Scheme (Pass)"):
                            if scheme_col in fieldnames:
                                for tok in extract_tokens(row.get(scheme_col, "")):
                                    key = scheme_col.lower().replace(' ', '_').replace('(', '').replace(')', '')
                                    shot_obj[f"{key}_{tok.replace(' ', '_').lower()}"] = tok

                    shot_list.append(shot_obj)

                # now handle free throws exactly the same way:
                if token == 'FT+':
                    ft_obj = {
                        'event':       'shot_attempt',
                        'shot_class':  'ft',
                        'result':      'made',
                        'drill_labels': labels,
                    }
                    ft_obj['shot_location'] = safe_str(row.get('Shot Location', ''))
                    shot_list.append(ft_obj)
                    continue
                elif token == 'FT-':
                    ft_obj = {
                        'event':       'shot_attempt',
                        'shot_class':  'ft',
                        'result':      'miss',
                        'drill_labels': labels,
                    }
                    ft_obj['shot_location'] = safe_str(row.get('Shot Location', ''))
                    shot_list.append(ft_obj)
                    continue

    stat = RecruitShotTypeStat(recruit_id=recruit.id, shot_type_details=json.dumps(shot_list))
    db.session.add(stat)
    db.session.commit()
    return stat


if __name__ == '__main__':
    with db.app.app_context():
        parse_recruits_csv(sys.argv[1], int(sys.argv[2]))
