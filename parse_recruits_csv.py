import csv
import json
import re
import sys

import pandas as pd

from models.recruit import Recruit, RecruitShotTypeStat
from yourapp import db


def safe_str(value):
    """Safely convert a value to a string, returning an empty string for NA values."""
    if pd.isna(value):
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value)


def extract_tokens(val):
    """Split a cell value on commas, strip whitespace, return list; return [] for NaN/non-str."""
    if pd.isna(val) or not isinstance(val, str):
        return []
    return [t.strip() for t in val.split(',') if t.strip()]


def parse_recruits_csv(csv_path, recruit_id):
    # 1. Load the recruit record
    recruit = db.session.get(Recruit, recruit_id)
    if not recruit:
        raise ValueError(f"No recruit with id={recruit_id}")

    shot_list = []
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = [str(h).strip() for h in (reader.fieldnames or [])]

        # 2. Identify the column matching recruit.name
        col_name = next((h for h in fieldnames if h.strip() == recruit.name), None)
        if not col_name:
            raise ValueError(f"Column for recruit '{recruit.name}' not found")

        for row in reader:
            # 3. Only rows where Row == recruit.name
            if str(row.get('Row', '')).strip() != recruit.name:
                continue

            # 4. Drill labels from 'Flags' column
            drill_val = row.get('Flags', '') or ''
            labels = [t.strip() for t in drill_val.split(',') if t.strip()]

            # 5. Extract the recruit's shot tokens
            cell = str(row.get(col_name, '') or '').strip()
            if not cell:
                continue
            tokens = extract_tokens(cell)

            # 6. Determine assisted_flag by scanning 'Assist Type', then all columns
            assisted_flag = any(
                t in ('Assist', 'Pot. Assist', 'Secondary Assist')
                for t in extract_tokens(row.get('Assist Type', '') or '')
            )
            if not assisted_flag:
                for h in fieldnames:
                    if any(
                        t in ('Assist', 'Pot. Assist', 'Secondary Assist')
                        for t in extract_tokens(row.get(h, '') or '')
                    ):
                        assisted_flag = True
                        break

            for token in tokens:
                # —— Handle ATR / 2FG / 3FG ——
                m = re.match(r'^(ATR|2FG|3FG)([+-])$', token, re.IGNORECASE)
                if m:
                    shot_key = m.group(1).lower()
                    result = 'made' if token.endswith('+') else 'miss'
                    poss_val = row.get('Shot Possession Type', '') or ''
                    possession_type = str(poss_val).strip()

                    shot_obj = {
                        'event': 'shot_attempt',
                        'shot_class': shot_key,
                        'result': result,
                        'possession_type': possession_type,
                        'Assisted': 'Assisted' if assisted_flag else '',
                        'Non-Assisted': '' if assisted_flag else 'Non-Assisted',
                        'drill_labels': labels,
                        'shot_location': safe_str(row.get('Shot Location', '')),
                    }

                    if shot_key in ("2fg", "atr"):
                        for detail_col in fieldnames:
                            detail_match = re.match(r"^2FG \((.+)\)$", detail_col, re.IGNORECASE)
                            if detail_match:
                                suffix = (
                                    detail_match.group(1)
                                    .lower()
                                    .replace(' ', '_')
                                    .replace('/', '_')
                                )
                                suffix = re.sub(r"_+", "_", suffix).strip('_')
                                key_2fg = f"2fg_{suffix}"
                                shot_obj[key_2fg] = safe_str(row.get(detail_col, ""))
                                if shot_key == "atr":
                                    shot_obj[f"atr_{suffix}"] = safe_str(row.get(detail_col, ""))

                        for scheme_col in fieldnames:
                            if re.match(r"^2FG Scheme \((Attack|Drive|Pass)\)$", scheme_col):
                                base = (
                                    scheme_col.lower()
                                    .replace(' ', '_')
                                    .replace('(', '')
                                    .replace(')', '')
                                )
                                val = safe_str(row.get(scheme_col, "")).strip()
                                if val:
                                    shot_obj[base] = val
                                    if shot_key == "atr":
                                        shot_obj[base.replace("2fg", "atr", 1)] = val

                    elif shot_key == "3fg":
                        for detail_col in fieldnames:
                            detail_match = re.match(r"^3FG \((.+)\)$", detail_col, re.IGNORECASE)
                            if detail_match:
                                suffix = (
                                    detail_match.group(1)
                                    .lower()
                                    .replace(' ', '_')
                                    .replace('/', '_')
                                )
                                suffix = re.sub(r"_+", "_", suffix).strip('_')
                                shot_obj[f"3fg_{suffix}"] = safe_str(row.get(detail_col, ""))

                        for scheme_col in fieldnames:
                            if re.match(r"^3FG Scheme \((Attack|Drive|Pass)\)$", scheme_col):
                                base = (
                                    scheme_col.lower()
                                    .replace(' ', '_')
                                    .replace('(', '')
                                    .replace(')', '')
                                )
                                val = safe_str(row.get(scheme_col, "")).strip()
                                if val:
                                    shot_obj[base] = val

                    shot_list.append(shot_obj)
                    continue

                # —— Handle Free Throws ——
                if token in ('FT+', 'FT-'):
                    ft_obj = {
                        'event': 'shot_attempt',
                        'shot_class': 'ft',
                        'result': 'made' if token == 'FT+' else 'miss',
                        'drill_labels': labels,
                        'shot_location': safe_str(row.get('Shot Location', '')),
                    }
                    shot_list.append(ft_obj)
                    continue

    # 7. Commit to RecruitShotTypeStat exactly like practice parser
    stat = RecruitShotTypeStat(
        recruit_id=recruit.id,
        shot_type_details=json.dumps(shot_list),
    )
    db.session.add(stat)
    db.session.commit()
    return stat


if __name__ == '__main__':
    with db.app.app_context():
        parse_recruits_csv(sys.argv[1], int(sys.argv[2]))
