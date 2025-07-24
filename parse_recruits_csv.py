import csv
import json
import re
import sys

import pandas as pd

from models.recruit import Recruit, RecruitShotTypeStat
from yourapp import db


def safe_str(value):
    """Return empty string if None, else str(value)."""
    return "" if value is None else str(value)


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
        fieldnames = reader.fieldnames or []

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

                    # Copy every detail column matching "ATR/2FG/3FG (<Detail>)"
                    for detail_col in fieldnames:
                        detail_match = re.match(
                            rf'^{re.escape(m.group(1))} \((.+)\)$',
                            detail_col,
                        )
                        if detail_match:
                            suffix = (
                                detail_match.group(1)
                                .lower()
                                .replace(' ', '_')
                                .replace('/', '_')
                            )
                            shot_obj[f"{shot_key}_{suffix}"] = safe_str(
                                row.get(detail_col, '')
                            )

                    # Copy every scheme column like "ATR/2FG/3FG Scheme (<Type>)"
                    for scheme_col in fieldnames:
                        if scheme_col.startswith(f"{m.group(1)} Scheme"):
                            base_key = (
                                scheme_col.lower()
                                .replace(' ', '_')
                                .replace('(', '')
                                .replace(')', '')
                            )
                            for tok in extract_tokens(row.get(scheme_col, '') or ''):
                                shot_obj[f"{base_key}_{tok.replace(' ', '_').lower()}"] = tok

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

