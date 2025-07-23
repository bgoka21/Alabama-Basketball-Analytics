import csv
import json
import os
from datetime import datetime

from models.recruit import Recruit, RecruitShotTypeStat
from yourapp import db  # import your SQLAlchemy db instance


def parse_recruits_csv(csv_path, recruit_id):
    recruit = db.session.query(Recruit).get(recruit_id)
    if not recruit:
        raise ValueError(f"No recruit with id={recruit_id}")

    shot_list = []
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("Row", "").strip() != recruit.name:
                continue

            # find the exactly matching recruit column header
            col = next(h for h in reader.fieldnames if h.strip() == recruit.name)
            tokens = [t.strip() for t in row[col].split(',') if t.strip()]

            assist = (
                "Assisted" if any("Assist" in row[c] for c in reader.fieldnames) else "Non-Assisted"
            )

            for token in tokens:
                if token.startswith("ATR"):
                    cls = "atr"
                elif token.startswith("2FG"):
                    cls = "fg2"
                elif token.startswith("3FG"):
                    cls = "fg3"
                else:
                    continue

                result = "made" if token.endswith("+") else "miss"
                shot_obj = {
                    "event": "shot_attempt",
                    "shot_class": cls,
                    "result": result,
                    "possession_type": row.get("POSSESSION TYPE", ""),
                    "assisted": assist,
                    "shot_location": row.get("Shot Location", ""),
                }
                shot_list.append(shot_obj)

    stat = RecruitShotTypeStat(
        recruit_id=recruit.id,
        shot_type_details=json.dumps(shot_list),
    )
    db.session.add(stat)
    db.session.commit()
    return stat


if __name__ == "__main__":
    import sys
    csv_path = sys.argv[1]
    recruit_id = int(sys.argv[2])
    stat = parse_recruits_csv(csv_path, recruit_id)
    print(f"Saved RecruitShotTypeStat id={stat.id} at {stat.created_at}")
