import csv, json, re
from models.recruit import Recruit, RecruitShotTypeStat
from yourapp import db


def parse_recruits_csv(csv_path, recruit_id):
    recruit = db.session.get(Recruit, recruit_id)
    if not recruit:
        raise ValueError(f"No recruit with id={recruit_id}")

    shot_list = []
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        # find the column for this recruit’s name
        col_name = next((h for h in reader.fieldnames if h.strip() == recruit.name), None)
        if not col_name:
            raise ValueError(f"Column for recruit '{recruit.name}' not found")

        for row in reader:
            if row.get("Row", "").strip() != recruit.name:
                continue

            # pre-compute flags
            assist_cell    = str(row.get("Assist", "") or "") + "," + str(row.get("Pot. Assist", "") or "")
            assisted_flag  = any(t in assist_cell for t in ("Assist", "Pot. Assist"))
            contest_cell   = str(row.get("Contest", "") or "")
            contested_flag = contest_cell.strip()  # e.g. "Contest", "No Contest", etc.

            tokens = [t.strip() for t in (row.get(col_name, "") or "").split(",") if t.strip()]
            for token in tokens:
                m = re.match(r"^(ATR|2FG|3FG)(\+|\-)$", token, re.IGNORECASE)
                if not m:
                    continue

                # core fields
                shot_key = m.group(1).lower().replace("2fg","fg2").replace("3fg","fg3")
                shot = {
                    "event":           "shot_attempt",
                    "shot_class":      shot_key,
                    "result":          "made" if token.endswith("+") else "miss",
                    "possession_type": row.get("POSSESSION TYPE", "").strip(),

                    # **subcategories**:
                    "Assisted":        "Assisted"     if assisted_flag else "",
                    "Non-Assisted":    ""             if assisted_flag else "Non-Assisted",
                    "Contested":       contested_flag,

                    # **any drill labels**:
                    "drill_labels": [
                        lbl.strip() for lbl in (row.get("DRILL LABELS", "") or "").split(",") if lbl.strip()
                    ],

                    # **shot location**:
                    "shot_location":   str(row.get("Shot Location", "") or "").strip(),
                }

                # **capture any extra sub-cols** like “2FG (Hands to Rim)”, “ATR (Hands to Rim)”, etc.
                for detail_col in reader.fieldnames:
                    if detail_col.startswith(f"{m.group(1)} (") and detail_col.endswith(")"):
                        key = detail_col.lower().replace(" ", "_").replace("(", "").replace(")", "")
                        shot[key] = row.get(detail_col, "")

                shot_list.append(shot)

    # commit exactly like Practice parser
    stat = RecruitShotTypeStat(
        recruit_id        = recruit.id,
        shot_type_details = json.dumps(shot_list),
    )
    db.session.add(stat)
    db.session.commit()
    return stat


if __name__ == "__main__":
    import sys
    csv_path, recruit_id = sys.argv[1], int(sys.argv[2])
    with db.app.app_context():
        parse_recruits_csv(csv_path, recruit_id)
