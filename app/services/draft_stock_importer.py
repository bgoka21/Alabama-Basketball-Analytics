import math
import pandas as pd
from app import db
from app.models.prospect import Prospect
from app.utils.measurements import parse_feet_inches


def _s(x):
    """Safe string: return '' for None/NaN, else stripped string."""
    if x is None:
        return ""
    if isinstance(x, float) and math.isnan(x):
        return ""
    return str(x).strip()


def _to_int_or_none(x):
    """Return int or None (handles NaN, '', None, '2024.0')."""
    if x is None:
        return None
    if isinstance(x, float) and math.isnan(x):
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _to_num(x):
    if x is None: return None
    s = str(x).replace("$","" ).replace(",","" ).strip()
    try: return float(s)
    except: return None


def import_workbook(xlsx_path_or_buffer, strict=True, commit_batch=500):
    """
    Import the multi-sheet Excel workbook into Prospect rows.
    Accepts a filesystem path or a file-like object (BytesIO).
    Upserts by (player, team, year). Returns a summary dict.
    """
    xl = pd.ExcelFile(xlsx_path_or_buffer)
    total_rows = 0
    upserts = 0
    per_sheet = []

    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        df.columns = [str(c).strip() for c in df.columns]

        # Required columns must exist; no auto-fix of Player.1
        required = ["Coach", "Player", "Team"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            msg = f"Sheet '{sheet}' missing required columns: {missing}."
            if strict:
                raise ValueError(msg)
            per_sheet.append({"sheet": sheet, "status": "skipped", "reason": msg})
            continue

        # Coerce numbers and parse measurements
        for c in ["Projected Money","Actual Money","NET","Age","Year","Projected Pick","Actual Pick"]:
            if c in df.columns:
                df[c] = df[c].apply(_to_num)

        df["height_in"]   = df["Height"].apply(parse_feet_inches)   if "Height"   in df.columns else None
        df["wingspan_in"] = df["WingSpan"].apply(parse_feet_inches) if "WingSpan" in df.columns else None
        df["ws_minus_h_in"] = df["wingspan_in"] - df["height_in"]

        # Compute NET where missing
        if "Actual Money" in df.columns and "Projected Money" in df.columns:
            df["NET"] = df["NET"].where(df["NET"].notna(), df["Actual Money"] - df["Projected Money"])

        if "Year" in df.columns:
            df["Year"] = df["Year"].apply(_to_int_or_none)

        rows = df.to_dict(orient="records")
        total_rows += len(rows)

        batch = 0
        for r in rows:
            coach = _s(r.get("Coach"))
            player = _s(r.get("Player"))
            team   = _s(r.get("Team"))
            year = r.get("Year")

            if not coach or not player or not team:
                continue

            obj = Prospect.query.filter_by(player=player, team=team, year=year).first()
            created = False
            if obj is None:
                obj = Prospect(player=player, team=team, year=year)
                created = True

            obj.sheet = sheet
            obj.coach = coach
            obj.coach_current_team = _s(r.get("Coach Current Team"))
            obj.coach_current_conference = _s(r.get("Coach Current Conference"))

            obj.player_class = _s(r.get("Class"))
            obj.age = _to_num(r.get("Age"))
            obj.player_conference = _s(r.get("Player Conference"))

            obj.projected_money = _to_num(r.get("Projected Money"))
            obj.actual_money = _to_num(r.get("Actual Money"))
            if obj.actual_money is not None and obj.projected_money is not None:
                obj.net = obj.actual_money - obj.projected_money
            else:
                obj.net = _to_num(r.get("NET"))

            obj.projected_pick = _to_num(r.get("Projected Pick"))
            obj.actual_pick    = _to_num(r.get("Actual Pick"))

            obj.height_raw = _s(r.get("Height"))
            obj.wingspan_raw = _s(r.get("WingSpan"))
            obj.height_in = r.get("height_in")
            obj.wingspan_in = r.get("wingspan_in")
            obj.ws_minus_h_in = r.get("ws_minus_h_in")

            obj.home_city = _s(r.get("Home City"))
            obj.home_state = _s(r.get("Home State"))
            obj.country = _s(r.get("Country"))

            if created:
                db.session.add(obj)
            upserts += 1
            batch += 1
            if batch >= commit_batch:
                db.session.commit()
                batch = 0

        if batch > 0:
            db.session.commit()

        per_sheet.append({"sheet": sheet, "rows": len(rows), "status": "ok"})

    return {"sheets": len(xl.sheet_names), "rows": total_rows, "upserts": upserts, "per_sheet": per_sheet}
