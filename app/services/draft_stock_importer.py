import math
import re
import pandas as pd
from app import db
from app.models.prospect import Prospect
from app.utils.measurements import parse_feet_inches

# --- START PATCH: header normalization & pick parsing helpers ---
HEADER_SYNONYMS = {
    # core identity
    "coach": ["coach", "coach name"],
    "player": ["player", "prospect", "athlete"],
    "team": ["team", "school", "program", "college"],
    "year": ["year", "recruiting year", "class year"],
    # money
    "projected_money": [
        "projected money",
        "proj money",
        "projected $",
        "proj $",
        "projected_amount",
    ],
    "actual_money": [
        "actual money",
        "act money",
        "actual $",
        "act $",
        "actual_amount",
    ],
    "net": ["net", "difference", "net $"],
    # picks
    "projected_pick_raw": [
        "projected pick",
        "proj pick",
        "projected draft pick",
        "proj draft pick",
        "projected_pick",
    ],
    "actual_pick_raw": [
        "actual pick",
        "draft pick",
        "actual draft pick",
        "actual_pick",
    ],
    # optional coach context
    "coach_current_team": [
        "coach current team",
        "coach team",
        "coach_team",
        "current team",
    ],
    "coach_current_conference": [
        "coach current conference",
        "coach conference",
        "coach_conf",
        "current conference",
    ],
}


def _normalize_col(s: str) -> str:
    return re.sub(r"[\s_]+", " ", (s or "")).strip().lower()


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Rename df columns to canonical names defined in HEADER_SYNONYMS when possible."""
    col_map = {}
    inv = {}
    for canon, alts in HEADER_SYNONYMS.items():
        for a in alts:
            inv[_normalize_col(a)] = canon
    for c in df.columns:
        key = _normalize_col(str(c))
        col_map[c] = inv.get(key, c)  # fallback to original if no match
    return df.rename(columns=col_map)


def parse_pick_to_int(raw: str) -> int | None:
    """Extract the first integer pick from a raw string. Returns None for UDFA/blank/unparseable."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    low = s.lower()
    if low in {"udfa", "ufa", "n/a", "na", "-", "undrafted"}:
        return None
    # common formats: "20", "20th", "Round 2, Pick 45", "No. 12", "Pick #30"
    m = re.search(r"(\d+)", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None
# --- END PATCH ---


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


def _pick_to_int(x):
    """
    Convert '20th' -> 20, '23' -> 23, 'Undrafted'/'UDFA' -> None, else None.
    Leaves non-numeric descriptors (e.g., 'Lottery') as None; we keep text in *_raw.
    """
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in ("nan", "undrafted", "udfa", "u.d.f.a."):
        return None
    # strip ordinal suffixes and any non-digits, keep leading number
    m = re.match(r"^\s*(\d+)", s.replace("st","" ).replace("nd","" ).replace("rd","" ).replace("th","" ))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


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
        df = xl.parse(sheet, dtype=str)
        df = normalize_headers(df)

        # --- START PATCH: ensure 'team' exists and is clean ---
        if "team" not in df.columns:
            # If the sheet truly has no team info, create an empty column (won't crash drilldown)
            df["team"] = None

        # Strip whitespace
        df["team"] = df["team"].fillna("").astype(str).str.strip()
        # --- END PATCH ---

        # Required columns must exist; no auto-fix of Player.1
        required = ["coach", "player", "team"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            msg = f"Sheet '{sheet}' missing required columns: {missing}."
            if strict:
                raise ValueError(msg)
            per_sheet.append({"sheet": sheet, "status": "skipped", "reason": msg})
            continue

        # strip whitespace across known text fields (if present)
        for col in ["coach", "player", "team", "coach_current_team", "coach_current_conference"]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.strip()

        # coerce numeric-ish money and year, robustly
        for col in ["projected_money", "actual_money", "net", "year"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .replace({r"[\$,]": ""}, regex=True)
                    .apply(lambda v: None if str(v).strip() in {"", "n/a", "na", "-"} else v)
                )
                if col == "year":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                else:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

        df["height_in"] = df["Height"].apply(parse_feet_inches) if "Height" in df.columns else None
        df["wingspan_in"] = df["WingSpan"].apply(parse_feet_inches) if "WingSpan" in df.columns else None
        df["ws_minus_h_in"] = df["wingspan_in"] - df["height_in"]

        # Compute NET where missing
        if "actual_money" in df.columns and "projected_money" in df.columns:
            df["net"] = df["net"].where(df["net"].notna(), df["actual_money"] - df["projected_money"])

        # ensure both pick raw columns exist, even if missing in the sheet
        for col in ["projected_pick_raw", "actual_pick_raw"]:
            if col not in df.columns:
                df[col] = None

        # derive numeric picks
        df["projected_pick"] = df["projected_pick_raw"].apply(parse_pick_to_int)
        df["actual_pick"] = df["actual_pick_raw"].apply(parse_pick_to_int)

        if "year" in df.columns:
            df["year"] = df["year"].apply(_to_int_or_none)

        rows = df.to_dict(orient="records")
        total_rows += len(rows)

        batch = 0
        for r in rows:
            coach = _s(r.get("coach"))
            player = _s(r.get("player"))
            team = _s(r.get("team"))
            year = _to_int_or_none(r.get("year"))

            if not coach or not player or not team:
                continue

            obj = Prospect.query.filter_by(player=player, team=team, year=year).first()
            created = False
            if obj is None:
                obj = Prospect(player=player, team=team, year=year)
                created = True

            obj.sheet = sheet
            obj.coach = coach
            # --- START PATCH: persist player team ---
            obj.team = (r.get("team") or "").strip() or None
            # --- END PATCH ---
            obj.coach_current_team = _s(r.get("coach_current_team"))
            obj.coach_current_conference = _s(r.get("coach_current_conference"))

            obj.player_class = _s(r.get("Class"))
            obj.age = _to_num(r.get("Age"))
            obj.player_conference = _s(r.get("Player Conference"))

            obj.projected_money = _to_num(r.get("projected_money"))
            obj.actual_money = _to_num(r.get("actual_money"))
            if obj.actual_money is not None and obj.projected_money is not None:
                obj.net = obj.actual_money - obj.projected_money
            else:
                obj.net = _to_num(r.get("net"))

            # --- START PATCH: persist pick fields on Prospect ---
            proj_pick_raw = _s(r.get("projected_pick_raw"))
            act_pick_raw = _s(r.get("actual_pick_raw"))
            obj.projected_pick_raw = proj_pick_raw or None
            obj.actual_pick_raw = act_pick_raw or None
            obj.projected_pick = int(r["projected_pick"]) if pd.notna(r.get("projected_pick")) else None
            obj.actual_pick = int(r["actual_pick"]) if pd.notna(r.get("actual_pick")) else None
            # --- END PATCH ---

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
