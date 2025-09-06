import math
import pandas as pd
from flask import current_app
from app import db
from app.models.prospect import Prospect
from app.models.coach import Coach
from app.utils.measurements import parse_feet_inches
from app.utils.import_utils import (
    normalize_headers,
    strip_cols,
    parse_currency,
    parse_int,
    validate_required,
)
from app.utils.coach_names import normalize_coach_name


def _s(x):
    """Safe string: return '' for None/NaN, else stripped string."""
    if x is None:
        return ""
    if isinstance(x, float) and math.isnan(x):
        return ""
    return str(x).strip()


def _to_num(x):
    if x is None: return None
    s = str(x).replace("$","" ).replace(",","" ).strip()
    try: return float(s)
    except: return None


UNDRAFTED_TOKENS = {'undrafted'}
NA_TOKENS = {'n/a', 'na'}


def parse_pick_field(raw):
    """
    Returns (num, text):
      - If raw is numeric (e.g., 23 or "23" or 23.0) -> (23, None)
      - If raw is "Undrafted" (any case) -> (None, "Undrafted")
      - If raw is "N/A" (any case) -> (None, "N/A")
      - If raw is blank/NaN -> (None, None)
      - If raw is other non-numeric text -> (None, str(raw).strip())
    """
    if raw is None:
        return (None, None)
    try:
        if isinstance(raw, float) and math.isnan(raw):
            return (None, None)
    except Exception:
        pass

    s = str(raw).strip()
    if not s:
        return (None, None)

    low = s.lower()
    if low in UNDRAFTED_TOKENS:
        return (None, "Undrafted")
    if low in NA_TOKENS or low == 'n\\a':
        return (None, "N/A")

    try:
        n = float(s)
        if math.isnan(n):
            return (None, None)
        return (int(n), None)
    except Exception:
        return (None, s)


def import_workbook(xlsx_path_or_buffer, strict=True, replace=False, commit_batch=500):
    """Import the multi-sheet Excel workbook into Prospect rows.

    Args:
        xlsx_path_or_buffer: Path or buffer to the workbook.
        strict: Whether to error on missing required columns.
        replace: When True, existing ``Prospect`` rows for a sheet (and
            year if provided) are deleted before importing new data.
        commit_batch: Unused; maintained for compatibility.
    """
    xl = pd.ExcelFile(xlsx_path_or_buffer)
    total_rows = 0
    inserted_total = 0
    updated_total = 0
    skipped_total = 0
    per_sheet: list[dict] = []
    coach_records: dict[str, dict[str, str | None]] = {}

    # --- First, handle the coaches sheet (case/whitespace-insensitive) ---
    coaches_sheet = next(
        (s for s in xl.sheet_names if s.strip().lower() == "coaches"),
        None,
    )
    if coaches_sheet is not None:
        try:
            try:
                df = pd.read_excel(xl, sheet_name=coaches_sheet, dtype=str)
            except Exception:
                df = pd.read_excel(
                    xl, sheet_name=coaches_sheet, dtype=str, engine="openpyxl"
                )
            df = normalize_headers(df)
            strip_cols(df, ["coach", "current_team", "current_conference"])
            rows = df.to_dict(orient="records")
            total_rows += len(rows)
            for row in rows:
                coach_raw = (row.get("coach") or "").strip()
                if not coach_raw:
                    continue
                _, coach = normalize_coach_name(coach_raw)
                team = (row.get("current_team") or "").strip() or None
                conf = (row.get("current_conference") or "").strip() or None
                info = coach_records.get(coach, {})
                if team:
                    info["current_team"] = team
                if conf:
                    info["current_conference"] = conf
                coach_records[coach] = info
            per_sheet.append(
                {
                    "sheet": coaches_sheet.strip(),
                    "rows": len(rows),
                    "inserted": 0,
                    "updated": 0,
                    "skipped": 0,
                    "status": "ok",
                }
            )

            # Upsert coaches before processing other sheets
            for name, info in coach_records.items():
                existing = Coach.query.filter_by(name=name).first()
                if existing:
                    team = info.get("current_team")
                    conf = info.get("current_conference")
                    if team:
                        existing.current_team = team
                    if conf:
                        existing.current_conference = conf
                else:
                    db.session.add(
                        Coach(
                            name=name,
                            current_team=info.get("current_team"),
                            current_conference=info.get("current_conference"),
                        )
                    )
            db.session.commit()
            # reset to capture any new/updated info from other sheets separately
            coach_records = {}
        except Exception as e:
            current_app.logger.exception(
                f"[MoneyBoard Import] Sheet '{coaches_sheet}' failed: {e}"
            )
            per_sheet.append(
                {
                    "sheet": coaches_sheet.strip(),
                    "status": "failed",
                    "reason": str(e),
                }
            )

    # --- Process all remaining sheets ---
    for sheet_name in xl.sheet_names:
        if sheet_name == coaches_sheet:
            continue
        sheet = sheet_name.strip()
        try:
            try:
                df = pd.read_excel(xl, sheet_name=sheet_name, dtype=str)
            except Exception:
                df = pd.read_excel(xl, sheet_name=sheet_name, dtype=str, engine="openpyxl")

            df = normalize_headers(df)

            ok, missing = validate_required(df)
            if not ok:
                msg = f"Sheet '{sheet}' missing required columns: {missing}"
                current_app.logger.error(f"[MoneyBoard Import] {msg}")
                if strict:
                    raise ValueError(msg)

            strip_cols(df, ["coach", "player", "team", "coach_current_team", "coach_current_conference"])

            for col in ("projected_money", "actual_money", "net"):
                if col in df.columns:
                    df[col] = df[col].apply(parse_currency)

            if "year" in df.columns:
                df["year"] = df["year"].apply(parse_int)

            if replace:
                years = []
                if "year" in df.columns:
                    years = [int(y) for y in df["year"].dropna().unique().tolist()]
                q = Prospect.query.filter(Prospect.sheet == sheet)
                if years:
                    q = q.filter(Prospect.year.in_(years))
                q.delete(synchronize_session=False)
                db.session.commit()

            for col in ("projected_pick_raw", "actual_pick_raw"):
                if col not in df.columns:
                    df[col] = None

            if "net" in df.columns:
                df["net"] = df["net"].where(df["net"].notna(), None)
            else:
                df["net"] = None

            if "projected_money" in df.columns and "actual_money" in df.columns:
                df["net"] = df.apply(
                    lambda r: (r["actual_money"] - r["projected_money"])
                    if r.get("actual_money") is not None and r.get("projected_money") is not None
                    else r.get("net"),
                    axis=1,
                )

            df["height_in"] = df["Height"].apply(parse_feet_inches) if "Height" in df.columns else None
            df["wingspan_in"] = df["WingSpan"].apply(parse_feet_inches) if "WingSpan" in df.columns else None
            df["ws_minus_h_in"] = df["wingspan_in"] - df["height_in"]

            rows = df.to_dict(orient="records")
            total_rows += len(rows)

            inserted = 0
            updated = 0
            skipped = 0

            for idx, row in enumerate(rows):
                try:
                    coach_raw = (row.get("coach") or "").strip()
                    _, coach = normalize_coach_name(coach_raw)
                    coach_team = (row.get("coach_current_team") or "").strip() or None
                    coach_conf = (row.get("coach_current_conference") or "").strip() or None
                    if coach:
                        info = coach_records.get(coach, {})
                        if coach_team:
                            info["current_team"] = coach_team
                        if coach_conf:
                            info["current_conference"] = coach_conf
                        coach_records[coach] = info
                    if coach_raw and coach_raw != coach:
                        current_app.logger.info(
                            f"[MoneyBoard Import] normalized coach '{coach_raw}' -> '{coach}'"
                        )
                    player = (row.get("player") or "").strip()
                    team = (row.get("team") or "").strip()
                    year = row.get("year")
                    if not coach or not player or not team or year is None or (isinstance(year, float) and pd.isna(year)):
                        skipped += 1
                        current_app.logger.warning(
                            f"[MoneyBoard Import] Skipping row {idx}: missing identity fields (coach='{coach}', player='{player}', team='{team}', year='{year}')"
                        )
                        continue
                    existing = (
                        Prospect.query.filter_by(
                            coach=coach, player=player, team=team, year=int(year)
                        ).first()
                    )
                    proj_money = row.get("projected_money")
                    act_money = row.get("actual_money")
                    net_val = row.get("net")
                    proj_pick_raw = row.get("projected_pick_raw")
                    act_pick_raw = row.get("actual_pick_raw")
                    proj_num, proj_text = parse_pick_field(proj_pick_raw)
                    act_num, act_text = parse_pick_field(act_pick_raw)
                    sheet_tag = (row.get("sheet") or sheet).strip()

                    if existing:
                        existing.projected_money = proj_money if proj_money is not None else existing.projected_money
                        existing.actual_money = act_money if act_money is not None else existing.actual_money
                        existing.net = net_val if net_val is not None else existing.net

                        if proj_pick_raw is not None:
                            existing.projected_pick_raw = proj_pick_raw
                        if act_pick_raw is not None:
                            existing.actual_pick_raw = act_pick_raw
                        if proj_num is not None:
                            existing.projected_pick = proj_num
                        if act_num is not None:
                            existing.actual_pick = act_num
                        if proj_text is not None:
                            existing.projected_pick_text = proj_text
                        if act_text is not None:
                            existing.actual_pick_text = act_text

                        if coach_team:
                            existing.coach_current_team = coach_team
                        if coach_conf:
                            existing.coach_current_conference = coach_conf
                        existing.sheet = sheet_tag

                        existing.player_class = _s(row.get("Class")) or existing.player_class
                        age_val = _to_num(row.get("Age"))
                        existing.age = age_val if age_val is not None else existing.age
                        existing.player_conference = _s(row.get("Player Conference")) or existing.player_conference
                        existing.height_raw = _s(row.get("Height")) or existing.height_raw
                        existing.wingspan_raw = _s(row.get("WingSpan")) or existing.wingspan_raw
                        existing.height_in = row.get("height_in") if row.get("height_in") is not None else existing.height_in
                        existing.wingspan_in = row.get("wingspan_in") if row.get("wingspan_in") is not None else existing.wingspan_in
                        existing.ws_minus_h_in = row.get("ws_minus_h_in") if row.get("ws_minus_h_in") is not None else existing.ws_minus_h_in
                        existing.home_city = _s(row.get("Home City")) or existing.home_city
                        existing.home_state = _s(row.get("Home State")) or existing.home_state
                        existing.country = _s(row.get("Country")) or existing.country

                        updated += 1
                    else:
                        p = Prospect(
                            coach=coach,
                            player=player,
                            team=team,
                            year=int(year),
                            projected_money=proj_money,
                            actual_money=act_money,
                            net=net_val,
                            projected_pick_raw=proj_pick_raw,
                            actual_pick_raw=act_pick_raw,
                            projected_pick=proj_num,
                            actual_pick=act_num,
                            projected_pick_text=proj_text,
                            actual_pick_text=act_text,
                            coach_current_team=coach_team,
                            coach_current_conference=coach_conf,
                            sheet=sheet_tag,
                            player_class=_s(row.get("Class")) or None,
                            age=_to_num(row.get("Age")),
                            player_conference=_s(row.get("Player Conference")) or None,
                            height_raw=_s(row.get("Height")) or None,
                            wingspan_raw=_s(row.get("WingSpan")) or None,
                            height_in=row.get("height_in"),
                            wingspan_in=row.get("wingspan_in"),
                            ws_minus_h_in=row.get("ws_minus_h_in"),
                            home_city=_s(row.get("Home City")) or None,
                            home_state=_s(row.get("Home State")) or None,
                            country=_s(row.get("Country")) or None,
                        )
                        db.session.add(p)
                        inserted += 1
                except Exception as e:
                    skipped += 1
                    current_app.logger.exception(
                        f"[MoneyBoard Import] Skipping row {idx} in sheet '{sheet}': {e}"
                    )
                    continue

            db.session.commit()
            inserted_total += inserted
            updated_total += updated
            skipped_total += skipped
            per_sheet.append({
                "sheet": sheet,
                "rows": len(rows),
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
                "status": "ok",
            })

        except Exception as e:
            current_app.logger.exception(f"[MoneyBoard Import] Sheet '{sheet}' failed: {e}")
            per_sheet.append({"sheet": sheet, "status": "failed", "reason": str(e)})
            continue

    for name, info in coach_records.items():
        existing = Coach.query.filter_by(name=name).first()
        if existing:
            team = info.get("current_team")
            conf = info.get("current_conference")
            if team:
                existing.current_team = team
            if conf:
                existing.current_conference = conf
        else:
            db.session.add(
                Coach(
                    name=name,
                    current_team=info.get("current_team"),
                    current_conference=info.get("current_conference"),
                )
            )
    db.session.commit()

    current_app.logger.info(
        f"[MoneyBoard Import] Complete. inserted={inserted_total} updated={updated_total} skipped={skipped_total}"
    )

    return {
        "sheets": len(xl.sheet_names),
        "rows": total_rows,
        "upserts": inserted_total + updated_total,
        "per_sheet": per_sheet,
    }
