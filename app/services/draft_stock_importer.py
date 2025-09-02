import math
import pandas as pd
from flask import current_app
from app import db
from app.models.prospect import Prospect
from app.utils.measurements import parse_feet_inches
from app.utils.import_utils import (
    normalize_headers,
    strip_cols,
    parse_currency,
    parse_int,
    parse_pick_to_int,
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


def import_workbook(xlsx_path_or_buffer, strict=True, commit_batch=500):
    """Import the multi-sheet Excel workbook into Prospect rows."""
    xl = pd.ExcelFile(xlsx_path_or_buffer)
    total_rows = 0
    inserted_total = 0
    updated_total = 0
    skipped_total = 0
    per_sheet: list[dict] = []

    for sheet in xl.sheet_names:
        try:
            try:
                df = pd.read_excel(xl, sheet_name=sheet, dtype=str)
            except Exception:
                df = pd.read_excel(xl, sheet_name=sheet, dtype=str, engine="openpyxl")

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

            for col in ("projected_pick_raw", "actual_pick_raw"):
                if col not in df.columns:
                    df[col] = None
            df["projected_pick"] = df["projected_pick_raw"].apply(parse_pick_to_int)
            df["actual_pick"] = df["actual_pick_raw"].apply(parse_pick_to_int)

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
                coach_raw = (row.get("coach") or "").strip()
                _, coach = normalize_coach_name(coach_raw)
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
                proj_pick = row.get("projected_pick")
                act_pick = row.get("actual_pick")
                coach_team = (row.get("coach_current_team") or "").strip() or None
                coach_conf = (row.get("coach_current_conference") or "").strip() or None
                sheet_tag = (row.get("sheet") or sheet).strip()

                if existing:
                    existing.projected_money = proj_money if proj_money is not None else existing.projected_money
                    existing.actual_money = act_money if act_money is not None else existing.actual_money
                    existing.net = net_val if net_val is not None else existing.net

                    if proj_pick_raw:
                        existing.projected_pick_raw = proj_pick_raw
                    if act_pick_raw:
                        existing.actual_pick_raw = act_pick_raw
                    if proj_pick is not None:
                        existing.projected_pick = int(proj_pick)
                    if act_pick is not None:
                        existing.actual_pick = int(act_pick)

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
                        projected_pick=int(proj_pick) if proj_pick is not None else None,
                        actual_pick=int(act_pick) if act_pick is not None else None,
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

    current_app.logger.info(
        f"[MoneyBoard Import] Complete. inserted={inserted_total} updated={updated_total} skipped={skipped_total}"
    )

    return {
        "sheets": len(xl.sheet_names),
        "rows": total_rows,
        "upserts": inserted_total + updated_total,
        "per_sheet": per_sheet,
    }
