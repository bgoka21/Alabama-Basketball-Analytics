import click
import pandas as pd
from app import db
from app.models.prospect import Prospect
from app.utils.measurements import parse_feet_inches


def _to_num(x):
    if x is None:
        return None
    s = str(x).replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def _clean_columns(df):
    # Only trim column names. Do NOT auto-merge or rename any columns.
    df.columns = [str(c).strip() for c in df.columns]
    return df


@click.command("import_draft_stock")
@click.argument("xlsx_path", type=click.Path(exists=True))
@click.option("--commit-batch", default=500, show_default=True, help="DB commit every N rows")
@click.option("--strict", is_flag=True, default=True, help="Fail fast if required columns are missing.")
def import_draft_stock(xlsx_path, commit_batch, strict):
    """
    Import the multi-sheet Excel workbook (NBA - Draft Stock Analysis.xlsx).
    Parses height/wingspan, coerces money columns, computes NET, and upserts Prospects.
    NOTE: Does NOT auto-fix ACC 'Player.1'. Ensure the workbook has a proper 'Player' column.
    """
    click.echo(f"📥 Loading workbook: {xlsx_path}")
    xl = pd.ExcelFile(xlsx_path)
    total_rows = 0
    upserts = 0

    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        df = _clean_columns(df)

        # Validate required columns exist as-is
        required = ["Coach", "Player", "Team"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            msg = f"❌ Sheet '{sheet}' missing required columns: {missing}. Please fix the workbook and re-run."
            if strict:
                raise click.ClickException(msg)
            else:
                click.echo(msg)
                continue

        # Warn if 'Player.1' exists (user will fix upstream)
        if "Player.1" in df.columns:
            click.echo(f"⚠️  Sheet '{sheet}' contains 'Player.1'. Not importing from this column. Ensure 'Player' is correct before import.")

        # Coerce numerics
        for c in ["Projected Money", "Actual Money", "NET", "Age", "Year"]:
            if c in df.columns:
                df[c] = df[c].apply(_to_num)

        # Parse measurements
        df["height_in"] = df["Height"].apply(parse_feet_inches) if "Height" in df.columns else None
        df["wingspan_in"] = df["WingSpan"].apply(parse_feet_inches) if "WingSpan" in df.columns else None
        df["ws_minus_h_in"] = df["wingspan_in"] - df["height_in"]

        # Compute NET if missing
        if "Actual Money" in df.columns and "Projected Money" in df.columns:
            df["NET"] = df["NET"].where(df["NET"].notna(), df["Actual Money"] - df["Projected Money"])

        # Normalize Year to int if possible
        if "Year" in df.columns:
            df["Year"] = df["Year"].apply(lambda x: int(x) if x is not None and str(x).strip() != "" else None)

        rows = df.to_dict(orient="records")
        click.echo(f"— {sheet}: {len(rows)} rows")
        total_rows += len(rows)

        batch = 0
        for r in rows:
            coach = (r.get("Coach") or "").strip()
            player = (r.get("Player") or "").strip()
            team = (r.get("Team") or "").strip()
            year = r.get("Year")

            if not coach or not player or not team:
                continue  # skip incomplete keys

            # Upsert by (player, team, year)
            obj = Prospect.query.filter_by(player=player, team=team, year=year).first()
            created = False
            if obj is None:
                obj = Prospect(player=player, team=team, year=year)
                created = True

            obj.sheet = sheet

            obj.coach = coach
            obj.coach_current_team = r.get("Coach Current Team")
            obj.coach_current_conference = r.get("Coach Current Conference")

            obj.player_class = r.get("Class")
            obj.age = _to_num(r.get("Age"))
            obj.player_conference = r.get("Player Conference")

            obj.projected_money = _to_num(r.get("Projected Money"))
            obj.actual_money = _to_num(r.get("Actual Money"))
            if obj.actual_money is not None and obj.projected_money is not None:
                obj.net = obj.actual_money - obj.projected_money
            else:
                obj.net = _to_num(r.get("NET"))

            obj.height_raw = r.get("Height")
            obj.wingspan_raw = r.get("WingSpan")
            obj.height_in = r.get("height_in")
            obj.wingspan_in = r.get("wingspan_in")
            obj.ws_minus_h_in = r.get("ws_minus_h_in")

            obj.home_city = r.get("Home City")
            obj.home_state = r.get("Home State")
            obj.country = r.get("Country")

            if created:
                db.session.add(obj)
            upserts += 1
            batch += 1

            if batch >= commit_batch:
                db.session.commit()
                batch = 0

        if batch > 0:
            db.session.commit()

    click.echo(f"✅ Done. Sheets processed: {len(xl.sheet_names)}  | rows scanned: {total_rows}  | upserts: {upserts}")
