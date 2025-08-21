import os
import csv
import hashlib
import logging
from typing import Optional, List, Dict

import pandas as pd
import click
from flask import current_app
from flask.cli import with_appcontext

from models.database import db
from models.recruit import Recruit
from models.eybl import ExternalIdentityMap, UnifiedStats

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# CSV Readers
# ---------------------------------------------------------------------------

def _read_csv_robust(path: str) -> pd.DataFrame:
    """Read CSV handling UTF-8/Latin-1 encodings and leading ``sep=`` rows."""
    encodings = ["utf-8", "latin-1"]
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc) as f:
                first_line = f.readline()
            skip = 1 if first_line.lower().startswith("sep=") else 0
            df = pd.read_csv(path, encoding=enc, skiprows=skip)
            df.columns = [c.strip() for c in df.columns]
            return df
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8", b"", 0, 1, "Unable to decode CSV")


def read_overall_csv(path: str) -> pd.DataFrame:
    """Read the Synergy Overall CSV."""
    logger.info("Reading overall CSV %s", path)
    return _read_csv_robust(path)


def read_assists_csv(path: str) -> pd.DataFrame:
    logger.info("Reading assists CSV %s", path)
    return _read_csv_robust(path)


def read_fg_attempts_csv(path: str) -> pd.DataFrame:
    logger.info("Reading FG attempts CSV %s", path)
    return _read_csv_robust(path)

# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------


def to_float(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, str):
        val = val.replace(",", "").strip()
        if val == "":
            return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def pct_to_decimal(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, str):
        val = val.strip().replace("%", "")
    f = to_float(val)
    if f is None:
        return None
    if f > 1:
        f = f / 100.0
    return f


def clean_name(s: Optional[str]) -> str:
    if not s:
        return ""
    return " ".join(str(s).lower().split())


def clean_team(s: Optional[str]) -> str:
    if not s:
        return ""
    return " ".join(str(s).lower().split())


def deterministic_external_key(player: str, team: str, season_year: Optional[int], circuit: str) -> str:
    base = f"{player}|{team}|{season_year or ''}|{circuit}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

# ---------------------------------------------------------------------------
# Normalize & merge
# ---------------------------------------------------------------------------

def normalize_and_merge(overall_df: pd.DataFrame, assists_df: pd.DataFrame, fg_att_df: pd.DataFrame,
                        *, circuit: str, season_year: Optional[int], season_type: str = "AAU") -> pd.DataFrame:
    overall_df = overall_df.copy() if overall_df is not None else pd.DataFrame()
    assists_df = assists_df.copy() if assists_df is not None else pd.DataFrame()
    fg_att_df = fg_att_df.copy() if fg_att_df is not None else pd.DataFrame()

    for df in (overall_df, assists_df, fg_att_df):
        if not df.empty:
            df.columns = [c.strip() for c in df.columns]

    merged = pd.merge(overall_df, assists_df, on=["Player", "Team"], how="outer", suffixes=("", "_ast"))
    if not fg_att_df.empty:
        merged = pd.merge(merged, fg_att_df, on=["Player", "Team"], how="left", suffixes=("", "_fg"))

    records = []
    for _, row in merged.iterrows():
        player = row.get("Player")
        team = row.get("Team")
        gp = to_float(row.get("GP"))
        pts = to_float(row.get("Pts"))
        ppg = to_float(row.get("PPG"))
        if ppg is None and pts is not None and gp and gp > 0:
            ppg = pts / gp
        ast = to_float(row.get("Ast"))
        if ast is None:
            ast = to_float(row.get("AST/G"))
        ast_to = to_float(row.get("Ast/TO"))
        if ast_to is None:
            ast_to = to_float(row.get("AST/TO"))
        tov = ast / ast_to if ast and ast_to and ast_to > 0 else None
        fg_pct = pct_to_decimal(row.get("FG%"))
        ppp_main = to_float(row.get("PP(P+A)"))
        ppp_overall = to_float(row.get("PPP"))
        poss = to_float(row.get("Poss"))
        ppp = ppp_main if ppp_main is not None else (
            ppp_overall if ppp_overall is not None else (
                pts / poss if pts is not None and poss and poss > 0 else None
            )
        )
        rec = {
            "player": player,
            "team": team,
            "gp": gp,
            "ppg": ppg,
            "ast": ast,
            "tov": tov,
            "fg_pct": fg_pct,
            "ppp": ppp,
            "circuit": circuit,
            "season_year": season_year,
            "season_type": season_type,
            "raw_pts": pts,
            "raw_poss": poss,
            "raw_ppp": ppp_overall,
            "raw_pppa": ppp_main,
        }
        records.append(rec)
    df = pd.DataFrame(records)
    return df

# ---------------------------------------------------------------------------
# Auto-matching to recruits
# ---------------------------------------------------------------------------

def auto_match_to_recruits(df: pd.DataFrame) -> List[Dict]:
    recruits = db.session.query(Recruit.id, Recruit.name, Recruit.aau_team).all()
    name_exact = {r.name: r for r in recruits}
    name_team_exact = {(r.name, r.aau_team or ""): r for r in recruits}
    name_norm = {clean_name(r.name): r for r in recruits}
    name_team_norm = {(clean_name(r.name), clean_team(r.aau_team)): r for r in recruits}

    results: List[Dict] = []
    for _, row in df.iterrows():
        player = row["player"] or ""
        team = row["team"] or ""
        ext_key = deterministic_external_key(player, team, row["season_year"], row["circuit"])
        recruit_id = None
        confidence = 0.0
        is_verified = False

        r = name_team_exact.get((player, team))
        if r:
            recruit_id = r.id
            confidence = 1.0
            is_verified = True
        else:
            r = name_exact.get(player)
            if r:
                recruit_id = r.id
                confidence = 0.9
                is_verified = True
            else:
                r = name_team_norm.get((clean_name(player), clean_team(team)))
                if r:
                    recruit_id = r.id
                    confidence = 0.9
                    is_verified = True
                else:
                    r = name_norm.get(clean_name(player))
                    if r:
                        recruit_id = r.id
                        confidence = 0.8

        data = {
            "external_key": ext_key,
            "player_name_external": player,
            "team_external": team,
            "circuit": row["circuit"],
            "season_year": row["season_year"],
            "season_type": row["season_type"],
            "recruit_id": recruit_id,
            "match_confidence": confidence,
            "is_verified": confidence >= 0.9,
        }
        results.append(data)

        existing = ExternalIdentityMap.query.filter_by(external_key=ext_key).one_or_none()
        if existing:
            if confidence > (existing.match_confidence or 0):
                existing.recruit_id = recruit_id
                existing.match_confidence = confidence
                existing.is_verified = confidence >= 0.9
                existing.player_name_external = player
                existing.team_external = team
                existing.circuit = row["circuit"]
                existing.season_year = row["season_year"]
                existing.season_type = row["season_type"]
        else:
            entry = ExternalIdentityMap(**data)
            db.session.add(entry)
    db.session.flush()
    return results

# ---------------------------------------------------------------------------
# Promote verified stats
# ---------------------------------------------------------------------------

def promote_verified_stats(merged_df: pd.DataFrame, *, circuit: str, season_year: Optional[int],
                           season_type: str, original_filenames: List[str]) -> Dict:
    keys = [deterministic_external_key(r.player, r.team, season_year, circuit) for r in merged_df.itertuples()]
    verified_maps = ExternalIdentityMap.query.filter(
        ExternalIdentityMap.external_key.in_(keys),
        ExternalIdentityMap.is_verified.is_(True)
    ).all()
    mapping = {m.external_key: m for m in verified_maps}

    inserted = updated = skipped = 0
    anomalies: List[str] = []

    for row in merged_df.itertuples():
        key = deterministic_external_key(row.player, row.team, season_year, circuit)
        mapping_row = mapping.get(key)
        if not mapping_row or not mapping_row.recruit_id:
            skipped += 1
            continue
        recruit_id = mapping_row.recruit_id
        unique_filter = dict(
            recruit_id=recruit_id,
            circuit=circuit,
            season_year=season_year,
            season_type=season_type,
            team_name=row.team,
        )
        existing = UnifiedStats.query.filter_by(**unique_filter).one_or_none()
        values = dict(
            gp=row.gp,
            ppg=row.ppg,
            ast=row.ast,
            tov=row.tov,
            fg_pct=row.fg_pct,
            ppp=row.ppp,
            source_system="synergy_portal_csv",
            original_filenames=",".join(original_filenames),
        )
        if existing:
            for k, v in values.items():
                setattr(existing, k, v)
            existing.ingested_at = db.func.now()
            updated += 1
        else:
            stats = UnifiedStats(**unique_filter, **values)
            db.session.add(stats)
            inserted += 1
    db.session.flush()
    return {"inserted": inserted, "updated": updated, "skipped": skipped, "anomalies": anomalies}

# ---------------------------------------------------------------------------
# CLI Command
# ---------------------------------------------------------------------------

@click.command("eybl_import")
@click.option("--circuit", required=True)
@click.option("--season-year", type=int)
@click.option("--season-type", default="AAU")
@click.option("--overall", type=click.Path(exists=True), required=True)
@click.option("--assists", type=click.Path(exists=True), required=True)
@click.option("--fgatt", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, default=False)
@with_appcontext
def eybl_import_command(circuit, season_year, season_type, overall, assists, fgatt, dry_run):
    """Import EYBL/AAU stats from Synergy CSV exports."""
    overall_df = read_overall_csv(overall)
    assists_df = read_assists_csv(assists)
    fg_df = read_fg_attempts_csv(fgatt) if fgatt else pd.DataFrame()

    merged_df = normalize_and_merge(overall_df, assists_df, fg_df,
                                    circuit=circuit, season_year=season_year, season_type=season_type)
    matches = auto_match_to_recruits(merged_df)
    db.session.commit()

    total_rows = len(merged_df)
    counts = {
        'ppg': merged_df['ppg'].notna().sum(),
        'ast': merged_df['ast'].notna().sum(),
        'tov': merged_df['tov'].notna().sum(),
        'fg_pct': merged_df['fg_pct'].notna().sum(),
        'ppp': merged_df['ppp'].notna().sum(),
    }
    verified = sum(1 for m in matches if m['is_verified'])
    unmatched = sum(1 for m in matches if m['recruit_id'] is None)

    preview_cols = ['player', 'team', 'gp', 'ppg', 'ast', 'tov', 'fg_pct', 'ppp']
    df_preview = merged_df[preview_cols]

    anomalies = []
    for r in merged_df.itertuples():
        if r.fg_pct is not None and not (0 <= r.fg_pct <= 1):
            anomalies.append(f"FG% out of range for {r.player}")
        if r.ppp is not None and not (0.6 <= r.ppp <= 1.5):
            anomalies.append(f"PPP out of range for {r.player}")
        if r.gp is not None and r.gp < 1:
            anomalies.append(f"GP < 1 for {r.player}")
        if r.raw_poss is not None and r.raw_poss <= 0 and r.raw_pppa is None and r.raw_ppp is None:
            anomalies.append(f"Poss <=0 for {r.player}")

    if dry_run:
        preview_dir = current_app.config['INGEST_PREVIEWS_DIR']
        os.makedirs(preview_dir, exist_ok=True)
        preview_path = os.path.join(preview_dir, f"eybl_{circuit}_{season_year}.csv")
        df_preview.to_csv(preview_path, index=False)
        click.echo(f"Parsed {total_rows} rows. Field counts: {counts}")
        click.echo(f"Auto-verified: {verified}, Unmatched: {unmatched}")
        if anomalies:
            click.echo("Anomalies (sample):")
            for a in anomalies[:5]:
                click.echo(f" - {a}")
        click.echo(f"Preview written to {preview_path}")
    else:
        summary = promote_verified_stats(merged_df, circuit=circuit, season_year=season_year,
                                         season_type=season_type, original_filenames=[overall, assists] + ([fgatt] if fgatt else []))
        db.session.commit()
        snapshot_dir = current_app.config['INGEST_SNAPSHOTS_DIR']
        os.makedirs(snapshot_dir, exist_ok=True)
        snapshot_path = os.path.join(snapshot_dir, f"eybl_{circuit}_{season_year}.csv")
        df_preview.to_csv(snapshot_path, index=False)
        click.echo(f"Summary: {summary}")
        click.echo(f"Snapshot written to {snapshot_path}")
