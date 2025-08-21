from typing import List, Optional, Dict
from sqlalchemy import desc
from models.eybl import UnifiedStats


def _row_to_dict(row: UnifiedStats) -> Dict:
    return {
        "circuit": row.circuit,
        "season_year": row.season_year,
        "season_type": row.season_type,
        "team_name": row.team_name,
        "gp": row.gp,
        "ppg": row.ppg,
        "ast": row.ast,
        "tov": row.tov,
        "ppp": row.ppp,
        "pnr_poss": row.pnr_poss,
        "pnr_ppp": row.pnr_ppp,
        "pnr_to_pct": row.pnr_to_pct,
        "pnr_score_pct": row.pnr_score_pct,
        "ingested_at": row.ingested_at,
    }


def get_circuit_stats_for_recruit(recruit_id: int, *, circuits: Optional[List[str]] = None,
                                   season_year: Optional[int] = None) -> List[Dict]:
    """Fetch unified circuit stats for a given recruit.

    Filters may be applied for specific circuits or season year. Results are ordered
    with newest seasons first (season_year desc, ingested_at desc).
    """
    query = UnifiedStats.query.filter_by(recruit_id=recruit_id)
    if circuits:
        query = query.filter(UnifiedStats.circuit.in_(circuits))
    if season_year:
        query = query.filter_by(season_year=season_year)
    query = query.order_by(desc(UnifiedStats.season_year), desc(UnifiedStats.ingested_at))
    rows = query.all()
    return [_row_to_dict(r) for r in rows]


def get_latest_circuit_stat(recruit_id: int, circuit: str) -> Optional[Dict]:
    """Return the most recent stat row for the given circuit."""
    stats = get_circuit_stats_for_recruit(recruit_id, circuits=[circuit])
    return stats[0] if stats else None
