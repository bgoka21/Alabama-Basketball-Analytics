from sqlalchemy import func, Integer, Float
from yourapp.models import PracticeStats


def get_practice_team_totals(session, raw=False):
    """Return aggregated totals or raw PracticeStats records."""
    if raw:
        return session.query(PracticeStats).filter(PracticeStats.practice_id != None).all()

    totals = {}
    numeric_cols = [
        col for col in PracticeStats.__table__.columns
        if isinstance(col.type, (Integer, Float))
    ]
    for col in numeric_cols:
        totals[col.name] = session.query(
            func.coalesce(func.sum(col), 0)
        ).scalar()
    return totals
