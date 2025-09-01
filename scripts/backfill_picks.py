# --- START FILE: scripts/backfill_picks.py ---
import re
from flask import current_app
from app import create_app, db
from app.models import Prospect  # adjust import to your models path


def parse_pick_to_int(raw: str) -> int | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() in {"udfa", "ufa", "n/a", "na", "-", "undrafted"}:
        return None
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None


def main():
    app = create_app()
    with app.app_context():
        q = Prospect.query.all()
        updated = 0
        for p in q:
            dirty = False
            if p.projected_pick is None and p.projected_pick_raw:
                val = parse_pick_to_int(p.projected_pick_raw)
                if val is not None:
                    p.projected_pick = val
                    dirty = True
            if p.actual_pick is None and p.actual_pick_raw:
                val = parse_pick_to_int(p.actual_pick_raw)
                if val is not None:
                    p.actual_pick = val
                    dirty = True
            if dirty:
                updated += 1
        if updated:
            db.session.commit()
        print(f"✅ Backfill complete. Updated rows: {updated}")


if __name__ == "__main__":
    main()
# --- END FILE ---

