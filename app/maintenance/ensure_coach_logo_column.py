from contextlib import contextmanager

def _load_app_and_db():
    try:
        from app import create_app, db
        app = create_app()
    except Exception:
        from app import app, db
    return app, db

@contextmanager
def ctx(app):
    with app.app_context():
        yield

app, db = _load_app_and_db()

with ctx(app):
    from sqlalchemy import inspect, text
    engine = db.engine
    insp = inspect(engine)

    # find coach table
    candidates = []
    try:
        mappers = list(db.Model.registry.mappers)
    except Exception:
        mappers = []
        for cls in getattr(db.Model, "_decl_class_registry", {}).values():
            try:
                if hasattr(cls, "__mapper__"):
                    mappers.append(cls.__mapper__)
            except Exception:
                pass

    for mp in mappers:
        cols = set(getattr(mp, "columns", {}).keys())
        name = mp.local_table.name
        if "coach" in name.lower():
            candidates.append(name)

    if not candidates:
        raise SystemExit("Could not find coach table")

    table = sorted(candidates, key=lambda t: ("coach" in t.lower(), len(t)) , reverse=True)[0]
    existing = {c["name"] for c in insp.get_columns(table)}
    if "team_logo_url" not in existing:
        q = engine.dialect.identifier_preparer.quote
        ddl = f'ALTER TABLE {q(table)} ADD COLUMN {q("team_logo_url")} VARCHAR(255) NULL'
        with engine.begin() as con:
            con.execute(text(ddl))
        print("Added team_logo_url to", table)
    else:
        print("team_logo_url already present on", table)
