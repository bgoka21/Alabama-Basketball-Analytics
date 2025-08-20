# scripts/one_off_add_recruit_cols.py
from app import create_app, db
from sqlalchemy import text

app = create_app()
ddl_cmds = [
    ("aau_team", "ALTER TABLE recruit ADD COLUMN aau_team TEXT"),
    ("ppg", "ALTER TABLE recruit ADD COLUMN ppg REAL"),
]

def column_exists(engine, table, column):
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return any(r[1] == column for r in rows)  # r[1] = name

with app.app_context():
    for col, ddl in ddl_cmds:
        try:
            if column_exists(db.engine, "recruit", col):
                print(f"Skipped: {col} already exists")
            else:
                db.session.execute(text(ddl))
                db.session.commit()
                print(f"Applied: {ddl}")
        except Exception as e:
            print(f"Skipped (error): {ddl} -> {e}")
    print("✅ Finished")
