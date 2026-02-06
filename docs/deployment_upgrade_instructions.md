# Deployment Database Upgrade Instructions

The following manual steps are required to upgrade the live Flask application's database schema to include the `1f9735f5da8c_add_contest_shot_type_breakdown` migration:

1. **Activate the Flask virtual environment on the deployment host**.
   ```bash
   source /path/to/venv/bin/activate
   ```

2. **Run the database migrations** using Flask-Migrate or Alembic so that the `1f9735f5da8c_add_contest_shot_type_breakdown.py` migration is applied.
   ```bash
   flask db upgrade
   # or, equivalently
   alembic upgrade head
   ```

3. **Recreate the SQLite database file if the deployment relies on a pre-generated database**. Back up the existing file first, then rebuild it.
   ```bash
   cp instance/app.db instance/app.db.bak-$(date +%Y%m%d%H%M%S)
   python recreate_db.py
   ```

4. **Restart the Flask application** so the running process uses the upgraded schema. The exact command depends on the process manager (e.g., `systemctl`, `supervisorctl`, `pm2`, `gunicorn`, etc.).
   ```bash
   # example using systemd
   sudo systemctl restart alabama-basketball.service
   ```

These steps must be executed on the deployment environment because they modify the live database and running Flask service.
