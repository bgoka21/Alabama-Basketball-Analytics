# Alabama Basketball Analytics: Codebase Orientation

## High-Level Overview
- **Purpose** – Internal web app that ingests Alabama basketball game/practice data, serves analytics dashboards, and tracks player development for staff and players.【F:README.md†L1-L54】
- **Framework** – Flask application factory living in `app/__init__.py` wires together SQLAlchemy, Flask-Login, APScheduler, and optional auth blueprint while ensuring database tables and ingest directories exist.【F:app/__init__.py†L94-L310】
- **Data layer** – SQLAlchemy models in `models/database.py` represent seasons, games, practices, stat lines, skill entries, and supporting tables used throughout the app.【F:models/database.py†L6-L200】

## Application Structure
| Area | What Lives There | Highlights |
| --- | --- | --- |
| `app/` | Core Flask package (factory, blueprints, CLI, utilities). | `create_app()` sets configuration, registers blueprints (`public`, `admin`, `recruits`, `merge`), and attaches before-request hooks for access control and analytics logging.【F:app/__init__.py†L244-L325】 |
| `admin/` | Admin blueprint with the bulk of data management views, imports, and leaderboard math. | Routes orchestrate CSV ingestion, practice parsing, roster tools, and leaderboard computations across many helpers.【F:admin/routes.py†L34-L200】 |
| `public/` | Auth-protected dashboards for staff/players. | Provides cover page, game leaderboards, practice summaries, and injects “last stats update” metadata for templates.【F:public/routes.py†L49-L180】 |
| `routes.py` | Legacy/global routes mounted after factory. | Handles PDF exports, Synergy API proxies, and practice totals endpoint that reuse admin helpers and enforce login rules.【F:routes.py†L34-L200】 |
| `services/` | Background jobs and external data fetchers. | Utility modules back ingestion flows (e.g., EYBL imports) and background scrapers such as the NBA Summer League collector.【F:services/nba_stats.py†L11-L87】 |
| `templates/` & `static/` | Tailwind-powered frontend and shared components. | `templates/base.html` supplies the sidebar layout, responsive styles, flash messaging, and hooks for PDF export scripts.【F:templates/base.html†L1-L102】 |
| `tests/` | Extensive pytest suite. | Covers leaderboard formatting, permissions, practice parsing, recruits flows, etc., providing living documentation for expected behaviors.【F:tests/test_leaderboard_new_stats.py†L1-L160】 |

## Data & Domain Concepts
- **Season/Session hierarchy** – Seasons group games and practices; sessions provide named date ranges (e.g., summer workouts) used by filters throughout the app.【F:models/database.py†L41-L99】
- **Game & practice stats** – `TeamStats`, `PlayerStats`, and `BlueCollarStats` track granular metrics that feed leaderboards, practice reports, and player detail views.【F:models/database.py†L103-L200】
- **Skill development** – The `SkillEntry` model (also emphasized in the README) records shooting drills, NBA 100, and other workouts; keep using it for skill-dev changes.【F:README.md†L3-L4】【F:models/database.py†L140-L168】

## Request Flow & Permissions
1. **Factory bootstrap** – `create_app()` configures database, upload folders, PDF options, CLI commands, and background scheduler lock to avoid duplicate jobs.【F:app/__init__.py†L33-L360】
2. **Blueprint routing** – Admin routes expose ingestion tools and analytics, while public routes render dashboards; global routes add PDF and Synergy integrations.【F:app/__init__.py†L244-L320】【F:routes.py†L34-L200】
3. **Access control** – Flask-Login manages sessions; before-request handlers prevent player accounts from hitting staff-only endpoints and log page views for analytics.【F:app/__init__.py†L154-L311】
4. **Templates** – Pages inherit `base.html`, get Tailwind styling, and include shared scripts for table sorting and PDF exports.【F:templates/base.html†L1-L102】

## Tooling & Automation
- **CLI commands** – App factory registers EYBL ingest and draft stock import commands for data maintenance tasks.【F:app/__init__.py†L323-L362】
- **Background scheduler** – `init_scheduler()` uses file locking so APScheduler starts once per worker process, enabling cron-like jobs (e.g., refreshing leaderboards).【F:app/__init__.py†L33-L60】
- **Testing** – Run `pytest -q`; tests span practice parsers, roster tools, skill workflows, and more—reading them is the fastest way to learn business rules.【F:README.md†L22-L28】【F:tests/test_leaderboard_new_stats.py†L1-L160】

## Suggested Next Steps for New Contributors
1. **Run the app locally** – Follow README setup (`pip install -r requirements.txt`, `python app.py`) and create an admin user via `create_user.py` to explore the UI.【F:README.md†L6-L45】
2. **Explore the database models** – Map relationships (Season → Game/Practice, SkillEntry ↔ Roster) to understand how stats link together.【F:models/database.py†L41-L200】
3. **Trace a leaderboard** – Start at a public leaderboard view, follow into admin helper functions, and inspect how data flows from SQLAlchemy to Jinja tables in real time.【F:public/routes.py†L141-L180】【F:admin/routes.py†L44-L200】
4. **Review ingestion scripts** – Check admin routes for CSV parsing hooks and `services/eybl_ingest.py` to learn how new data enters the system (paired with tests like `test_practice_parser_new_stats.py`).【F:admin/routes.py†L44-L127】
5. **Use tests as documentation** – Pick a test module relevant to your feature area (e.g., `test_player_detail_label_filter.py`, `test_practice_home_label_filter.py`) to see expected edge cases and payload shapes.【F:tests/test_leaderboard_new_stats.py†L1-L160】

Understanding these pieces will give you the context needed to extend Alabama Basketball Analytics safely and consistently.
