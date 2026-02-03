# bball-analytics

This project analyzes basketball statistics and practice data for the Alabama team. The **Skill Development** tab relies solely on the `SkillEntry` model to track drills and NBA 100 entries. Earlier changes introducing an `Nba100Entry` model were reverted. Make sure any new code continues using `SkillEntry` for all skill-development features.


### Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

### Running the Application

Start the server with Python so all routes load correctly:

```bash
python app.py
```

### Running Tests

After installing dependencies, run tests:

```bash
pytest -q
```

### Summer League Stats

After logging in, visit `/summer_stats` to view box scores for Alabama alumni from yesterday's NBA Summer League games.

### Creating an Admin Account

Administrative features require a user with admin privileges. Update the
`create_user.py` script with your desired username and password and run it:

```bash
python create_user.py
```

The script creates an account marked as an administrator. When logged in with an
admin account, the sidebar will include a **Draft Upload** link which is hidden
for regular users.

### Development Plan View

Visit `/admin/player/<player name>/development` to view a player's current season development plan alongside season stats.

### Player Headshots

Administrators can upload player headshots from the roster management page. The
app accepts `.png` and `.jpg` images and stores them in `static/headshots/`.

### PDF Shot Report Generation

The app generates downloadable PDF shot reports for individual players and for the full team:

- **Player PDF**: triggered from the player view by calling `/pdf/player/<player_id>/generate`.
- **Team PDF**: triggered from the team stats page by calling `/pdf/team/generate`, which merges all player reports.

PDF content is built in `app/utils/pdf_generator.py` and uses data assembled by
`app/utils/pdf_data_compiler.py`. To add new breakdown categories or court zones,
update `utils/shottype.gather_labels_for_shot` (for new labels) and
`utils/shot_location_map.normalize_shot_location` (for zone definitions). Color
thresholds for green/red highlights live in the PDF generator where shot chart
and breakdown tables are styled. Page layout sequencing is controlled by the
`ShotTypeReportGenerator.generate` story order, so new pages can be inserted
there as needed.
