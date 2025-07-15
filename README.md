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
