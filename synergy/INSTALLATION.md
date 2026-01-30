# Synergy Integration for Alabama Basketball Analytics

## 🎯 What This Adds

This integration adds **Synergy Sports API** capabilities to your existing Flask app, allowing you to:
- ✅ View Pick & Roll statistics (Ball Handler & Roll Man)
- ✅ Auto-refresh data from Synergy API
- ✅ Cache data in your existing SQLite database
- ✅ Expand to other play types (Isolation, Post-Up, etc.) easily
- ✅ Match your existing Alabama crimson theme

---

## 📁 Files to Add

```
Alabama-Basketball-Analytics/
├── synergy/                          # NEW FOLDER
│   ├── __init__.py                  # Make it a module
│   ├── synergy_routes.py            # Flask routes/endpoints
│   ├── synergy_models.py            # Database models
│   ├── synergy_client.py            # API client
│   └── synergy_auth.py              # Authentication
├── templates/
│   └── synergy/                     # NEW FOLDER
│       ├── dashboard.html           # Synergy dashboard
│       └── pnr_analysis.html        # PnR analysis page
└── .env                             # UPDATE - add Synergy credentials
```

---

## 🚀 Installation Steps

### Step 1: Copy Files to Your Repo

1. Download the `synergy_integration.zip` from this conversation
2. Extract it
3. Copy the `synergy/` folder to your project root
4. Copy the `templates/synergy/` folder to your `templates/` directory

### Step 2: Add Database Models

Open `models/database.py` and add these imports at the top:

```python
# Add to imports
from datetime import datetime
```

Then add these two models to the file (at the bottom, before the last line):

```python
class SynergyCache(db.Model):
    """Tracks when Synergy data was last refreshed"""
    __tablename__ = 'synergy_cache'
    
    id = db.Column(db.Integer, primary_key=True)
    cache_key = db.Column(db.String(100), unique=True, nullable=False, index=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    metadata = db.Column(db.Text)
    
    def __repr__(self):
        return f'<SynergyCache {self.cache_key}>'


class SynergyPnRStats(db.Model):
    """Pick & Roll statistics from Synergy API"""
    __tablename__ = 'synergy_pnr_stats'
    
    id = db.Column(db.Integer, primary_key=True)
    player_id = db.Column(db.String(50), nullable=False, index=True)
    player_name = db.Column(db.String(200), nullable=False)
    season_id = db.Column(db.String(50), nullable=False, index=True)
    team_id = db.Column(db.String(50), nullable=False)
    play_type = db.Column(db.String(50), nullable=False, index=True)
    defensive = db.Column(db.Boolean, default=False, nullable=False)
    possessions = db.Column(db.Integer, default=0)
    points = db.Column(db.Integer, default=0)
    ppp = db.Column(db.Float, default=0.0)
    fg_made = db.Column(db.Integer, default=0)
    fg_attempt = db.Column(db.Integer, default=0)
    fg_percent = db.Column(db.Float, default=0.0)
    turnovers = db.Column(db.Integer, default=0)
    fouls = db.Column(db.Integer, default=0)
    games_played = db.Column(db.Integer, default=0)
    raw_data = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f'<SynergyPnRStats {self.player_name} - {self.play_type}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'player_id': self.player_id,
            'player_name': self.player_name,
            'play_type': self.play_type,
            'possessions': self.possessions,
            'points': self.points,
            'ppp': round(self.ppp, 3),
            'fg_made': self.fg_made,
            'fg_attempt': self.fg_attempt,
            'fg_percent': round(self.fg_percent * 100, 1),
            'turnovers': self.turnovers,
            'games_played': self.games_played
        }
    
    @property
    def to_percent(self):
        if self.possessions == 0:
            return 0
        return round((self.turnovers / self.possessions) * 100, 1)
```

### Step 3: Register the Blueprint

Find your main app file (likely `routes.py` or where you create the Flask app) and add:

```python
# At the top, add import
from synergy.synergy_routes import synergy_bp

# Where you register blueprints (look for app.register_blueprint), add:
app.register_blueprint(synergy_bp)
```

### Step 4: Add Synergy Credentials to .env

Add these lines to your `.env` file:

```bash
# Synergy API Credentials
SYNERGY_CLIENT_ID=client.basketball.alabamambb
SYNERGY_CLIENT_SECRET=0vBg4oX7mqNx
```

### Step 5: Update synergy_client.py Config

Open `synergy/synergy_client.py` and at the top, update the imports:

```python
import os
from dotenv import load_dotenv

load_dotenv()

# Add these config lines after imports
SYNERGY_CLIENT_ID = os.getenv("SYNERGY_CLIENT_ID")
SYNERGY_CLIENT_SECRET = os.getenv("SYNERGY_CLIENT_SECRET")
SYNERGY_SCOPE = "api.basketball.external"
SYNERGY_TOKEN_URL = "https://auth.synergysportstech.com/connect/token"
SYNERGY_BASE_URL = "https://basketball.synergysportstech.com/external"
TEAM_NAME = "Alabama"
LEAGUE_NAME_HINTS = ["College", "Men"]
```

### Step 6: Create Database Tables

Run these commands in your project directory:

```bash
python
>>> from app import app, db
>>> with app.app_context():
...     db.create_all()
>>> exit()
```

Or use Flask-Migrate if you have it:

```bash
flask db migrate -m "Add Synergy tables"
flask db upgrade
```

### Step 7: Add Navigation Link

Add a link to your sidebar/navigation (wherever your "Management" tab is):

```html
<li class="nav-item">
    <a class="nav-link" href="{{ url_for('synergy.dashboard') }}">
        <i class="fas fa-chart-bar"></i> Synergy Stats
    </a>
</li>
```

---

## 🎨 Customizing to Match Your Design

The templates use these Alabama-themed classes that you may need to adjust:

### Colors Used:
- **Crimson**: `#9E1B32` (Alabama's crimson)
- **Classes**: `.bg-crimson`, `.text-crimson`, `.btn-outline-crimson`

### CSS Framework:
The templates assume **Bootstrap 4/5**. If you're using something different, you may need to update:
- Card classes (`.card`, `.card-header`, `.card-body`)
- Button classes (`.btn`, `.btn-sm`)
- Table classes (`.table`, `.table-hover`)

### Icons:
Uses **Font Awesome**. If you don't have it, add to your base template:

```html
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
```

---

## 🧪 Testing the Integration

1. **Start your Flask app:**
   ```bash
   python app.py
   ```

2. **Navigate to:** `http://localhost:5000/synergy/`

3. **First time:** Click "Fetch Data" (admin only) to pull from Synergy API

4. **View PnR Analysis:** Click "Pick & Roll Analysis" card

---

## 🔧 Adding More Play Types (Future)

To add Isolation, Post-Up, etc.:

1. Update `PLAY_TYPES` in `synergy_client.py`:
   ```python
   PLAY_TYPES = {
       'ball_handler': 'PandRBallHandler',
       'roll_man': 'PandRRollMan',
       'isolation': 'Isolation',      # NEW
       'postup': 'PostUp',            # NEW
   }
   ```

2. Add fetch logic in `synergy_routes.py` `api_refresh()` function

3. Create new template pages for each play type

---

## 📊 API Endpoints

Your website can now call these endpoints:

```javascript
// Get ball handler stats
fetch('/synergy/api/pnr/ball-handlers')
  .then(r => r.json())
  .then(data => console.log(data));

// Get roll man stats  
fetch('/synergy/api/pnr/roll-men')
  .then(r => r.json())
  .then(data => console.log(data));

// Get summary
fetch('/synergy/api/pnr/summary')
  .then(r => r.json())
  .then(data => console.log(data));

// Refresh data (admin only)
fetch('/synergy/api/refresh', { method: 'POST' })
  .then(r => r.json())
  .then(data => console.log(data));
```

---

## ⚙️ Auto-Refresh Setup (Optional)

To auto-refresh Synergy data daily, add a cron job or use Flask-APScheduler:

```python
# Install: pip install flask-apscheduler

from flask_apscheduler import APScheduler

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

@scheduler.task('cron', id='refresh_synergy', hour=6)  # 6 AM daily
def refresh_synergy_data():
    with app.app_context():
        # Call your refresh function
        from synergy.synergy_routes import refresh_all_data
        refresh_all_data()
```

---

## 🐛 Troubleshooting

### "Module not found: synergy"
- Make sure you created `synergy/__init__.py` (can be empty)
- Check folder is named `synergy` not `synergy_integration`

### "No module named 'dotenv'"
```bash
pip install python-dotenv
```

### "Table doesn't exist"
- Run `db.create_all()` as shown in Step 6

### "401 Unauthorized"
- Check your `.env` has correct Synergy credentials
- Verify credentials are loaded with `print(os.getenv("SYNERGY_CLIENT_ID"))`

### Templates not found
- Make sure templates are in `templates/synergy/`
- Check your `template_folder` path in blueprint registration

---

## 🎯 Next Steps

1. **Test it locally** - Make sure everything works
2. **Customize styling** - Match your exact design
3. **Add to git** - Commit the new files
4. **Deploy** - Push to production
5. **Expand** - Add more play types!

---

## 📞 Need Help?

If you run into issues:
1. Check the console for errors
2. Verify all files are in the right place
3. Make sure database tables were created
4. Test API calls directly in browser: `/synergy/api/pnr/summary`

---

**Roll Tide! 🐘** This integration will give your team powerful Synergy analytics right in your existing platform!
