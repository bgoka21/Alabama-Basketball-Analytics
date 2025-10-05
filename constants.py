from stats_config import LEADERBOARD_STATS

# Preserve order while listing each leaderboard stat key exactly once.
LEADERBOARD_STAT_KEYS = list(dict.fromkeys(stat["key"] for stat in LEADERBOARD_STATS))
