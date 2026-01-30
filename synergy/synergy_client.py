"""
Synergy API Client - Handles all API calls
"""
import requests
import json
from datetime import datetime
from flask import current_app
from . import synergy_auth

# Configuration will be loaded from Flask app config
TEAM_NAME = "Alabama"
LEAGUE_NAME_HINTS = ["College", "Men"]


class SynergyClient:
    def __init__(self):
        # Get credentials from Flask app config
        synergy_auth.SYNERGY_CLIENT_ID = current_app.config.get('SYNERGY_CLIENT_ID')
        synergy_auth.SYNERGY_CLIENT_SECRET = current_app.config.get('SYNERGY_CLIENT_SECRET')
        
        self.base_url = "https://basketball.synergysportstech.com/external/api"
        self.headers = {
            "Authorization": f"Bearer {synergy_auth.get_synergy_token()}",
            "Content-Type": "application/json",
        }
    
    def _get(self, url, params=None):
        """Make GET request with error handling"""
        r = requests.get(url, headers=self.headers, params=params)
        if r.status_code >= 400:
            try:
                print("ERROR BODY:", r.json())
            except Exception:
                print("ERROR BODY (text):", r.text[:1000])
            r.raise_for_status()
        return r.json()
    
    def _save_raw_data(self, data, filename):
        """Save raw API response for debugging/caching (optional)"""
        # Optional: implement if you want to save raw responses
        # For now, just return None to avoid filesystem dependencies
        return None
    
    def get_leagues(self):
        """Fetch all available leagues"""
        data = self._get(f"{self.base_url}/leagues", params={"skip": 0, "take": 512})
        self._save_raw_data(data, "leagues")
        return data
        
    def pick_best_league(self, leagues):
        """Select the best matching league based on hints"""
        # Handle response - could be list of objects or list of {data: {...}}
        data = leagues.get("data", leagues)
        if not isinstance(data, list):
            data = [data]
        
        best = None
        best_score = -1

        for item in data:
            # Extract actual league data (may be nested in "data" key)
            lg = item.get("data", item)
            
            name = (lg.get("name") or lg.get("leagueName") or "").lower()
            score = 0
            for hint in LEAGUE_NAME_HINTS:
                if hint.lower() in name:
                    score += 1
            if score > best_score:
                best_score = score
                best = lg
        
        return best
    
    def get_seasons(self, league_id):
        """Fetch seasons for a league"""
        data = self._get(f"{self.base_url}/leagues/{league_id}/seasons", 
                        params={"skip": 0, "take": 512})
        self._save_raw_data(data, f"seasons_league_{league_id}")
        return data
        
    def find_latest_season(self, seasons):
        """Find the most recent season"""
        data = seasons.get("data", seasons)
        
        if not isinstance(data, list):
            data = [data]
        
        latest = None
        latest_year = -1

        for item in data:
            # Extract actual season data (may be nested in "data" key)
            season = item.get("data", item)
            
            # Get season name like "2024-2025"
            name = season.get("name") or season.get("seasonName") or ""
            
            # Extract the ending year from names like "2024-2025"
            try:
                if "-" in name:
                    end_year = int(name.split("-")[1])
                else:
                    # Try to parse as single year
                    end_year = int(name)
            except (ValueError, IndexError):
                continue
            
            if end_year > latest_year:
                latest_year = end_year
                latest = season
        
        return latest
    
    def get_teams(self, league_id):
        """Get all teams with pagination"""
        all_teams = []
        skip = 0
        page_size = 512
        
        while True:
            response = self._get(
                f"{self.base_url}/leagues/{league_id}/teams",
                params={"skip": skip, "take": page_size}
            )
            
            data = response.get("data", [])
            if not data:
                break
            
            all_teams.extend(data)
            
            # If we got less than page_size, we're done
            if len(data) < page_size:
                break
            
            skip += page_size
        
        return {"data": all_teams}
    
    def find_team_by_name(self, teams, team_name):
        """Find team by exact name match"""
        data = teams.get("data", teams)
        if not isinstance(data, list):
            data = [data]
        
        team_name_lower = team_name.lower().strip()
        
        for item in data:
            # Extract actual team data
            team = item.get("data", item)
            name = (team.get("name") or team.get("teamName") or "").lower().strip()
            
            # Exact match only (avoids "NorthAlabama" matching "Alabama")
            if name == team_name_lower:
                return team
        
        return None
    
    def get_players(self, team_id, season_id):
        """Fetch players for a team in a season"""
        data = self._get(
            f"{self.base_url}/teams/{team_id}/players",
            params={
                "seasonId": season_id,
                "skip": 0,
                "take": 512
            }
        )
        self._save_raw_data(data, f"players_team_{team_id}_season_{season_id}")
        return data
    
    def get_player_playtype_stats(self, season_id, team_id, play_type, 
                                  defensive=False, player_id=None):
        """
        Fetch player play type statistics
        
        Args:
            season_id: Season identifier
            team_id: Team identifier
            play_type: One of PandRBallHandler, PandRRollMan, etc.
            defensive: Whether to get defensive stats
            player_id: Optional specific player ID
        """
        url = f"{self.base_url}/seasons/{season_id}/events/reports/playerplaytypestats"
        
        params = {
            "teamId": team_id,
            "defensive": str(defensive).lower(),
            "playType": play_type,
            "skip": 0,
            "take": 512,
        }
        
        if player_id:
            params["playerId"] = player_id
        
        data = self._get(url, params=params)
        
        # Save with descriptive filename
        filename = f"pnr_{play_type}_team_{team_id}_season_{season_id}"
        if player_id:
            filename += f"_player_{player_id}"
        self._save_raw_data(data, filename)
        
        return data
    
    def get_team_context(self):
        """
        Get complete team context (league, season, team, players)
        Returns a dict with all necessary IDs and metadata
        """
        print("🔍 Fetching team context for Alabama...")
        
        # 1. Get leagues
        print("  → Fetching leagues...")
        leagues = self.get_leagues()
        league = self.pick_best_league(leagues)
        if not league:
            raise RuntimeError("No leagues found")
        
        league_id = league.get("id") or league.get("leagueId")
        league_name = league.get("name") or league.get("leagueName")
        print(f"  ✓ League: {league_name} (ID: {league_id})")
        
        # 2. Get seasons
        print("  → Fetching seasons...")
        seasons = self.get_seasons(league_id)
        season = self.find_latest_season(seasons)
        if not season:
            raise RuntimeError("No seasons found")
        
        season_id = season.get("id") or season.get("seasonId")
        season_name = season.get("name") or season.get("seasonName")
        print(f"  ✓ Season: {season_name} (ID: {season_id})")
        
        # 3. Get teams
        print("  → Fetching teams...")
        teams = self.get_teams(league_id)
        team = self.find_team_by_name(teams, TEAM_NAME)
        if not team:
            raise RuntimeError(f"Team '{TEAM_NAME}' not found")
        
        team_id = team.get("id") or team.get("teamId")
        team_name = team.get("name") or team.get("teamName")
        print(f"  ✓ Team: {team_name} (ID: {team_id})")
        
        # 4. Get players
        print("  → Fetching roster...")
        players = self.get_players(team_id, season_id)
        player_count = len(players.get("data", []))
        print(f"  ✓ Roster: {player_count} players")
        
        context = {
            "league_id": league_id,
            "league_name": league_name,
            "season_id": season_id,
            "season_name": season_name,
            "team_id": team_id,
            "team_name": team_name,
            "players": players.get("data", [])
        }
        
        print(f"\n✓ Team context retrieved\n")
        
        return context
