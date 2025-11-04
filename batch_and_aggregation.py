# batch_and_aggregation.py

import os
import shutil
import logging
from models.database import db, Season, Game, PlayerStats, TeamStats
from test_parse import parse_csv  # Ensure this path is correct for your project
from app import app  # Import the Flask app for the application context

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_multiple_csvs(directory_path, season_id, archive_path):
    """
    Process all CSV files in the given directory for a specified season.
    Each CSV file is processed by calling the parse_csv() function from test_parse.py.
    After successful processing, the file is moved to the archive folder.
    """
    if not os.path.isdir(directory_path):
        logger.error(f"Directory {directory_path} does not exist.")
        return

    # Ensure archive folder exists
    if not os.path.isdir(archive_path):
        os.makedirs(archive_path)

    csv_files = [os.path.join(directory_path, filename)
                 for filename in os.listdir(directory_path)
                 if filename.lower().endswith('.csv')]
    logger.info(f"Found {len(csv_files)} CSV files in {directory_path}")

    for file_path in csv_files:
        try:
            logger.info(f"Processing {file_path}...")
            # Process the CSV file. The parse_csv function checks for an existing Game record.
            parse_csv(file_path, game_id=None, season_id=season_id)
            # If processing is successful, move the file to the archive folder.
            archive_file = os.path.join(archive_path, os.path.basename(file_path))
            shutil.move(file_path, archive_file)
            logger.info(f"Moved {file_path} to archive folder: {archive_file}")
        except Exception as e:
            logger.exception(f"Error processing file {file_path}: {e}")
    logger.info("Batch processing complete.")

def aggregate_player_stats(season_id):
    """
    Aggregate player statistics for a given season.
    Returns a list of dictionaries (one per player) with season totals.
    """
    from sqlalchemy import func

    aggregated = (
        db.session.query(
            PlayerStats.player_name,
            func.sum(PlayerStats.points).label("total_points"),
            func.sum(PlayerStats.assists).label("total_assists"),
            func.sum(PlayerStats.turnovers).label("total_turnovers"),
            func.sum(PlayerStats.atr_makes).label("total_atr_makes"),
            func.sum(PlayerStats.atr_attempts).label("total_atr_attempts"),
            func.sum(PlayerStats.fg2_makes).label("total_fg2_makes"),
            func.sum(PlayerStats.fg2_attempts).label("total_fg2_attempts"),
            func.sum(PlayerStats.fg3_makes).label("total_fg3_makes"),
            func.sum(PlayerStats.fg3_attempts).label("total_fg3_attempts"),
            func.sum(PlayerStats.ftm).label("total_ftm"),
            func.sum(PlayerStats.fta).label("total_fta")
        )
        .join(Game, PlayerStats.game_id == Game.id)
        .filter(Game.season_id == season_id)
        .group_by(PlayerStats.player_name)
        .all()
    )
    return aggregated

def aggregate_team_stats(season_id):
    """
    Aggregate team statistics for a given season.
    Returns aggregated stats as a dictionary.
    """
    from sqlalchemy import func

    aggregated = (
        db.session.query(
            func.sum(TeamStats.total_points).label("total_points"),
            func.sum(TeamStats.total_assists).label("total_assists"),
            func.sum(TeamStats.total_turnovers).label("total_turnovers"),
            func.sum(TeamStats.total_atr_makes).label("total_atr_makes"),
            func.sum(TeamStats.total_atr_attempts).label("total_atr_attempts"),
            func.sum(TeamStats.total_fg2_makes).label("total_fg2_makes"),
            func.sum(TeamStats.total_fg2_attempts).label("total_fg2_attempts"),
            func.sum(TeamStats.total_fg3_makes).label("total_fg3_makes"),
            func.sum(TeamStats.total_fg3_attempts).label("total_fg3_attempts"),
            func.sum(TeamStats.total_ftm).label("total_ftm"),
            func.sum(TeamStats.total_fta).label("total_fta"),
            func.sum(TeamStats.total_fouls_drawn).label("total_fouls_drawn"),
            func.sum(TeamStats.total_blue_collar).label("total_blue_collar"),
            func.sum(TeamStats.total_possessions).label("total_possessions")
        )
        .join(Game, TeamStats.game_id == Game.id)
        .filter(Game.season_id == season_id)
        .one()
    )
    return aggregated

if __name__ == "__main__":
    season_id = 1  # Set your season_id accordingly

    # Run batch processing on CSV files from the uploads folder
    process_multiple_csvs("data/uploads/", season_id, "data/processed/")
    
    # Use an application context for aggregation queries
    with app.app_context():
        # Aggregate player stats and print the results
        player_stats = aggregate_player_stats(season_id)
        for ps in player_stats:
            logger.info(ps)
    
        # Aggregate team stats and print the results
        team_totals = aggregate_team_stats(season_id)
        logger.info(team_totals)
