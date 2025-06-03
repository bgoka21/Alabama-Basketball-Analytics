from parse_practice_csv import parse_practice_csv

file_path = "data/uploads/Practice_Example_File.csv"
season_id = 1
category = "Official Practices"
from datetime import datetime
file_date = datetime.strptime("2024-10-23", "%Y-%m-%d").date()


parse_practice_csv(file_path, season_id, category, file_date)
