"""Admin-only utility to verify a Final CSV output."""

import argparse
import sys

import pandas as pd

PROTECTED_COLUMNS = [
    "Timeline",
    "Start time",
    "Duration",
    "Row",
    "Instance Number",
]

OVERWRITE_GROUPS = [
    "Offense",
    "Defense",
    "PnR",
    "Offense Rebound Opportunities",
    "Defense Rebound Opportunities",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify Final CSV structure for admin use.",
    )
    parser.add_argument("csv_path", help="Path to Final CSV")
    args = parser.parse_args()

    df = pd.read_csv(args.csv_path)
    if "Row" not in df.columns:
        print("ERROR: Missing required 'Row' column.")
        return 1

    print("Row group counts:")
    counts = df["Row"].value_counts(dropna=False)
    for group, count in counts.items():
        print(f"- {group}: {count}")

    player_exists = (df["Row"] == "Player").any()
    print(f"Player rows present: {'YES' if player_exists else 'NO'}")

    missing_groups = [group for group in OVERWRITE_GROUPS if group not in counts.index]
    if missing_groups:
        print("Missing overwrite groups:")
        for group in missing_groups:
            print(f"- {group}")
    else:
        print("Overwrite groups present: YES")

    missing_columns = [col for col in PROTECTED_COLUMNS if col not in df.columns]
    if missing_columns:
        print("Missing protected columns:")
        for col in missing_columns:
            print(f"- {col}")
    else:
        print("Protected columns present: YES")

    return 0 if player_exists and not missing_groups and not missing_columns else 2


if __name__ == "__main__":
    sys.exit(main())
