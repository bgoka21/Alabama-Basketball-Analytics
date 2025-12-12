import csv
import re
from collections import OrderedDict
from typing import Any, Dict, Iterable, List, Optional

from models.database import db
from models.scout import (
    ScoutGame,
    ScoutPlaycallMapping,
    ScoutPossession,
    normalize_playcall,
)
from scout.schema import ensure_scout_possession_schema


def _extract_points(shot_value: str) -> int:
    """Parse the Shot column to extract point contributions for a row."""
    if not shot_value:
        return 0

    points = 0
    for token in re.findall(r"-?\d+", str(shot_value)):
        value = int(token)
        if value in (1, 2, 3):
            points += value
    return points


def _determine_bucket(playcall: str) -> str:
    trimmed = playcall.strip()
    upper_playcall = trimmed.upper()
    if upper_playcall.startswith("BOB"):
        return "BOB"
    if upper_playcall.startswith("SOB"):
        return "SOB"
    return "STANDARD"


def _should_exclude(playcall: str) -> bool:
    return playcall.strip().lower().startswith("transition")


def _resolve_field_name(fieldnames: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    lookup = {name.lower(): name for name in fieldnames}
    for candidate in candidates:
        normalized = candidate.lower()
        if normalized in lookup:
            return lookup[normalized]
    return None


def parse_playcalls_csv(file_path: str) -> List[Dict[str, Any]]:
    """Parse a scout playcalls CSV into possession payloads.

    Returns a list of dictionaries with instance_number, playcall, bucket, and points.
    """

    with open(file_path, newline="", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        if not reader.fieldnames:
            return []

        instance_field = _resolve_field_name(reader.fieldnames, ["instance number", "instance", "instance_number"])
        playcall_field = _resolve_field_name(reader.fieldnames, ["playcall"])
        shot_field = _resolve_field_name(reader.fieldnames, ["shot"])
        optional_fields = {
            "series": _resolve_field_name(reader.fieldnames, ["series"]),
            "family": _resolve_field_name(reader.fieldnames, ["family"]),
        }

        if not instance_field or not playcall_field:
            raise ValueError(
                "CSV is missing required columns (instance number and playcall) for scout playcall parsing."
            )

        instance_data: "OrderedDict[str, Dict[str, object]]" = OrderedDict()

        for row in reader:
            instance_number = (row.get(instance_field) or "").strip()
            if not instance_number:
                continue

            if instance_number not in instance_data:
                instance_data[instance_number] = {"playcall": None, "points": 0}
                for field in optional_fields:
                    if optional_fields[field]:
                        instance_data[instance_number][field] = None

            anchored_playcall: Optional[str] = instance_data[instance_number]["playcall"]  # type: ignore[index]
            candidate_playcall = (row.get(playcall_field) or "").strip()
            if not anchored_playcall and candidate_playcall:
                instance_data[instance_number]["playcall"] = candidate_playcall

            for field, column in optional_fields.items():
                if not column or field not in instance_data[instance_number]:
                    continue
                anchored_value: Optional[str] = instance_data[instance_number][field]  # type: ignore[index]
                candidate_value = (row.get(column) or "").strip()
                if not anchored_value and candidate_value:
                    instance_data[instance_number][field] = candidate_value

            shot_value = row.get(shot_field) if shot_field else ""
            instance_data[instance_number]["points"] = int(
                instance_data[instance_number]["points"]
            ) + _extract_points(shot_value)  # type: ignore[index]

    possessions: List[Dict[str, Any]] = []
    for instance_number, data in instance_data.items():
        playcall_value = (data.get("playcall") or "").strip()
        if not playcall_value:
            # Skip instances that never received a playcall value.
            continue

        if _should_exclude(playcall_value):
            continue

        bucket = _determine_bucket(playcall_value)
        possession_payload = {
            "instance_number": instance_number,
            "playcall": playcall_value,
            "bucket": bucket,
            "points": int(data.get("points") or 0),
        }

        for field in optional_fields:
            if field in data:
                possession_payload[field] = data.get(field)

        possessions.append(possession_payload)

    return possessions


def store_scout_playcalls(file_path: str, scout_game: ScoutGame) -> int:
    """Parse and persist scout playcalls for a single ScoutGame.

    Returns the count of new ScoutPossession rows inserted.
    """

    ensure_scout_possession_schema(db.engine)

    parsed_possessions = parse_playcalls_csv(file_path)
    if not parsed_possessions:
        return 0

    playcall_keys = {normalize_playcall(item['playcall']) for item in parsed_possessions}
    existing_mappings = {
        mapping.playcall_key: mapping
        for mapping in ScoutPlaycallMapping.query.filter(
            ScoutPlaycallMapping.playcall_key.in_(playcall_keys)
        ).all()
    }

    existing_instances = {
        row.instance_number
        for row in ScoutPossession.query.with_entities(ScoutPossession.instance_number).filter_by(
            scout_game_id=scout_game.id
        )
    }

    new_records = []
    for possession in parsed_possessions:
        if possession["instance_number"] in existing_instances:
            continue

        playcall_key = normalize_playcall(possession["playcall"])
        mapping = existing_mappings.get(playcall_key)

        mapped_series = (mapping.canonical_series or "") if mapping else ""
        mapped_family = (mapping.canonical_family or "") if mapping else ""

        new_records.append(
            ScoutPossession(
                scout_game_id=scout_game.id,
                instance_number=possession["instance_number"],
                playcall=possession["playcall"],
                family=mapped_family or possession.get("family"),
                series=mapped_series or possession.get("series"),
                bucket=possession["bucket"],
                points=possession["points"],
            )
        )

    if not new_records:
        return 0

    db.session.add_all(new_records)
    db.session.commit()
    return len(new_records)
