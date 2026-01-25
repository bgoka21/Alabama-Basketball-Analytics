"""Command-line wrapper for exporting CSV data to Sportscode XML."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from scripts.export_xml import export_csv_to_sportscode_xml


def _resolve_output_path(csv_path: Path, xml_path: str | None) -> Path:
    if xml_path:
        return Path(xml_path)
    return csv_path.with_suffix(".xml")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export a Hudl-compatible CSV to Sportscode XML."
    )
    parser.add_argument("csv_path", help="Path to the input CSV file.")
    parser.add_argument(
        "xml_path",
        nargs="?",
        default=None,
        help="Optional path to write the XML output.",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    xml_path = _resolve_output_path(csv_path, args.xml_path)

    try:
        export_csv_to_sportscode_xml(csv_path, xml_path)
    except Exception as exc:  # noqa: BLE001 - surface CLI error details
        print(f"Failed to export XML: {exc}", file=sys.stderr)
        return 1

    print(f"Exported XML to {xml_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
