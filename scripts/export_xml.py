"""Export Hudl-compatible CSV data to Sportscode XML."""

from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET


def _split_label_values(value: str) -> Iterable[str]:
    for part in value.split(","):
        trimmed = part.strip()
        if trimmed:
            yield trimmed


def export_csv_to_sportscode_xml(csv_path: str | Path, xml_path: str | Path) -> None:
    """Convert a Hudl-compatible CSV to Sportscode XML.

    Args:
        csv_path: Path to the input CSV.
        xml_path: Path to write the XML output.
    """

    csv_path = Path(csv_path)
    xml_path = Path(xml_path)

    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        headers = next(reader, None)
        if not headers or len(headers) < 5:
            raise ValueError("CSV must have at least 5 columns.")

        root = ET.Element("sportscode")
        instances_element = ET.SubElement(root, "instances")

        row_names: list[str] = []
        seen_rows: set[str] = set()

        for row_index, row in enumerate(reader):
            if len(row) < 5:
                raise ValueError(f"Row {row_index} must have at least 5 columns.")

            row_name = row[3].strip()
            if row_name and row_name not in seen_rows:
                seen_rows.add(row_name)
                row_names.append(row_name)

            start_value = Decimal(row[1].strip())
            duration_value = Decimal(row[2].strip())
            end_value = start_value + duration_value

            instance_element = ET.SubElement(instances_element, "instance")
            ET.SubElement(instance_element, "ID").text = str(row_index)
            ET.SubElement(instance_element, "code").text = row_name
            ET.SubElement(instance_element, "start").text = str(start_value)
            ET.SubElement(instance_element, "end").text = str(end_value)

            for header, cell in zip(headers[5:], row[5:]):
                cell_value = cell.strip()
                if not cell_value:
                    continue
                for label_text in _split_label_values(cell_value):
                    label_element = ET.SubElement(instance_element, "label")
                    ET.SubElement(label_element, "group").text = header
                    ET.SubElement(label_element, "text").text = label_text

        rows_element = ET.SubElement(root, "ROWS")
        for sort_order, row_name in enumerate(row_names):
            row_element = ET.SubElement(rows_element, "row")
            ET.SubElement(row_element, "sort_order").text = str(sort_order)
            ET.SubElement(row_element, "code").text = row_name
            ET.SubElement(row_element, "R").text = "0"
            ET.SubElement(row_element, "G").text = "0"
            ET.SubElement(row_element, "B").text = "0"

    tree = ET.ElementTree(root)
    with xml_path.open("wb") as xml_file:
        tree.write(xml_file, encoding="utf-16", xml_declaration=True)
