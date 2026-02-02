"""Tests for PDF generation and edge cases."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pytest
from PyPDF2 import PdfReader

from app import create_app
from app.utils.pdf_data_compiler import compile_player_shot_data
from app.utils.pdf_generator import ShotTypeReportGenerator
from models.database import PlayerStats, Roster, db


OUTPUT_DIR = Path("/tmp/test_pdfs")
MAX_PDF_SIZE_BYTES = 5 * 1024 * 1024


@pytest.fixture(scope="module")
def app_context():
    app = create_app()
    with app.app_context():
        yield app


def _ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _sanitize_filename(value: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in value)
    return safe.strip("_") or "player"


def _attempts_from_fga(value: str) -> int:
    try:
        _, attempts = value.split("-", 1)
        return int(attempts)
    except (ValueError, AttributeError):
        return 0


def _iter_players(limit: int = 3) -> list[Roster]:
    return Roster.query.order_by(Roster.id.asc()).limit(limit).all()


def _generate_player_pdf(player: Roster) -> bytes:
    player_data = compile_player_shot_data(player, db.session)
    generator = ShotTypeReportGenerator(player_data)
    return generator.generate()


def _write_pdf(player: Roster, pdf_bytes: bytes) -> Path:
    _ensure_output_dir()
    filename = _sanitize_filename(player.player_name or f"player_{player.id}")
    path = OUTPUT_DIR / f"{filename}.pdf"
    path.write_bytes(pdf_bytes)
    return path


def _validate_pdf(path: Path) -> None:
    file_size = path.stat().st_size
    assert file_size < MAX_PDF_SIZE_BYTES, f"{path} is too large: {file_size} bytes"

    reader = PdfReader(str(path))
    assert len(reader.pages) == 4, f"{path} should have 4 pages, found {len(reader.pages)}"
    for idx, page in enumerate(reader.pages, start=1):
        try:
            _ = page.extract_text()
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(f"Page {idx} failed to render: {exc}") from exc


def _collect_shot_stats(player_data: dict) -> Iterable[dict]:
    for key in ("atr", "2fg", "3fg"):
        shot_data = player_data.get(key, {})
        for bucket in ("total", "transition", "half_court"):
            stats = shot_data.get(bucket, {})
            if stats:
                yield stats


def _has_zero_shots(player_data: dict) -> bool:
    for stats in _collect_shot_stats(player_data):
        if _attempts_from_fga(stats.get("fga", "")) == 0:
            return True
    return False


def _has_perfect_shooting(player_data: dict) -> bool:
    for stats in _collect_shot_stats(player_data):
        attempts = _attempts_from_fga(stats.get("fga", ""))
        if attempts and float(stats.get("fg_pct", 0)) >= 100:
            return True
    return False


def _find_missing_data_player() -> Roster | None:
    return (
        Roster.query.outerjoin(PlayerStats, PlayerStats.player_name == Roster.player_name)
        .filter(PlayerStats.id.is_(None))
        .order_by(Roster.id.asc())
        .first()
    )


def _find_player_by_condition(condition) -> Roster | None:
    for player in Roster.query.order_by(Roster.id.asc()).all():
        player_data = compile_player_shot_data(player, db.session)
        if condition(player_data):
            return player
    return None


def test_generate_player_pdfs_and_validate(app_context):  # noqa: ARG001
    players = _iter_players()
    if len(players) < 2:
        pytest.skip("Not enough players in the database to generate sample PDFs.")

    for player in players:
        pdf_bytes = _generate_player_pdf(player)
        output_path = _write_pdf(player, pdf_bytes)
        _validate_pdf(output_path)
        print(f"✅ Generated PDF for {player.player_name} at {output_path}")


def test_edge_case_zero_shots(app_context):  # noqa: ARG001
    player = _find_player_by_condition(_has_zero_shots)
    if not player:
        pytest.skip("No player found with zero shots in a category.")

    pdf_bytes = _generate_player_pdf(player)
    output_path = _write_pdf(player, pdf_bytes)
    _validate_pdf(output_path)
    print(f"✅ Zero-shot edge case validated for {player.player_name}")


def test_edge_case_perfect_shooting(app_context):  # noqa: ARG001
    player = _find_player_by_condition(_has_perfect_shooting)
    if not player:
        pytest.skip("No player found with 100% shooting in a category.")

    pdf_bytes = _generate_player_pdf(player)
    output_path = _write_pdf(player, pdf_bytes)
    _validate_pdf(output_path)
    print(f"✅ Perfect-shooting edge case validated for {player.player_name}")


def test_edge_case_missing_data(app_context):  # noqa: ARG001
    player = _find_missing_data_player()
    if not player:
        pytest.skip("No player found with missing data (no PlayerStats).")

    pdf_bytes = _generate_player_pdf(player)
    output_path = _write_pdf(player, pdf_bytes)
    _validate_pdf(output_path)
    print(f"✅ Missing-data edge case validated for {player.player_name}")
