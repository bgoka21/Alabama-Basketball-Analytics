"""PDF generation routes for player and team reports."""

from __future__ import annotations

from datetime import date
from io import BytesIO

from flask import Blueprint, jsonify, send_file
from PyPDF2 import PdfMerger

from app.utils.pdf_data_compiler import compile_player_shot_data
from app.utils.pdf_generator import ShotTypeReportGenerator
from models.database import Roster, db

pdf_bp = Blueprint("pdf", __name__)


@pdf_bp.route("/pdf/player/<int:player_id>/generate")
def generate_player_pdf(player_id: int):
    try:
        player = Roster.query.get_or_404(player_id)
        player_data = compile_player_shot_data(player, db.session)
        generator = ShotTypeReportGenerator(player_data)
        pdf_bytes = generator.generate()
        filename = f"{player.player_name.replace(' ', '_')}_Shot_Report_{date.today().isoformat()}.pdf"
        return send_file(
            BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=filename,
        )
    except Exception as exc:
        return jsonify({"error": "Failed to generate player PDF", "details": str(exc)}), 500


@pdf_bp.route("/pdf/team/generate")
def generate_team_pdf():
    try:
        players = Roster.query.all()
        if not players:
            return jsonify({"error": "No players found to generate team report."}), 404

        merger = PdfMerger()
        for player in players:
            player_data = compile_player_shot_data(player, db.session)
            generator = ShotTypeReportGenerator(player_data)
            pdf_bytes = generator.generate()
            merger.append(BytesIO(pdf_bytes))

        output_buffer = BytesIO()
        merger.write(output_buffer)
        merger.close()
        output_buffer.seek(0)
        filename = f"Team_Shot_Reports_{date.today().isoformat()}.pdf"
        return send_file(
            output_buffer,
            mimetype="application/pdf",
            as_attachment=True,
            download_name=filename,
        )
    except Exception as exc:
        return jsonify({"error": "Failed to generate team PDF", "details": str(exc)}), 500
