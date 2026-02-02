"""PDF generation routes for player and team reports."""

from __future__ import annotations

from datetime import date
from io import BytesIO

import logging

from flask import Blueprint, current_app, jsonify, send_file
from PyPDF2 import PdfMerger
from sqlalchemy.exc import SQLAlchemyError

from app.utils.pdf_data_compiler import compile_player_shot_data
from app.utils.pdf_generator import ShotTypeReportGenerator
from models.database import Roster, db

pdf_bp = Blueprint("pdf", __name__)
logger = logging.getLogger(__name__)


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
    except SQLAlchemyError:
        logger.exception("Database error while generating player PDF for %s", player_id)
        db.session.rollback()
        return jsonify({"error": "Database error while generating player PDF."}), 500
    except Exception:
        current_app.logger.exception("Failed to generate player PDF for %s", player_id)
        return jsonify({"error": "Failed to generate player PDF. Please try again later."}), 500


@pdf_bp.route("/pdf/team/generate")
def generate_team_pdf():
    try:
        players = Roster.query.all()
        if not players:
            return jsonify({"error": "No players found to generate team report."}), 404

        merger = PdfMerger()
        total_players = len(players)
        logger.info("Generating team PDF for %s players.", total_players)
        for idx, player in enumerate(players, start=1):
            logger.info("Generating PDF for %s (%s/%s).", player.player_name, idx, total_players)
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
    except SQLAlchemyError:
        logger.exception("Database error while generating team PDF.")
        db.session.rollback()
        return jsonify({"error": "Database error while generating team PDF."}), 500
    except Exception:
        current_app.logger.exception("Failed to generate team PDF")
        return jsonify({"error": "Failed to generate team PDF. Please try again later."}), 500
