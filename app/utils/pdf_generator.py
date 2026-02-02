"""PDF generator for player shot type reports."""

from __future__ import annotations

from io import BytesIO
from typing import Mapping

from reportlab.graphics import renderPDF
from reportlab.graphics.shapes import Drawing, Rect, Circle, Line, String
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
    Paragraph,
    PageBreak,
)


class ShotTypeReportGenerator:
    """Generate a four-page PDF report for a player."""

    def __init__(self, player_data: Mapping[str, object]):
        self.player_data = player_data
        self.buffer = BytesIO()
        self.pagesize = letter
        self.width, self.height = self.pagesize

        self.crimson = colors.HexColor("#9E1B32")
        self.green = colors.HexColor("#90EE90")
        self.red = colors.HexColor("#FFB6C1")
        self.tan = colors.HexColor("#F5DEB3")
        self.light_gray = colors.HexColor("#E0E0E0")
        self.medium_gray = colors.HexColor("#828A8F")

    def generate(self) -> bytes:
        """Build all four pages and return PDF bytes."""
        doc = SimpleDocTemplate(
            self.buffer,
            pagesize=self.pagesize,
            rightMargin=0.5 * inch,
            leftMargin=0.5 * inch,
            topMargin=0.5 * inch,
            bottomMargin=0.5 * inch,
        )

        story = []
        story.extend(self._create_cover_page())
        story.append(PageBreak())
        story.append(Paragraph("At The Rim | Individual Breakdown", self._header_style()))
        story.append(PageBreak())
        story.append(Paragraph("Non-ATR 2FG | Individual Breakdown", self._header_style()))
        story.append(PageBreak())
        story.append(Paragraph("3FG Shots | Individual Breakdown", self._header_style()))

        doc.build(story)
        pdf_content = self.buffer.getvalue()
        self.buffer.close()
        return pdf_content

    def _create_cover_page(self):
        elements = []
        header = Paragraph(
            f"{self.player_data.get('season', '')} Season Totals",
            ParagraphStyle(
                "season_header",
                fontSize=16,
                textColor=self.medium_gray,
                alignment=TA_CENTER,
                spaceAfter=12,
            ),
        )
        elements.append(header)

        player_name = f"#{self.player_data.get('number', '')} {self.player_data.get('name', '')}".strip()
        elements.append(
            Paragraph(
                player_name,
                ParagraphStyle(
                    "player_name",
                    fontSize=36,
                    alignment=TA_CENTER,
                    spaceAfter=14,
                ),
            )
        )

        stats_table = Table(
            [
                ["FT%", "TS%", "PPS", "EFG%"],
                [
                    f"{self.player_data.get('ft_pct', 0)}%",
                    f"{self.player_data.get('ts_pct', 0)}%",
                    f"{self.player_data.get('pps', 0)}",
                    f"{self.player_data.get('efg_pct', 0)}%",
                ],
            ],
            colWidths=[1.6 * inch] * 4,
        )
        stats_table.setStyle(
            TableStyle(
                [
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 11),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                    ("TOPPADDING", (0, 1), (-1, 1), 6),
                ]
            )
        )
        elements.append(stats_table)
        elements.append(Spacer(1, 0.25 * inch))

        summary_boxes = Table(
            [
                [
                    self._create_summary_box("ATR", self.player_data.get("atr", {}).get("total", {}), self.green),
                    self._create_summary_box("2FG", self.player_data.get("2fg", {}).get("total", {}), self.tan),
                    self._create_summary_box("3FG", self.player_data.get("3fg", {}).get("total", {}), self.green),
                ]
            ],
            colWidths=[2.2 * inch] * 3,
        )
        summary_boxes.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
        elements.append(summary_boxes)
        elements.append(Spacer(1, 0.3 * inch))

        charts_row = Table(
            [
                [
                    self._create_shot_chart("TOTAL", self.player_data.get("atr", {}).get("shot_charts", {}).get("total", {})),
                    self._create_shot_chart("HALF COURT", self.player_data.get("atr", {}).get("shot_charts", {}).get("half_court", {})),
                    self._create_shot_chart("TRANSITION", self.player_data.get("atr", {}).get("shot_charts", {}).get("transition", {})),
                ]
            ],
            colWidths=[2.4 * inch] * 3,
        )
        charts_row.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
        elements.append(charts_row)
        elements.append(Spacer(1, 0.3 * inch))
        elements.append(self._create_footer())
        return elements

    def _create_summary_box(self, title, data, bgcolor):
        box = Table(
            [
                [title],
                ["FGA", "FG%", "PPS", "Freq%"],
                [
                    data.get("fga", "0-0"),
                    f"{data.get('fg_pct', 0)}%",
                    data.get("pps", 0),
                    f"{data.get('freq', 0)}%",
                ],
            ],
            colWidths=[0.55 * inch] * 4,
        )
        box.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), bgcolor),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                ]
            )
        )
        return box

    def _create_shot_chart(self, title: str, zone_data: Mapping[str, Mapping[str, object]]):
        drawing = Drawing(180, 170)
        drawing.add(Rect(5, 5, 170, 160, strokeColor=colors.black, fillColor=None))
        drawing.add(Rect(65, 5, 50, 60, strokeColor=colors.black, fillColor=None))
        drawing.add(Line(5, 85, 175, 85, strokeColor=colors.black))
        drawing.add(Circle(90, 20, 4, strokeColor=colors.black, fillColor=None))

        drawing.add(
            String(
                90,
                150,
                "ALABAMA",
                fontSize=20,
                fillColor=colors.Color(0, 0, 0, alpha=0.08),
                textAnchor="middle",
            )
        )
        drawing.add(String(90, 162, title, fontSize=10, textAnchor="middle"))

        zones = list(zone_data.items())
        zone_boxes = [
            (10, 95, 50, 40),
            (65, 95, 50, 40),
            (120, 95, 50, 40),
            (10, 45, 50, 40),
            (65, 45, 50, 40),
            (120, 45, 50, 40),
        ]
        for idx, (zone_name, payload) in enumerate(zones[: len(zone_boxes)]):
            x, y, w, h = zone_boxes[idx]
            pct = float(payload.get("pct", 0) or 0)
            if pct >= 60:
                fill = self.green
            elif pct <= 25:
                fill = self.red
            else:
                fill = self.light_gray
            drawing.add(Rect(x, y, w, h, fillColor=fill, strokeColor=colors.black))
            drawing.add(String(x + w / 2, y + h / 2 + 8, f"{int(round(pct))}%", fontSize=8, textAnchor="middle"))
            drawing.add(
                String(
                    x + w / 2,
                    y + h / 2 - 6,
                    f"{payload.get('made', 0)}/{payload.get('attempts', 0)}",
                    fontSize=7,
                    textAnchor="middle",
                )
            )

        return renderPDF.GraphicsFlowable(drawing)

    def _create_footer(self):
        return Paragraph(
            "BAMALYTICS",
            ParagraphStyle("footer", alignment=TA_CENTER, fontSize=10, textColor=self.medium_gray),
        )

    def _header_style(self):
        return ParagraphStyle("page_header", fontSize=12, textColor=self.crimson, alignment=TA_CENTER)

    def _create_breakdown_header(self, title: str):
        header_table = Table(
            [["ALABAMA CRIMSON TIDE", title]],
            colWidths=[3.5 * inch, 3.5 * inch],
        )
        header_table.setStyle(
            TableStyle(
                [
                    ("ALIGN", (0, 0), (0, 0), "LEFT"),
                    ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                    ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 11),
                    ("TEXTCOLOR", (0, 0), (0, 0), self.crimson),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ]
            )
        )
        return header_table

    def _create_player_summary(self, shot_type: str):
        data = self.player_data.get(shot_type, {})
        total = data.get("total", {})
        transition = data.get("transition", {})
        half_court = data.get("half_court", {})

        player_name = f"#{self.player_data.get('number', '')} {self.player_data.get('name', '')}".strip()
        summary_table = Table(
            [
                ["", "TOTALS", "", "", "", "TRANSITION", "", "", "", "HALF COURT", "", "", ""],
                ["PLAYER", "FGA", "FG%", "PPS", "Freq%", "FGA", "FG%", "PPS", "Freq%", "FGA", "FG%", "PPS", "Freq%"],
                [
                    player_name,
                    total.get("fga", "0-0"),
                    f"{total.get('fg_pct', 0)}%",
                    total.get("pps", 0),
                    f"{total.get('freq', 0)}%",
                    transition.get("fga", "0-0"),
                    f"{transition.get('fg_pct', 0)}%",
                    transition.get("pps", 0),
                    f"{transition.get('freq', 0)}%",
                    half_court.get("fga", "0-0"),
                    f"{half_court.get('fg_pct', 0)}%",
                    half_court.get("pps", 0),
                    f"{half_court.get('freq', 0)}%",
                ],
            ],
            colWidths=[1.2 * inch] + [0.6 * inch] * 12,
        )
        summary_table.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                    ("BACKGROUND", (1, 0), (4, 0), self.light_gray),
                    ("BACKGROUND", (5, 0), (8, 0), self.light_gray),
                    ("BACKGROUND", (9, 0), (12, 0), self.light_gray),
                    ("SPAN", (1, 0), (4, 0)),
                    ("SPAN", (5, 0), (8, 0)),
                    ("SPAN", (9, 0), (12, 0)),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("ALIGN", (0, 1), (0, -1), "LEFT"),
                    ("FONTNAME", (0, 0), (-1, 1), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )
        return summary_table
