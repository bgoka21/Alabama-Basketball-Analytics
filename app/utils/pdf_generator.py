"""PDF generator for player shot type reports."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
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
from app.grades import grade_token


class ShotTypeReportGenerator:
    """Generate a four-page PDF report for a player."""

    _base_court_drawing: Drawing | None = None

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
            rightMargin=0.35 * inch,
            leftMargin=0.35 * inch,
            topMargin=0.35 * inch,
            bottomMargin=0.35 * inch,
        )
        story = self.get_story_elements()
        doc.build(story)
        pdf_content = self.buffer.getvalue()
        self.buffer.close()
        return pdf_content

    def get_story_elements(self):
        """Return the report's story elements without building a PDF."""
        # Page layout ordering lives here; adjust the sequence or add new
        # pages by inserting additional _create_*_page calls into the story.
        story = []
        story.extend(self._create_cover_page())
        story.append(PageBreak())
        story.extend(self._create_atr_page())
        story.append(PageBreak())
        story.extend(self._create_2fg_page())
        story.append(PageBreak())
        story.extend(self._create_3fg_page())
        return story

    def _create_cover_page(self):
        elements = []
        elements.append(
            Paragraph(
                "ALABAMA CRIMSON TIDE",
                ParagraphStyle(
                    "cover_brand",
                    fontSize=14,
                    textColor=self.crimson,
                    alignment=TA_CENTER,
                    spaceAfter=8,
                ),
            )
        )
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

        number = self.player_data.get("number") or ""
        name = self.player_data.get("name") or "N/A"
        player_name = f"#{number} {name}".strip().replace("# ", "")
        elements.append(
            Paragraph(
                player_name,
                ParagraphStyle(
                    "player_name",
                    fontSize=36,
                    alignment=TA_CENTER,
                    spaceAfter=10,
                ),
            )
        )
        elements.append(Spacer(1, 0.15 * inch))

        shot_type_totals = self.player_data.get("shot_type_totals")
        summary_boxes = Table(
            [
                [
                    self._create_summary_box("ATR", getattr(shot_type_totals, "atr", None), self.green),
                    self._create_summary_box("2FG", getattr(shot_type_totals, "fg2", None), self.tan),
                    self._create_summary_box("3FG", getattr(shot_type_totals, "fg3", None), self.green),
                ]
            ],
            colWidths=[2.2 * inch] * 3,
        )
        summary_boxes.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
        elements.append(summary_boxes)
        elements.append(Spacer(1, 0.2 * inch))
        elements.append(self._create_footer())
        return elements

    def _create_summary_box(self, title, data, bgcolor):
        makes = getattr(data, "makes", 0) if data else 0
        attempts = getattr(data, "attempts", 0) if data else 0
        fg_pct = getattr(data, "fg_pct", 0) if data else 0      # already 0-100
        pps = getattr(data, "pps", 0) if data else 0
        freq = getattr(data, "freq", 0) if data else 0          # already 0-100
        box = Table(
            [
                [title],
                ["FGA", "FG%", "PPS", "Freq%"],
                [
                    f"{makes}-{attempts}",
                    f"{fg_pct:.1f}%",
                    f"{pps:.2f}",
                    f"{freq:.1f}%",
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
                    ("FONTSIZE", (0, 0), (-1, -1), 7),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                ]
            )
        )
        return box

    def _create_shot_chart(self, title: str, zone_data: Mapping[str, Mapping[str, object]]):
        drawing = self._get_base_court_drawing()
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
            # Adjust color thresholds here to tune the green/red grading bands.
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

    def _get_base_court_drawing(self) -> Drawing:
        if self._base_court_drawing is None:
            base = Drawing(180, 170)
            base.add(Rect(5, 5, 170, 160, strokeColor=colors.black, fillColor=None))
            base.add(Rect(65, 5, 50, 60, strokeColor=colors.black, fillColor=None))
            base.add(Line(5, 85, 175, 85, strokeColor=colors.black))
            base.add(Circle(90, 20, 4, strokeColor=colors.black, fillColor=None))
            base.add(
                String(
                    90,
                    150,
                    "ALABAMA",
                    fontSize=20,
                    fillColor=colors.Color(0, 0, 0, alpha=0.08),
                    textAnchor="middle",
                )
            )
            self._base_court_drawing = base
        return deepcopy(self._base_court_drawing)

    def _create_footer(self):
        return Paragraph(
            "BAMALYTICS",
            ParagraphStyle("footer", alignment=TA_CENTER, fontSize=10, textColor=self.medium_gray),
        )

    @staticmethod
    def _format_pct(value: float) -> str:
        """Format a 0-100 pct value as a string like '60.6%'."""
        return f"{float(value or 0):.1f}%"

    def _header_style(self):
        return ParagraphStyle("page_header", fontSize=10, textColor=self.crimson, alignment=TA_CENTER)

    @staticmethod
    def _grade_fill(metric_key: str, value: float) -> colors.Color | None:
        token = grade_token(metric_key, value)
        if not token:
            return None
        token_parts = token.split()
        token_value = next((part for part in token_parts if part.startswith("grade-token--")), None)
        palette = {
            "grade-token--0": "#ff5050",
            "grade-token--1": "#ff8c8c",
            "grade-token--2": "#ffb4b4",
            "grade-token--3": "#fff5a8",
            "grade-token--4": "#ffe138",
            "grade-token--5": "#bef9be",
            "grade-token--6": "#aaeeaa",
            "grade-token--7": "#8cd98c",
            "grade-token--8": "#64c064",
        }
        color_hex = palette.get(token_value)
        if not color_hex:
            return None
        return colors.HexColor(color_hex)

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
                    ("FONTSIZE", (0, 0), (-1, -1), 10),
                    ("TEXTCOLOR", (0, 0), (0, 0), self.crimson),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )
        return header_table

    def _create_player_summary(self, shot_type: str):
        shot_type_totals = self.player_data.get("shot_type_totals")
        shot_summaries = self.player_data.get("shot_summaries", {})
        summary_key = {"atr": "atr", "2fg": "fg2", "3fg": "fg3"}.get(shot_type, shot_type)
        summary = shot_summaries.get(summary_key)
        total = summary.total if summary else None
        transition = summary.transition if summary else None
        half_court = summary.halfcourt if summary else None
        totals_bucket = getattr(shot_type_totals, summary_key, None) if shot_type_totals else None

        number = self.player_data.get("number") or ""
        name = self.player_data.get("name") or "N/A"
        player_name = f"#{number} {name}".strip().replace("# ", "")
        total_freq = getattr(totals_bucket, "freq", None) if totals_bucket else None
        summary_table = Table(
            [
                ["", "TOTALS", "", "", "", "TRANSITION", "", "", "", "HALF COURT", "", "", ""],
                ["PLAYER", "FGA", "FG%", "PPS", "Freq%", "FGA", "FG%", "PPS", "Freq%", "FGA", "FG%", "PPS", "Freq%"],
                [
                    player_name,
                    f"{getattr(total, 'makes', 0)}-{getattr(total, 'attempts', 0)}",
                    self._format_pct(getattr(total, "fg_pct", 0)),          # 0-100, no scaling needed
                    f"{getattr(total, 'pps', 0):.2f}",
                    f"{total_freq:.1f}%" if total_freq is not None else "—",
                    f"{getattr(transition, 'makes', 0)}-{getattr(transition, 'attempts', 0)}",
                    self._format_pct(getattr(transition, "fg_pct", 0)),     # 0-100, no scaling needed
                    f"{getattr(transition, 'pps', 0):.2f}",
                    "—",
                    f"{getattr(half_court, 'makes', 0)}-{getattr(half_court, 'attempts', 0)}",
                    self._format_pct(getattr(half_court, "fg_pct", 0)),     # 0-100, no scaling needed
                    f"{getattr(half_court, 'pps', 0):.2f}",
                    "—",
                ],
            ],
            colWidths=[1.2 * inch] + [0.6 * inch] * 12,
        )
        grade_metric = {"atr": "atr2fg_pct", "fg2": "fg2_pct", "fg3": "fg3_pct"}.get(summary_key)
        style_commands = [
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
            ("FONTSIZE", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
        ]
        fg_total = getattr(total, "fg_pct", 0)
        fg_transition = getattr(transition, "fg_pct", 0)
        fg_halfcourt = getattr(half_court, "fg_pct", 0)
        pps_total = getattr(total, "pps", 0)
        pps_transition = getattr(transition, "pps", 0)
        pps_halfcourt = getattr(half_court, "pps", 0)
        fg_cells = [(2, fg_total), (6, fg_transition), (10, fg_halfcourt)]
        pps_cells = [(3, pps_total), (7, pps_transition), (11, pps_halfcourt)]
        if grade_metric:
            for col, value in fg_cells:
                fill = self._grade_fill(grade_metric, value)
                if fill:
                    style_commands.append(("BACKGROUND", (col, 2), (col, 2), fill))
        for col, value in pps_cells:
            fill = self._grade_fill("pps", value)
            if fill:
                style_commands.append(("BACKGROUND", (col, 2), (col, 2), fill))
        summary_table.setStyle(TableStyle(style_commands))
        return summary_table

    def _create_breakdown_table(self, shot_type: str):
        shot_summaries = self.player_data.get("shot_summaries", {})
        summary_key = {"atr": "atr", "2fg": "fg2", "3fg": "fg3"}.get(shot_type, shot_type)
        summary = shot_summaries.get(summary_key)
        breakdown = summary.cats if summary else {}
        empty_bucket = SimpleNamespace(
            total=SimpleNamespace(attempts=0, makes=0, fg_pct=0, pps=0, freq_pct=0),
            transition=SimpleNamespace(attempts=0, makes=0, fg_pct=0, pps=0, freq_pct=0),
            halfcourt=SimpleNamespace(attempts=0, makes=0, fg_pct=0, pps=0, freq_pct=0),
        )
        if shot_type == "atr":
            groups = [
                ("Assisted", ["Assisted", "Non-Assisted"]),
                ("Dribble", ["Dribble", "No Dribble"]),
                ("RA", ["Restricted Area", "Non Restricted Area"]),
                ("Feet", ["Off 1 Foot", "Off 2 Feet"]),
                ("Hands", ["Left Hand Finish", "Right Hand Finish", "Hands To Rim", "Hands Away From Rim"]),
                ("PA", ["Play Action", "No Play Action"]),
                ("Defenders", ["Primary Defender", "Secondary Defender", "Multiple Defenders", "Unguarded"]),
                ("Type", ["Dunk", "Catch", "Floater", "Layup", "Turnaround J", "Pull Up", "Step Back"]),
                ("Other", ["Blocked"]),
                ("Scheme – Drive", ["Middle Drive", "Baseline Drive", "Slot Drive", "Drive Right", "Drive Left"]),
                ("Scheme – Attack", ["Beast / Post", "DHO / Get", "Iso", "PnR Handler", "Off Closeout", "PnR Sneak", "Transition Push", "OREB Putback"]),
                ("Scheme – Pass", [
                    "Swing", "Check Down", "Off Screen", "1 More", "Lift", "PnR Pocket", "Post Entry", "Drift", "PnR Lob", "Post Pass Out", "Kickdown",
                    "PnR Late Roll", "Dump Off", "Slot Skip", "Pocket Extra", "Lob", "Nail Pitch", "DHO / Get", "PnR Pop", "Slash / Cut", "Reshape",
                    "Shake", "Skip", "Pull Behind", "Outlet", "Press Break", "Cross Court", "Under OB", "Kick Ahead", "Dagger", "Side / Press OB",
                ]),
            ]
        elif shot_type == "2fg":
            groups = [
                ("Assisted", ["Assisted", "Non-Assisted"]),
                ("Dribble", ["Dribble", "No Dribble"]),
                ("RA", ["Restricted Area", "Non Restricted Area"]),
                ("Feet", ["Off 1 Foot", "Off 2 Feet"]),
                ("Hands", ["Left Hand Finish", "Right Hand Finish", "Hands To Rim", "Hands Away From Rim"]),
                ("PA", ["Play Action", "No Play Action"]),
                ("Defenders", ["Primary Defender", "Secondary Defender", "Multiple Defenders", "Unguarded"]),
                ("Type", ["Dunk", "Catch", "Floater", "Layup", "Turnaround J", "Pull Up", "Step Back"]),
                ("Other", ["Blocked"]),
                ("Scheme – Drive", ["Middle Drive", "Baseline Drive", "Slot Drive", "Drive Right", "Drive Left"]),
                ("Scheme – Attack", ["Beast / Post", "DHO / Get", "Iso", "PnR Handler", "Off Closeout", "PnR Sneak", "Transition Push", "OREB Putback"]),
                ("Scheme – Pass", [
                    "Swing", "Check Down", "Off Screen", "1 More", "Lift", "PnR Pocket", "Post Entry", "Drift", "PnR Lob", "Post Pass Out", "Kickdown",
                    "PnR Late Roll", "Dump Off", "Slot Skip", "Pocket Extra", "Lob", "Nail Pitch", "DHO / Get", "PnR Pop", "Slash / Cut", "Reshape",
                    "Shake", "Skip", "Pull Behind", "Outlet", "Press Break", "Cross Court", "Under OB", "Kick Ahead", "Dagger", "Side / Press OB",
                ]),
            ]
        else:
            groups = [
                ("Assisted", ["Assisted", "Non-Assisted"]),
                ("Type", ["Catch and Shoot", "Pull Up", "Step Back", "Catch and Hold", "Slide Dribble"]),
                ("Line", ["On The Line", "Off The Line"]),
                ("Pocket", ["Shot Pocket", "Non-Shot Pocket"]),
                ("Move", ["Stationary", "On Move"]),
                ("Balance", ["On Balance", "Off Balance"]),
                ("Contested", ["Contested", "Uncontested", "Late Contest", "Blocked"]),
                ("Footwork", ["WTN Left-Right", "WTN Right-Left", "Left-Right", "Right-Left", "Hop"]),
                ("Good/Bad", ["Good", "Bad", "Neutral Three"]),
                ("Shrink", ["Shrink", "Non-Shrink"]),
                ("Scheme – Attack", ["Beast / Post", "DHO / Get", "PnR Handler", "Iso", "Off Closeout", "PnR Sneak", "Transition Push", "OREB Putback"]),
                ("Scheme – Drive", ["Drive Right", "Drive Left", "Dip"]),
                ("Scheme – Pass", [
                    "Swing", "Checkdown", "Off Screen", "1 More", "Lift", "Drift", "Post Entry", "Post Pass Out",
                    "Kickdown", "Slot Skip", "Pocket Extra", "Lob", "Nail Pitch", "DHO / Get", "PnR Pop", "Reshape", "Shake", "Skip",
                    "Pull Behind", "Outlet", "Press Break", "Cross Court", "Under OB", "Kick Ahead", "Dagger", "Side / Press OB",
                ]),
            ]
        header_row = [
            "BREAKDOWN",
            "TOTALS",
            "",
            "",
            "",
            "TRANSITION",
            "",
            "",
            "",
            "HALF COURT",
            "",
            "",
            "",
        ]
        subheader_row = [
            "",
            "FGA",
            "FG%",
            "PPS",
            "Freq",
            "FGA",
            "FG%",
            "PPS",
            "Freq",
            "FGA",
            "FG%",
            "PPS",
            "Freq",
        ]
        rows = [header_row, subheader_row]
        row_keys = []
        data_row_indices = []
        divider_rows = []
        for _group_name, categories in groups:
            divider_rows.append(len(rows))
            rows.append([""] * 13)
            for label_key in categories:
                buckets = breakdown.get(label_key, empty_bucket)
                total = buckets.total
                transition = buckets.transition
                half_court = buckets.halfcourt
                row_keys.append(label_key)
                data_row_indices.append(len(rows))
                # All fg_pct and freq_pct values are 0-100; format directly.
                rows.append(
                    [
                        label_key,
                        f"{total.makes}-{total.attempts}",
                        f"{total.fg_pct:.1f}%",
                        f"{total.pps:.2f}",
                        f"{total.freq_pct:.1f}%" if total.attempts else "—",
                        f"{transition.makes}-{transition.attempts}",
                        f"{transition.fg_pct:.1f}%",
                        f"{transition.pps:.2f}",
                        f"{transition.freq_pct:.1f}%" if transition.attempts else "—",
                        f"{half_court.makes}-{half_court.attempts}",
                        f"{half_court.fg_pct:.1f}%",
                        f"{half_court.pps:.2f}",
                        f"{half_court.freq_pct:.1f}%" if half_court.attempts else "—",
                    ]
                )

        row_heights = []
        for idx in range(len(rows)):
            if idx in divider_rows:
                row_heights.append(3)
            else:
                row_heights.append(None)
        table = Table(rows, colWidths=[1.6 * inch] + [0.45 * inch] * 12, rowHeights=row_heights)
        style_commands = [
            ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
            ("SPAN", (1, 0), (4, 0)),
            ("SPAN", (5, 0), (8, 0)),
            ("SPAN", (9, 0), (12, 0)),
            ("BACKGROUND", (0, 0), (-1, 1), self.light_gray),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("ALIGN", (0, 2), (0, -1), "LEFT"),
            ("FONTNAME", (0, 0), (-1, 1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
            ("TOPPADDING", (0, 0), (-1, -1), 1),
        ]

        for row_idx in divider_rows:
            style_commands.append(("BACKGROUND", (0, row_idx), (-1, row_idx), self.crimson))
            style_commands.append(("GRID", (0, row_idx), (-1, row_idx), 0, colors.white))

        grade_metric = {"atr": "atr2fg_pct", "2fg": "fg2_pct", "3fg": "fg3_pct"}.get(shot_type)
        for row_idx, row_key in zip(data_row_indices, row_keys):
            for col_offset, bucket_key in ((2, "total"), (6, "transition"), (10, "halfcourt")):
                bucket = getattr(breakdown.get(row_key), bucket_key, None)
                if grade_metric:
                    fg_fill = self._grade_fill(grade_metric, getattr(bucket, "fg_pct", 0))
                    if fg_fill:
                        style_commands.append(("BACKGROUND", (col_offset, row_idx), (col_offset, row_idx), fg_fill))
                pps_fill = self._grade_fill("pps", getattr(bucket, "pps", 0))
                if pps_fill:
                    style_commands.append(("BACKGROUND", (col_offset + 1, row_idx), (col_offset + 1, row_idx), pps_fill))

        table.setStyle(TableStyle(style_commands))
        return table

    def _create_atr_page(self):
        elements = [
            self._create_breakdown_header("At The Rim | Individual Breakdown"),
            Spacer(1, 0.1 * inch),
            self._create_player_summary("atr"),
            Spacer(1, 0.1 * inch),
            self._create_breakdown_table("atr"),
            Spacer(1, 0.1 * inch),
            self._create_footer(),
        ]
        return elements

    def _create_2fg_page(self):
        elements = [
            self._create_breakdown_header("Non-ATR 2FG | Individual Breakdown"),
            Spacer(1, 0.1 * inch),
            self._create_player_summary("2fg"),
            Spacer(1, 0.1 * inch),
            self._create_breakdown_table("2fg"),
            Spacer(1, 0.1 * inch),
            self._create_footer(),
        ]
        return elements

    def _create_3fg_page(self):
        elements = [
            self._create_breakdown_header("3FG Shots | Individual Breakdown"),
            Spacer(1, 0.1 * inch),
            self._create_player_summary("3fg"),
            Spacer(1, 0.1 * inch),
            self._create_breakdown_table("3fg"),
            Spacer(1, 0.1 * inch),
            self._create_footer(),
        ]
        return elements
