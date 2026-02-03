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
        self.margin = 0
        self.base_font_size = 8.5
        self.header_font_size = 10
        self.section_header_font_size = 9
        self.totals_strip_font_size = 8.5
        self.row_height = 12
        self.vert_padding = 2
        self.horiz_padding = 4
        self.section_space_before = 6
        self.section_space_after = 4
        self.line_height = 1.0
        self.header_spacer = 0.05 * inch
        self.summary_spacer = 0.08 * inch
        self.columns_footer_spacer = 0.05 * inch

        self.crimson = colors.HexColor("#9E1B32")
        self.green = colors.HexColor("#90EE90")
        self.red = colors.HexColor("#FFB6C1")
        self.tan = colors.HexColor("#F5DEB3")
        self.light_gray = colors.HexColor("#E0E0E0")
        self.medium_gray = colors.HexColor("#828A8F")

        self.atr_breakdown_order = [
            "Assisted",
            "Non-Assisted",
            "Dribble",
            "No Dribble",
            "Restricted Area",
            "Non Restricted Area",
            "Off 1 Foot",
            "Off 2 Feet",
            "Left Hand Finish",
            "Right Hand Finish",
            "Hands To Rim",
            "Hands Away From Rim",
            "Play Action",
            "No Play Action",
            "Primary Defender",
            "Secondary Defender",
            "Multiple Defenders",
            "Unguarded",
            "Dunk",
            "Layup",
            "Floater",
            "Blocked",
        ]
        self.atr_off_dribble_order = [
            "Baseline Drive",
            "Middle Drive",
            "Slot Drive",
            "Drive Left",
            "Drive Right",
            "Beast / Post",
            "DHO / Get",
            "PnR Handler",
            "PnR Sneak",
            "Off Closeout",
            "Iso",
            "OREB Putback",
            "Transition Push",
        ]
        self.off_pass_type_order = [
            "Perimeter Pass",
            "Swing",
            "1 More",
            "Skip",
            "Pass Out",
            "Check Down",
            "Lift",
            "Drift",
            "Kickdown",
            "Slot Skip",
            "Nail Pitch",
            "Shake",
            "Pull Behind",
            "Dagger",
            "Pocket Extra (Out)",
            "Post Pass Out",
            "Pass In",
            "Slash / Cut",
            "Pocket Extra (In)",
            "Dump Off",
            "Lob",
            "Post Entry",
            "PnR Pass to Screener",
            "PnR Pocket",
            "PnR Lob",
            "PnR Late Roll",
            "PnR Pop",
            "Off Screen Pass",
            "Handoff",
            "Transition Pass",
            "Outlet",
            "Cross Court",
            "Kickahead",
            "Inbound Pass",
            "Under OB",
            "Side / Press OB",
        ]
        self.non_atr_breakdown_order = [
            "Assisted",
            "Non-Assisted",
            "Dribble",
            "No Dribble",
            "Restricted Area",
            "Non Restricted Area",
            "Off 1 Foot",
            "Off 2 Feet",
            "Left Hand Finish",
            "Right Hand Finish",
            "Hands To Rim",
            "Hands Away From Rim",
            "Play Action",
            "No Play Action",
            "Primary Defender",
            "Secondary Defender",
            "Multiple Defenders",
            "Unguarded",
            "Layup",
            "Floater",
            "Turnaround J",
            "Pull Up",
            "Step Back",
            "Catch",
        ]
        self.three_breakdown_order = [
            "Assisted",
            "Non-Assisted",
            "Catch and Shoot",
            "Catch and Hold",
            "Slide Dribble",
            "Step Back",
            "Pull Up",
            "On The Line",
            "Off The Line",
            "Shot Pocket",
            "Non-Shot Pocket",
            "Stationary",
            "On Move",
            "Contested",
            "Uncontested",
            "Late Contest",
            "Blocked",
            "Hop",
            "Right-Left",
            "Left-Right",
            "WTN Right-Left",
            "WTN Left-Right",
            "Shrink",
            "Non-Shrink",
        ]
        self.three_off_dribble_order = [
            "Drive Left",
            "Drive Right",
            "Dip",
            "Beast / Post",
            "DHO / Get",
            "PnR Handler",
            "PnR Sneak",
            "Off Closeout",
            "Iso",
            "OREB Putback",
            "Transition Push",
        ]

    def generate(self) -> bytes:
        """Build all four pages and return PDF bytes."""
        self._validate_layout()
        doc = SimpleDocTemplate(
            self.buffer,
            pagesize=self.pagesize,
            rightMargin=self.margin,
            leftMargin=self.margin,
            topMargin=self.margin,
            bottomMargin=self.margin,
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
        elements.append(self._create_stat_strip())
        elements.append(Spacer(1, 0.2 * inch))

        shot_type_totals = self.player_data.get("shot_type_totals")
        atr_totals = getattr(shot_type_totals, "atr", None) if shot_type_totals else None
        fg2_totals = getattr(shot_type_totals, "fg2", None) if shot_type_totals else None
        fg3_totals = getattr(shot_type_totals, "fg3", None) if shot_type_totals else None
        atr_fill = self._grade_fill("atr2fg_pct", getattr(atr_totals, "fg_pct", 0)) or self.light_gray
        fg2_fill = self._grade_fill("fg2_pct", getattr(fg2_totals, "fg_pct", 0)) or self.light_gray
        fg3_fill = self._grade_fill("fg3_pct", getattr(fg3_totals, "fg_pct", 0)) or self.light_gray
        summary_boxes = Table(
            [
                [
                    self._create_summary_box("ATR", atr_totals, atr_fill),
                    self._create_summary_box("2FG", fg2_totals, fg2_fill),
                    self._create_summary_box("3FG", fg3_totals, fg3_fill),
                ]
            ],
            colWidths=[2.2 * inch] * 3,
        )
        summary_boxes.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
        elements.append(summary_boxes)
        elements.append(Spacer(1, 0.25 * inch))
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

    def _create_stat_strip(self):
        stats = self.player_data.get("season_stats", {}) or {}
        ft_pct = stats.get("ft_pct")
        ts_pct = stats.get("ts_pct")
        pps = stats.get("pps")
        efg_pct = stats.get("efg_pct")
        rows = [
            ["FT%", "TS%", "PPS", "EFG%"],
            [
                self._format_pct(ft_pct) if ft_pct is not None else "—",
                self._format_pct(ts_pct) if ts_pct is not None else "—",
                f"{pps:.2f}" if pps is not None else "—",
                self._format_pct(efg_pct) if efg_pct is not None else "—",
            ],
        ]
        table = Table(rows, colWidths=[1.7 * inch] * 4)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), self.light_gray),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                    ("TOPPADDING", (0, 0), (-1, -1), 2),
                ]
            )
        )
        return table

    def _create_shot_chart(
        self,
        title: str,
        zone_data: Mapping[str, Mapping[str, object]],
        metric_key: str = "fg2_pct",
    ):
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
            fill = self._grade_fill(metric_key, pct) or self.light_gray
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
        return ParagraphStyle(
            "page_header",
            fontSize=self.header_font_size,
            leading=self.header_font_size * self.line_height,
            textColor=self.crimson,
            alignment=TA_CENTER,
        )

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
            rowHeights=[self.row_height],
        )
        header_table.setStyle(
            TableStyle(
                [
                    ("ALIGN", (0, 0), (0, 0), "LEFT"),
                    ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                    ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), self.header_font_size),
                    ("LEADING", (0, 0), (-1, -1), self.header_font_size * self.line_height),
                    ("TEXTCOLOR", (0, 0), (0, 0), self.crimson),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), self.vert_padding),
                    ("TOPPADDING", (0, 0), (-1, -1), self.vert_padding),
                    ("LEFTPADDING", (0, 0), (-1, -1), self.horiz_padding),
                    ("RIGHTPADDING", (0, 0), (-1, -1), self.horiz_padding),
                ]
            )
        )
        return header_table

    def _build_breakdown_lookup(self, shot_type: str):
        shot_summaries = self.player_data.get("shot_summaries", {})
        summary_key = {"atr": "atr", "2fg": "fg2", "3fg": "fg3"}.get(shot_type, shot_type)
        summary = shot_summaries.get(summary_key)
        breakdown = summary.cats if summary else {}
        empty_bucket = SimpleNamespace(
            total=SimpleNamespace(attempts=0, makes=0, fg_pct=0, pps=0, freq_pct=0),
            transition=SimpleNamespace(attempts=0, makes=0, fg_pct=0, pps=0, freq_pct=0),
            halfcourt=SimpleNamespace(attempts=0, makes=0, fg_pct=0, pps=0, freq_pct=0),
        )
        return breakdown, empty_bucket

    def _create_section_table(self, title, labels, breakdown, empty_bucket, col_width, grade_metric):
        header_row = ["", "FGA", "FG%", "PPS", "Freq"]
        rows = [[title, "", "", "", ""], header_row]
        for label in labels:
            bucket = breakdown.get(label, empty_bucket)
            total = bucket.total
            rows.append(
                [
                    label,
                    f"{total.makes}-{total.attempts}",
                    f"{total.fg_pct:.1f}%",
                    f"{total.pps:.2f}",
                    f"{total.freq_pct:.1f}%" if total.attempts else "—",
                ]
            )

        label_width = 0.54 * col_width
        stat_width = (col_width - label_width) / 4
        table = Table(rows, colWidths=[label_width] + [stat_width] * 4, rowHeights=[self.row_height] * len(rows))
        style_commands = [
            ("SPAN", (0, 0), (-1, 0)),
            ("BACKGROUND", (0, 0), (-1, 0), self.crimson),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("BACKGROUND", (0, 1), (-1, 1), self.light_gray),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("ALIGN", (0, 2), (0, -1), "LEFT"),
            ("FONTNAME", (0, 0), (-1, 1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), self.section_header_font_size),
            ("FONTSIZE", (0, 1), (-1, -1), self.base_font_size),
            ("LEADING", (0, 0), (-1, 0), self.section_header_font_size * self.line_height),
            ("LEADING", (0, 1), (-1, -1), self.base_font_size * self.line_height),
            ("GRID", (0, 0), (-1, -1), 0.35, colors.black),
            ("BOTTOMPADDING", (0, 0), (-1, -1), self.vert_padding),
            ("TOPPADDING", (0, 0), (-1, -1), self.vert_padding),
            ("LEFTPADDING", (0, 0), (-1, -1), self.horiz_padding),
            ("RIGHTPADDING", (0, 0), (-1, -1), self.horiz_padding),
        ]
        if grade_metric:
            for row_idx, label in enumerate(labels, start=2):
                bucket = breakdown.get(label, empty_bucket)
                fg_fill = self._grade_fill(grade_metric, getattr(bucket.total, "fg_pct", 0))
                if fg_fill:
                    style_commands.append(("BACKGROUND", (2, row_idx), (2, row_idx), fg_fill))
                pps_fill = self._grade_fill("pps", getattr(bucket.total, "pps", 0))
                if pps_fill:
                    style_commands.append(("BACKGROUND", (3, row_idx), (3, row_idx), pps_fill))
        table.setStyle(TableStyle(style_commands))
        return table

    def _create_columns_layout(self, shot_type, left_sections, right_sections, max_pass_rows=None):
        usable_width = self.width - (2 * self.margin)
        col_width = usable_width / 2
        col_height = self._available_column_height()

        breakdown, empty_bucket = self._build_breakdown_lookup(shot_type)
        grade_metric = {"atr": "atr2fg_pct", "2fg": "fg2_pct", "3fg": "fg3_pct"}.get(shot_type)

        def build_section(title, labels):
            if title == "OFF PASS TYPE" and max_pass_rows:
                labels = labels[:max_pass_rows]
            return self._create_section_table(title, labels, breakdown, empty_bucket, col_width, grade_metric), labels

        left_blocks = [build_section(title, labels) for title, labels in left_sections]
        right_blocks = [build_section(title, labels) for title, labels in right_sections]

        left_content = self._stack_sections(left_blocks)
        right_content = self._stack_sections(right_blocks)

        table = Table(
            [[left_content, right_content]],
            colWidths=[col_width, col_width],
            rowHeights=[col_height],
        )
        table.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                    ("TOPPADDING", (0, 0), (-1, -1), 0),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ]
            )
        )
        return table

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
            rowHeights=[self.row_height] * 3,
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
            ("FONTSIZE", (0, 0), (-1, -1), self.totals_strip_font_size),
            ("LEADING", (0, 0), (-1, -1), self.totals_strip_font_size * self.line_height),
            ("BOTTOMPADDING", (0, 0), (-1, -1), self.vert_padding),
            ("TOPPADDING", (0, 0), (-1, -1), self.vert_padding),
            ("LEFTPADDING", (0, 0), (-1, -1), self.horiz_padding),
            ("RIGHTPADDING", (0, 0), (-1, -1), self.horiz_padding),
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
            row_heights.append(self.row_height)
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
            ("FONTSIZE", (0, 0), (-1, 0), self.section_header_font_size),
            ("FONTSIZE", (0, 1), (-1, -1), self.base_font_size),
            ("LEADING", (0, 0), (-1, 0), self.section_header_font_size * self.line_height),
            ("LEADING", (0, 1), (-1, -1), self.base_font_size * self.line_height),
            ("BOTTOMPADDING", (0, 0), (-1, -1), self.vert_padding),
            ("TOPPADDING", (0, 0), (-1, -1), self.vert_padding),
            ("LEFTPADDING", (0, 0), (-1, -1), self.horiz_padding),
            ("RIGHTPADDING", (0, 0), (-1, -1), self.horiz_padding),
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
            Spacer(1, self.header_spacer),
            self._create_player_summary("atr"),
            Spacer(1, self.summary_spacer),
            self._create_columns_layout(
                "atr",
                left_sections=[
                    ("BREAKDOWN", self.atr_breakdown_order),
                    ("OFF DRIBBLE TYPE", self.atr_off_dribble_order),
                ],
                right_sections=[
                    ("OFF PASS TYPE", self.off_pass_type_order),
                ],
            ),
            Spacer(1, self.columns_footer_spacer),
            self._create_footer(),
        ]
        return elements

    def _create_2fg_page(self):
        elements = [
            self._create_breakdown_header("Non-ATR 2FG | Individual Breakdown"),
            Spacer(1, self.header_spacer),
            self._create_player_summary("2fg"),
            Spacer(1, self.summary_spacer),
            self._create_columns_layout(
                "2fg",
                left_sections=[
                    ("BREAKDOWN", self.non_atr_breakdown_order),
                    ("OFF DRIBBLE TYPE", self.atr_off_dribble_order),
                ],
                right_sections=[
                    ("OFF PASS TYPE", self.off_pass_type_order),
                ],
            ),
            Spacer(1, self.columns_footer_spacer),
            self._create_footer(),
        ]
        return elements

    def _create_3fg_page(self):
        elements = [
            self._create_breakdown_header("3FG Shots | Individual Breakdown"),
            Spacer(1, self.header_spacer),
            self._create_player_summary("3fg"),
            Spacer(1, self.summary_spacer),
            self._create_columns_layout(
                "3fg",
                left_sections=[
                    ("BREAKDOWN", self.three_breakdown_order),
                    ("OFF DRIBBLE TYPE", self.three_off_dribble_order),
                ],
                right_sections=[
                    ("OFF PASS TYPE", self.off_pass_type_order),
                ],
                max_pass_rows=24,
            ),
            Spacer(1, self.columns_footer_spacer),
            self._create_footer(),
        ]
        return elements

    def _available_column_height(self) -> float:
        content_height = self.height - (2 * self.margin)
        fixed_height = (
            self.row_height
            + self.header_spacer
            + (self.row_height * 3)
            + self.summary_spacer
            + self.columns_footer_spacer
            + (self.header_font_size * self.line_height)
        )
        return content_height - fixed_height

    def _stack_sections(self, sections):
        flowables = []
        for table, _labels in sections:
            flowables.append(Spacer(1, self.section_space_before))
            flowables.append(table)
            flowables.append(Spacer(1, self.section_space_after))
        return flowables

    def _section_height(self, label_count: int) -> float:
        return self.section_space_before + self.section_space_after + (self.row_height * (label_count + 2))

    def _validate_layout(self) -> None:
        story = self.get_story_elements()
        page_breaks = sum(isinstance(item, PageBreak) for item in story)
        if page_breaks != 3:
            raise ValueError("Shot type report must contain exactly 4 pages.")
        for shot_type, left_sections, right_sections, max_pass_rows in (
            ("atr", [("BREAKDOWN", self.atr_breakdown_order), ("OFF DRIBBLE TYPE", self.atr_off_dribble_order)], [("OFF PASS TYPE", self.off_pass_type_order)], None),
            ("2fg", [("BREAKDOWN", self.non_atr_breakdown_order), ("OFF DRIBBLE TYPE", self.atr_off_dribble_order)], [("OFF PASS TYPE", self.off_pass_type_order)], None),
            ("3fg", [("BREAKDOWN", self.three_breakdown_order), ("OFF DRIBBLE TYPE", self.three_off_dribble_order)], [("OFF PASS TYPE", self.off_pass_type_order)], 24),
        ):
            col_height = self._available_column_height()
            if col_height <= 0:
                raise ValueError(f"{shot_type} layout exceeds page height.")
            left_height = 0
            for title, labels in left_sections:
                if title == "OFF PASS TYPE" and max_pass_rows:
                    labels = labels[:max_pass_rows]
                left_height += self._section_height(len(labels))
            right_height = 0
            for title, labels in right_sections:
                if title == "OFF PASS TYPE" and max_pass_rows:
                    labels = labels[:max_pass_rows]
                right_height += self._section_height(len(labels))
            if left_height > col_height or right_height > col_height:
                raise ValueError(f"{shot_type} column content exceeds available height.")
