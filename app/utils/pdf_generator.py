"""PDF generator for player shot type reports - IMPROVED STYLING VERSION."""

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
from reportlab.pdfgen import canvas
from reportlab.platypus import Spacer, Table, TableStyle, Paragraph
from app.grades import grade_token


class ShotTypeReportGenerator:
    """Generate a four-page PDF report for a player with enhanced styling."""

    _base_court_drawing: Drawing | None = None

    def __init__(self, player_data: Mapping[str, object]):
        self.player_data = player_data
        self.buffer = BytesIO()
        self.pagesize = letter
        self.width, self.height = self.pagesize
        self.margin = 0
        self.atr_margin = 0.15 * inch

        # IMPROVED: Slightly larger base font for better readability
        self.base_font_size = 7.5
        self.header_font_size = 10
        self.header_title_font_size = 13  # Increased from 12.5
        self.header_subtitle_font_size = 11
        self.section_header_font_size = 8.5  # Increased from 9
        self.totals_strip_font_size = 9
        
        # IMPROVED: Better row heights for breathing room (balanced for page fit)
        self.row_height = 9.5  # Slightly increased from 11 (was 11.5, reduced for fit)
        self.header_row_height = self.row_height * 1.4
        self.totals_row_height = self.row_height * 1.5
        self.breakdown_row_height = self.row_height * 1.15
        self.off_pass_row_height = self.row_height * 1.1
        
        # IMPROVED: Better padding for visual comfort (balanced for page fit)
        self.vert_padding = 0.8  # Increased from 1 (was 1.5, reduced for fit)
        self.horiz_padding = 2  # Slightly tighter for cleaner numeric columns
        self.header_vert_padding = self.vert_padding + 3
        
        self.section_space_before = 6
        self.section_space_after = 6
        self.section_space_before_tight = max(0, self.section_space_before - 4)
        self.section_space_after_tight = max(0, self.section_space_after - 4)
        self.off_dribble_extra_space = 4
        self.off_pass_extra_space = 0
        self.group_spacing = 2
        self.section_label_font_scale = 0.85
        self.line_height = 1.0
        self.header_spacer = 0.05 * inch
        self.summary_spacer = 0.12 * inch
        self.columns_footer_spacer = 0.1 * inch
        self.atr_summary_spacer = 0.08 * inch
        self.atr_row_height_scale = 1.0
        self.atr_section_space_before = self.section_space_before
        self.atr_section_space_after = self.section_space_after
        self.atr_section_header_font_size = self.section_header_font_size + 0.6
        self.atr_section_header_padding = self.vert_padding + 3
        self.atr_color_soften_factor = 0.6
        self.atr_column_gutter = 12

        # IMPROVED: Enhanced color palette with better contrast and professionalism
        self.crimson = colors.HexColor("#9E1B32")
        self.crimson_dark = colors.HexColor("#7A1426")  # NEW: Darker crimson for depth
        self.green = colors.HexColor("#90EE90")
        self.red = colors.HexColor("#FFB6C1")
        self.tan = colors.HexColor("#F5DEB3")
        self.light_gray = colors.HexColor("#E8E8E8")  # Slightly lighter
        self.very_light_gray = colors.HexColor("#FCFCFC")  # Even lighter for subtle zebra
        self.totals_header_gray = colors.HexColor("#D8D8D8")  # Slightly lighter
        self.medium_gray = colors.HexColor("#6B7278")  # Darker for better contrast
        self.freq_text_gray = colors.HexColor("#8B9299")  # Adjusted for readability
        self.border_color = colors.HexColor("#CCCCCC")  # NEW: Softer borders

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
        pdf_canvas = canvas.Canvas(self.buffer, pagesize=self.pagesize)
        self._render_cover_page(pdf_canvas)
        pdf_canvas.showPage()
        self._render_atr_page(pdf_canvas)
        pdf_canvas.showPage()
        self._render_2fg_page(pdf_canvas)
        pdf_canvas.showPage()
        self._render_3fg_page(pdf_canvas)
        pdf_canvas.save()
        pdf_content = self.buffer.getvalue()
        self.buffer.close()
        return pdf_content

    def get_story_elements(self):
        """Return the report's page elements without building a PDF."""
        return self._page_sequence()

    def _page_sequence(self):
        return [
            self._create_cover_page(),
            self._create_atr_page(),
            self._create_2fg_page(),
            self._create_3fg_page(),
        ]

    def _render_page(self, pdf_canvas: canvas.Canvas, elements, margin: float | None = None):
        page_margin = self.margin if margin is None else margin
        usable_width = self.width - (2 * page_margin)
        cursor_y = self.height - page_margin
        for element in elements:
            _, element_height = element.wrap(usable_width, cursor_y)
            if cursor_y - element_height < page_margin:
                raise ValueError("Page content exceeds available height.")
            cursor_y -= element_height
            element.drawOn(pdf_canvas, page_margin, cursor_y)

    def _render_cover_page(self, pdf_canvas: canvas.Canvas) -> None:
        self._render_page(pdf_canvas, self._create_cover_page())

    def _render_atr_page(self, pdf_canvas: canvas.Canvas) -> None:
        self._render_page(pdf_canvas, self._create_atr_page(), margin=self.margin + self.atr_margin)

    def _render_2fg_page(self, pdf_canvas: canvas.Canvas) -> None:
        self._render_page(pdf_canvas, self._create_2fg_page())

    def _render_3fg_page(self, pdf_canvas: canvas.Canvas) -> None:
        self._render_page(pdf_canvas, self._create_3fg_page())

    def _create_cover_page(self):
        elements = []
        # IMPROVED: Better brand header with letter-spacing effect
        elements.append(
            Paragraph(
                "ALABAMA CRIMSON TIDE",
                ParagraphStyle(
                    "cover_brand",
                    fontSize=14.5,  # Slightly larger
                    textColor=self.crimson,
                    alignment=TA_CENTER,
                    spaceAfter=8,
                    fontName="Helvetica-Bold",  # Bold for brand
                ),
            )
        )
        header = Paragraph(
            f"{self.player_data.get('season', '')} Season Totals",
            ParagraphStyle(
                "season_header",
                fontSize=16.5,  # Slightly larger
                textColor=self.medium_gray,
                alignment=TA_CENTER,
                spaceAfter=12,
            ),
        )
        elements.append(header)

        number = self.player_data.get("number") or ""
        name = self.player_data.get("name") or "N/A"
        player_name = f"#{number} {name}".strip().replace("# ", "")
        # IMPROVED: Larger, bolder player name
        elements.append(
            Paragraph(
                player_name,
                ParagraphStyle(
                    "player_name",
                    fontSize=38,  # Increased from 36
                    alignment=TA_CENTER,
                    spaceAfter=10,
                    fontName="Helvetica-Bold",  # Bold
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
        atr_fill = self._soften_color(self._grade_fill("atr2fg_pct", getattr(atr_totals, "fg_pct", 0))) or self.light_gray
        fg2_fill = self._soften_color(self._grade_fill("fg2_pct", getattr(fg2_totals, "fg_pct", 0))) or self.light_gray
        fg3_fill = self._soften_color(self._grade_fill("fg3_pct", getattr(fg3_totals, "fg_pct", 0))) or self.light_gray
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
        summary_boxes._is_main_content = True
        elements.append(summary_boxes)
        elements.append(Spacer(1, 0.25 * inch))
        elements.append(self._create_footer(1, "Season Totals"))
        return elements

    def _create_summary_box(self, title, data, bgcolor):
        makes = getattr(data, "makes", 0) if data else 0
        attempts = getattr(data, "attempts", 0) if data else 0
        fg_pct = getattr(data, "fg_pct", 0) if data else 0
        pps = getattr(data, "pps", 0) if data else 0
        freq = getattr(data, "freq", 0) if data else 0
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
        # IMPROVED: Better borders and styling for summary boxes
        box.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), bgcolor),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
                    ("FONTNAME", (2, 2), (2, 2), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 7.5),  # Slightly larger
                    ("FONTSIZE", (3, 2), (3, 2), 8),
                    ("TEXTCOLOR", (0, 2), (0, 2), self.medium_gray),
                    ("TEXTCOLOR", (3, 2), (3, 2), self.freq_text_gray),
                    ("GRID", (0, 0), (-1, -1), 0.8, self.border_color),  # Thicker, softer borders
                    ("LINEBELOW", (0, 0), (-1, 0), 1.2, self.crimson),  # Crimson accent line
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
        # IMPROVED: Better borders and header styling
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), self.light_gray),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 8.5),  # Slightly larger
                    ("FONTSIZE", (0, 1), (-1, 1), 9.5),  # Larger values
                    ("GRID", (0, 0), (-1, -1), 0.8, self.border_color),  # Softer borders
                    ("LINEBELOW", (0, 0), (-1, 0), 1.2, self.crimson_dark),  # Dark crimson accent
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),  # Balanced padding
                    ("TOPPADDING", (0, 0), (-1, -1), 2.5),
                ]
            )
        )
        table._is_main_content = True
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
            fill = self._soften_color(self._grade_fill(metric_key, pct), 0.35) or self.light_gray
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

    def _create_footer(self, page_number: int, shot_label: str):
        footer_table = Table(
            [[f"Page {page_number} of 4", shot_label, "BAMALYTICS"]],
            colWidths=[2.5 * inch, 3.5 * inch, 2.5 * inch],
        )
        # IMPROVED: Better footer styling with accent line
        footer_table.setStyle(
            TableStyle(
                [
                    ("LINEABOVE", (0, 0), (-1, 0), 1, self.crimson),  # Crimson line instead of gray
                    ("ALIGN", (0, 0), (0, 0), "LEFT"),
                    ("ALIGN", (1, 0), (1, 0), "CENTER"),
                    ("ALIGN", (2, 0), (2, 0), "RIGHT"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica"),
                    ("FONTSIZE", (0, 0), (-1, 0), 8),
                    ("TEXTCOLOR", (0, 0), (-1, 0), self.medium_gray),
                    ("TEXTCOLOR", (2, 0), (2, 0), self.crimson),  # Crimson for branding
                    ("TOPPADDING", (0, 0), (-1, 0), 3.5),  # Balanced padding
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                ]
            )
        )
        return footer_table

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

    @staticmethod
    def _soften_color(color: colors.Color, factor: float = 0.45) -> colors.Color:
        """Blend a color with white to reduce saturation."""
        if color is None:
            return color
        factor = max(0.0, min(factor, 1.0))
        red = color.red + (1 - color.red) * factor
        green = color.green + (1 - color.green) * factor
        blue = color.blue + (1 - color.blue) * factor
        return colors.Color(red, green, blue)

    def _create_breakdown_header(self, title: str):
        header_table = Table(
            [
                ["ALABAMA CRIMSON TIDE"],
                [title],
            ],
            colWidths=[7 * inch],
            rowHeights=[8, 14],
        )
        header_table.setStyle(
            TableStyle([
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                
                # Brand (subtle)
                ("FONTNAME", (0, 0), (0, 0), "Helvetica"),
                ("FONTSIZE", (0, 0), (0, 0), 9),
                ("TEXTCOLOR", (0, 0), (0, 0), self.medium_gray),
                ("BOTTOMPADDING", (0, 0), (0, 0), 2),
                
                # Page title (primary)
                ("FONTNAME", (0, 1), (0, 1), "Helvetica-Bold"),
                ("FONTSIZE", (0, 1), (0, 1), 14),
                ("TEXTCOLOR", (0, 1), (0, 1), self.crimson),
                ("TOPPADDING", (0, 1), (0, 1), 2),
                ("BOTTOMPADDING", (0, 1), (0, 1), 4),
                
                # Accent line
                ("LINEBELOW", (0, 1), (0, 1), 1.5, self.crimson),
            ])
        )
        header_table._is_header = True
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

    def _create_section_table(
        self,
        title,
        labels,
        breakdown,
        empty_bucket,
        col_width,
        grade_metric,
        row_height,
        label_font_scale=1.0,
        group_breaks=None,
        section_header_font_size=None,
        header_padding=None,
        color_soften_factor=None,
        zero_attempts_style=False,
    ):
        header_row = ["", "FGA", "FG%", "PPS", "Freq"]
        rows = [[title, "", "", "", ""], header_row]
        attempts_by_row = []
        for label in labels:
            bucket = breakdown.get(label, empty_bucket)
            total = bucket.total
            attempts_by_row.append(getattr(total, "attempts", 0))
            rows.append(
                [
                    label,
                    f"{total.makes}-{total.attempts}",
                    f"{total.fg_pct:.1f}%",
                    f"{total.pps:.2f}",
                    f"{total.freq_pct:.1f}%" if total.attempts else "—",
                ]
            )

        label_width = 0.6 * col_width
        stat_width = (col_width - label_width) / 4
        header_height = row_height * 1.15
        section_header_font_size = section_header_font_size or self.section_header_font_size
        header_padding = self.vert_padding + 2 if header_padding is None else header_padding
        table = Table(
            rows,
            colWidths=[label_width] + [stat_width] * 4,
            rowHeights=[header_height] + [row_height] * (len(rows) - 1),
        )
        # IMPROVED: Better section table styling with rounded corners effect
        style_commands = [
            ("SPAN", (0, 0), (-1, 0)),
            ("BACKGROUND", (0, 0), (-1, 0), colors.white),
            ("TEXTCOLOR", (0, 0), (-1, 0), self.crimson),
            ("BACKGROUND", (0, 1), (-1, 1), self.light_gray),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("ALIGN", (0, 2), (0, -1), "LEFT"),
            ("FONTNAME", (0, 0), (-1, 1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), section_header_font_size),
            ("FONTSIZE", (0, 1), (-1, -1), self.base_font_size),
            ("FONTSIZE", (0, 2), (0, -1), self.base_font_size * label_font_scale),
            ("LEADING", (0, 0), (-1, 0), section_header_font_size * self.line_height),
            ("LEADING", (0, 1), (-1, -1), self.base_font_size * self.line_height),
            ("LINEABOVE", (0, 0), (-1, 0), 1.2, self.crimson),
            ("LINEBELOW", (0, 1), (-1, 1), 0.6, self.border_color),
            ("BOX", (0, 0), (-1, -1), 0.8, self.border_color),
            ("BOTTOMPADDING", (0, 0), (-1, -1), self.vert_padding),
            ("TOPPADDING", (0, 0), (-1, -1), self.vert_padding),
            ("TOPPADDING", (0, 0), (-1, 0), header_padding),
            ("BOTTOMPADDING", (0, 0), (-1, 0), header_padding),
            ("LEFTPADDING", (0, 0), (-1, -1), self.horiz_padding),
            ("RIGHTPADDING", (0, 0), (-1, -1), self.horiz_padding),
            ("TEXTCOLOR", (1, 2), (1, -1), self.medium_gray),
            ("TEXTCOLOR", (4, 2), (4, -1), self.freq_text_gray),
            ("FONTSIZE", (4, 2), (4, -1), 8),
            ("FONTNAME", (3, 2), (3, -1), "Helvetica-Bold"),
        ]
        if group_breaks:
            for row_idx in group_breaks:
                style_commands.append(("BOTTOMPADDING", (0, row_idx), (-1, row_idx), self.group_spacing + 0.5))  # Balanced spacing
        for idx, _attempts in enumerate(attempts_by_row, start=2):
            if (idx - 2) % 2 == 1:
                style_commands.append(("BACKGROUND", (0, idx), (-1, idx), self.very_light_gray))
        if grade_metric:
            for row_idx, label in enumerate(labels, start=2):
                if attempts_by_row[row_idx - 2] == 0 and zero_attempts_style:
                    continue
        
                bucket = breakdown.get(label, empty_bucket)
        
                # PPS coloring ONLY (single visual signal)
                pps_fill = self._grade_fill("pps", getattr(bucket.total, "pps", 0))
                if pps_fill and color_soften_factor is not None:
                    pps_fill = self._soften_color(pps_fill, factor=color_soften_factor)
                if pps_fill:
                    style_commands.append(("BACKGROUND", (3, row_idx), (3, row_idx), pps_fill))
        
        if zero_attempts_style:
            for row_idx, attempts in enumerate(attempts_by_row, start=2):
                if attempts == 0:
                    style_commands.append(("TEXTCOLOR", (0, row_idx), (-1, row_idx), self.freq_text_gray))
                    style_commands.append(("BACKGROUND", (0, row_idx), (-1, row_idx), self.very_light_gray))
        
        table.setStyle(TableStyle(style_commands))
        return table


    def _breakdown_group_breaks(self, shot_type: str, labels):
        group_sizes_map = {
            "atr": [4, 8, 2, 4, 4],
            "2fg": [2, 2, 2, 2, 4, 2, 4, 6],
            "3fg": [2, 5, 2, 2, 2, 4, 5, 2],
        }
        group_sizes = group_sizes_map.get(shot_type, [])
        break_indices = []
        row_cursor = 2
        label_cursor = 0
        for size in group_sizes:
            label_cursor += size
            if label_cursor >= len(labels):
                break
            row_cursor += size
            break_indices.append(row_cursor - 1)
        return break_indices

    def _create_columns_layout(
        self,
        shot_type,
        left_sections,
        right_sections,
        max_pass_rows=None,
        section_space_before=None,
        section_space_after=None,
        margin=None,
        row_height_scale=1.0,
        section_header_font_size=None,
        header_padding=None,
        color_soften_factor=None,
        zero_attempts_style=False,
        gutter=None,
    ):
        page_margin = self.margin if margin is None else margin
        usable_width = self.width - (2 * page_margin)
        col_width = usable_width / 2

        breakdown, empty_bucket = self._build_breakdown_lookup(shot_type)
        grade_metric = {"atr": "atr2fg_pct", "2fg": "fg2_pct", "3fg": "fg3_pct"}.get(shot_type)

        def build_section(title, labels):
            if title == "OFF PASS TYPE" and max_pass_rows:
                labels = labels[:max_pass_rows]
            if title == "BREAKDOWN":
                table_width = col_width
                row_height = self.breakdown_row_height * row_height_scale
                group_breaks = self._breakdown_group_breaks(shot_type, labels)
            elif title == "OFF DRIBBLE TYPE":
                table_width = col_width
                row_height = self.breakdown_row_height * row_height_scale
                group_breaks = None
            else:
                table_width = col_width
                row_height = self.off_pass_row_height * row_height_scale
                group_breaks = None
            table = self._create_section_table(
                title,
                labels,
                breakdown,
                empty_bucket,
                table_width,
                grade_metric,
                row_height,
                label_font_scale=self.section_label_font_scale,
                group_breaks=group_breaks,
                section_header_font_size=section_header_font_size,
                header_padding=header_padding,
                color_soften_factor=color_soften_factor,
                zero_attempts_style=zero_attempts_style,
            )
            table.hAlign = "CENTER"
            return table, labels

        left_blocks = []
        for title, labels in left_sections:
            left_blocks.append(build_section(title, labels))
        right_blocks = []
        for title, labels in right_sections:
            right_blocks.append(build_section(title, labels))

        left_content = self._stack_sections(
            left_blocks,
            extra_spacing={"OFF DRIBBLE TYPE": self.off_dribble_extra_space},
            section_space_before=section_space_before,
            section_space_after=section_space_after,
        )
        right_content = self._stack_sections(
            right_blocks,
            extra_spacing={"OFF PASS TYPE": self.off_pass_extra_space},
            section_space_before=section_space_before,
            section_space_after=section_space_after,
        )

        table = Table(
            [[left_content, right_content]],
            colWidths=[col_width, col_width],
        )
        gutter = 6 if gutter is None else gutter
        table.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (0, 0), gutter),
                    ("LEFTPADDING", (1, 0), (1, 0), gutter),
                    ("TOPPADDING", (0, 0), (-1, -1), 0),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ]
            )
        )
        table._is_main_content = True
        return table

    def _create_player_summary(
        self,
        shot_type: str,
        margin: float | None = None,
        row_height: float | None = None,
        color_soften_factor: float | None = None,
    ):
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
        page_margin = self.margin if margin is None else margin
        usable_width = self.width - (2 * page_margin)
        label_width = usable_width * 0.4
        stat_width = (usable_width - label_width) / 12
        row_height = self.totals_row_height if row_height is None else row_height
        summary_table = Table(
            [
                ["", "TOTALS", "", "", "", "TRANSITION", "", "", "", "HALF COURT", "", "", ""],
                ["PLAYER", "FGA", "FG%", "PPS", "Freq%", "FGA", "FG%", "PPS", "Freq%", "FGA", "FG%", "PPS", "Freq%"],
                [
                    player_name,
                    f"{getattr(total, 'makes', 0)}-{getattr(total, 'attempts', 0)}",
                    self._format_pct(getattr(total, "fg_pct", 0)),
                    f"{getattr(total, 'pps', 0):.2f}",
                    f"{total_freq:.1f}%" if total_freq is not None else "—",
                    f"{getattr(transition, 'makes', 0)}-{getattr(transition, 'attempts', 0)}",
                    self._format_pct(getattr(transition, "fg_pct", 0)),
                    f"{getattr(transition, 'pps', 0):.2f}",
                    "—",
                    f"{getattr(half_court, 'makes', 0)}-{getattr(half_court, 'attempts', 0)}",
                    self._format_pct(getattr(half_court, "fg_pct", 0)),
                    f"{getattr(half_court, 'pps', 0):.2f}",
                    "—",
                ],
            ],
            colWidths=[label_width] + [stat_width] * 12,
            rowHeights=[row_height] * 3,
        )
        grade_metric = {"atr": "atr2fg_pct", "fg2": "fg2_pct", "fg3": "fg3_pct"}.get(summary_key)
        # IMPROVED: Better summary table styling with clearer sections
        style_commands = [
            ("BOX", (0, 0), (-1, -1), 1, self.border_color),  # Thicker, softer border
            ("LINEBELOW", (0, 1), (-1, 1), 0.8, self.border_color),
            ("BACKGROUND", (1, 0), (4, 0), self.totals_header_gray),
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
            ("TEXTCOLOR", (1, 2), (1, 2), self.medium_gray),
            ("TEXTCOLOR", (5, 2), (5, 2), self.medium_gray),
            ("TEXTCOLOR", (9, 2), (9, 2), self.medium_gray),
            ("TEXTCOLOR", (4, 2), (4, 2), self.freq_text_gray),
            ("TEXTCOLOR", (8, 2), (8, 2), self.freq_text_gray),
            ("TEXTCOLOR", (12, 2), (12, 2), self.freq_text_gray),
            ("FONTSIZE", (4, 2), (4, 2), 8),
            ("FONTSIZE", (8, 2), (8, 2), 8),
            ("FONTSIZE", (12, 2), (12, 2), 8),
            ("FONTNAME", (3, 2), (3, 2), "Helvetica-Bold"),
            ("FONTNAME", (7, 2), (7, 2), "Helvetica-Bold"),
            ("FONTNAME", (11, 2), (11, 2), "Helvetica-Bold"),
            # IMPROVED: Add vertical separators between sections
            ("LINEBEFORE", (5, 0), (5, -1), 0.8, self.border_color),
            ("LINEBEFORE", (9, 0), (9, -1), 0.8, self.border_color),
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
                fg_soften = 0.45 if color_soften_factor is None else color_soften_factor
                fill = self._soften_color(self._grade_fill(grade_metric, value), factor=fg_soften)
                if fill:
                    style_commands.append(("BACKGROUND", (col, 2), (col, 2), fill))
        for col, value in pps_cells:
            fill = self._grade_fill("pps", value)
            if fill and color_soften_factor is not None:
                fill = self._soften_color(fill, factor=color_soften_factor)
            if fill:
                style_commands.append(("BACKGROUND", (col, 2), (col, 2), fill))
        summary_table.setStyle(TableStyle(style_commands))
        summary_table._is_main_content = True
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
            row_heights.append(self.breakdown_row_height)
        row_order = {row_idx: order for order, row_idx in enumerate(data_row_indices)}
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
            ("LINEBELOW", (0, 0), (-1, 0), 0.6, colors.white),
            ("TEXTCOLOR", (1, 2), (1, -1), self.medium_gray),
            ("TEXTCOLOR", (5, 2), (5, -1), self.medium_gray),
            ("TEXTCOLOR", (9, 2), (9, -1), self.medium_gray),
            ("TEXTCOLOR", (4, 2), (4, -1), self.freq_text_gray),
            ("TEXTCOLOR", (8, 2), (8, -1), self.freq_text_gray),
            ("TEXTCOLOR", (12, 2), (12, -1), self.freq_text_gray),
            ("FONTSIZE", (4, 2), (4, -1), 8),
            ("FONTSIZE", (8, 2), (8, -1), 8),
            ("FONTSIZE", (12, 2), (12, -1), 8),
            ("FONTNAME", (3, 2), (3, -1), "Helvetica-Bold"),
            ("FONTNAME", (7, 2), (7, -1), "Helvetica-Bold"),
            ("FONTNAME", (11, 2), (11, -1), "Helvetica-Bold"),
        ]

        for row_idx in divider_rows:
            style_commands.append(("BACKGROUND", (0, row_idx), (-1, row_idx), self.crimson))
            style_commands.append(("GRID", (0, row_idx), (-1, row_idx), 0, colors.white))
            style_commands.append(("LINEBELOW", (0, row_idx), (-1, row_idx), 0.6, colors.white))

        grade_metric = {"atr": "atr2fg_pct", "2fg": "fg2_pct", "3fg": "fg3_pct"}.get(shot_type)
        for row_idx, row_key in zip(data_row_indices, row_keys):
            for col_offset, bucket_key in ((2, "total"), (6, "transition"), (10, "halfcourt")):
                bucket = getattr(breakdown.get(row_key), bucket_key, None)
                if grade_metric:
                    fg_fill = self._soften_color(self._grade_fill(grade_metric, getattr(bucket, "fg_pct", 0)))
                    if fg_fill:
                        style_commands.append(("BACKGROUND", (col_offset, row_idx), (col_offset, row_idx), fg_fill))
                pps_fill = self._grade_fill("pps", getattr(bucket, "pps", 0))
                if pps_fill:
                    style_commands.append(("BACKGROUND", (col_offset + 1, row_idx), (col_offset + 1, row_idx), pps_fill))
            if row_order[row_idx] % 2 == 1:
                style_commands.append(("BACKGROUND", (0, row_idx), (-1, row_idx), self.very_light_gray))

        table.setStyle(TableStyle(style_commands))
        return table

    def _create_atr_page(self):
        atr_margin = self.margin + self.atr_margin
        atr_layout = self._resolve_atr_layout()
        atr_row_height = self.totals_row_height * atr_layout.row_height_scale
        elements = [
            self._create_breakdown_header("At The Rim | Individual Breakdown"),
            Spacer(1, self.header_spacer),
            self._create_player_summary(
                "atr",
                margin=atr_margin,
                row_height=atr_row_height,
                color_soften_factor=self.atr_color_soften_factor,
            ),
            Spacer(1, atr_layout.summary_spacer),
            self._create_columns_layout(
                "atr",
                left_sections=[
                    ("BREAKDOWN", self.atr_breakdown_order),
                    ("OFF DRIBBLE TYPE", self.atr_off_dribble_order),
                ],
                right_sections=[
                    ("OFF PASS TYPE", self.off_pass_type_order),
                ],
                margin=atr_margin,
                row_height_scale=atr_layout.row_height_scale,
                section_space_before=atr_layout.section_space_before,
                section_space_after=atr_layout.section_space_after,
                section_header_font_size=self.atr_section_header_font_size,
                header_padding=self.atr_section_header_padding,
                color_soften_factor=self.atr_color_soften_factor,
                zero_attempts_style=True,
                gutter=self.atr_column_gutter,
            ),
            Spacer(1, self.columns_footer_spacer),
            self._create_footer(2, "ATR"),
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
                section_space_before=self.section_space_before_tight,
                section_space_after=self.section_space_after_tight,
            ),
            Spacer(1, self.columns_footer_spacer),
            self._create_footer(3, "Non-ATR 2FG"),
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
            self._create_footer(4, "3FG"),
        ]
        return elements

    def _available_column_height(
        self,
        margin: float | None = None,
        summary_spacer: float | None = None,
        summary_row_height: float | None = None,
    ) -> float:
        page_margin = self.margin if margin is None else margin
        content_height = self.height - (2 * page_margin)
        usable_width = self.width - (2 * page_margin)
        header_height = self._create_breakdown_header("Shot Breakdown").wrap(usable_width, content_height)[1]
        summary_height = self._create_player_summary(
            "atr",
            margin=page_margin,
            row_height=summary_row_height,
        ).wrap(usable_width, content_height)[1]
        footer_height = self._create_footer(1, "Summary").wrap(usable_width, content_height)[1]
        summary_spacer = self.summary_spacer if summary_spacer is None else summary_spacer
        fixed_height = (
            header_height
            + self.header_spacer
            + summary_height
            + summary_spacer
            + self.columns_footer_spacer
            + footer_height
        )
        return content_height - fixed_height

    def _has_main_content(self, elements):
        return any(getattr(element, "_is_main_content", False) for element in elements)

    def _validate_page_content(self, pages):
        if len(pages) != 4:
            raise ValueError("Shot type report must contain exactly 4 pages.")
        for page in pages:
            if not self._has_main_content(page):
                raise ValueError("Each page must contain main content.")

    def _stack_sections(self, sections, extra_spacing=None, section_space_before=None, section_space_after=None):
        flowables = []
        extra_spacing = extra_spacing or {}
        section_space_before = self.section_space_before if section_space_before is None else section_space_before
        section_space_after = self.section_space_after if section_space_after is None else section_space_after
        for table, _labels in sections:
            section_title = table._cellvalues[0][0] if table._cellvalues else ""
            space_before = section_space_before + extra_spacing.get(section_title, 0)
            flowables.append(Spacer(1, space_before))
            flowables.append(table)
            flowables.append(Spacer(1, section_space_after))
        return flowables

    def _section_height(
        self,
        label_count: int,
        row_height: float,
        extra_before: float = 0,
        section_space_before: float | None = None,
        section_space_after: float | None = None,
    ) -> float:
        section_space_before = self.section_space_before if section_space_before is None else section_space_before
        section_space_after = self.section_space_after if section_space_after is None else section_space_after
        return section_space_before + extra_before + section_space_after + (row_height * (label_count + 2))

    def _validate_layout(self) -> None:
        pages = self._page_sequence()
        self._validate_page_content(pages)
        for shot_type, left_sections, right_sections, max_pass_rows, section_space_before, section_space_after in (
            (
                "atr",
                [("BREAKDOWN", self.atr_breakdown_order), ("OFF DRIBBLE TYPE", self.atr_off_dribble_order)],
                [("OFF PASS TYPE", self.off_pass_type_order)],
                None,
                self.atr_section_space_before,
                self.atr_section_space_after,
            ),
            (
                "2fg",
                [("BREAKDOWN", self.non_atr_breakdown_order), ("OFF DRIBBLE TYPE", self.atr_off_dribble_order)],
                [("OFF PASS TYPE", self.off_pass_type_order)],
                None,
                self.section_space_before_tight,
                self.section_space_after_tight,
            ),
            (
                "3fg",
                [("BREAKDOWN", self.three_breakdown_order), ("OFF DRIBBLE TYPE", self.three_off_dribble_order)],
                [("OFF PASS TYPE", self.off_pass_type_order)],
                24,
                None,
                None,
            ),
        ):
            atr_page = shot_type == "atr"
            page_margin = self.margin + self.atr_margin if atr_page else self.margin
            atr_layout = self._resolve_atr_layout() if atr_page else None
            row_height_scale = atr_layout.row_height_scale if atr_layout else 1.0
            section_space_before = atr_layout.section_space_before if atr_layout else section_space_before
            section_space_after = atr_layout.section_space_after if atr_layout else section_space_after
            col_height = self._available_column_height(
                margin=page_margin,
                summary_spacer=atr_layout.summary_spacer if atr_layout else None,
                summary_row_height=self.totals_row_height * row_height_scale if atr_layout else None,
            )
            if col_height <= 0:
                raise ValueError(f"{shot_type} layout exceeds page height.")
            columns = self._create_columns_layout(
                shot_type,
                left_sections,
                right_sections,
                max_pass_rows=max_pass_rows,
                section_space_before=section_space_before,
                section_space_after=section_space_after,
                margin=page_margin,
                row_height_scale=row_height_scale,
                section_header_font_size=self.atr_section_header_font_size if atr_page else None,
                header_padding=self.atr_section_header_padding if atr_page else None,
                color_soften_factor=self.atr_color_soften_factor if atr_page else None,
                zero_attempts_style=atr_page,
                gutter=self.atr_column_gutter if atr_page else None,
            )
            usable_width = self.width - (2 * page_margin)
            required_height = columns.wrap(usable_width, col_height)[1]
            if required_height > col_height:
                raise ValueError(f"{shot_type} column content exceeds available height.")

    def _resolve_atr_layout(self) -> SimpleNamespace:
        atr_margin = self.margin + self.atr_margin
        usable_width = self.width - (2 * atr_margin)
        variants = [
            {
                "row_height_scale": self.atr_row_height_scale,
                "summary_spacer": self.atr_summary_spacer,
                "section_space_before": self.atr_section_space_before,
                "section_space_after": self.atr_section_space_after,
            },
            {
                "row_height_scale": 1.03,
                "summary_spacer": self.summary_spacer,
                "section_space_before": self.section_space_before_tight,
                "section_space_after": self.section_space_after_tight,
            },
            {
                "row_height_scale": 1.0,
                "summary_spacer": max(0, self.summary_spacer - 2),
                "section_space_before": self.section_space_before_tight,
                "section_space_after": self.section_space_after_tight,
            },
        ]
        for variant in variants:
            col_height = self._available_column_height(
                margin=atr_margin,
                summary_spacer=variant["summary_spacer"],
                summary_row_height=self.totals_row_height * variant["row_height_scale"],
            )
            columns = self._create_columns_layout(
                "atr",
                left_sections=[
                    ("BREAKDOWN", self.atr_breakdown_order),
                    ("OFF DRIBBLE TYPE", self.atr_off_dribble_order),
                ],
                right_sections=[
                    ("OFF PASS TYPE", self.off_pass_type_order),
                ],
                margin=atr_margin,
                row_height_scale=variant["row_height_scale"],
                section_space_before=variant["section_space_before"],
                section_space_after=variant["section_space_after"],
                section_header_font_size=self.atr_section_header_font_size,
                header_padding=self.atr_section_header_padding,
                color_soften_factor=self.atr_color_soften_factor,
                zero_attempts_style=True,
                gutter=self.atr_column_gutter,
            )
            required_height = columns.wrap(usable_width, col_height)[1]
            if required_height <= col_height:
                return SimpleNamespace(**variant)
        raise ValueError("atr column content exceeds available height.")
