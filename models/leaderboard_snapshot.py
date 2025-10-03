"""Database model for cached leaderboard snapshots."""
from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Optional, Sequence

from models.database import db


class LeaderboardSnapshot(db.Model):
    """Persisted aggregates for leaderboard slices."""

    __tablename__ = "leaderboard_snapshots"

    id = db.Column(db.Integer, primary_key=True)
    season_id = db.Column(db.Integer, db.ForeignKey("season.id"), nullable=False, index=True)
    stat_key = db.Column(db.String(64), nullable=False, index=True)
    start_date = db.Column(db.Date, nullable=True)
    end_date = db.Column(db.Date, nullable=True)
    label_key = db.Column(db.String(255), nullable=False, default="")
    label_values = db.Column(db.JSON, nullable=False, default=list)
    player_totals = db.Column(db.JSON, nullable=False, default=dict)
    shot_details = db.Column(db.JSON, nullable=False, default=dict)
    team_totals = db.Column(db.JSON, nullable=True)
    player_keys = db.Column(db.JSON, nullable=False, default=list)
    leaderboard_rows = db.Column(db.JSON, nullable=False, default=list)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        db.UniqueConstraint(
            "season_id",
            "stat_key",
            "start_date",
            "end_date",
            "label_key",
            name="uq_leaderboard_snapshot_filters",
        ),
    )

    @staticmethod
    def normalize_date(value: Optional[object]) -> Optional[date]:
        """Coerce various date inputs into ``date`` objects."""

        if value in (None, ""):
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise ValueError(f"Unsupported date value: {value!r}")

    @classmethod
    def label_key_from_labels(cls, normalized_labels: Sequence[str]) -> str:
        """Return the canonical cache key fragment for the provided labels."""

        if not normalized_labels:
            return ""
        return "|".join(normalized_labels)

    @classmethod
    def fetch(
        cls,
        season_id: int,
        stat_key: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        normalized_labels: Sequence[str] = (),
    ) -> Optional["LeaderboardSnapshot"]:
        """Return the snapshot matching the provided filters, if present."""

        label_key = cls.label_key_from_labels(normalized_labels)
        return (
            cls.query.filter_by(
                season_id=season_id,
                stat_key=stat_key,
                start_date=start_date,
                end_date=end_date,
                label_key=label_key,
            )
            .order_by(cls.updated_at.desc())
            .first()
        )

    def update_from_baseline(
        self,
        baseline: dict,
        normalized_labels: Sequence[str],
    ) -> None:
        """Overwrite the stored aggregates from a freshly built baseline."""

        self.label_values = list(normalized_labels)
        self.label_key = self.label_key_from_labels(normalized_labels)
        self.player_totals = dict(baseline.get("player_totals") or {})
        self.shot_details = dict(baseline.get("shot_details") or {})
        team_totals = baseline.get("team_totals")
        if isinstance(team_totals, tuple):
            team_totals = list(team_totals)
        self.team_totals = team_totals
        player_keys: Iterable[str] = baseline.get("all_players") or []
        self.player_keys = list(player_keys)
        leaderboard_rows = baseline.get("leaderboard") or []
        serialized_rows = [
            list(row) if isinstance(row, tuple) else row
            for row in leaderboard_rows
        ]
        self.leaderboard_rows = serialized_rows
        self.updated_at = datetime.utcnow()

    def to_components(self) -> dict:
        """Return the stored aggregates in application-friendly formats."""

        team_totals = self.team_totals
        if isinstance(team_totals, list):
            team_totals = tuple(team_totals)

        rows = []
        for row in self.leaderboard_rows or []:
            if isinstance(row, list):
                rows.append(tuple(row))
            else:
                rows.append(row)

        return {
            "player_totals": dict(self.player_totals or {}),
            "shot_details": dict(self.shot_details or {}),
            "team_totals": team_totals,
            "player_keys": list(self.player_keys or []),
            "leaderboard": rows,
            "labels": tuple(self.label_values or []),
        }

    @classmethod
    def upsert(
        cls,
        season_id: int,
        stat_key: str,
        start_date: Optional[date],
        end_date: Optional[date],
        normalized_labels: Sequence[str],
        baseline: dict,
    ) -> "LeaderboardSnapshot":
        """Create or update the snapshot for the supplied filters."""

        snapshot = cls.fetch(
            season_id,
            stat_key,
            start_date=start_date,
            end_date=end_date,
            normalized_labels=normalized_labels,
        )
        if snapshot is None:
            snapshot = cls(
                season_id=season_id,
                stat_key=stat_key,
                start_date=start_date,
                end_date=end_date,
            )
        snapshot.update_from_baseline(baseline, normalized_labels)
        db.session.add(snapshot)
        return snapshot

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return (
            f"<LeaderboardSnapshot season={self.season_id} stat={self.stat_key} "
            f"start={self.start_date} end={self.end_date} labels={self.label_values}>"
        )
