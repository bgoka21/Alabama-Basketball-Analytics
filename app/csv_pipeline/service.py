"""CSV Pipeline data transformation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import pandas as pd

PROTECTED_COLUMNS = [
    "Timeline",
    "Start time",
    "Duration",
    "Row",
    "Instance Number",
    "Instance number",
]

PROTECTED_COLUMN_SET = set(PROTECTED_COLUMNS)

TIME_LIKE_COLUMNS = {
    "Timeline",
    "Start time",
    "Duration",
}

OVERWRITE_ROWS = {
    "Offense",
    "Defense",
    "PnR",
    "Offense Rebound Opportunities",
    "Defense Rebound Opportunities",
}


@dataclass(frozen=True)
class GroupInputs:
    offense_shot_type: pd.DataFrame
    offense_shot_creation: pd.DataFrame
    offense_turnover_type: pd.DataFrame
    defense_possessions: pd.DataFrame
    defense_gap_help: pd.DataFrame
    defense_shot_contest: pd.DataFrame
    defense_pass_contest: pd.DataFrame
    pnr_gap_help: pd.DataFrame
    pnr_grade: pd.DataFrame
    offense_rebound: pd.DataFrame
    defense_rebound: pd.DataFrame


@dataclass(frozen=True)
class GroupFilenames:
    offense_shot_type: str
    offense_shot_creation: str
    offense_turnover_type: str
    defense_possessions: str
    defense_gap_help: str
    defense_shot_contest: str
    defense_pass_contest: str
    pnr_gap_help: str
    pnr_grade: str
    offense_rebound: str
    defense_rebound: str


class CsvPipelineError(ValueError):
    """Raised when CSV pipeline inputs are invalid."""


def _ensure_row_column(df: pd.DataFrame, filename: str) -> None:
    if "Row" not in df.columns:
        raise CsvPipelineError(f"{filename} is missing required 'Row' column.")


def _player_columns(df: pd.DataFrame) -> list[str]:
    return [col for col in df.columns if isinstance(col, str) and col.startswith("#")]


def _extract_group(df: pd.DataFrame, row_label: str, filename: str) -> pd.DataFrame:
    _ensure_row_column(df, filename)
    group = df[df["Row"] == row_label].copy()
    return group.reset_index(drop=True)


def _strip_grouped_columns(df: pd.DataFrame, row_label: str) -> pd.DataFrame:
    cleaned = df.drop(
        columns=[col for col in df.columns if col in PROTECTED_COLUMN_SET],
        errors="ignore",
    ).copy()
    cleaned.insert(0, "Row", row_label)
    return cleaned


def _normalize_cell(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if pd.isna(value):
        return ""
    return str(value)


def _split_tokens(value: object) -> list[str]:
    raw = _normalize_cell(value)
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def _merge_union_cell(base_value: object, donor_value: object) -> str:
    base_tokens = _split_tokens(base_value)
    donor_tokens = _split_tokens(donor_value)
    for token in donor_tokens:
        if token not in base_tokens:
            base_tokens.append(token)
    return ", ".join(base_tokens)


def _format_timedelta(value: pd.Timedelta) -> str:
    total_seconds = int(value.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _normalize_protected_values(
    df: pd.DataFrame, columns: Iterable[str]
) -> pd.DataFrame:
    normalized = df[list(columns)].copy()
    normalized = normalized.where(pd.notna(normalized), "")
    normalized = normalized.apply(
        lambda column: column.map(
            lambda value: value.strip() if isinstance(value, str) else value
        )
    )
    normalized = normalized.astype(str)
    for column in TIME_LIKE_COLUMNS.intersection(normalized.columns):
        series = normalized[column].str.strip()
        time_mask = series.str.contains(
            r":|\b\d+(?:\.\d+)?\s*[smhd]\b", regex=True
        )
        parsed = pd.to_timedelta(series.where(time_mask, pd.NA), errors="coerce")
        if parsed.notna().any():
            formatted = parsed.map(
                lambda value: _format_timedelta(value) if pd.notna(value) else ""
            )
            normalized.loc[parsed.notna(), column] = formatted[parsed.notna()]
    return normalized


def _validate_group_rows(
    groups: Sequence[pd.DataFrame],
    filenames: Sequence[str],
    group_name: str,
) -> None:
    base = groups[0]
    for df, name in zip(groups[1:], filenames[1:]):
        if len(df) != len(base):
            raise CsvPipelineError(
                f"{group_name} row count mismatch: {name} has {len(df)} rows, "
                f"expected {len(base)}."
            )

        skip_col = base.columns[0] if len(base.columns) > 0 else None
        shared_cols = [
            col
            for col in PROTECTED_COLUMN_SET
            if col in base.columns
            and col in df.columns
            and col != skip_col
            and col != "Timeline"
        ]
        if shared_cols:
            base_vals = _normalize_protected_values(
                base, shared_cols
            ).reset_index(drop=True)
            other_vals = _normalize_protected_values(
                df, shared_cols
            ).reset_index(drop=True)
            if not base_vals.equals(other_vals):
                for col in shared_cols:
                    mismatches = base_vals[col] != other_vals[col]
                    if mismatches.any():
                        row_idx = mismatches[mismatches].index[0]
                        base_value = base_vals.at[row_idx, col]
                        other_value = other_vals.at[row_idx, col]
                        raise CsvPipelineError(
                            f"{group_name} protected column mismatch between "
                            f"{filenames[0]} and {name}: column '{col}' at row "
                            f"{row_idx} differs (base='{base_value}', "
                            f"other='{other_value}')."
                        )


def _combine_disjoint_columns(
    base: pd.DataFrame,
    donors: Sequence[pd.DataFrame],
    group_name: str,
) -> pd.DataFrame:
    combined = base.copy()
    existing = {col for col in combined.columns if col not in PROTECTED_COLUMN_SET}
    for donor in donors:
        for col in donor.columns:
            if col in PROTECTED_COLUMN_SET:
                continue
            if col in existing:
                raise CsvPipelineError(
                    f"{group_name} column overlap detected for '{col}'."
                )
            combined[col] = donor[col].values
            existing.add(col)
    return combined


def _apply_union_players(
    base: pd.DataFrame,
    donors: Sequence[pd.DataFrame],
) -> pd.DataFrame:
    merged = base.copy()
    base_index = list(range(len(merged)))
    for donor in donors:
        donor_players = _player_columns(donor)
        for col in donor_players:
            if col not in merged.columns:
                merged[col] = ""
            for idx in base_index:
                merged.at[idx, col] = _merge_union_cell(
                    merged.at[idx, col],
                    donor.at[idx, col] if col in donor.columns else "",
                )
    return merged


def build_offense_group(
    shot_type: pd.DataFrame,
    shot_creation: pd.DataFrame,
    turnover_type: pd.DataFrame,
    filenames: Sequence[str],
) -> pd.DataFrame:
    group_label = "Offense"
    groups = [
        _strip_grouped_columns(
            _extract_group(shot_type, group_label, filenames[0]), group_label
        ),
        _strip_grouped_columns(
            _extract_group(shot_creation, group_label, filenames[1]), group_label
        ),
        _strip_grouped_columns(
            _extract_group(turnover_type, group_label, filenames[2]), group_label
        ),
    ]
    _validate_group_rows(groups, filenames, group_label)
    base = groups[0]
    donors = [
        groups[1].drop(
            columns=[c for c in groups[1].columns if c in PROTECTED_COLUMN_SET],
            errors="ignore",
        ),
        groups[2].drop(
            columns=[c for c in groups[2].columns if c in PROTECTED_COLUMN_SET],
            errors="ignore",
        ),
    ]
    return _combine_disjoint_columns(base, donors, group_label)


def build_defense_group(
    defensive_possessions: pd.DataFrame,
    gap_help: pd.DataFrame,
    shot_contest: pd.DataFrame,
    pass_contest: pd.DataFrame,
    filenames: Sequence[str],
) -> pd.DataFrame:
    group_label = "Defense"
    groups = [
        _strip_grouped_columns(
            _extract_group(defensive_possessions, group_label, filenames[0]),
            group_label,
        ),
        _strip_grouped_columns(
            _extract_group(gap_help, group_label, filenames[1]), group_label
        ),
        _strip_grouped_columns(
            _extract_group(shot_contest, group_label, filenames[2]), group_label
        ),
        _strip_grouped_columns(
            _extract_group(pass_contest, group_label, filenames[3]), group_label
        ),
    ]
    _validate_group_rows(groups, filenames, group_label)
    base = groups[0]
    donor_players = []
    for donor in groups[1:]:
        donor_players.append(donor[_player_columns(donor)].copy())
    return _apply_union_players(base, donor_players)


def build_pnr_group(
    gap_help: pd.DataFrame,
    grade: pd.DataFrame,
    filenames: Sequence[str],
) -> pd.DataFrame:
    group_label = "PnR"
    groups = [
        _strip_grouped_columns(
            _extract_group(gap_help, group_label, filenames[0]), group_label
        ),
        _strip_grouped_columns(
            _extract_group(grade, group_label, filenames[1]), group_label
        ),
    ]
    _validate_group_rows(groups, filenames, group_label)
    base = groups[0]
    keep_cols = [
        col
        for col in base.columns
        if col in PROTECTED_COLUMN_SET or col.startswith("#")
    ]
    base = base[keep_cols].copy()
    donor = groups[1][_player_columns(groups[1])].copy()
    return _apply_union_players(base, [donor])


def build_rebound_groups(
    offense_rebound: pd.DataFrame,
    defense_rebound: pd.DataFrame,
    filenames: Sequence[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    offense_label = "Offense Rebound Opportunities"
    defense_label = "Defense Rebound Opportunities"
    off_group = _extract_group(offense_rebound, offense_label, filenames[0])
    def_group = _extract_group(defense_rebound, defense_label, filenames[1])
    if off_group.empty:
        raise CsvPipelineError(f"{filenames[0]} has no '{offense_label}' rows.")
    if def_group.empty:
        raise CsvPipelineError(f"{filenames[1]} has no '{defense_label}' rows.")
    return (
        _strip_grouped_columns(off_group, offense_label),
        _strip_grouped_columns(def_group, defense_label),
    )


def _align_columns(base_columns: Iterable[str], df: pd.DataFrame) -> pd.DataFrame:
    columns = list(base_columns)
    for col in df.columns:
        if col not in columns:
            columns.append(col)
    return df.reindex(columns=columns)


def build_final_csv(
    pre_combined: pd.DataFrame,
    inputs: GroupInputs,
    filenames: GroupFilenames,
) -> pd.DataFrame:
    _ensure_row_column(pre_combined, "Pre-Combined CSV")

    offense = build_offense_group(
        inputs.offense_shot_type,
        inputs.offense_shot_creation,
        inputs.offense_turnover_type,
        [
            filenames.offense_shot_type,
            filenames.offense_shot_creation,
            filenames.offense_turnover_type,
        ],
    )
    defense = build_defense_group(
        inputs.defense_possessions,
        inputs.defense_gap_help,
        inputs.defense_shot_contest,
        inputs.defense_pass_contest,
        [
            filenames.defense_possessions,
            filenames.defense_gap_help,
            filenames.defense_shot_contest,
            filenames.defense_pass_contest,
        ],
    )
    pnr = build_pnr_group(
        inputs.pnr_gap_help,
        inputs.pnr_grade,
        [
            filenames.pnr_gap_help,
            filenames.pnr_grade,
        ],
    )
    off_reb, def_reb = build_rebound_groups(
        inputs.offense_rebound,
        inputs.defense_rebound,
        [
            filenames.offense_rebound,
            filenames.defense_rebound,
        ],
    )

    retained = pre_combined[~pre_combined["Row"].isin(OVERWRITE_ROWS)].copy()

    final_columns = list(pre_combined.columns)
    for df in (offense, defense, pnr, off_reb, def_reb):
        for col in df.columns:
            if col not in final_columns:
                final_columns.append(col)

    output_frames = [
        retained,
        offense,
        defense,
        pnr,
        off_reb,
        def_reb,
    ]

    aligned = [_align_columns(final_columns, frame) for frame in output_frames]
    return pd.concat(aligned, ignore_index=True)
