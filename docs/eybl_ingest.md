# EYBL/AAU Ingestion CLI

## Usage
```
flask eybl_import --circuit EYBL --season-year 2025 \
    --overall path/to/Overall.csv --assists path/to/AllPossAssists.csv [--fgatt path/to/FGAttempts.csv] [--season-type AAU] [--dry-run]
```

* `--dry-run` writes a preview to `instance/ingest_previews` and does not promote stats.
* Without `--dry-run` verified rows are upserted into `unified_stats` and a snapshot is written to `instance/ingest_snapshots`.

## Required Columns
- **Overall.csv**: `Player`, `Team`, `GP`, `PPG` or `Pts`, `FG%`, `PPP`, `Poss`, `Pts`
- **AllPossAssists.csv**: `Player`, `Team`, `Ast` or `AST/G`, `Ast/TO`, `PP(P+A)`
- **FGAttempts.csv**: `Player`, `Team`, 2FG/3FG split columns (optional)

## Stat Mapping Rules
- `PPP` priority: `PP(P+A)` → `PPP` → `Pts/Poss`
- `TOV` derived: `AST / (Ast/TO)`
- `FG%` stored as decimals (e.g., 0.453 for 45.3%)
- Upserts keyed by `(recruit_id, circuit, season_year, season_type, team_name)`

## Outputs
- Preview CSVs: `instance/ingest_previews/`
- Snapshot CSVs: `instance/ingest_snapshots/`
