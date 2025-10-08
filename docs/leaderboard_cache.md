# Leaderboard Cache Guide

This reference explains how leaderboard snapshots are built, stored, and rendered. It links the cache helpers in `services/` with the admin/public templates and the client-side controller so reviewers have one place to verify cache-related pull requests.

## Key Modules
- `services/cache_leaderboard.py` – orchestrates cache reads/writes against the `CachedLeaderboard` model, handles snapshot freshness, and exposes helpers consumed by Flask routes.
- `services/leaderboard_cache.py` – converts computed leaderboard data into the normalized payload that gets persisted as JSON. Also exposes manifest helpers and the in-process cache used outside the snapshot pipeline.
- `admin/_leaderboard_helpers.py` – single source of truth for column definitions and table formatting. Both cache builders defer to this module so column metadata stays consistent.
- `admin/routes.py` & `public/routes.py` – request handlers that fetch cached payloads (falling back to live recompute) and shape the initial context for templates.
- `static/js/leaderboard-controller.js` – hydrates the admin leaderboard page, normalizing payloads fetched from `/admin/api/leaderboards/<season_id>/all` and rendering tables on stat changes.
- Templates: `templates/_leaderboard_block.html`, `admin/leaderboard.html`, and `leaderboard.html` render the table markup, embed the initial payload, and wire up the dropdown controls shared by admin/public pages.

## Snapshot Build Pipeline
1. **Compute:** The compute callable (default: `admin.routes.build_leaderboard_cache_payload`) returns a mapping with `config`, `rows`, `team_totals`, and optional `variant`/`aux_table`. (`cache_build_one` accepts dependency injection for tests.)
2. **Normalize:** `services/cache_leaderboard.cache_build_one` passes the compute result to `services.leaderboard_cache.build_leaderboard_payload`, which builds a full payload using `admin._leaderboard_helpers.build_leaderboard_table` for column definitions and formatting.
3. **Persist:** `services.leaderboard_cache.save_snapshot` serializes the payload (with a SHA256 etag) into the `cached_leaderboards` table and prunes older snapshots per stat/season. The manifest written alongside the payload records the builder metadata for debugging.
4. **Return:** The computed payload is returned to the caller so routes can serve the freshly-built data without hitting the database again.

`cache_build_all` loops through every stat key (or a provided subset), batching writes in a single transaction. CLI helpers and parse workflows call this function after ingest to warm the cache.

## Cached Payload Shape
Every stored snapshot is a JSON mapping with the following fields:
- `schema_version` & `formatter_version` – bump these when the payload contract changes so stale rows are ignored or rebuilt automatically.
- `season_id`, `stat_key`, `built_at` – identifiers plus an ISO timestamp of when the snapshot was generated.
- `columns_manifest` – simplified column descriptions the front end uses for rendering and sorting. Each entry includes the rendered key, label, alignment, and any `value_key` that points to the raw metric source. (The client merges these entries with the richer `columns` metadata to retain grouped headers and sizing.)
- `columns` & `column_keys` – the full column config returned by `build_leaderboard_table` for server-rendered tables. `column_keys` is redundant but convenient for templates that expect just the keys. Client-side rendering preserves the classes, groups, and sortable hints supplied here so cached payloads match SSR output.
- `rows` – list of normalized row mappings with:
  - `rank` and `display` (player/rank text) for table headers.
  - `metrics` – per-column objects containing `text` (display value) and `raw` (numeric sort value). When the compute result provides both display/metric fields the `value_key` keeps them in sync; otherwise raw values are inferred from text.
- `totals` – optional mapping shaped just like a row’s `metrics` block plus a `display.player` label (defaults to “Team Totals”).
- `aux_table` – optional nested payload mirroring `columns_manifest`, `rows`, `totals`, and table metadata for the dual-table layouts used on a handful of stats.
- `default_sort`, `has_data`, `table_id`, `config`, and optional `variant` – metadata carried through from the table builder so the UI can retain default ordering and context.

When `services/leaderboard_cache.compute_columns_for` or related helpers are invoked directly (for diagnostics or background jobs) they respect the same structure, ensuring any consumer of a snapshot can rely on the schema above.

## Freshness & Refresh Flow
- The admin/public routes first call `cache_get_leaderboard(season_id, stat_key)`, which reads the latest snapshot via `fetch_latest_snapshot`.
- Each payload carries a `last_built_at` (for legacy compatibility) and `built_at`. The admin API and templates surface these timestamps so reviewers know when the data was generated.
- `_CACHE_TTL` in `services/cache_leaderboard` is six hours. `maybe_schedule_refresh` parses the timestamp embedded in the payload and triggers `schedule_refresh` when the cache is older than that window or missing a timestamp.
- `schedule_refresh` synchronously rebuilds the target stat, ensuring the next request observes fresh data. CLI and background jobs can also call `cache_build_all` to preemptively refresh everything after a data ingest.
- Snapshot tables retain only the newest rows per stat/season (default retain count of one) so diagnosing rebuild history relies on checking the manifest or enabling richer retention via caller overrides.

## How Pages Consume Snapshots
### Admin
- `admin/routes.leaderboard` embeds the selected stat’s payload in a `<script id="initial-leaderboard-payload">` tag during SSR so the first paint is instant.
- Once hydrated, `static/js/leaderboard-controller.js` normalizes that payload (ensuring `columns_manifest`, `rows`, `display`, and `metrics` exist), merges the manifest with the richer `columns` config, and renders the unified table markup (wrapper div, accent bar, caption, `<thead>/<tfoot>` structure) used by the server macros. Sort buttons continue to rely on the `metrics[*].raw` values. It then fetches `/admin/api/leaderboards/<season_id>/all` to load every stat’s snapshot in the background for instant dropdown switches.
- Missing snapshots are logged to the console so reviewers can spot cache gaps when QAing.

### Public
- `public/routes.season_leaderboard` follows the same flow: try the cached snapshot (`cache_get_leaderboard`), rebuild via `cache_build_one` on a miss, and fall back to `format_leaderboard_payload` when filters (e.g., practice labels) require on-the-fly computation.
- The public template reuses the shared table macros, so the payload’s `columns`/`rows` align with the SSR expectations while the `columns_manifest`/`metrics` blocks allow client-side enhancements when needed. The browser renderer follows the same merge logic as the admin page so cached payloads continue to respect grouped headers and totals rows.

## Styling Expectations
- Server-rendered views use `render_table` from `macros/table.html`, which already applies the shared styles documented in `docs/tables.md`.
- The client-side renderer now mirrors that markup exactly: it reuses the accent wrapper, table classes, grouped headers, and totals footer derived from the cached `columns` metadata. Header labels still come from `columns_manifest[*].label`, numeric columns should set `align: 'right'`, and sortable metrics must populate `metrics[*].raw` so JavaScript sorting behaves consistently.
- Auxiliary tables should follow the same manifest + rows contract; when present the controller renders them into `#leaderboard-aux` with identical styling classes.

By adhering to this structure, cached leaderboards remain interchangeable with live-computed tables, making it safe to review cache-related changes without re-reading every consumer.
