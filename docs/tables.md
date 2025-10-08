# Unified Tables Guide

## Purpose
One table system powers sortable, accessible data views with a consistent card treatment. Every unified table uses the shared macro so the look, feel, and keyboard/screen reader behavior stay aligned while enabling click-to-sort.

## How to Use the Macro
```jinja
{% from 'macros/table.html' import render_table %}
{% set columns = [
  {'key':'player','label':'Player','sortable':True,'align':'left'},
  {'key':'pts','label':'PTS','sortable':True,'align':'right'},
  {'key':'efg','label':'eFG%','sortable':True,'align':'right'},
] %}
{{ render_table(id='example', columns=columns, rows=rows, totals=None, dense=True, sticky_header=False, caption='Example') }}
```
- **Columns**: dictionaries with `key`, `label`, `sortable`, `align` (`left`|`right`|`center`), and optional `width` for utility classes.
- **Rows**: list of dicts or objects keyed by `columns[*].key`.
- **Totals**: optional dict whose keys match column keys; renders a styled footer row.
- **Sticky header**: pass `sticky_header=True` for long scrollable tables.

## Shaping Cell Values for Correct Sorting
Use a dict to separate what the user sees (`display`) from the machine-sort value (`data_value`).

```python
{"display": "5/11 (45.5%)", "data_value": "0.455"}
```

Helpers available in routes for consistent formatting:
- `num(x)` → numeric display plus `data_value`
- `pct(x)` (0..1) → “45.5%” with `data_value` of `0.455`
- `ratio(makes, atts)` → “m/a (pct%)” with pct decimal for sorting
- `dt_iso(iso, pretty)` → friendly date string with ISO `data_value`

## Sorting Rules
- Click any header (or focus then press Enter/Space) to toggle ascending/descending.
- Type inference covers numbers, percents, currency, and dates; everything else sorts as text.
- Empty cells fall to the bottom on ascending sorts and rise to the top on descending sorts.
- Only one column is active at a time; `aria-sort` reflects the current state.

## Opt-out (Shot Type Tables)
Shot Type tables intentionally keep legacy styling. Add `class="table-exempt"` to skip unified styling/sorting and leave those tables untouched.

## Design Tokens & Theming
- Crimson focus ring with hover/active states captured in `static/css/tables.css`.
- Supports light/dark themes automatically.
- Mobile-friendly via horizontal scroll wrappers; enable sticky headers when needed.

## Do / Don’t
- **Do** right-align numeric columns and provide `data_value` for mixed display content.
- **Do** reuse helpers or the dict shape for percentages, currency, ratios, and dates.
- **Don’t** add inline styles or custom borders; rely on the shared stylesheet.
- **Don’t** remove `.table-exempt` from Shot Type tables or migrate them to the macro.

## Related Guides
- [Leaderboard Cache Guide](./leaderboard_cache.md) – end-to-end reference for the snapshot pipeline powering the admin and public leaderboards.
