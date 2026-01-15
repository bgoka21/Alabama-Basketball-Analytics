# Shot Location Mapping

## How normalization works

Raw shot location values are normalized in `utils/shot_location_map.py` using
`SHOT_LOCATION_MAP`. When new raw values appear in data exports (for example
from `data/processed/*.csv`), add them to the map so that downstream analytics
and shot chart rendering use consistent canonical zones.

## Mapping workflow

1. Scan incoming CSVs for distinct `Shot Location` values.
2. Compare those values against `SHOT_LOCATION_MAP` to identify anything new.
3. Decide the canonical zone (existing or new) that best matches the raw value.
4. Update tests (see `tests/test_shot_location_map.py`) to cover the new inputs.
5. If a new canonical zone is introduced, update shot chart positioning in
   `static/js/shot_chart.js` so the visualization places it correctly.

## Recent additions

- `Left SC` → `short_corner_left`
- `Right SC` → `short_corner_right`
