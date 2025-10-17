# Correlation Analytics Workspace — Task Breakdown

## Phase 1: Foundations & Data Services
1. **Correlation service layer**
   - [ ] Create `services/correlation.py` with helpers to normalize practice and game stat queries into Pandas DataFrames.
   - [ ] Implement shared correlation computation (Pearson + Spearman) returning coefficients, scatter datasets, and sample counts.
   - [ ] Add unit tests covering mixed metric sources, empty datasets, and single-player scenarios.

2. **Admin API endpoint**
   - [ ] Add `/admin/api/correlation/workbench` route that accepts one or more study definitions and delegates to the correlation service.
   - [ ] Implement request validation (metric catalog checks, roster filters, date windows) and consistent error responses.
   - [ ] Cache study results keyed by metric/filter tuple to keep repeated requests fast during exploration.

## Phase 2: Front-End Workspace Surface
3. **Template & bootstrap**
   - [x] Create `templates/admin/correlation_workbench.html` patterned after the custom practice stats layout with left-side filter controls and a main chart canvas.
   - [x] Inject roster, practice, and leaderboard catalogs via the Flask context for client initialization.
   - [x] Wire the page into the admin navigation with permission checks consistent with other analytics pages.

4. **Client controller & visualization**
   - [x] Build `static/js/correlation_workbench.js` to manage state, fetch studies from the new endpoint, and render Chart.js scatter/heatmap components.
   - [ ] Support multi-study panels, study presets (CRUD via existing presets API), and exports (CSV + PNG via html2canvas).
   - [ ] Add automated front-end tests (Jest) for state reducers/helpers and lint updates if required.

## Phase 3: Enhancements & Insights
5. **Narrative insights**
   - [ ] Extend the correlation service to summarize strongest positive/negative relationships and surface them as “insight chips” on the UI.
   - [ ] Guard against noisy results by suppressing insights when sample counts fall below configurable thresholds.

6. **Per-event timelines**
   - [ ] Add optional grouping to compute per-practice/per-game correlations for single-player time-series studies.
   - [ ] Update the UI to allow toggling between aggregate-per-player and per-event views with contextual tooltips.

## Phase 4: Documentation & Ops
7. **User documentation**
   - [ ] Document the workspace workflow, presets, and export steps in `docs/analytics_workspace.md` and link it from internal onboarding guides.

8. **Monitoring & maintenance**
   - [ ] Add logging/metrics for correlation requests (duration, cache hits, failures) to feed into existing observability dashboards.
   - [ ] Schedule periodic review to refresh preset defaults and verify stat catalog alignment each season.

---

### Acceptance criteria
- Admin users can open the Analytics Workspace, build correlation studies across practice/game stats, view scatter plots, export results, and save/load presets.
- Correlation computations are validated by automated tests and observability alerts on failure paths.
- Documentation covers data definitions, UI flows, and troubleshooting guidance for analysts.
