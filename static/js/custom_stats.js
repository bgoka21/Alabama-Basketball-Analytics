(function () {
  'use strict';

  const REFRESH_DEBOUNCE_MS = 150;
  const PNG_CDN_URL = 'https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js';
  const PRESETS_URL = '/admin/api/presets';
  const TOAST_TIMEOUT_MS = 2500;

  function buildJsonHeaders() {
    const headers = { 'Content-Type': 'application/json' };
    if (window.__CSRF__) {
      headers['X-CSRFToken'] = window.__CSRF__;
    }
    return headers;
  }

  async function safeErr(res) {
    try {
      const data = await res.json();
      if (data && typeof data.error === 'string' && data.error.trim()) {
        return data.error.trim();
      }
    } catch (error) {
      // ignore parse error
    }
    return res.statusText || 'Request failed';
  }

  const api = {
    async listPresets({ preset_type, q = '' } = {}) {
      const params = new URLSearchParams();
      if (preset_type) {
        params.set('preset_type', preset_type);
      }
      if (typeof q === 'string' && q.trim()) {
        params.set('q', q.trim());
      }
      const query = params.toString();
      const url = query ? `${PRESETS_URL}?${query}` : PRESETS_URL;
      const res = await fetch(url, { credentials: 'same-origin' });
      if (!res.ok) {
        throw new Error(await safeErr(res));
      }
      try {
        const payload = await res.json();
        if (payload && Array.isArray(payload.presets)) {
          return payload.presets;
        }
        if (payload && Array.isArray(payload.team)) {
          return payload.team;
        }
        if (Array.isArray(payload)) {
          return payload;
        }
      } catch (error) {
        // ignore parse error and fall back to empty array
      }
      return [];
    },

    async createPreset(payload) {
      const res = await fetch(PRESETS_URL, {
        method: 'POST',
        headers: buildJsonHeaders(),
        credentials: 'same-origin',
        body: JSON.stringify(payload)
      });
      if (!res.ok) {
        throw new Error(await safeErr(res));
      }
      return res.json();
    },

    async updatePreset(id, patch) {
      const res = await fetch(`${PRESETS_URL}/${id}`, {
        method: 'PATCH',
        headers: buildJsonHeaders(),
        credentials: 'same-origin',
        body: JSON.stringify(patch)
      });
      if (!res.ok) {
        throw new Error(await safeErr(res));
      }
      try {
        return await res.json();
      } catch (error) {
        return null;
      }
    },

    async deletePreset(id) {
      const res = await fetch(`${PRESETS_URL}/${id}`, {
        method: 'DELETE',
        headers: buildJsonHeaders(),
        credentials: 'same-origin'
      });
      if (!res.ok) {
        throw new Error(await safeErr(res));
      }
      if (res.status === 204) {
        return null;
      }
      try {
        return await res.json();
      } catch (error) {
        return null;
      }
    }
  };

  let toastRoot = null;

  function getToastRoot() {
    if (toastRoot) {
      return toastRoot;
    }
    toastRoot = document.createElement('div');
    toastRoot.className = 'fixed inset-x-0 top-4 z-50 flex flex-col items-center gap-2 px-4 sm:items-end sm:px-6';
    document.body.appendChild(toastRoot);
    return toastRoot;
  }

  function notify(type, message) {
    const root = getToastRoot();
    const toast = document.createElement('div');
    const base = 'transition-opacity duration-200 ease-in-out px-4 py-2 rounded-lg shadow-lg text-sm font-medium';
    let variant = 'bg-gray-900 text-white';
    if (type === 'error') {
      variant = 'bg-red-600 text-white';
    } else if (type === 'success') {
      variant = 'bg-green-600 text-white';
    } else if (type === 'info') {
      variant = 'bg-blue-600 text-white';
    }
    toast.className = `${base} ${variant}`;
    toast.textContent = message || '';
    root.appendChild(toast);
    window.setTimeout(() => {
      toast.classList.add('opacity-0');
      window.setTimeout(() => {
        if (toast.parentNode === root) {
          root.removeChild(toast);
        }
      }, 200);
    }, TOAST_TIMEOUT_MS);
  }

  function setButtonLoading(button, loading) {
    if (!button) {
      return;
    }
    const isLoading = Boolean(loading);
    button.disabled = isLoading;
    button.classList.toggle('opacity-50', isLoading);
    button.classList.toggle('pointer-events-none', isLoading);
  }

  function initCustomStatsPage(config) {
    if (!config) {
      console.error('[custom-stats] Missing configuration');
      return;
    }

    const elements = {
      playerRoot: document.getElementById('custom-player-select'),
      statSearch: document.getElementById('stat-search'),
      statGroups: document.getElementById('stat-groups'),
      dateFrom: document.getElementById('custom-date-from'),
      dateTo: document.getElementById('custom-date-to'),
      gameSearch: document.getElementById('custom-game-search'),
      gameDropdown: document.getElementById('custom-game-dropdown'),
      gameChips: document.getElementById('custom-game-chips'),
      gameClear: document.getElementById('custom-game-clear'),
      gameHelp: document.getElementById('custom-game-help'),
      gameRow: document.getElementById('custom-game-row'),
      gamePicker: document.getElementById('custom-game-picker'),
      modeToggle: document.getElementById('custom-mode-toggle'),
      autoRefresh: document.getElementById('custom-auto-refresh'),
      tableContainer: document.getElementById('custom-table-container'),
      exportCsv: document.getElementById('export-csv'),
      exportPng: document.getElementById('export-png'),
      presetsPanel: document.getElementById('presets-panel'),
      presetTabs: Array.from(document.querySelectorAll('#presets-panel .preset-tab')),
      presetPanes: Array.from(document.querySelectorAll('#presets-panel .preset-pane')),
      playersPresetName: document.getElementById('players-preset-name'),
      playersPresetSave: document.getElementById('players-preset-save'),
      playersPresetsList: document.getElementById('players-presets-list'),
      statsPresetName: document.getElementById('stats-preset-name'),
      statsPresetSave: document.getElementById('stats-preset-save'),
      statsPresetsList: document.getElementById('stats-presets-list'),
      datesPresetName: document.getElementById('dates-preset-name'),
      datesPresetSave: document.getElementById('dates-preset-save'),
      datesPresetsList: document.getElementById('dates-presets-list'),
      combinedWrapper: document.getElementById('combined-presets-wrapper'),
      combinedPresetsList: document.getElementById('combined-presets-list'),
      combinedAccordion: document.getElementById('combined-presets-accordion'),
      sourceToggle: document.getElementById('custom-source-toggle'),
      modeLabel: document.getElementById('mode-toggle-label'),
      sourceHeading: document.getElementById('custom-source-heading'),
      sourceDescription: document.getElementById('custom-source-description')
    };

    if (!elements.playerRoot || !elements.tableContainer || !elements.statGroups) {
      console.warn('[custom-stats] Required DOM nodes are missing.');
      return;
    }

    const defaultSource = normalizeSource(config && config.defaultSource ? config.defaultSource : 'practice');
    const initialDateFrom = elements.dateFrom && elements.dateFrom.value ? elements.dateFrom.value : null;
    const initialDateTo = elements.dateTo && elements.dateTo.value ? elements.dateTo.value : null;

    const state = {
      roster: normalizeRosterData(resolveRosterData(config)),
      selectedPlayers: [],
      selectedPlayerIds: new Set(),
      fieldOrder: new Map(),
      selectedFields: [],
      selectedFieldsBySource: { practice: [], game: [] },
      mode: 'totals',
      modeSelections: { practice: 'totals', game: 'totals' },
      source: defaultSource,
      autoRefresh: Boolean(elements.autoRefresh ? elements.autoRefresh.checked : true),
      lastPayload: null,
      presets: { players: [], stats: [], dates: [], combined: [] },
      dateSelections: {
        practice: { from: null, to: null },
        game: { from: null, to: null }
      },
      gameSelections: { practice: [], game: [] },
      gameOptions: [],
      fieldCheckboxes: [],
      refreshTimer: null,
      activeRequest: null,
      html2CanvasPromise: null,
      requestRefresh: null
    };

    state.dateSelections[defaultSource] = {
      from: initialDateFrom,
      to: initialDateTo
    };

    const playerUI = buildPlayerPicker(elements.playerRoot, state, queueRefresh);

    state.selectedFieldsBySource.practice = state.selectedFields.slice();
    state.modeSelections.practice = state.mode;

    hydrateSourceToggle(elements, state, config, queueRefresh, playerUI, setSource);

    hydrateDates(elements, state, queueRefresh);
    hydrateGames(elements, state, config, queueRefresh);
    hydrateModeToggle(elements.modeToggle, state, queueRefresh, elements);
    hydrateAutoRefresh(elements.autoRefresh, state);
    hydratePresets(config, elements, state, queueRefresh, playerUI, setSource);
    hydrateExports(config, elements, state);

    updateSourceHeadline(state, elements);
    updateSourceButtons(state, elements);
    updateModeToggleLabels(state, elements);
    updateGameSelectorVisibility(state, elements);

    setSource(state.source, { force: true, queueRefresh: false }).then(() => {
      if (state.autoRefresh) {
        queueRefresh('init');
      }
    });

    function queueRefresh(reason) {
      if (!state.autoRefresh) {
        return;
      }
      if (state.refreshTimer) {
        clearTimeout(state.refreshTimer);
      }
      state.refreshTimer = window.setTimeout(() => {
        state.refreshTimer = null;
        refreshTable(config, elements, state);
      }, REFRESH_DEBOUNCE_MS);
    }

    state.requestRefresh = queueRefresh;

    function resolveFieldsUrl(source) {
      const normalized = normalizeSource(source);
      if (normalized === 'game') {
        return config.fieldsUrlGame || config.fieldsUrl;
      }
      return config.fieldsUrlPractice || config.fieldsUrl;
    }

    async function setSource(nextSource, options = {}) {
      const normalized = normalizeSource(nextSource);
      const force = Boolean(options.force);
      const previousSource = state.source;

      const currentDates = getDateSelection(state, previousSource);
      const nextFrom = elements.dateFrom ? (elements.dateFrom.value ? elements.dateFrom.value : null) : currentDates.from;
      const nextTo = elements.dateTo ? (elements.dateTo.value ? elements.dateTo.value : null) : currentDates.to;
      setDateSelection(state, previousSource, {
        from: elements.dateFrom ? nextFrom : currentDates.from,
        to: elements.dateTo ? nextTo : currentDates.to
      });

      if (!force && normalized === previousSource) {
        if (Array.isArray(options.presetFields)) {
          state.selectedFields = options.presetFields.slice();
        } else {
          state.selectedFields = state.selectedFieldsBySource[normalized]
            ? state.selectedFieldsBySource[normalized].slice()
            : [];
        }
        const desiredMode = normalizeModeForSource(
          options.modeOverride ?? state.modeSelections[normalized],
          normalized
        );
        if (state.mode !== desiredMode) {
          state.mode = desiredMode;
        }
        state.modeSelections[normalized] = state.mode;
        updateModeButtons(state);
        updateModeToggleLabels(state, elements);
        updateSourceHeadline(state, elements);
        updateSourceButtons(state, elements);
        const activeDates = getDateSelection(state, normalized);
        if (elements.dateFrom) {
          elements.dateFrom.value = activeDates.from || '';
        }
        if (elements.dateTo) {
          elements.dateTo.value = activeDates.to || '';
        }
        updateGameSelectorVisibility(state, elements);
        syncFieldCheckboxes(state);
        if (options.queueRefresh !== false) {
          queueRefresh('source');
        }
        return false;
      }

      state.selectedFieldsBySource[previousSource] = state.selectedFields.slice();
      state.modeSelections[previousSource] = state.mode;

      state.source = normalized;
      if (Array.isArray(options.presetFields)) {
        state.selectedFields = options.presetFields.slice();
      } else {
        state.selectedFields = state.selectedFieldsBySource[normalized]
          ? state.selectedFieldsBySource[normalized].slice()
          : [];
      }

      const nextMode = normalizeModeForSource(
        options.modeOverride ?? state.modeSelections[normalized],
        normalized
      );
      state.mode = nextMode;
      state.modeSelections[normalized] = nextMode;

      updateModeButtons(state);
      updateModeToggleLabels(state, elements);
      updateSourceHeadline(state, elements);
      updateSourceButtons(state, elements);
      const activeDates = getDateSelection(state, normalized);
      if (elements.dateFrom) {
        elements.dateFrom.value = activeDates.from || '';
      }
      if (elements.dateTo) {
        elements.dateTo.value = activeDates.to || '';
      }
      const nextGameSelection = getGameSelection(state, normalized);
      setGameSelection(state, normalized, nextGameSelection);
      state.lastPayload = null;

      if (elements.statSearch) {
        elements.statSearch.value = '';
      }

      const url = resolveFieldsUrl(normalized);
      await fetchFields(url, elements.statGroups, elements.statSearch, state, queueRefresh, {
        skipRefresh: true
      });

      state.selectedFields = dedupeAndSortFields(state.selectedFields, state.fieldOrder);
      state.fieldOrder = rebuildFieldOrder(state.selectedFields, state.fieldOrder);
      state.selectedFieldsBySource[normalized] = state.selectedFields.slice();
      syncFieldCheckboxes(state);
      updateGameSelectorVisibility(state, elements);

      if (options.queueRefresh !== false) {
        queueRefresh('source');
      }

      return true;
    }

    playerUI.onChange = queueRefresh;
  }

  function resolveRosterData(config) {
    if (config && Array.isArray(config.roster)) {
      return config.roster;
    }

    const root = document.getElementById('custom-player-select');
    if (root && root.dataset && root.dataset.roster) {
      try {
        return JSON.parse(root.dataset.roster);
      } catch (error) {
        console.error('[custom-stats] Failed to parse roster data attribute', error);
      }
    }

    const script = document.getElementById('custom-stats-roster-data');
    if (script && script.textContent) {
      try {
        return JSON.parse(script.textContent);
      } catch (error) {
        console.error('[custom-stats] Failed to parse roster bootstrap script', error);
      }
    }

    if (window.__CUSTOM_STATS_PAGE_PROPS__ && Array.isArray(window.__CUSTOM_STATS_PAGE_PROPS__.players)) {
      return window.__CUSTOM_STATS_PAGE_PROPS__.players;
    }

    if (window.__CUSTOM_STATS_ROSTER__ && Array.isArray(window.__CUSTOM_STATS_ROSTER__)) {
      return window.__CUSTOM_STATS_ROSTER__;
    }

    return [];
  }

  function normalizeRosterData(rawRoster) {
    if (!Array.isArray(rawRoster)) {
      return [];
    }

    const normalized = rawRoster
      .map((entry, index) => {
        const id = normalizeId(entry);
        if (id === null) {
          return null;
        }

        const label = String(entry.label || entry.name || entry.player_name || entry.player || '').trim();
        const jersey = determineJersey(entry, label);
        const searchTokens = buildSearchTokens(entry, label, jersey);

        return {
          id,
          label: label || String(entry.display || entry.text || id),
          jersey,
          tokens: searchTokens,
          index
        };
      })
      .filter(Boolean);

    normalized.sort((a, b) => {
      const aHasJersey = Number.isFinite(a.jersey);
      const bHasJersey = Number.isFinite(b.jersey);
      if (aHasJersey && bHasJersey) {
        if (a.jersey !== b.jersey) {
          return a.jersey - b.jersey;
        }
        return a.label.localeCompare(b.label, undefined, { sensitivity: 'base' });
      }
      if (aHasJersey && !bHasJersey) {
        return -1;
      }
      if (!aHasJersey && bHasJersey) {
        return 1;
      }
      const labelCompare = a.label.localeCompare(b.label, undefined, { sensitivity: 'base' });
      if (labelCompare !== 0) {
        return labelCompare;
      }
      return a.index - b.index;
    });

    return normalized;
  }

  function normalizeId(entry) {
    const candidate = entry && (entry.id ?? entry.player_id ?? entry.value ?? entry.pk ?? null);
    if (candidate === null || candidate === undefined) {
      return null;
    }
    const asNumber = Number(candidate);
    if (Number.isNaN(asNumber)) {
      return null;
    }
    return asNumber;
  }

  function determineJersey(entry, label) {
    if (entry && entry.jersey !== undefined && entry.jersey !== null) {
      const parsed = Number(entry.jersey);
      return Number.isFinite(parsed) ? parsed : null;
    }
    if (entry && entry.number !== undefined && entry.number !== null) {
      const parsed = Number(entry.number);
      return Number.isFinite(parsed) ? parsed : null;
    }
    const match = /^\s*#?(\d+)/.exec(label || '');
    if (!match) {
      return null;
    }
    const parsed = Number(match[1]);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function buildSearchTokens(entry, label, jersey) {
    const alt = entry && (entry.search || entry.search_text || entry.searchText || '');
    const text = [label, alt, jersey ? `#${jersey}` : '']
      .filter(Boolean)
      .join(' ')
      .toLowerCase();
    return text.replace(/[^a-z0-9\s]/gi, ' ').replace(/\s+/g, ' ').trim().split(' ').filter(Boolean);
  }

  function buildPlayerPicker(root, state, queueRefresh) {
    root.innerHTML = '';

    const wrapper = document.createElement('div');
    wrapper.className = 'space-y-3';

    const chipsRow = document.createElement('div');
    chipsRow.className = 'flex flex-wrap gap-2';
    wrapper.appendChild(chipsRow);

    const searchWrapper = document.createElement('div');
    searchWrapper.className = 'relative';
    const searchInput = document.createElement('input');
    searchInput.type = 'search';
    searchInput.placeholder = 'Search players';
    searchInput.className = 'w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-[#9E1B32] focus:outline-none focus:ring-2 focus:ring-[#9E1B32]/40';
    searchWrapper.appendChild(searchInput);
    wrapper.appendChild(searchWrapper);

    const optionList = document.createElement('ul');
    optionList.className = 'max-h-56 overflow-y-auto divide-y divide-gray-200 border border-gray-200 rounded-md bg-white shadow-sm';
    wrapper.appendChild(optionList);

    root.appendChild(wrapper);

    const playerUI = {
      onChange: null
    };

    function triggerChange() {
      if (typeof playerUI.onChange === 'function') {
        playerUI.onChange('players');
      }
    }

    function updateStateFromSelection() {
      const ids = Array.from(state.selectedPlayerIds.values());
      const ordered = state.roster
        .filter((entry) => ids.includes(entry.id))
        .map((entry) => entry.id);
      const prev = state.selectedPlayers.slice();
      state.selectedPlayers = ordered;
      const changed = prev.length !== ordered.length || prev.some((value, index) => value !== ordered[index]);
      if (changed) {
        triggerChange();
      }
    }

    function removePlayer(id) {
      state.selectedPlayerIds.delete(id);
      renderChips();
      renderOptions();
      updateStateFromSelection();
    }

    function addPlayer(id) {
      if (state.selectedPlayerIds.has(id)) {
        return;
      }
      state.selectedPlayerIds.add(id);
      renderChips();
      renderOptions();
      updateStateFromSelection();
    }

    function normalizePlayerId(value) {
      if (typeof value === 'number') {
        return Number.isInteger(value) ? value : null;
      }
      if (typeof value === 'string') {
        let trimmed = value.trim();
        if (!trimmed) {
          return null;
        }
        if (trimmed.startsWith('+')) {
          trimmed = trimmed.slice(1);
        }
        if (!/^[-]?\d+$/.test(trimmed)) {
          return null;
        }
        const parsed = Number.parseInt(trimmed, 10);
        return Number.isNaN(parsed) ? null : parsed;
      }
      return null;
    }

    function setSelected(ids) {
      const prev = state.selectedPlayers.slice();
      const rosterIds = new Set(state.roster.map((entry) => entry.id));
      const seen = new Set();
      const normalized = [];
      if (Array.isArray(ids)) {
        ids.forEach((value) => {
          const parsed = normalizePlayerId(value);
          if (parsed === null) {
            return;
          }
          if (!rosterIds.has(parsed) || seen.has(parsed)) {
            return;
          }
          seen.add(parsed);
          normalized.push(parsed);
        });
      }
      state.selectedPlayerIds = new Set(normalized);
      renderChips();
      renderOptions();
      updateStateFromSelection();
      const next = state.selectedPlayers.slice();
      if (next.length !== prev.length) {
        return true;
      }
      return next.some((value, index) => value !== prev[index]);
    }

    function renderChips() {
      chipsRow.innerHTML = '';
      const ordered = state.roster.filter((entry) => state.selectedPlayerIds.has(entry.id));
      if (!ordered.length) {
        const empty = document.createElement('p');
        empty.className = 'text-xs text-gray-500';
        empty.textContent = 'No players selected.';
        chipsRow.appendChild(empty);
        return;
      }

      ordered.forEach((entry) => {
        const chip = document.createElement('button');
        chip.type = 'button';
        chip.className = 'inline-flex items-center gap-1 rounded-full bg-[#9E1B32] px-3 py-1 text-xs font-semibold text-white shadow-sm hover:bg-[#7b1427]';
        chip.textContent = entry.label;
        chip.addEventListener('click', () => removePlayer(entry.id));
        chipsRow.appendChild(chip);
      });
    }

    function filterRoster(query) {
      const normalized = normalizeQuery(query);
      if (!normalized.length) {
        return state.roster.filter((entry) => !state.selectedPlayerIds.has(entry.id));
      }
      return state.roster.filter((entry) => {
        if (state.selectedPlayerIds.has(entry.id)) {
          return false;
        }
        return normalized.every((token) => entry.tokens.some((t) => t.startsWith(token)));
      });
    }

    function renderOptions() {
      optionList.innerHTML = '';
      const matches = filterRoster(searchInput.value);
      if (!matches.length) {
        const empty = document.createElement('li');
        empty.className = 'px-3 py-2 text-xs text-gray-500';
        empty.textContent = state.roster.length
          ? 'No matches found. Try a different search.'
          : 'Roster not available yet.';
        optionList.appendChild(empty);
        return;
      }
      matches.forEach((entry) => {
        const item = document.createElement('li');
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'flex w-full items-center justify-between px-3 py-2 text-sm hover:bg-gray-50';
        const labelSpan = document.createElement('span');
        labelSpan.textContent = entry.label;
        const addSpan = document.createElement('span');
        addSpan.className = 'text-xs text-[#9E1B32] font-semibold';
        addSpan.textContent = 'Add';
        button.appendChild(labelSpan);
        button.appendChild(addSpan);
        button.addEventListener('click', () => addPlayer(entry.id));
        item.appendChild(button);
        optionList.appendChild(item);
      });
    }

    searchInput.addEventListener('input', renderOptions);
    searchInput.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        event.preventDefault();
        const matches = filterRoster(searchInput.value);
        if (matches.length) {
          addPlayer(matches[0].id);
          searchInput.select();
        }
      }
    });

    renderChips();
    renderOptions();

    playerUI.setSelected = (ids) => setSelected(Array.isArray(ids) ? ids : []);
    playerUI.clearSelection = () => setSelected([]);

    return playerUI;
  }

  function normalizeQuery(query) {
    if (!query) {
      return [];
    }
    return String(query)
      .toLowerCase()
      .replace(/[^a-z0-9\s]/gi, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .split(' ')
      .filter(Boolean);
  }

  function normalizeSource(value) {
    if (value === null || value === undefined || value === '') {
      return 'practice';
    }
    const text = String(value).trim().toLowerCase();
    return text === 'game' ? 'game' : 'practice';
  }

  function normalizeModeForSource(mode, source) {
    const normalizedSource = normalizeSource(source);
    const raw = mode === null || mode === undefined ? '' : String(mode).trim().toLowerCase();
    if (normalizedSource === 'game') {
      if (raw === 'per_practice') {
        return 'per_game';
      }
      return raw === 'per_game' || raw === 'totals' ? raw : 'totals';
    }
    if (raw === 'per_game') {
      return 'per_practice';
    }
    return raw === 'per_practice' || raw === 'totals' ? raw : 'totals';
  }

  function fetchFields(url, container, searchInput, state, queueRefresh, options = {}) {
    if (!url) {
      container.innerHTML = '<p class="text-xs text-red-600">Missing stat field endpoint.</p>';
      return Promise.resolve(null);
    }

    container.innerHTML = '<p class="text-xs text-gray-500">Loading stat catalog…</p>';

    return fetch(url, { credentials: 'same-origin' })
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Failed to load fields (${response.status})`);
        }
        return response.json();
      })
      .then((catalog) => {
        buildFieldPicker(catalog, container, state, queueRefresh, options);
        if (searchInput && !searchInput.dataset.customStatsBound) {
          searchInput.addEventListener('input', () => filterFields(searchInput.value, state));
          searchInput.dataset.customStatsBound = '1';
        }
        return catalog;
      })
      .catch((error) => {
        console.error('[custom-stats] Failed to load field catalog', error);
        container.innerHTML = '<p class="text-xs text-red-600">Unable to load stat catalog.</p>';
        return null;
      });
  }

  function buildFieldPicker(catalog, container, state, queueRefresh, options = {}) {
    container.innerHTML = '';
    state.fieldCheckboxes = [];
    state.fieldOrder = new Map();

    if (!catalog || typeof catalog !== 'object') {
      container.innerHTML = '<p class="text-xs text-red-600">Stat catalog unavailable.</p>';
      return;
    }

    const availableKeys = new Set();
    Object.values(catalog).forEach((fields) => {
      if (!Array.isArray(fields)) {
        return;
      }
      fields.forEach((field) => {
        if (field && field.key) {
          availableKeys.add(String(field.key));
        }
      });
    });

    let orderCounter = 0;
    Object.entries(catalog).forEach(([groupLabel, fields]) => {
      const details = document.createElement('details');
      details.className = 'border border-gray-200 rounded-lg';
      const summary = document.createElement('summary');
      summary.className = 'cursor-pointer select-none px-3 py-2 text-sm font-semibold text-gray-800';
      summary.textContent = groupLabel;
      details.appendChild(summary);

      const list = document.createElement('div');
      list.className = 'px-3 py-2 space-y-2';
      if (Array.isArray(fields)) {
        fields.forEach((field) => {
          if (!field || !field.key) {
            return;
          }
          const key = field.key;
          state.fieldOrder.set(key, orderCounter++);
          const label = document.createElement('label');
          label.className = 'flex items-center gap-2 text-sm text-gray-700';
          label.dataset.key = key;
          label.dataset.search = `${(field.label || key).toLowerCase()} ${String(key).toLowerCase()}`;
          const checkbox = document.createElement('input');
          checkbox.type = 'checkbox';
          checkbox.value = key;
          checkbox.className = 'h-4 w-4 text-[#9E1B32] border-gray-300 rounded';
          checkbox.addEventListener('change', () => {
            if (checkbox.checked) {
              if (!state.selectedFields.includes(key)) {
                state.selectedFields.push(key);
              }
            } else {
              state.selectedFields = state.selectedFields.filter((k) => k !== key);
            }
            state.selectedFields = dedupeAndSortFields(state.selectedFields, state.fieldOrder);
            state.selectedFieldsBySource[state.source] = state.selectedFields.slice();
            queueRefresh('fields');
          });
          const text = document.createElement('span');
          text.textContent = field.label || key;
          label.appendChild(checkbox);
          label.appendChild(text);
          list.appendChild(label);
          state.fieldCheckboxes.push({ checkbox, label });
        });
      }
      details.appendChild(list);
      container.appendChild(details);
    });

    state.fieldOrder = rebuildFieldOrder(state.selectedFields, state.fieldOrder);

    const previousSelection = state.selectedFields.slice();
    const filteredSelection = previousSelection.filter((key) => availableKeys.has(key));
    const selectionChanged = filteredSelection.length !== previousSelection.length;
    state.selectedFields = dedupeAndSortFields(filteredSelection, state.fieldOrder);
    state.selectedFieldsBySource[state.source] = state.selectedFields.slice();
    syncFieldCheckboxes(state);

    if (selectionChanged && options.skipRefresh !== true && state.autoRefresh) {
      queueRefresh('fields');
    }
  }

  function dedupeAndSortFields(fields, orderMap) {
    const seen = new Set();
    const filtered = [];
    fields.forEach((key) => {
      if (!seen.has(key)) {
        seen.add(key);
        filtered.push(key);
      }
    });
    filtered.sort((a, b) => {
      const aOrder = orderMap.has(a) ? orderMap.get(a) : Number.MAX_SAFE_INTEGER;
      const bOrder = orderMap.has(b) ? orderMap.get(b) : Number.MAX_SAFE_INTEGER;
      if (aOrder !== bOrder) {
        return aOrder - bOrder;
      }
      return a.localeCompare(b);
    });
    return filtered;
  }

  function rebuildFieldOrder(selectedKeys, baseOrder) {
    const hasMap = baseOrder instanceof Map;
    const next = new Map();
    let counter = 0;

    (selectedKeys || []).forEach((key) => {
      if (hasMap && baseOrder.has(key) && !next.has(key)) {
        next.set(key, counter++);
      }
    });

    if (hasMap) {
      baseOrder.forEach((_, key) => {
        if (!next.has(key)) {
          next.set(key, counter++);
        }
      });
    }

    return next;
  }

  function reorderSelectedFields(list, fromKey, toKey, insertAfter) {
    if (!Array.isArray(list)) {
      return null;
    }
    const working = list.slice();
    const fromIndex = working.indexOf(fromKey);
    const targetIndex = working.indexOf(toKey);
    if (fromIndex === -1 || targetIndex === -1) {
      return null;
    }
    const [moved] = working.splice(fromIndex, 1);
    let insertIndex = insertAfter ? targetIndex + 1 : targetIndex;
    if (fromIndex < insertIndex) {
      insertIndex -= 1;
    }
    working.splice(insertIndex, 0, moved);
    return working;
  }

  function filterFields(query, state) {
    const normalized = normalizeQuery(query);
    const groups = new Map();

    state.fieldCheckboxes.forEach(({ label }) => {
      const key = label.dataset.key;
      const searchValue = label.dataset.search || '';
      const matches = normalized.length === 0 || normalized.every((token) => searchValue.includes(token));
      label.style.display = matches ? '' : 'none';
      const parent = label.closest('details');
      if (parent) {
        const groupState = groups.get(parent) || { visible: 0, total: 0 };
        groupState.total += 1;
        if (matches) {
          groupState.visible += 1;
        }
        groups.set(parent, groupState);
      }
    });

    groups.forEach((info, details) => {
      const shouldOpen = normalized.length && info.visible > 0;
      if (shouldOpen) {
        details.open = true;
      }
      const hidden = info.visible === 0;
      details.style.display = hidden ? 'none' : '';
    });
  }

  function getDateSelection(state, source) {
    if (!state || !state.dateSelections) {
      return { from: null, to: null };
    }
    const normalized = normalizeSource(source || state.source);
    const selection = state.dateSelections[normalized] || { from: null, to: null };
    if (!state.dateSelections[normalized]) {
      state.dateSelections[normalized] = selection;
    }
    return {
      from: selection.from || null,
      to: selection.to || null
    };
  }

  function setDateSelection(state, source, { from = null, to = null } = {}) {
    if (!state.dateSelections) {
      state.dateSelections = { practice: { from: null, to: null }, game: { from: null, to: null } };
    }
    const normalized = normalizeSource(source || state.source);
    if (!state.dateSelections[normalized]) {
      state.dateSelections[normalized] = { from: null, to: null };
    }
    state.dateSelections[normalized].from = from || null;
    state.dateSelections[normalized].to = to || null;
  }

  function getGameSelection(state, source) {
    if (!state || !state.gameSelections) {
      return [];
    }
    const normalized = normalizeSource(source || state.source);
    const selection = Array.isArray(state.gameSelections[normalized]) ? state.gameSelections[normalized] : [];
    state.gameSelections[normalized] = selection;
    return selection.slice();
  }

  function setGameSelection(state, source, gameIds = []) {
    if (!state.gameSelections) {
      state.gameSelections = { practice: [], game: [] };
    }
    const normalized = normalizeSource(source || state.source);
    const normalizedIds = [];
    const seen = new Set();
    (Array.isArray(gameIds) ? gameIds : []).forEach((value) => {
      const parsed = Number(value);
      if (!Number.isFinite(parsed) || seen.has(parsed)) {
        return;
      }
      seen.add(parsed);
      normalizedIds.push(parsed);
    });
    state.gameSelections[normalized] = normalizedIds;
  }

  function hydrateDates(elements, state, queueRefresh) {
    const activeDates = getDateSelection(state);

    if (elements.dateFrom) {
      elements.dateFrom.value = activeDates.from || '';
      elements.dateFrom.addEventListener('change', () => {
        const nextFrom = elements.dateFrom.value ? elements.dateFrom.value : null;
        const current = getDateSelection(state);
        setDateSelection(state, state.source, { from: nextFrom, to: current.to });
        state.lastPayload = null;
        if (typeof queueRefresh === 'function') {
          queueRefresh('dateFrom');
        }
      });
    }

    if (elements.dateTo) {
      elements.dateTo.value = activeDates.to || '';
      elements.dateTo.addEventListener('change', () => {
        const nextTo = elements.dateTo.value ? elements.dateTo.value : null;
        const current = getDateSelection(state);
        setDateSelection(state, state.source, { from: current.from, to: nextTo });
        state.lastPayload = null;
        if (typeof queueRefresh === 'function') {
          queueRefresh('dateTo');
        }
      });
    }
  }

  function hydrateGames(elements, state, config, queueRefresh) {
    const searchInput = elements.gameSearch;
    const dropdown = elements.gameDropdown;
    const chips = elements.gameChips;
    const clearButton = elements.gameClear;
    const helper = elements.gameHelp;

    if (!searchInput || !dropdown || !chips) {
      return;
    }

    let activeIndex = -1;
    let filteredGames = [];
    let loading = false;

    function updateHelpMessage(text) {
      if (helper) {
        helper.textContent = text || '';
      }
    }

    function closeDropdown() {
      dropdown.classList.add('hidden');
      activeIndex = -1;
    }

    function formatGameLabel(game) {
      if (!game) return '';
      const label = String(game.label || '').trim();
      if (label) return label;
      if (game.date && game.opponent) {
        return `${game.date} vs ${game.opponent}`;
      }
      if (game.date) return String(game.date);
      return `Game #${game.id}`;
    }

    function renderChips() {
      const selection = getGameSelection(state, state.source);
      chips.innerHTML = '';

      if (!selection.length) {
        chips.textContent = 'No games selected (optional).';
        chips.classList.add('text-gray-500');
        if (clearButton) {
          clearButton.classList.add('hidden');
        }
        return;
      }

      chips.classList.remove('text-gray-500');
      const fragment = document.createDocumentFragment();
      const selectionSet = new Set(selection);
      state.gameOptions
        .filter((game) => selectionSet.has(Number(game.id)))
        .forEach((game) => {
          const chip = document.createElement('span');
          chip.className =
            'inline-flex items-center gap-2 rounded-full bg-gray-100 px-3 py-1 text-xs font-medium text-gray-800 border border-gray-200';
          chip.textContent = formatGameLabel(game);

          const removeBtn = document.createElement('button');
          removeBtn.type = 'button';
          removeBtn.className = 'text-gray-500 hover:text-gray-700 focus:outline-none';
          removeBtn.innerHTML = '&times;';
          removeBtn.addEventListener('click', () => {
            const nextSelection = selection.filter((id) => id !== Number(game.id));
            setGameSelection(state, state.source, nextSelection);
            state.lastPayload = null;
            renderChips();
            if (state.autoRefresh && state.source === 'game') {
              queueRefresh('games-remove');
            }
          });
          chip.appendChild(removeBtn);
          fragment.appendChild(chip);
        });

      chips.appendChild(fragment);
      if (clearButton) {
        clearButton.classList.toggle('hidden', selection.length === 0);
      }
    }

    function handleSelection(gameId) {
      const parsedId = Number(gameId);
      if (!Number.isFinite(parsedId)) {
        return;
      }
      const current = getGameSelection(state, state.source);
      if (current.includes(parsedId)) {
        closeDropdown();
        return;
      }
      const next = current.concat(parsedId);
      setGameSelection(state, state.source, next);
      state.lastPayload = null;
      renderChips();
      if (state.autoRefresh && state.source === 'game') {
        queueRefresh('games');
      }
      closeDropdown();
      searchInput.value = '';
      filteredGames = state.gameOptions.slice();
      renderDropdown();
    }

    function renderDropdown() {
      dropdown.innerHTML = '';
      const selection = new Set(getGameSelection(state, state.source));

      if (loading) {
        updateHelpMessage('Loading games…');
        const loadingItem = document.createElement('div');
        loadingItem.className = 'px-3 py-2 text-sm text-gray-500';
        loadingItem.textContent = 'Loading games…';
        dropdown.appendChild(loadingItem);
        dropdown.classList.remove('hidden');
        return;
      }

      if (!state.gameOptions.length) {
        updateHelpMessage('No games are available yet. Upload or tag games to enable this filter.');
        const empty = document.createElement('div');
        empty.className = 'px-3 py-2 text-sm text-gray-500';
        empty.textContent = 'No games available';
        dropdown.appendChild(empty);
        dropdown.classList.remove('hidden');
        return;
      }

      if (!filteredGames.length) {
        updateHelpMessage('No matches. Try a different search or adjust your dates.');
        const empty = document.createElement('div');
        empty.className = 'px-3 py-2 text-sm text-gray-500';
        empty.textContent = 'No games match your search.';
        dropdown.appendChild(empty);
        dropdown.classList.remove('hidden');
        return;
      }

      updateHelpMessage('Choose specific games (optional). Leave empty to include all games in the date window.');

      const list = document.createElement('div');
      list.setAttribute('role', 'listbox');
      filteredGames.forEach((game, index) => {
        const item = document.createElement('button');
        const isActive = index === activeIndex;
        const isSelected = selection.has(Number(game.id));
        item.type = 'button';
        item.className = `flex w-full items-center justify-between px-3 py-2 text-left text-sm hover:bg-gray-50 ${
          isActive ? 'bg-gray-100' : ''
        } ${isSelected ? 'font-semibold text-[#9E1B32]' : 'text-gray-800'}`;
        item.dataset.gameId = game.id;
        item.innerHTML = `<span>${formatGameLabel(game)}</span>${isSelected ? '<span class="text-xs">Selected</span>' : ''}`;
        item.addEventListener('click', () => handleSelection(game.id));
        list.appendChild(item);
      });

      dropdown.appendChild(list);
      dropdown.classList.remove('hidden');
    }

    function filterGames(query) {
      const term = String(query || '').toLowerCase().trim();
      if (!term) {
        filteredGames = state.gameOptions.slice();
        activeIndex = -1;
        renderDropdown();
        return;
      }
      filteredGames = state.gameOptions.filter((game) => formatGameLabel(game).toLowerCase().includes(term));
      activeIndex = filteredGames.length ? 0 : -1;
      renderDropdown();
    }

    function handleKeydown(event) {
      if (dropdown.classList.contains('hidden')) {
        return;
      }
      if (event.key === 'ArrowDown') {
        event.preventDefault();
        if (filteredGames.length) {
          activeIndex = (activeIndex + 1) % filteredGames.length;
          renderDropdown();
        }
      } else if (event.key === 'ArrowUp') {
        event.preventDefault();
        if (filteredGames.length) {
          activeIndex = activeIndex <= 0 ? filteredGames.length - 1 : activeIndex - 1;
          renderDropdown();
        }
      } else if (event.key === 'Enter') {
        if (activeIndex >= 0 && filteredGames[activeIndex]) {
          event.preventDefault();
          handleSelection(filteredGames[activeIndex].id);
        }
      } else if (event.key === 'Escape') {
        closeDropdown();
      }
    }

    searchInput.addEventListener('focus', () => {
      dropdown.classList.remove('hidden');
      renderDropdown();
    });

    searchInput.addEventListener('input', (event) => {
      filterGames(event.target.value || '');
    });

    searchInput.addEventListener('keydown', handleKeydown);

    if (clearButton) {
      clearButton.addEventListener('click', () => {
        setGameSelection(state, state.source, []);
        state.lastPayload = null;
        renderChips();
        if (state.autoRefresh && state.source === 'game') {
          queueRefresh('games-clear');
        }
      });
    }

    document.addEventListener('click', (event) => {
      if (!elements.gamePicker || elements.gamePicker.contains(event.target)) {
        return;
      }
      closeDropdown();
    });

    async function loadGames() {
      const url = config && config.gamesUrl ? config.gamesUrl : null;
      if (!url) {
        state.gameOptions = [];
        filteredGames = [];
        renderDropdown();
        return;
      }
      loading = true;
      renderDropdown();
      try {
        const res = await fetch(url, { credentials: 'same-origin' });
        if (!res.ok) {
          throw new Error(`Failed to load games (${res.status})`);
        }
        const payload = await res.json();
        const games = Array.isArray(payload.games) ? payload.games : Array.isArray(payload) ? payload : [];
        state.gameOptions = games;
        filteredGames = games.slice();
        loading = false;
        renderChips();
        renderDropdown();
        if (state.autoRefresh && state.source === 'game') {
          queueRefresh('games-loaded');
        }
      } catch (error) {
        console.error('[custom-stats] Unable to load games', error);
        loading = false;
        state.gameOptions = [];
        filteredGames = [];
        updateHelpMessage('Unable to load games. Please refresh or check your connection.');
        renderDropdown();
      }
    }

    elements.renderGameSelection = renderChips;
    elements.closeGameDropdown = closeDropdown;

    renderDropdown();
    renderChips();
    loadGames();
  }

  function hydrateModeToggle(container, state, queueRefresh, elements) {
    if (!container) {
      return;
    }

    const buttons = Array.from(container.querySelectorAll('.mode-pill'));
    buttons.forEach((button) => {
      button.addEventListener('click', () => {
        const rawMode = button.dataset.mode;
        const nextMode = normalizeModeForSource(rawMode, state.source);
        if (!nextMode || state.mode === nextMode) {
          return;
        }
        state.mode = nextMode;
        state.modeSelections[state.source] = nextMode;
        buttons.forEach((btn) => {
          const isActive = btn === button;
          btn.classList.toggle('active', isActive);
          btn.classList.toggle('bg-[#9E1B32]', isActive);
          btn.classList.toggle('text-white', isActive);
          btn.classList.toggle('text-gray-700', !isActive);
        });
        updateModeToggleLabels(state, elements);
        queueRefresh('mode');
      });
    });

    updateModeButtons(state);
    updateModeToggleLabels(state, elements);
  }

  function hydrateAutoRefresh(input, state) {
    if (!input) {
      return;
    }
    state.autoRefresh = Boolean(input.checked);
    input.addEventListener('change', () => {
      state.autoRefresh = Boolean(input.checked);
    });
  }

  function updateSourceHeadline(state, elements) {
    if (elements.sourceHeading) {
      elements.sourceHeading.textContent = state.source === 'game' ? 'Game' : 'Practice';
    }
    if (elements.sourceDescription) {
      elements.sourceDescription.textContent = state.source === 'game' ? 'game' : 'practice';
    }
  }

  function updateSourceButtons(state, elements) {
    if (!elements.sourceToggle) {
      return;
    }
    const buttons = Array.from(elements.sourceToggle.querySelectorAll('.source-pill'));
    buttons.forEach((button) => {
      const target = normalizeSource(button.dataset.source);
      const isActive = target === state.source;
      button.classList.toggle('active', isActive);
      button.classList.toggle('bg-[#9E1B32]', isActive);
      button.classList.toggle('text-white', isActive);
      button.classList.toggle('text-gray-700', !isActive);
    });
  }

  function updateGameSelectorVisibility(state, elements) {
    if (!elements.gameRow) {
      return;
    }
    const isGame = state.source === 'game';
    elements.gameRow.classList.toggle('hidden', !isGame);
    if (isGame && typeof elements.renderGameSelection === 'function') {
      elements.renderGameSelection();
    }
    if (!isGame && typeof elements.closeGameDropdown === 'function') {
      elements.closeGameDropdown();
    }
  }

  function updateModeToggleLabels(state, elements) {
    if (elements.modeLabel) {
      elements.modeLabel.textContent = state.source === 'game' ? 'Totals vs. Per Game' : 'Totals vs. Per Practice';
    }
    const container = document.getElementById('custom-mode-toggle');
    if (!container) {
      return;
    }
    const perButton = container.querySelector('.mode-pill[data-role="per"]');
    if (perButton) {
      const nextMode = state.source === 'game' ? 'per_game' : 'per_practice';
      const label = state.source === 'game' ? 'Per Game' : 'Per Practice';
      perButton.dataset.mode = nextMode;
      perButton.textContent = label;
    }
  }

  function hydrateSourceToggle(elements, state, config, queueRefresh, playerUI, setSource) {
    if (!elements.sourceToggle || typeof setSource !== 'function') {
      return;
    }

    const buttons = Array.from(elements.sourceToggle.querySelectorAll('.source-pill'));
    buttons.forEach((button) => {
      button.addEventListener('click', async () => {
        const target = normalizeSource(button.dataset.source);
        if (target === state.source) {
          return;
        }
        await setSource(target, { force: true });
      });
    });
  }

  function hydratePresets(config, elements, state, queueRefresh, playerUI, setSource) {
    if (!elements.presetsPanel) {
      return;
    }

    const activateTab = setupPresetTabs(elements.presetTabs, elements.presetPanes);

    const containers = {
      players: elements.playersPresetsList,
      stats: elements.statsPresetsList,
      dates: elements.datesPresetsList,
      combined: elements.combinedPresetsList
    };

    const containerOptions = {
      combined: {
        combinedWrapper: elements.combinedWrapper,
        accordion: elements.combinedAccordion
      }
    };

    Object.entries(containers).forEach(([type, container]) => {
      if (!container) {
        return;
      }
      container.addEventListener('click', async (event) => {
        const button = event.target.closest('button[data-action]');
        if (!button) {
          return;
        }
        event.preventDefault();
        const id = button.dataset.id;
        const action = button.dataset.action;
        const presets = Array.isArray(state.presets[type]) ? state.presets[type] : [];
        const preset = presets.find((p) => String(p.id) === String(id));
        if (!preset) {
          notify('error', 'Preset not found.');
          return;
        }

        if (action === 'apply') {
          let changed = false;
          let sourceChanged = false;

          if (preset.source_default) {
            const desiredSource = normalizeSource(preset.source_default);
            if (desiredSource !== state.source && typeof setSource === 'function') {
              await setSource(desiredSource, { force: true, queueRefresh: false });
              sourceChanged = true;
            }
          }

          if (preset.preset_type === 'combined') {
            changed = applyCombinedPreset(preset, state, playerUI, elements);
          } else if (type === 'players') {
            changed = applyPlayersPreset(preset, state, playerUI);
          } else if (type === 'stats') {
            changed = applyStatsPreset(preset, state, elements);
          } else if (type === 'dates') {
            changed = applyDatesPreset(preset, state, elements);
          }
          if (changed || sourceChanged) {
            state.lastPayload = null;
            queueRefresh(`preset-${type}`);
          }
          notify('success', 'Preset applied.');
          return;
        }

        if (action === 'rename') {
          const nextNameRaw = window.prompt('Rename preset', preset.name || '');
          if (nextNameRaw === null) {
            return;
          }
          const trimmed = nextNameRaw.trim();
          if (!trimmed) {
            notify('error', 'Preset name cannot be empty.');
            return;
          }
          if (trimmed === (preset.name || '')) {
            return;
          }
          try {
            const updated = await api.updatePreset(preset.id, { name: trimmed });
            preset.name = (updated && updated.name) || trimmed;
            sortPresetsByName(state.presets[type]);
            renderPresetList(type, container, state.presets[type], containerOptions[type]);
            notify('success', 'Preset renamed.');
          } catch (error) {
            console.error('[custom-stats] Failed to rename preset', error);
            notify('error', error.message || 'Unable to rename preset.');
          }
          return;
        }

        if (action === 'delete') {
          const confirmed = window.confirm('Delete this preset? This cannot be undone.');
          if (!confirmed) {
            return;
          }
          try {
            await api.deletePreset(preset.id);
            state.presets[type] = presets.filter((p) => String(p.id) !== String(preset.id));
            renderPresetList(type, container, state.presets[type], containerOptions[type]);
            notify('success', 'Preset deleted.');
          } catch (error) {
            console.error('[custom-stats] Failed to delete preset', error);
            notify('error', error.message || 'Unable to delete preset.');
          }
        }
      });
    });

    async function loadInitialPresets() {
      try {
        const [players, stats, dates, combined] = await Promise.all([
          api.listPresets({ preset_type: 'players' }),
          api.listPresets({ preset_type: 'stats' }),
          api.listPresets({ preset_type: 'dates' }),
          api.listPresets({ preset_type: 'combined' })
        ]);
        state.presets.players = Array.isArray(players) ? players : [];
        state.presets.stats = Array.isArray(stats) ? stats : [];
        state.presets.dates = Array.isArray(dates) ? dates : [];
        state.presets.combined = Array.isArray(combined) ? combined : [];
        sortPresetsByName(state.presets.players);
        sortPresetsByName(state.presets.stats);
        sortPresetsByName(state.presets.dates);
        sortPresetsByName(state.presets.combined);
        renderPresetList('players', containers.players, state.presets.players, containerOptions.players);
        renderPresetList('stats', containers.stats, state.presets.stats, containerOptions.stats);
        renderPresetList('dates', containers.dates, state.presets.dates, containerOptions.dates);
        renderPresetList('combined', containers.combined, state.presets.combined, containerOptions.combined);
      } catch (error) {
        console.error('[custom-stats] Failed to load presets', error);
        notify('error', 'Unable to load presets.');
        Object.entries(containers).forEach(([type, container]) => {
          if (!container) {
            return;
          }
          renderPresetError(container, 'Unable to load presets.');
          state.presets[type] = [];
          const options = containerOptions[type];
          if (options && options.combinedWrapper) {
            options.combinedWrapper.classList.remove('hidden');
          }
        });
      }
    }

    loadInitialPresets();

    if (elements.playersPresetSave) {
      elements.playersPresetSave.addEventListener('click', async () => {
        const name = (elements.playersPresetName && elements.playersPresetName.value || '').trim();
        if (!name) {
          notify('error', 'Enter a name for the preset.');
          return;
        }
        if (!state.selectedPlayers.length) {
          notify('error', 'Select at least one player to save this preset.');
          return;
        }
        const payload = {
          name,
          preset_type: 'players',
          player_ids: state.selectedPlayers.slice(),
          fields: [],
          date_from: null,
          date_to: null,
          mode_default: state.mode,
          source_default: state.source || 'practice',
          visibility: 'team'
        };
        if (elements.playersPresetSave.disabled) {
          return;
        }
        setButtonLoading(elements.playersPresetSave, true);
        try {
          const created = await api.createPreset(payload);
          if (elements.playersPresetName) {
            elements.playersPresetName.value = '';
          }
          if (created && created.id) {
            state.presets.players.push(created);
            sortPresetsByName(state.presets.players);
            renderPresetList('players', containers.players, state.presets.players, containerOptions.players);
          } else {
            await loadInitialPresets();
          }
          notify('success', 'Players preset saved.');
        } catch (error) {
          console.error('[custom-stats] Failed to save players preset', error);
          notify('error', error.message || 'Unable to save preset.');
        } finally {
          setButtonLoading(elements.playersPresetSave, false);
        }
      });
    }

    if (elements.statsPresetSave) {
      elements.statsPresetSave.addEventListener('click', async () => {
        const name = (elements.statsPresetName && elements.statsPresetName.value || '').trim();
        if (!name) {
          notify('error', 'Enter a name for the preset.');
          return;
        }
        if (!state.selectedFields.length) {
          notify('error', 'Select at least one stat to save this preset.');
          return;
        }
        const payload = {
          name,
          preset_type: 'stats',
          player_ids: [],
          fields: state.selectedFields.slice(),
          date_from: null,
          date_to: null,
          mode_default: state.mode,
          source_default: state.source || 'practice',
          visibility: 'team'
        };
        if (elements.statsPresetSave.disabled) {
          return;
        }
        setButtonLoading(elements.statsPresetSave, true);
        try {
          const created = await api.createPreset(payload);
          if (elements.statsPresetName) {
            elements.statsPresetName.value = '';
          }
          if (created && created.id) {
            state.presets.stats.push(created);
            sortPresetsByName(state.presets.stats);
            renderPresetList('stats', containers.stats, state.presets.stats, containerOptions.stats);
          } else {
            await loadInitialPresets();
          }
          notify('success', 'Stats preset saved.');
        } catch (error) {
          console.error('[custom-stats] Failed to save stats preset', error);
          notify('error', error.message || 'Unable to save preset.');
        } finally {
          setButtonLoading(elements.statsPresetSave, false);
        }
      });
    }

    if (elements.datesPresetSave) {
      elements.datesPresetSave.addEventListener('click', async () => {
        const name = (elements.datesPresetName && elements.datesPresetName.value || '').trim();
        if (!name) {
          notify('error', 'Enter a name for the preset.');
          return;
        }
        const activeDates = getDateSelection(state);
        const currentFrom = elements.dateFrom ? elements.dateFrom.value || activeDates.from : activeDates.from;
        const currentTo = elements.dateTo ? elements.dateTo.value || activeDates.to : activeDates.to;
        const from = currentFrom ? currentFrom : null;
        const to = currentTo ? currentTo : null;
        const gameIds = getGameSelection(state, state.source);
        if (!from && !to) {
          notify('error', 'Select at least one date to save this preset.');
          return;
        }
        if (from && to && from > to) {
          notify('error', 'The start date must be before or equal to the end date.');
          return;
        }
        const payload = {
          name,
          preset_type: 'dates',
          player_ids: [],
          fields: [],
          date_from: from,
          date_to: to,
          game_ids: gameIds,
          mode_default: state.mode,
          source_default: state.source || 'practice',
          visibility: 'team'
        };
        if (elements.datesPresetSave.disabled) {
          return;
        }
        setButtonLoading(elements.datesPresetSave, true);
        try {
          const created = await api.createPreset(payload);
          if (elements.datesPresetName) {
            elements.datesPresetName.value = '';
          }
          if (created && created.id) {
            state.presets.dates.push(created);
            sortPresetsByName(state.presets.dates);
            renderPresetList('dates', containers.dates, state.presets.dates, containerOptions.dates);
          } else {
            await loadInitialPresets();
          }
          notify('success', 'Dates preset saved.');
        } catch (error) {
          console.error('[custom-stats] Failed to save dates preset', error);
          notify('error', error.message || 'Unable to save preset.');
        } finally {
          setButtonLoading(elements.datesPresetSave, false);
        }
      });
    }

    if (typeof activateTab === 'function') {
      activateTab('players');
    }
  }

  function renderPresetError(container, message) {
    if (!container) {
      return;
    }
    container.innerHTML = '';
    const errorNode = document.createElement('p');
    errorNode.className = 'py-2 text-xs text-red-600';
    errorNode.textContent = message;
    container.appendChild(errorNode);
  }

  function renderPresetList(type, container, presets, options = {}) {
    if (!container) {
      return;
    }
    container.innerHTML = '';

    const combinedWrapper = options.combinedWrapper || null;
    const accordion = options.accordion || null;

    if (!Array.isArray(presets) || !presets.length) {
      if (combinedWrapper) {
        combinedWrapper.classList.add('hidden');
        if (accordion) {
          accordion.open = false;
        }
      } else {
        const empty = document.createElement('p');
        empty.className = 'py-3 text-xs text-gray-500';
        if (type === 'players') {
          empty.textContent = 'No players presets yet';
        } else if (type === 'stats') {
          empty.textContent = 'No stats presets yet';
        } else if (type === 'dates') {
          empty.textContent = 'No dates presets yet';
        } else {
          empty.textContent = 'No presets found';
        }
        container.appendChild(empty);
      }
      return;
    }

    if (combinedWrapper) {
      combinedWrapper.classList.remove('hidden');
    }

    presets.forEach((preset) => {
      const row = document.createElement('div');
      row.className = 'flex items-center justify-between gap-3 py-2';

      const name = document.createElement('div');
      name.className = 'truncate font-medium text-gray-900';
      name.textContent = preset.name || 'Preset';

      const actions = document.createElement('div');
      actions.className = 'flex gap-2';

      const applyButton = document.createElement('button');
      applyButton.type = 'button';
      applyButton.dataset.action = 'apply';
      applyButton.dataset.id = preset.id;
      applyButton.className = 'px-2 py-1 text-xs font-medium rounded-md border border-gray-300 text-gray-700 hover:bg-gray-100';
      applyButton.textContent = 'Apply';

      const renameButton = document.createElement('button');
      renameButton.type = 'button';
      renameButton.dataset.action = 'rename';
      renameButton.dataset.id = preset.id;
      renameButton.className = 'px-2 py-1 text-xs font-medium rounded-md border border-gray-300 text-gray-700 hover:bg-gray-100';
      renameButton.textContent = 'Rename';

      const deleteButton = document.createElement('button');
      deleteButton.type = 'button';
      deleteButton.dataset.action = 'delete';
      deleteButton.dataset.id = preset.id;
      deleteButton.className = 'px-2 py-1 text-xs font-medium rounded-md border border-red-200 text-red-600 hover:bg-red-50';
      deleteButton.textContent = 'Delete';

      actions.appendChild(applyButton);
      actions.appendChild(renameButton);
      actions.appendChild(deleteButton);

      row.appendChild(name);
      row.appendChild(actions);
      container.appendChild(row);
    });
  }

  function setupPresetTabs(tabs, panes) {
    const tabList = Array.isArray(tabs) ? tabs : [];
    const paneList = Array.isArray(panes) ? panes : [];
    if (!tabList.length || !paneList.length) {
      return () => {};
    }

    function activate(type) {
      tabList.forEach((tab) => {
        const isActive = tab.dataset.tab === type;
        tab.classList.toggle('active', isActive);
        tab.classList.toggle('border-b-2', isActive);
        tab.classList.toggle('border-gray-900', isActive);
        tab.classList.toggle('text-gray-900', isActive);
        tab.classList.toggle('font-semibold', isActive);
        tab.classList.toggle('text-gray-500', !isActive);
        tab.classList.toggle('font-medium', !isActive);
      });

      paneList.forEach((pane) => {
        const matches = pane.dataset.pane === type;
        pane.classList.toggle('hidden', !matches);
      });

      return type;
    }

    tabList.forEach((tab) => {
      tab.addEventListener('click', (event) => {
        event.preventDefault();
        const type = tab.dataset.tab;
        if (type) {
          activate(type);
        }
      });
    });

    const initial =
      tabList.find((tab) => tab.classList.contains('active'))?.dataset.tab ||
      (tabList[0] && tabList[0].dataset.tab);

    if (initial) {
      activate(initial);
    }

    return activate;
  }

  function sortPresetsByName(list) {
    if (!Array.isArray(list)) {
      return;
    }
    list.sort((a, b) => String(a?.name || '').localeCompare(String(b?.name || ''), undefined, { sensitivity: 'base' }));
  }

  function applyPlayersPreset(preset, state, playerUI) {
    if (!preset || !playerUI || typeof playerUI.setSelected !== 'function') {
      return false;
    }
    const incoming = Array.isArray(preset.player_ids) ? preset.player_ids.slice() : [];
    return Boolean(playerUI.setSelected(incoming));
  }

  function applyStatsPreset(preset, state, elements) {
    if (!preset) {
      return false;
    }
    const incoming = Array.isArray(preset.fields) ? preset.fields : [];
    const prepared = incoming
      .map((rawKey) => (typeof rawKey === 'string' ? rawKey.trim() : String(rawKey || '').trim()))
      .filter((key) => {
        if (!key) {
          return false;
        }
        if (state.fieldOrder instanceof Map && state.fieldOrder.size && !state.fieldOrder.has(key)) {
          return false;
        }
        return true;
      });

    const next = dedupeAndSortFields(prepared, state.fieldOrder);
    const prev = state.selectedFields.slice();
    state.selectedFields = next;
    state.fieldOrder = rebuildFieldOrder(next, state.fieldOrder);
    state.selectedFieldsBySource[state.source] = next.slice();
    syncFieldCheckboxes(state);

    let changed = prev.length !== next.length || prev.some((value, index) => value !== next[index]);

    if (preset.mode_default) {
      const desiredMode = normalizeModeForSource(preset.mode_default, state.source);
      if (state.mode !== desiredMode) {
        state.mode = desiredMode;
        state.modeSelections[state.source] = desiredMode;
        updateModeButtons(state);
        updateModeToggleLabels(state, elements);
        changed = true;
      }
    }

    return changed;
  }

  function applyDatesPreset(preset, state, elements) {
    const from = preset && preset.date_from ? preset.date_from : null;
    const to = preset && preset.date_to ? preset.date_to : null;
    const previous = getDateSelection(state);
    let changed = previous.from !== from || previous.to !== to;
    setDateSelection(state, state.source, { from, to });
    if (elements.dateFrom) {
      elements.dateFrom.value = from || '';
    }
    if (elements.dateTo) {
      elements.dateTo.value = to || '';
    }
    const nextGames = Array.isArray(preset && preset.game_ids) ? preset.game_ids : [];
    const previousGames = getGameSelection(state);
    setGameSelection(state, state.source, nextGames);
    const storedGames = getGameSelection(state);
    const gamesChanged =
      previousGames.length !== storedGames.length || previousGames.some((value, idx) => value !== storedGames[idx]);
    if (typeof elements.renderGameSelection === 'function') {
      elements.renderGameSelection();
    }
    if (gamesChanged) {
      changed = true;
    }
    updateGameSelectorVisibility(state, elements);
    return changed;
  }

  function applyCombinedPreset(preset, state, playerUI, elements) {
    if (!preset) {
      return false;
    }
    let changed = false;

    if (playerUI && typeof playerUI.setSelected === 'function') {
      const existingPlayers = state.selectedPlayers.slice();
      const seenPlayers = new Set(existingPlayers.map((value) => String(value)));
      const incomingPlayers = Array.isArray(preset.player_ids) ? preset.player_ids : [];
      incomingPlayers.forEach((playerId) => {
        const key = String(playerId);
        if (!key) {
          return;
        }
        if (!seenPlayers.has(key)) {
          seenPlayers.add(key);
          existingPlayers.push(playerId);
        }
      });
      if (playerUI.setSelected(existingPlayers)) {
        changed = true;
      }
    }

    const incomingFields = Array.isArray(preset.fields) ? preset.fields : [];
    if (incomingFields.length) {
      const mergedFields = state.selectedFields.slice();
      const seenFields = new Set(mergedFields);
      let fieldsChanged = false;
      incomingFields.forEach((rawKey) => {
        const key = typeof rawKey === 'string' ? rawKey : String(rawKey || '').trim();
        if (!key) {
          return;
        }
        if (state.fieldOrder instanceof Map && state.fieldOrder.size && !state.fieldOrder.has(key)) {
          return;
        }
        if (!seenFields.has(key)) {
          seenFields.add(key);
          mergedFields.push(key);
          fieldsChanged = true;
        }
      });
      if (fieldsChanged) {
        state.selectedFields = dedupeAndSortFields(mergedFields, state.fieldOrder);
        state.fieldOrder = rebuildFieldOrder(state.selectedFields, state.fieldOrder);
        state.selectedFieldsBySource[state.source] = state.selectedFields.slice();
        changed = true;
      }
    }

    syncFieldCheckboxes(state);

    if (preset.mode_default) {
      const desiredMode = normalizeModeForSource(preset.mode_default, state.source);
      if (state.mode !== desiredMode) {
        state.mode = desiredMode;
        state.modeSelections[state.source] = desiredMode;
        updateModeButtons(state);
        updateModeToggleLabels(state, elements);
        changed = true;
      }
    }

    const nextDateFrom = preset && preset.date_from ? preset.date_from : null;
    const nextDateTo = preset && preset.date_to ? preset.date_to : null;
    const previousDates = getDateSelection(state);
    if (previousDates.from !== nextDateFrom || previousDates.to !== nextDateTo) {
      setDateSelection(state, state.source, { from: nextDateFrom, to: nextDateTo });
      if (elements && elements.dateFrom) {
        elements.dateFrom.value = nextDateFrom || '';
      }
      if (elements && elements.dateTo) {
        elements.dateTo.value = nextDateTo || '';
      }
      changed = true;
    }

    const nextGames = Array.isArray(preset && preset.game_ids) ? preset.game_ids : [];
    const previousGames = getGameSelection(state);
    setGameSelection(state, state.source, nextGames);
    const storedGames = getGameSelection(state);
    const gamesChanged =
      previousGames.length !== storedGames.length || previousGames.some((value, idx) => value !== storedGames[idx]);
    if (elements && typeof elements.renderGameSelection === 'function') {
      elements.renderGameSelection();
    }
    if (gamesChanged) {
      changed = true;
    }
    updateGameSelectorVisibility(state, elements);

    return changed;
  }

  function updateModeButtons(state) {
    const container = document.getElementById('custom-mode-toggle');
    if (!container) {
      return;
    }
    const buttons = Array.from(container.querySelectorAll('.mode-pill'));
    buttons.forEach((btn) => {
      const isActive = btn.dataset.mode === state.mode;
      btn.classList.toggle('active', isActive);
      btn.classList.toggle('bg-[#9E1B32]', isActive);
      btn.classList.toggle('text-white', isActive);
      btn.classList.toggle('text-gray-700', !isActive);
    });
  }

  function syncFieldCheckboxes(state) {
    const selected = new Set(state.selectedFields);
    state.fieldCheckboxes.forEach(({ checkbox }) => {
      checkbox.checked = selected.has(checkbox.value);
    });
  }


  function hydrateExports(config, elements, state) {
    if (elements.exportCsv) {
      elements.exportCsv.addEventListener('click', () => {
        const payload = buildPayload(state);
        if (!payload.fields.length && !payload.player_ids.length) {
          alert('Select players and stats before exporting.');
          return;
        }
        exportCsv(config.exportCsvUrl, payload);
      });
    }

    if (elements.exportPng) {
      elements.exportPng.addEventListener('click', async () => {
        const payload = buildPayload(state);
        if (!payload.fields.length && !payload.player_ids.length) {
          alert('Select players and stats before exporting.');
          return;
        }
        try {
          const html2canvas = await loadHtml2Canvas(state);
          const container = document.getElementById('custom-table-container');
          if (!container) {
            throw new Error('Missing table container');
          }

          const tableBodyWrapper = container.querySelector('.table-card-body');
          const tableElement = container.querySelector('table');
          const target = tableBodyWrapper || tableElement || container;
          const previousOverflowX = target.style.overflowX;
          const previousWidth = target.style.width;
          const desiredWidth = target.scrollWidth;

          if (desiredWidth && Number.isFinite(desiredWidth)) {
            target.style.overflowX = 'visible';
            target.style.width = `${desiredWidth}px`;
          }

          let canvas;
          try {
            canvas = await html2canvas(target, { backgroundColor: '#ffffff', scale: 2 });
          } finally {
            target.style.overflowX = previousOverflowX;
            target.style.width = previousWidth;
          }
          canvas.toBlob((blob) => {
            if (!blob) {
              console.error('[custom-stats] Failed to render PNG');
              return;
            }
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = `custom_stats_${state.source || 'practice'}.png`;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);
          });
        } catch (error) {
          console.error('[custom-stats] PNG export failed', error);
          alert('Unable to export PNG right now.');
        }
      });
    }
  }

  function loadHtml2Canvas(state) {
    if (window.html2canvas) {
      return Promise.resolve(window.html2canvas);
    }
    if (!state.html2CanvasPromise) {
      state.html2CanvasPromise = new Promise((resolve, reject) => {
        const script = document.createElement('script');
        script.src = PNG_CDN_URL;
        script.async = true;
        script.onload = () => {
          if (window.html2canvas) {
            resolve(window.html2canvas);
          } else {
            reject(new Error('html2canvas failed to load.'));
          }
        };
        script.onerror = () => reject(new Error('html2canvas failed to load.'));
        document.body.appendChild(script);
      });
    }
    return state.html2CanvasPromise;
  }

  function parseFilenameFromDisposition(disposition) {
    if (!disposition || typeof disposition !== 'string') {
      return null;
    }

    const utf8Match = /filename\*\s*=\s*(?:UTF-8''|"?)([^;\"]+)/i.exec(disposition);
    if (utf8Match && utf8Match[1]) {
      const candidate = utf8Match[1].replace(/"/g, '').trim();
      try {
        return decodeURIComponent(candidate);
      } catch (error) {
        return candidate;
      }
    }

    const asciiMatch = /filename\s*=\s*"?([^";]+)"?/i.exec(disposition);
    if (asciiMatch && asciiMatch[1]) {
      return asciiMatch[1].trim();
    }

    return null;
  }

  function exportCsv(url, payload) {
    if (!url) {
      alert('CSV export is not configured.');
      return;
    }
    fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'text/csv',
        'X-Requested-With': 'XMLHttpRequest'
      },
      credentials: 'same-origin',
      body: JSON.stringify(payload)
    })
      .then((response) => {
        if (!response.ok) {
          throw new Error('Failed to export CSV');
        }
        const disposition = response.headers.get('Content-Disposition') || response.headers.get('content-disposition');
        const fallback = `custom_stats_${payload && payload.source ? payload.source : 'practice'}.csv`;
        const filename = parseFilenameFromDisposition(disposition) || fallback;
        return response.blob().then((blob) => ({ blob, filename }));
      })
      .then(({ blob, filename }) => {
        const urlObject = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = urlObject;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(urlObject);
      })
      .catch((error) => {
        console.error('[custom-stats] CSV export failed', error);
        alert('Unable to export CSV right now.');
      });
  }

  function enableColumnDrag(tableContainer, state, config, elements) {
    if (!tableContainer) {
      return;
    }

    const table = tableContainer.querySelector('table[data-draggable-headers="true"]');
    if (!table) {
      return;
    }

    const headerRows = Array.from(table.querySelectorAll('tr[data-header-row="columns"]'));
    if (!headerRows.length) {
      return;
    }

    const handles = headerRows.flatMap((row) => Array.from(row.querySelectorAll('[data-drag-handle]')));
    if (!handles.length) {
      return;
    }

    let dragKey = null;
    let activeTarget = null;

    function clearHighlights() {
      if (activeTarget) {
        activeTarget.classList.remove('drag-target');
      }
      activeTarget = null;
    }

    function resetDragState() {
      clearHighlights();
      dragKey = null;
    }

    function handleDrop(event) {
      if (!dragKey) {
        return;
      }
      event.preventDefault();
      const th = event.target.closest('th[data-key]');
      clearHighlights();
      if (!th) {
        resetDragState();
        return;
      }
      const targetKey = th.dataset.key;
      if (!targetKey || targetKey === dragKey || targetKey === 'player') {
        resetDragState();
        return;
      }

      const rect = th.getBoundingClientRect();
      const insertAfter = event.clientX > rect.left + rect.width / 2;
      const nextOrder = reorderSelectedFields(state.selectedFields, dragKey, targetKey, insertAfter);
      if (nextOrder) {
        state.selectedFields = nextOrder;
        state.selectedFieldsBySource[state.source] = nextOrder.slice();
        state.fieldOrder = rebuildFieldOrder(nextOrder, state.fieldOrder);
        state.lastPayload = null;
        if (state.autoRefresh && typeof state.requestRefresh === 'function') {
          state.requestRefresh('column-reorder');
        } else if (config && elements) {
          refreshTable(config, elements, state);
        }
      }
      resetDragState();
    }

    headerRows.forEach((row) => {
      if (row.dataset.dragBound === '1') {
        return;
      }
      row.dataset.dragBound = '1';
      row.addEventListener('dragover', (event) => {
        if (!dragKey) {
          return;
        }
        const th = event.target.closest('th[data-key]');
        if (!th || th.dataset.key === 'player' || th.dataset.key === dragKey) {
          return;
        }
        event.preventDefault();
        if (activeTarget && activeTarget !== th) {
          activeTarget.classList.remove('drag-target');
        }
        activeTarget = th;
        th.classList.add('drag-target');
      });
      row.addEventListener('dragleave', (event) => {
        const th = event.target.closest('th[data-key]');
        if (th && th === activeTarget) {
          th.classList.remove('drag-target');
          activeTarget = null;
        }
      });
      row.addEventListener('drop', handleDrop);
    });

    handles.forEach((handle) => {
      if (handle.dataset.dragBound === '1') {
        return;
      }
      handle.dataset.dragBound = '1';
      handle.addEventListener('dragstart', (event) => {
        const th = event.target.closest('th[data-key]');
        const key = th ? th.dataset.key : null;
        if (!key || key === 'player') {
          event.preventDefault();
          return;
        }
        dragKey = key;
        clearHighlights();
        event.dataTransfer.effectAllowed = 'move';
        try {
          event.dataTransfer.setData('text/plain', key);
        } catch (error) {
          // ignore
        }
      });
      handle.addEventListener('dragend', resetDragState);
      handle.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
      });
    });
  }

  function refreshTable(config, elements, state) {
    const payload = buildPayload(state, elements);
    state.lastPayload = payload;

    if (payload.source === 'game' && (!state.gameOptions || state.gameOptions.length === 0)) {
      if (elements.gameHelp) {
        elements.gameHelp.textContent = 'No games are available for filtering. Please add game data to continue.';
      }
      if (elements.tableContainer) {
        elements.tableContainer.innerHTML = '<div class="rounded-xl border border-dashed border-gray-300 px-6 py-12 text-center text-sm text-gray-500">No games are available for the current filters.</div>';
      }
      return;
    }

    if (!config.dataUrl) {
      console.error('[custom-stats] Missing data URL');
      return;
    }

    if (state.activeRequest) {
      state.activeRequest.abort();
    }

    const controller = new AbortController();
    state.activeRequest = controller;

    if (elements.tableContainer) {
      elements.tableContainer.innerHTML = '<div class="rounded-xl border border-dashed border-gray-300 px-6 py-12 text-center text-sm text-gray-500">Loading…</div>';
    }

    fetch(config.dataUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Requested-With': 'XMLHttpRequest'
      },
      credentials: 'same-origin',
      body: JSON.stringify(payload),
      signal: controller.signal
    })
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Request failed with status ${response.status}`);
        }
        return response.text();
      })
      .then((html) => {
        if (controller.signal.aborted) {
          return;
        }
        if (elements.tableContainer) {
          elements.tableContainer.innerHTML = html;
          enableColumnDrag(elements.tableContainer, state, config, elements);
        }
        reapplyTableSort();
      })
      .catch((error) => {
        if (controller.signal.aborted) {
          return;
        }
        console.error('[custom-stats] Failed to refresh table', error);
        if (elements.tableContainer) {
          elements.tableContainer.innerHTML = '<div class="rounded-xl border border-red-200 bg-red-50 px-6 py-12 text-center text-sm text-red-600">Unable to load data for the selected filters.</div>';
        }
      })
      .finally(() => {
        if (state.activeRequest === controller) {
          state.activeRequest = null;
        }
      });
  }

  function buildPayload(state, elements) {
    const source = normalizeSource(state.source);
    const payload = {
      player_ids: state.selectedPlayers.slice(),
      fields: state.selectedFields.slice(),
      mode: normalizeModeForSource(state.mode, source),
      source
    };
    const activeDates = getDateSelection(state, source);
    if (activeDates.from) {
      payload.date_from = activeDates.from;
    }
    if (activeDates.to) {
      payload.date_to = activeDates.to;
    }
    if (source === 'game') {
      const selectedGames = getGameSelection(state, source);
      if (selectedGames.length) {
        payload.game_ids = selectedGames.slice();
      }
    }
    return payload;
  }

  function reapplyTableSort() {
    if (typeof window.dispatchEvent === 'function') {
      window.dispatchEvent(new CustomEvent('custom:table:updated'));
    }
    if (document && typeof document.querySelectorAll === 'function') {
      const script = document.querySelector('script[src*="table-sort.js"]');
      if (script && !script.dataset.customStatsReloaded) {
        script.dataset.customStatsReloaded = 'true';
      }
    }
  }

  window.initCustomStatsPage = initCustomStatsPage;
})();
