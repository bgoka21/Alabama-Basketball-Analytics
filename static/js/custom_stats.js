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
    async listPresets({ preset_type, q = '' }) {
      const url = `${PRESETS_URL}?preset_type=${encodeURIComponent(preset_type)}&q=${encodeURIComponent(q)}`;
      const res = await fetch(url, { credentials: 'same-origin' });
      if (!res.ok) {
        throw new Error(await safeErr(res));
      }
      try {
        return await res.json();
      } catch (error) {
        return [];
      }
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
      datesPresetsList: document.getElementById('dates-presets-list')
    };

    if (!elements.playerRoot || !elements.tableContainer || !elements.statGroups) {
      console.warn('[custom-stats] Required DOM nodes are missing.');
      return;
    }

    const state = {
      roster: normalizeRosterData(resolveRosterData(config)),
      selectedPlayers: [],
      selectedPlayerIds: new Set(),
      fieldOrder: new Map(),
      selectedFields: [],
      mode: 'totals',
      source: 'practice',
      autoRefresh: Boolean(elements.autoRefresh ? elements.autoRefresh.checked : true),
      lastPayload: null,
      presets: { players: [], stats: [], dates: [] },
      dateFrom: elements.dateFrom && elements.dateFrom.value ? elements.dateFrom.value : null,
      dateTo: elements.dateTo && elements.dateTo.value ? elements.dateTo.value : null,
      fieldCheckboxes: [],
      refreshTimer: null,
      activeRequest: null,
      html2CanvasPromise: null
    };

    const playerUI = buildPlayerPicker(elements.playerRoot, state, queueRefresh);

    fetchFields(config.fieldsUrl, elements.statGroups, elements.statSearch, state, queueRefresh);

    hydrateDates(elements, state, queueRefresh);
    hydrateModeToggle(elements.modeToggle, state, queueRefresh);
    hydrateAutoRefresh(elements.autoRefresh, state);
    hydratePresets(config, elements, state, queueRefresh, playerUI);
    hydrateExports(config, elements, state);

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

  function fetchFields(url, container, searchInput, state, queueRefresh) {
    if (!url) {
      container.innerHTML = '<p class="text-xs text-red-600">Missing stat field endpoint.</p>';
      return;
    }

    container.innerHTML = '<p class="text-xs text-gray-500">Loading stat catalog…</p>';

    fetch(url, { credentials: 'same-origin' })
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Failed to load fields (${response.status})`);
        }
        return response.json();
      })
      .then((catalog) => {
        buildFieldPicker(catalog, container, state, queueRefresh);
        if (searchInput) {
          searchInput.addEventListener('input', () => filterFields(searchInput.value, state));
        }
      })
      .catch((error) => {
        console.error('[custom-stats] Failed to load field catalog', error);
        container.innerHTML = '<p class="text-xs text-red-600">Unable to load stat catalog.</p>';
      });
  }

  function buildFieldPicker(catalog, container, state, queueRefresh) {
    container.innerHTML = '';
    state.fieldCheckboxes = [];
    state.fieldOrder = new Map();

    if (!catalog || typeof catalog !== 'object') {
      container.innerHTML = '<p class="text-xs text-red-600">Stat catalog unavailable.</p>';
      return;
    }

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

  function hydrateDates(elements, state, queueRefresh) {
    if (elements.dateFrom) {
      elements.dateFrom.value = state.dateFrom || '';
      elements.dateFrom.addEventListener('change', () => {
        state.dateFrom = elements.dateFrom.value ? elements.dateFrom.value : null;
        state.lastPayload = null;
        if (typeof queueRefresh === 'function') {
          queueRefresh('dateFrom');
        }
      });
    } else {
      state.dateFrom = null;
    }

    if (elements.dateTo) {
      elements.dateTo.value = state.dateTo || '';
      elements.dateTo.addEventListener('change', () => {
        state.dateTo = elements.dateTo.value ? elements.dateTo.value : null;
        state.lastPayload = null;
        if (typeof queueRefresh === 'function') {
          queueRefresh('dateTo');
        }
      });
    } else {
      state.dateTo = null;
    }
  }

  function hydrateModeToggle(container, state, queueRefresh) {
    if (!container) {
      return;
    }

    const buttons = Array.from(container.querySelectorAll('.mode-pill'));
    buttons.forEach((button) => {
      button.addEventListener('click', () => {
        const mode = button.dataset.mode;
        if (!mode || state.mode === mode) {
          return;
        }
        state.mode = mode;
        buttons.forEach((btn) => {
          const isActive = btn === button;
          btn.classList.toggle('active', isActive);
          btn.classList.toggle('bg-[#9E1B32]', isActive);
          btn.classList.toggle('text-white', isActive);
          btn.classList.toggle('text-gray-700', !isActive);
        });
        queueRefresh('mode');
      });
    });
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

  function hydratePresets(config, elements, state, queueRefresh, playerUI) {
    if (!elements.presetsPanel) {
      return;
    }

    const activateTab = setupPresetTabs(elements.presetTabs, elements.presetPanes);

    const containers = {
      players: elements.playersPresetsList,
      stats: elements.statsPresetsList,
      dates: elements.datesPresetsList
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
          if (preset.preset_type === 'combined') {
            changed = applyCombinedPreset(preset, state, playerUI);
          } else if (type === 'players') {
            changed = applyPlayersPreset(preset, state, playerUI);
          } else if (type === 'stats') {
            changed = applyStatsPreset(preset, state);
          } else if (type === 'dates') {
            changed = applyDatesPreset(preset, state, elements);
          }
          if (changed) {
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
            renderPresetList(type, container, state.presets[type]);
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
            renderPresetList(type, container, state.presets[type]);
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
        const [players, stats, dates] = await Promise.all([
          api.listPresets({ preset_type: 'players' }),
          api.listPresets({ preset_type: 'stats' }),
          api.listPresets({ preset_type: 'dates' })
        ]);
        state.presets.players = Array.isArray(players) ? players : [];
        state.presets.stats = Array.isArray(stats) ? stats : [];
        state.presets.dates = Array.isArray(dates) ? dates : [];
        sortPresetsByName(state.presets.players);
        sortPresetsByName(state.presets.stats);
        sortPresetsByName(state.presets.dates);
        renderPresetList('players', containers.players, state.presets.players);
        renderPresetList('stats', containers.stats, state.presets.stats);
        renderPresetList('dates', containers.dates, state.presets.dates);
      } catch (error) {
        console.error('[custom-stats] Failed to load presets', error);
        notify('error', 'Unable to load presets.');
        Object.entries(containers).forEach(([type, container]) => {
          if (!container) {
            return;
          }
          renderPresetError(container, 'Unable to load presets.');
          state.presets[type] = [];
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
        try {
          const created = await api.createPreset(payload);
          if (elements.playersPresetName) {
            elements.playersPresetName.value = '';
          }
          if (created && created.id) {
            state.presets.players.push(created);
            sortPresetsByName(state.presets.players);
            renderPresetList('players', containers.players, state.presets.players);
          } else {
            await loadInitialPresets();
          }
          notify('success', 'Players preset saved.');
        } catch (error) {
          console.error('[custom-stats] Failed to save players preset', error);
          notify('error', error.message || 'Unable to save preset.');
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
        try {
          const created = await api.createPreset(payload);
          if (elements.statsPresetName) {
            elements.statsPresetName.value = '';
          }
          if (created && created.id) {
            state.presets.stats.push(created);
            sortPresetsByName(state.presets.stats);
            renderPresetList('stats', containers.stats, state.presets.stats);
          } else {
            await loadInitialPresets();
          }
          notify('success', 'Stats preset saved.');
        } catch (error) {
          console.error('[custom-stats] Failed to save stats preset', error);
          notify('error', error.message || 'Unable to save preset.');
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
        const currentFrom = elements.dateFrom ? elements.dateFrom.value : state.dateFrom;
        const currentTo = elements.dateTo ? elements.dateTo.value : state.dateTo;
        const from = currentFrom ? currentFrom : null;
        const to = currentTo ? currentTo : null;
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
          mode_default: state.mode,
          source_default: state.source || 'practice',
          visibility: 'team'
        };
        try {
          const created = await api.createPreset(payload);
          if (elements.datesPresetName) {
            elements.datesPresetName.value = '';
          }
          if (created && created.id) {
            state.presets.dates.push(created);
            sortPresetsByName(state.presets.dates);
            renderPresetList('dates', containers.dates, state.presets.dates);
          } else {
            await loadInitialPresets();
          }
          notify('success', 'Dates preset saved.');
        } catch (error) {
          console.error('[custom-stats] Failed to save dates preset', error);
          notify('error', error.message || 'Unable to save preset.');
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

  function renderPresetList(type, container, presets) {
    if (!container) {
      return;
    }
    container.innerHTML = '';

    if (!Array.isArray(presets) || !presets.length) {
      const empty = document.createElement('p');
      empty.className = 'py-3 text-xs text-gray-500';
      if (type === 'players') {
        empty.textContent = 'No players presets yet.';
      } else if (type === 'stats') {
        empty.textContent = 'No stats presets yet.';
      } else if (type === 'dates') {
        empty.textContent = 'No dates presets yet.';
      } else {
        empty.textContent = 'No presets found.';
      }
      container.appendChild(empty);
      return;
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

  function applyStatsPreset(preset, state) {
    if (!preset) {
      return false;
    }
    const incoming = Array.isArray(preset.fields) ? preset.fields : [];
    const seen = new Set();
    const next = [];
    incoming.forEach((rawKey) => {
      const key = typeof rawKey === 'string' ? rawKey : String(rawKey || '').trim();
      if (!key || seen.has(key)) {
        return;
      }
      if (state.fieldOrder instanceof Map && state.fieldOrder.size && !state.fieldOrder.has(key)) {
        return;
      }
      seen.add(key);
      next.push(key);
    });
    const prev = state.selectedFields.slice();
    state.selectedFields = next;
    syncFieldCheckboxes(state);
    if (prev.length !== next.length) {
      return true;
    }
    return prev.some((value, index) => value !== next[index]);
  }

  function applyDatesPreset(preset, state, elements) {
    const from = preset && preset.date_from ? preset.date_from : null;
    const to = preset && preset.date_to ? preset.date_to : null;
    const changed = state.dateFrom !== from || state.dateTo !== to;
    state.dateFrom = from;
    state.dateTo = to;
    if (elements.dateFrom) {
      elements.dateFrom.value = from || '';
    }
    if (elements.dateTo) {
      elements.dateTo.value = to || '';
    }
    return changed;
  }

  function applyCombinedPreset(preset, state, playerUI) {
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
        state.selectedFields = mergedFields;
        changed = true;
      }
    }

    syncFieldCheckboxes(state);

    if (preset.mode_default && (preset.mode_default === 'totals' || preset.mode_default === 'per_practice')) {
      if (state.mode !== preset.mode_default) {
        state.mode = preset.mode_default;
        updateModeButtons(state);
        changed = true;
      }
    }

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
          const canvas = await html2canvas(container, { backgroundColor: '#ffffff', scale: 2 });
          canvas.toBlob((blob) => {
            if (!blob) {
              console.error('[custom-stats] Failed to render PNG');
              return;
            }
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = 'practice_custom_stats.png';
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
        const filename = parseFilenameFromDisposition(disposition) || 'custom_stats.csv';
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

  function refreshTable(config, elements, state) {
    const payload = buildPayload(state, elements);
    state.lastPayload = payload;

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
    const payload = {
      player_ids: state.selectedPlayers.slice(),
      fields: state.selectedFields.slice(),
      mode: state.mode
    };
    if (state.dateFrom) {
      payload.date_from = state.dateFrom;
    }
    if (state.dateTo) {
      payload.date_to = state.dateTo;
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
