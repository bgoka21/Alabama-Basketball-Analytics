(function () {
  'use strict';

  const REFRESH_DEBOUNCE_MS = 150;
  const PNG_CDN_URL = 'https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js';

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
      presetName: document.getElementById('preset-name'),
      savePreset: document.getElementById('save-preset'),
      teamPresetList: document.getElementById('team-preset-list'),
      privatePresetList: document.getElementById('private-preset-list')
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
      autoRefresh: Boolean(elements.autoRefresh ? elements.autoRefresh.checked : true),
      lastPayload: null,
      presets: { team: [], private: [] },
      fieldCheckboxes: [],
      refreshTimer: null,
      activeRequest: null,
      html2CanvasPromise: null
    };

    const playerUI = buildPlayerPicker(elements.playerRoot, state, queueRefresh);

    fetchFields(config.fieldsUrl, elements.statGroups, elements.statSearch, state, queueRefresh);

    hydrateDates(elements, state);
    hydrateModeToggle(elements.modeToggle, state, queueRefresh);
    hydrateAutoRefresh(elements.autoRefresh, state);
    hydratePresets(config, elements, state, queueRefresh);
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

    if (elements.dateFrom) {
      elements.dateFrom.addEventListener('change', () => {
        state.lastPayload = null;
        queueRefresh('dateFrom');
      });
    }
    if (elements.dateTo) {
      elements.dateTo.addEventListener('change', () => {
        state.lastPayload = null;
        queueRefresh('dateTo');
      });
    }
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
      state.selectedPlayers = ordered;
      triggerChange();
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

  function hydrateDates(elements, state) {
    if (elements.dateFrom) {
      elements.dateFrom.addEventListener('change', () => {
        state.lastPayload = null;
      });
    }
    if (elements.dateTo) {
      elements.dateTo.addEventListener('change', () => {
        state.lastPayload = null;
      });
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

  function hydratePresets(config, elements, state, queueRefresh) {
    if (!config.presetsUrl) {
      return;
    }

    loadPresets(config.presetsUrl, state, elements);

    if (elements.savePreset) {
      elements.savePreset.addEventListener('click', () => {
        if (!elements.presetName) {
          return;
        }
        const name = (elements.presetName.value || '').trim();
        if (!name) {
          alert('Enter a preset name first.');
          return;
        }
        const payload = {
          name,
          fields: state.selectedFields.slice(),
          mode_default: state.mode,
          visibility: 'team',
          source_default: 'practice'
        };
        fetch(config.presetsUrl, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest'
          },
          credentials: 'same-origin',
          body: JSON.stringify(payload)
        })
          .then((response) => {
            if (!response.ok) {
              return response.json().catch(() => ({})).then((body) => {
                throw new Error(body.error || 'Unable to save preset');
              });
            }
            return response.json();
          })
          .then(() => {
            elements.presetName.value = '';
            loadPresets(config.presetsUrl, state, elements);
          })
          .catch((error) => {
            console.error('[custom-stats] Failed to save preset', error);
            alert(error.message || 'Failed to save preset.');
          });
      });
    }

    function applyPreset(preset) {
      if (!preset || !Array.isArray(preset.fields)) {
        return;
      }
      let changed = false;
      preset.fields.forEach((key) => {
        if (!state.fieldOrder.has(key)) {
          return;
        }
        if (!state.selectedFields.includes(key)) {
          state.selectedFields.push(key);
          changed = true;
        }
      });
      state.selectedFields = dedupeAndSortFields(state.selectedFields, state.fieldOrder);
      syncFieldCheckboxes(state);

      if (preset.mode_default && (preset.mode_default === 'totals' || preset.mode_default === 'per_practice')) {
        if (state.mode !== preset.mode_default) {
          state.mode = preset.mode_default;
          updateModeButtons(state);
          changed = true;
        }
      }

      if (changed) {
        queueRefresh('preset');
      }
    }

    const handleListClick = (event) => {
      const target = event.target.closest('[data-preset-id]');
      if (!target) {
        return;
      }
      const id = Number(target.dataset.presetId);
      const source = target.dataset.presetSource;
      const presets = source === 'team' ? state.presets.team : state.presets.private;
      const preset = presets.find((p) => Number(p.id) === id);
      applyPreset(preset);
    };

    if (elements.teamPresetList) {
      elements.teamPresetList.addEventListener('click', handleListClick);
    }
    if (elements.privatePresetList) {
      elements.privatePresetList.addEventListener('click', handleListClick);
    }
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

  function loadPresets(url, state, elements) {
    fetch(url, { credentials: 'same-origin' })
      .then((response) => {
        if (!response.ok) {
          throw new Error('Failed to load presets');
        }
        return response.json();
      })
      .then((payload) => {
        state.presets.team = Array.isArray(payload.team) ? payload.team : [];
        state.presets.private = Array.isArray(payload.private) ? payload.private : [];
        renderPresetList(elements.teamPresetList, state.presets.team, 'team');
        renderPresetList(elements.privatePresetList, state.presets.private, 'private');
      })
      .catch((error) => {
        console.error('[custom-stats] Failed to fetch presets', error);
        renderPresetList(elements.teamPresetList, [], 'team', true);
        renderPresetList(elements.privatePresetList, [], 'private', true);
      });
  }

  function renderPresetList(container, presets, source, failed = false) {
    if (!container) {
      return;
    }
    container.innerHTML = '';

    if (failed) {
      const errorNode = document.createElement('p');
      errorNode.className = 'text-xs text-red-600';
      errorNode.textContent = 'Unable to load presets.';
      container.appendChild(errorNode);
      return;
    }

    if (!presets.length) {
      const emptyNode = document.createElement('p');
      emptyNode.className = 'text-xs text-gray-500';
      if (source === 'private') {
        emptyNode.textContent = 'Create a preset to reuse your favorite stat combinations.';
      } else {
        emptyNode.textContent = 'No presets yet. Saved team presets will appear here.';
      }
      container.appendChild(emptyNode);
      return;
    }

    presets.forEach((preset) => {
      const row = document.createElement('div');
      row.className = 'flex items-center justify-between gap-3 rounded-lg border border-gray-200 px-3 py-2';
      const name = document.createElement('span');
      name.className = 'text-sm font-medium text-gray-800';
      name.textContent = preset.name || 'Preset';
      const meta = document.createElement('span');
      meta.className = 'text-xs text-gray-500';
      const fieldCount = Array.isArray(preset.fields) ? preset.fields.length : 0;
      meta.textContent = `${fieldCount} fields`;
      const load = document.createElement('button');
      load.type = 'button';
      load.className = 'rounded-md border border-[#9E1B32] px-3 py-1 text-xs font-semibold text-[#9E1B32] hover:bg-[#9E1B32] hover:text-white';
      load.textContent = 'Load';
      load.dataset.presetId = preset.id;
      load.dataset.presetSource = source;

      row.appendChild(name);
      row.appendChild(meta);
      row.appendChild(load);
      container.appendChild(row);
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
        return response.blob();
      })
      .then((blob) => {
        const urlObject = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = urlObject;
        link.download = 'practice_custom_stats.csv';
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
    const fromInput = elements && elements.dateFrom ? elements.dateFrom.value : document.getElementById('custom-date-from')?.value;
    const toInput = elements && elements.dateTo ? elements.dateTo.value : document.getElementById('custom-date-to')?.value;
    if (fromInput) {
      payload.date_from = fromInput;
    }
    if (toInput) {
      payload.date_to = toInput;
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
