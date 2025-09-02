(function () {
  const containers = document.querySelectorAll('[data-coach-scope]');
  if (!containers.length) return;

  containers.forEach(container => {
    const controls = container.querySelector('[data-controls]');
    const searchInput = container.querySelector('[data-search]');
    const countEl = container.querySelector('[data-row-count]');
    const statusEl = container.querySelector('[data-topn-status]');
    const toggleEl = container.querySelector('[data-topn-toggle]');
    const table = container.querySelector('table');
    if (!table) return;
    const tbody = table.querySelector('tbody');
    const headers = table.querySelectorAll('th[data-key]');
    const TOP_N = 25;
    let sortKey = 'net';
    let sortDir = 'desc';
    let filter = '';
    let showAll = false;

    const headerDefaults = {};
    const headerLabels = {};
    headers.forEach(h => {
      headerDefaults[h.dataset.key] = h.dataset.default || 'asc';
      headerLabels[h.dataset.key] = h.textContent.trim();
    });

    const rows = Array.from(tbody.querySelectorAll('tr')).map((tr, index) => {
      const cells = {};
      tr.querySelectorAll('[data-cell]').forEach(td => {
        const key = td.dataset.cell;
        if (td.dataset.sort !== undefined) {
          const val = td.dataset.sort;
          cells[key] = val === '' ? null : parseFloat(val);
        } else {
          cells[key] = td.textContent.trim().toLowerCase();
        }
      });
      const playerCell = tr.querySelector('[data-cell="player"]');
      const teamCell = tr.querySelector('[data-cell="team"]');
      const search = ((playerCell ? playerCell.textContent : '') + ' ' + (teamCell ? teamCell.textContent : '')).toLowerCase();
      return { el: tr, index, cells, search };
    });

    function updateHeaders() {
      headers.forEach(h => {
        const caret = h.querySelector('[data-caret]');
        if (h.dataset.key === sortKey) {
          h.setAttribute('aria-sort', sortDir === 'asc' ? 'ascending' : 'descending');
          if (caret) {
            caret.textContent = sortDir === 'asc' ? '▲' : '▼';
            caret.classList.remove('opacity-0');
          }
        } else {
          h.setAttribute('aria-sort', 'none');
          if (caret) caret.classList.add('opacity-0');
        }
      });
    }

    function apply() {
      const term = filter;
      const matches = [];
      rows.forEach(row => {
        if (!term || row.search.indexOf(term) !== -1) {
          matches.push(row);
        } else {
          row.el.classList.add('hidden');
        }
      });

      const dir = sortDir === 'asc' ? 1 : -1;
      matches.sort((a, b) => {
        const av = a.cells[sortKey];
        const bv = b.cells[sortKey];
        if (av == null && bv == null) return a.index - b.index;
        if (av == null) return 1;
        if (bv == null) return -1;
        if (typeof av === 'string' || typeof bv === 'string') {
          const cmp = String(av).localeCompare(String(bv));
          if (cmp !== 0) return cmp * dir;
        } else {
          if (av !== bv) return (av - bv) * dir;
        }
        return a.index - b.index;
      });

      matches.forEach((row, idx) => {
        tbody.appendChild(row.el);
        if (!showAll && idx >= TOP_N) {
          row.el.classList.add('hidden');
        } else {
          row.el.classList.remove('hidden');
        }
      });

      countEl.textContent = `${matches.length} players`;

      if (matches.length <= TOP_N) {
        statusEl.classList.add('hidden');
        toggleEl.classList.add('hidden');
      } else {
        statusEl.classList.remove('hidden');
        toggleEl.classList.remove('hidden');
        statusEl.textContent = showAll ? 'Showing all' : `Top ${TOP_N} by ${headerLabels[sortKey]}`;
        toggleEl.textContent = showAll ? `Show top ${TOP_N}` : 'Show all';
      }
    }

    headers.forEach(h => {
      h.addEventListener('click', () => {
        const key = h.dataset.key;
        if (sortKey === key) {
          sortDir = sortDir === 'asc' ? 'desc' : 'asc';
        } else {
          sortKey = key;
          sortDir = headerDefaults[key] || 'asc';
        }
        updateHeaders();
        apply();
      });
    });

    let timer;
    if (searchInput) {
      searchInput.addEventListener('input', () => {
        clearTimeout(timer);
        timer = setTimeout(() => {
          filter = searchInput.value.trim().toLowerCase();
          apply();
        }, 200);
      });
    }

    if (toggleEl) {
      toggleEl.addEventListener('click', () => {
        showAll = !showAll;
        if (!showAll) {
          container.scrollTop = 0;
        }
        apply();
      });
    }

    updateHeaders();
    apply();
    if (controls) controls.classList.remove('hidden');
  });
})();
