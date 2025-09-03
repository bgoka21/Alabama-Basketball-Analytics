// static/js/coach_autocomplete.js
(function () {
  // run after DOM is present (script is at page end, but this is safe)
  const select = document.getElementById('coach-search');
  const filter = document.getElementById('coach-filter');
  const holder = document.getElementById('coach-selected');
  const counter = document.getElementById('coach-count');
  const btn = document.getElementById('compare-btn');
  const clearBtn = document.getElementById('coach-clear');
  const csvLink = document.getElementById('compare-csv');
  const noMatches = document.getElementById('coach-no-matches');

  if (!select || select.dataset.enhanced) return;
  select.dataset.enhanced = '1';

  const MAX = (() => {
    const m = parseInt(select.getAttribute('data-max') || '10', 10);
    return Number.isFinite(m) ? m : 10;
  })();

  function selectedValues() {
    return Array.from(select.options).filter(o => o.selected).map(o => o.value);
  }

  function updateCounter() {
    if (counter) counter.textContent = `${selectedValues().length}/${MAX} selected`;
  }

  function updateButtonState() {
    if (!btn) return;
    const n = selectedValues().length;
    btn.disabled = (n < 2 || n > MAX);
  }

  function enforceMax() {
    const lock = selectedValues().length >= MAX;
    Array.from(select.options).forEach(opt => {
      opt.disabled = lock && !opt.selected;
    });
  }

  function updateCsvLink() {
    if (!csvLink) return;
    const url = new URL(csvLink.href, window.location.origin);
    // preserve existing non-coach params from current location
    const current = new URLSearchParams(window.location.search);
    for (const [k, v] of current.entries()) {
      if (k !== 'coaches') url.searchParams.set(k, v);
    }
    // replace coaches
    url.searchParams.delete('coaches');
    for (const v of selectedValues()) url.searchParams.append('coaches', v);
    csvLink.href = url.toString();
  }

  function refreshBadges() {
    if (!holder) return;
    const current = new Set(selectedValues().map(v => v.toLowerCase()));
    // remove stale
    holder.querySelectorAll('[data-coach-badge]').forEach(b => {
      const name = (b.getAttribute('data-coach-badge') || '').toLowerCase();
      if (!current.has(name)) b.remove();
    });
    // add new
    selectedValues().forEach(val => {
      const key = (val || '').toLowerCase();
      if (holder.querySelector(`[data-coach-badge="${key}"]`)) return;
      const badge = document.createElement('span');
      badge.className = 'inline-flex items-center gap-1 px-2 py-1 bg-gray-200 dark:bg-gray-700 rounded-full text-sm';
      badge.setAttribute('data-coach-badge', key);
      badge.setAttribute('data-testid', 'compare-badge');
      badge.textContent = val;

      const x = document.createElement('button');
      x.type = 'button';
      x.className = 'ml-1 opacity-70';
      x.textContent = '×';
      x.addEventListener('click', () => {
        // deselect option
        const opt = Array.from(select.options).find(o => (o.value || '').toLowerCase() === key);
        if (opt) opt.selected = false;
        badge.remove();
        updateCounter();
        updateButtonState();
        enforceMax();
        updateCsvLink();
      });
      badge.appendChild(x);
      holder.appendChild(badge);
    });
  }

  function optionMatches(opt, q) {
    if (!q) return true;
    const val = (opt.value || '').toLowerCase();
    const txt = (opt.textContent || '').toLowerCase();
    const query = q.toLowerCase().trim();
    // token-based match (handles "last, first" and partials)
    const qtok = query.split(/\s+/).filter(Boolean);
    const hay = (val + ' ' + txt).split(/\s+/).filter(Boolean);
    return qtok.every(t => hay.some(h => h.startsWith(t)));
  }

  function applyFilter() {
    if (!filter) return;
    const q = filter.value || '';
    let matches = 0;
    Array.from(select.options).forEach(opt => {
      const show = optionMatches(opt, q);
      opt.style.display = show ? '' : 'none';
      opt.hidden = !show;
      if (show) matches++;
    });

    // show list on focus/typing or when something is selected
    if (document.activeElement === filter || q || selectedValues().length) {
      select.classList.remove('hidden');
      // make it comfortably scrollable
      select.size = Math.min(Math.max(matches || 6, 6), 12);
      if (noMatches) noMatches.classList.toggle('hidden', matches !== 0);
    } else {
      select.classList.add('hidden');
      if (noMatches) noMatches.classList.add('hidden');
    }
  }

  function pickFirstMatch() {
    const q = (filter && filter.value) || '';
    // consider all options, not only visible (in case filter didn't run)
    const opt = Array.from(select.options).find(o => optionMatches(o, q));
    if (!opt) return false;
    opt.selected = true;
    return true;
  }

  function updateUI() {
    refreshChips();
    updateCounter();
    updateButtonState();
    enforceMax();
    updateCsvLink();
  }

  function refreshChips() { refreshBadges(); }

  // ——— events
  if (filter) {
    filter.setAttribute('autocomplete', 'off');
    filter.addEventListener('focus', applyFilter);
    filter.addEventListener('input', applyFilter);
    filter.addEventListener('keydown', (e) => {
      if (e.key !== 'Enter') return;
      e.preventDefault(); // never submit on Enter from the search box
      const added = pickFirstMatch();
      updateUI();
      if (filter) filter.value = '';
      applyFilter();
      // If nothing matched, still reveal the list so user sees available options
      if (!added) select.classList.remove('hidden');
    });
  }

  select.addEventListener('change', () => {
    updateUI();
    if (filter) filter.focus();
  });

  if (clearBtn) {
    clearBtn.addEventListener('click', (e) => {
      e.preventDefault();
      Array.from(select.options).forEach(o => { o.selected = false; });
      updateUI();
      applyFilter();
      if (filter) filter.focus();
    });
  }

  // Prevent form submission with 0 selected (common when user pressed Enter)
  const form = select.closest('form');
  if (form) {
    form.addEventListener('submit', (e) => {
      if (selectedValues().length === 0) {
        // Try to add first match from whatever is in the search box
        if (filter && filter.value && pickFirstMatch()) {
          updateUI();
          // allow submit now (don't prevent)
        } else {
          e.preventDefault();
          // gentle hint
          const status = document.getElementById('saved-sets-status');
          if (status) {
            status.classList.remove('sr-only');
            status.textContent = 'Select at least one coach before comparing.';
            setTimeout(() => status.classList.add('sr-only'), 2500);
          }
          if (filter) filter.focus();
        }
      }
    });
  }

  // init
  updateUI();
  applyFilter();
  if (selectedValues().length) select.classList.remove('hidden');
})();

