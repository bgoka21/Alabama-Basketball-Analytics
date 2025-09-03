(function () {
  const MAX = 10;
  const select = document.getElementById('coach-search');
  const filter = document.getElementById('coach-filter');
  const holder = document.getElementById('coach-selected');
  const counter = document.getElementById('coach-count');
  const btn = document.getElementById('compare-btn');
  const clearBtn = document.getElementById('coach-clear');
  const csvLink = document.getElementById('compare-csv');
  const noMatches = document.getElementById('coach-no-matches');
  const searchBtn = document.getElementById('coach-search-btn');
  if (!select || select.dataset.enhanced) return;
  select.dataset.enhanced = '1';

  function selectedValues() {
    return Array.from(select.selectedOptions).map(o => o.value);
  }

  function updateCounter() {
    if (counter) counter.textContent = `${selectedValues().length}/${MAX} selected`;
  }

  function updateButtonState() {
    if (!btn) return;
    const n = selectedValues().length;
    btn.disabled = (n < 1 || n > MAX);
  }

  function enforceMax() {
    const lock = selectedValues().length >= MAX;
    Array.from(select.options).forEach(opt => {
      if (lock && !opt.selected) {
        opt.disabled = true;
      } else {
        opt.disabled = false;
      }
    });
  }

  function updateCsvLink() {
    if (!csvLink) return;
    const params = new URLSearchParams(window.location.search);
    params.delete('coaches');
    selectedValues().forEach(v => params.append('coaches', v));
    csvLink.href = csvLink.getAttribute('href').split('?')[0] + '?' + params.toString();
  }

  function refreshBadges() {
    if (!holder) return;
    const current = new Set(selectedValues().map(v => v.toLowerCase()));
    // remove badges that are no longer selected
    Array.from(holder.querySelectorAll('[data-coach-badge]')).forEach(badge => {
      const name = badge.getAttribute('data-coach-badge');
      if (!current.has(name)) badge.remove();
    });
    // add badges for new selections
    selectedValues().forEach(val => {
      const key = val.toLowerCase();
      if (holder.querySelector(`[data-coach-badge='${key}']`)) return;
      const badge = document.createElement('span');
      badge.className = 'inline-flex items-center gap-1 px-2 py-1 bg-gray-200 dark:bg-gray-700 rounded-full text-sm';
      badge.setAttribute('data-coach-badge', key);
      badge.setAttribute('data-testid', 'compare-badge');
      badge.textContent = val;
      const x = document.createElement('button');
      x.type = 'button';
      x.className = 'ml-1 text-xs';
      x.textContent = '×';
      x.setAttribute('aria-label', `Remove ${val}`);
      x.addEventListener('click', () => {
        Array.from(select.options).forEach(opt => {
          if (opt.value === val) opt.selected = false;
        });
        refreshBadges();
        updateCounter();
        updateButtonState();
        enforceMax();
        updateCsvLink();
      });
      badge.appendChild(x);
      holder.appendChild(badge);
    });
  }

  function applyFilter() {
    if (!filter) return;
    const q = filter.value.toLowerCase();
    let matches = 0;

    Array.from(select.options).forEach(opt => {
      const show = opt.value.toLowerCase().includes(q);
      // Use display toggling for wider browser support
      opt.style.display = show ? '' : 'none';
      opt.hidden = !show;
      if (show) matches++;
    });

    const hasSelection = selectedValues().length > 0;
    // Show the list if: there’s a query with matches, OR the input is focused, OR something is already selected.
    if ((q && matches > 0) || document.activeElement === filter || hasSelection) {
      select.classList.remove('hidden');
      if (noMatches) noMatches.classList.add('hidden');
    } else if (q && matches === 0) {
      select.classList.add('hidden');
      if (noMatches) noMatches.classList.remove('hidden');
    } else {
      // No query and no selection: keep it hidden
      select.classList.add('hidden');
      if (noMatches) noMatches.classList.add('hidden');
    }
  }

  select.addEventListener('change', () => {
    refreshBadges();
    updateCounter();
    updateButtonState();
    enforceMax();
    updateCsvLink();

    // Clear the search *but* keep the list visible if something is selected
    if (filter) {
      filter.value = '';
    }
    applyFilter();

    // Optional: auto-compare only if you want. Keeping as-is is fine:
    try {
      if (typeof window.renderCoachCompare === 'function') {
        window.renderCoachCompare();
      }
    } catch {}
  });

  if (filter) {
    filter.addEventListener('input', applyFilter);
    filter.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault(); // prevent accidental form submit
        const firstVisible = Array.from(select.options).find(o => !o.hidden);
        if (firstVisible) {
          firstVisible.selected = true;
          const changeEvent = new Event('change', { bubbles: true });
          select.dispatchEvent(changeEvent);
        } else {
          // No match → show the list + "no matches"
          applyFilter();
          if (noMatches) noMatches.classList.remove('hidden');
        }
      }
    });

    // Also reveal the list when focusing the search box
    filter.addEventListener('focus', () => {
      applyFilter();
    });
  }

  if (searchBtn) {
    searchBtn.addEventListener('click', applyFilter);
    searchBtn.disabled = true;
    searchBtn.classList.add('hidden');
  }

  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      Array.from(select.options).forEach(opt => {
        opt.selected = false;
        opt.disabled = false;
      });
      if (filter) filter.value = '';
      applyFilter();
      refreshBadges();
      updateCounter();
      updateButtonState();
      enforceMax();
      updateCsvLink();
    });
  }

  // initial state
  refreshBadges();
  updateCounter();
  updateButtonState();
  enforceMax();
  updateCsvLink();
  applyFilter();
})();
