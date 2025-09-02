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
    btn.disabled = (n < 2 || n > MAX);
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
      // Use display toggling for wider browser support instead of the `hidden` attribute
      opt.style.display = show ? '' : 'none';
      if (show) matches++;
    });

    if (q) {
      if (matches > 0) {
        select.classList.remove('hidden');
        if (noMatches) noMatches.classList.add('hidden');
      } else {
        select.classList.add('hidden');
        if (noMatches) noMatches.classList.remove('hidden');
      }
    } else {
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
  });

  if (filter) {
    filter.addEventListener('input', applyFilter);
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
