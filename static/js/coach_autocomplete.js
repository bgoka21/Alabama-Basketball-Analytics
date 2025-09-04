// static/js/coach_autocomplete.js
(() => {
  const filter  = document.getElementById('coach-filter');
  const select  = document.getElementById('coach-search');
  const chips   = document.getElementById('coach-selected');
  const btn     = document.getElementById('compare-btn');
  const counter = document.getElementById('coach-count');   // optional
  const clear   = document.getElementById('coach-clear');   // optional
  const csvLink = document.getElementById('compare-csv');   // optional
  const noMatches = document.getElementById('coach-no-matches'); // optional

  if (!filter || !select) return;

  const MAX = (() => {
    const m = parseInt(select.getAttribute('data-max') || '10', 10);
    return Number.isFinite(m) ? m : 10;
  })();

    // ---------- helpers ----------
    const normalize = (s) => (s || '')
      .toLowerCase()
      .replace(/[^\w\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

    const tokens = (s) => normalize(s).split(' ').filter(Boolean);

    function optionMatches(opt, q) {
      if (!q) return true;
      const qTok = tokens(q);
      if (!qTok.length) return true;

      const label = (opt.textContent || opt.innerText || '') + ' ' + (opt.value || '');
      const oTok = tokens(label);
      if (!oTok.length) return false;

      // every query token must match the start of some option token
      return qTok.every(t => oTok.some(o => o === t || o.startsWith(t)));
    }

    function selectedValues() {
      return Array.from(select.options).filter(o => o.selected).map(o => o.value);
    }

    function enforceMax() {
      const sel = Array.from(select.options).filter(o => o.selected);
      if (sel.length <= MAX) return;
      let toDrop = sel.length - MAX;
      for (const o of sel) {
        if (toDrop <= 0) break;
        o.selected = false;
        toDrop--;
      }
    }

    function chipExists(val) {
      if (!chips) return false;
      // Avoid CSS.escape (Safari). Use dataset match instead.
      return Array.from(chips.querySelectorAll('[data-value]'))
        .some(el => (el.getAttribute('data-value') || '') === val);
    }

    function addChipFor(opt) {
      if (!chips) return;
      if (chipExists(opt.value)) return;

      const chip = document.createElement('button');
      chip.type = 'button';
      chip.dataset.value = opt.value;
      chip.className = 'inline-flex items-center rounded-full border px-3 py-1 text-sm mr-2 mb-2';
      chip.textContent = opt.textContent;

      const x = document.createElement('span');
      x.textContent = ' ×';
      x.className = 'ml-1 opacity-70';
      chip.appendChild(x);

      chip.addEventListener('click', () => {
        opt.selected = false;
        chip.remove();
        updateUI();
      });

      chips.appendChild(chip);
    }

    function refreshChips() {
      if (!chips) return;
      chips.innerHTML = '';
      for (const o of select.options) {
        if (o.selected) addChipFor(o);
      }
    }

    function updateCounter() {
      if (!counter) return;
      counter.textContent = `${selectedValues().length}/${MAX} selected`;
    }

    function updateButton() {
      if (!btn) return;
      const n = selectedValues().length;
      // Match server rule: require at least 2 to compare
      btn.disabled = (n < 2 || n > MAX);
    }

    function updateCsvLink() {
      if (!csvLink) return;
      const url = new URL(csvLink.href, window.location.origin);
      // keep non-coach params
      const current = new URLSearchParams(window.location.search);
      for (const [k, v] of current.entries()) {
        if (k !== 'coaches') url.searchParams.set(k, v);
      }
      // replace coaches
      url.searchParams.delete('coaches');
      for (const v of selectedValues()) url.searchParams.append('coaches', v);
      csvLink.href = url.toString();
    }

    function applyFilter() {
      const q = filter.value;
      let visible = 0;
      for (const opt of select.options) {
        const show = optionMatches(opt, q);
        opt.hidden = !show;
        opt.style.display = show ? '' : 'none';
        if (show) visible++;
      }

      // Show list when typing, on focus, or when there are selections
      if (q || document.activeElement === filter || selectedValues().length) {
        select.classList.remove('hidden');
        select.size = Math.min(Math.max(visible || 6, 6), 12);
        if (noMatches) noMatches.classList.toggle('hidden', visible !== 0);
      } else {
        select.classList.add('hidden');
        if (noMatches) noMatches.classList.add('hidden');
      }
    }

    function selectFirstMatchFromQuery() {
      const q = filter.value;
      const match = Array.from(select.options).find(o => optionMatches(o, q));
      if (!match) return false;
      match.selected = true;
      return true;
    }

    function clearSearchKeepList() {
      filter.value = '';
      applyFilter();
      filter.focus();
    }

    function updateUI() {
      enforceMax();
      refreshChips();
      updateCounter();
      updateButton();
      updateCsvLink();
    }

    // ---------- events ----------
    filter.setAttribute('autocomplete', 'off');
    filter.addEventListener('focus', applyFilter);
    filter.addEventListener('input', applyFilter);

    // Always prevent Enter from submitting; add first matching option
    filter.addEventListener('keydown', (e) => {
      if (e.key !== 'Enter') return;
      e.preventDefault();
      const added = selectFirstMatchFromQuery();
      updateUI();
      clearSearchKeepList();
      if (!added) {
        // no match → keep list open so the user can pick
        select.classList.remove('hidden');
      }
    });

    select.addEventListener('change', () => {
      updateUI();
      filter.focus();
    });

    if (clear) {
      clear.addEventListener('click', (e) => {
        e.preventDefault();
        for (const o of select.options) o.selected = false;
        updateUI();
        applyFilter();
        filter.focus();
      });
    }

    // Guard: prevent empty-submit; try to pick from query if user typed but didn't add
    const form = select.closest('form');
    if (form) {
      form.addEventListener('submit', (e) => {
        if (selectedValues().length < 2) {
          if (filter.value && selectFirstMatchFromQuery()) {
            updateUI();
            // allow submit if now >=2
            if (selectedValues().length >= 2) return;
          }
          e.preventDefault();
          if (noMatches) {
            noMatches.classList.remove('hidden');
            setTimeout(() => noMatches.classList.add('hidden'), 2000);
          }
          filter.focus();
        }
      });
    }

  // init
  updateUI();
  applyFilter();
  if (selectedValues().length) select.classList.remove('hidden');
})();

