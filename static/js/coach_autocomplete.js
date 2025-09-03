// static/js/coach_autocomplete.js
(() => {
  document.addEventListener('DOMContentLoaded', () => {
    const filter  = document.getElementById('coach-filter');
    const select  = document.getElementById('coach-search');
    const chips   = document.getElementById('coach-selected');
    const btn     = document.getElementById('compare-btn');
    const counter = document.getElementById('coach-count');   // ok if null
    const clear   = document.getElementById('coach-clear');   // ok if null

    if (!filter || !select) return;

    const MAX = (() => {
      const m = (select.dataset && select.dataset.max) ? parseInt(select.dataset.max, 10) : 10;
      return Number.isFinite(m) ? m : 10;
    })();

    // ---------- helpers ----------
    const normalize = (s) => (s || '')
      .toLowerCase()
      .replace(/[^\w\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

    const tokens = (s) => normalize(s).split(' ').filter(Boolean);

    // a match if *every* query token appears (prefix or full) in the option label/value tokens
    function optionMatches(opt, q) {
      if (!q) return true;
      const qTok = tokens(q);
      if (!qTok.length) return true;

      const label = (opt.textContent || opt.innerText || '') + ' ' + (opt.value || '');
      const oTok = tokens(label);
      if (!oTok.length) return false;

      return qTok.every(t => oTok.some(o => o === t || o.startsWith(t)));
    }

    function selectedValues() {
      return Array.from(select.options).filter(o => o.selected).map(o => o.value);
    }

    function enforceMax() {
      const sel = Array.from(select.options).filter(o => o.selected);
      if (sel.length <= MAX) return;
      // drop oldest selections first
      let toDrop = sel.length - MAX;
      for (const o of sel) {
        if (toDrop <= 0) break;
        o.selected = false;
        toDrop--;
      }
    }

    function addChipFor(opt) {
      if (!chips) return;
      const exists = chips.querySelector(`[data-value="${CSS.escape(opt.value)}"]`);
      if (exists) return;

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
      // allow comparing with >= 1 (change to 2 if you want)
      btn.disabled = selectedValues().length < 1;
    }

    function updateCsvLink() {
      // optional: if you have an element with id="compare-csv" that needs query sync
      const a = document.getElementById('compare-csv');
      if (!a) return;
      const url = new URL(a.href, window.location.origin);
      url.searchParams.delete('coaches');
      for (const v of selectedValues()) url.searchParams.append('coaches', v);
      a.href = url.toString();
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

      // Make the list visible when typing or when there are selections
      if (q || selectedValues().length) {
        select.classList.remove('hidden');
        // resize to a comfortable height based on visible options
        select.size = Math.min(Math.max(visible || 6, 6), 12);
      } else {
        select.classList.add('hidden');
      }
    }

    function selectFirstMatchFromQuery() {
      const q = filter.value;
      // search across *all* options (not only visible)
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
    filter.addEventListener('focus', applyFilter);
    filter.addEventListener('input', applyFilter);

    // Pressing Enter in the search:
    //  - prevents form submit
    //  - picks the first matching option (even if list is hidden)
    //  - clears the search but keeps the list accessible
    filter.addEventListener('keydown', (e) => {
      if (e.key !== 'Enter') return;
      e.preventDefault();
      const added = selectFirstMatchFromQuery();
      updateUI();
      clearSearchKeepList();

      // If nothing matched, still show the list so it's obvious
      if (!added) select.classList.remove('hidden');
    });

    // Change from the <select> (click, arrow+space, etc.)
    select.addEventListener('change', () => {
      updateUI();
      // keep user in the flow of adding more
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

    // ---------- init ----------
    updateUI();
    // If there were server-preselected coaches, reveal the list
    if (selectedValues().length) select.classList.remove('hidden');
  });
})();
