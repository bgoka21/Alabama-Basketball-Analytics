// static/js/coach_autocomplete.js
(() => {
  // If your IDs differ, update them here:
  const IDS = {
    filter: 'coach-filter',
    select: 'coach-search',
    chips: 'coach-selected',
    btn: 'compare-btn',
    counter: 'coach-count',        // optional
    clear: 'coach-clear',          // optional
    csvLink: 'compare-csv',        // optional
    noMatches: 'coach-no-matches', // optional
  };

  // Toggle for temporary debug logging (set to false when done)
  const DEBUG = false;
  const log = (...args) => DEBUG && console.log('[compare]', ...args);

  const normalize = (s) =>
    (s || '').toLowerCase().replace(/[^\w\s]/g, ' ').replace(/\s+/g, ' ').trim();
  const tokens = (s) => normalize(s).split(' ').filter(Boolean);

  function optionMatches(opt, q) {
    if (!q) return true;
    const qTok = tokens(q);
    if (!qTok.length) return true;

    const label = (opt.textContent || opt.innerText || '') + ' ' + (opt.value || '');
    const oTok = tokens(label);
    if (!oTok.length) return false;

    return qTok.every(t => oTok.some(o => o === t || o.startsWith(t)));
  }

  function init() {
    const filter   = document.getElementById(IDS.filter);
    const select   = document.getElementById(IDS.select);
    const chips    = document.getElementById(IDS.chips);
    const btn      = document.getElementById(IDS.btn);
    const counter  = document.getElementById(IDS.counter);
    const clear    = document.getElementById(IDS.clear);
    const csvLink  = document.getElementById(IDS.csvLink);
    const noMatch  = document.getElementById(IDS.noMatches);

    if (!filter || !select) {
      log('elements not present yet');
      return false;
    }

    const isSelect = (select instanceof HTMLSelectElement);
    const MAX = (() => {
      const m = parseInt(select.getAttribute('data-max') || '10', 10);
      return Number.isFinite(m) ? m : 10;
    })();

    const selectedValues = () => {
      if (isSelect) {
        return Array.from(select.options).filter(o => o.selected).map(o => o.value);
      }
      return Array.from(select.querySelectorAll('[data-selected="true"]')).map(el => el.dataset.value);
    };

    function updateUI() {
      const current = selectedValues();

      // Chips
      if (chips) {
        chips.innerHTML = '';
        current.forEach(v => {
          const label = isSelect
            ? ((Array.from(select.options).find(o => o.value === v)?.textContent) || v)
            : (select.querySelector(`[data-value="${CSS.escape(v)}"]`)?.dataset?.label || v);
          const chip = document.createElement('button');
          chip.type = 'button';
          chip.className = 'chip';
          chip.textContent = label.trim();
          chip.setAttribute('data-value', v);
          chip.addEventListener('click', () => {
            if (isSelect) {
              const opt = Array.from(select.options).find(o => o.value === v);
              if (opt) opt.selected = false;
            } else {
              const li = select.querySelector(`[data-value="${CSS.escape(v)}"]`);
              if (li) li.dataset.selected = 'false';
            }
            updateUI();
          });
          chips.appendChild(chip);
        });
      }

      // Count
      if (counter) counter.textContent = String(current.length);

      // Compare enabled
      if (btn) btn.disabled = current.length < 2;

      // CSV link lock
      if (csvLink) {
        const ok = current.length >= 2;
        csvLink.classList.toggle('opacity-50', !ok);
        csvLink.classList.toggle('pointer-events-none', !ok);
        csvLink.setAttribute('aria-disabled', ok ? 'false' : 'true');
      }

      // Enforce MAX by disabling non-selected options once max reached
      const atMax = current.length >= MAX;
      if (isSelect) {
        Array.from(select.options).forEach(opt => {
          if (!opt.selected) opt.disabled = atMax;
        });
      } else {
        Array.from(select.querySelectorAll('[data-value]')).forEach(li => {
          if (li.dataset.selected !== 'true') li.setAttribute('aria-disabled', String(atMax));
        });
      }
    }

    function applyFilter() {
      const q = filter.value;
      let visibleCount = 0;

      if (isSelect) {
        Array.from(select.options).forEach(opt => {
          const match = optionMatches(opt, q);
          opt.hidden = !match;
          if (match) visibleCount++;
        });
      } else {
        Array.from(select.querySelectorAll('[data-value]')).forEach(li => {
          const label = (li.dataset.label || li.textContent || '');
          const fakeOpt = { textContent: label, value: li.dataset.value };
          const match = optionMatches(fakeOpt, q);
          li.hidden = !match;
          if (match) visibleCount++;
        });
      }

      if (noMatch) noMatch.classList.toggle('hidden', !(q && visibleCount === 0));

    }

    function selectFirstMatchFromQuery() {
      const q = filter.value;

      if (isSelect) {
        const first = Array.from(select.options).find(opt => !opt.hidden && optionMatches(opt, q));
        if (!first) return false;
        if (!first.selected) {
          first.selected = true;
        } else {
          const next = Array.from(select.options).find(opt => !opt.hidden && !opt.selected);
          if (next) next.selected = true;
        }
        return true;
      } else {
        const first = Array.from(select.querySelectorAll('[data-value]')).find(li => !li.hidden);
        if (!first) return false;
        first.dataset.selected = 'true';
        return true;
      }
    }

    // Filter on interaction
    filter.addEventListener('input', applyFilter);

    // Enter should select, not submit
    filter.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        if (selectFirstMatchFromQuery()) updateUI();
      }
    });

    // Selection changes
    if (isSelect) {
      select.addEventListener('change', updateUI);
    } else {
      select.addEventListener('click', (e) => {
        const li = e.target.closest('[data-value]');
        if (!li) return;
        const atMax = selectedValues().length >= MAX;
        if (li.dataset.selected === 'true') {
          li.dataset.selected = 'false';
        } else if (!atMax) {
          li.dataset.selected = 'true';
        }
        updateUI();
      });
    }

    // Clear button
    if (clear) {
      clear.addEventListener('click', (e) => {
        e.preventDefault();
        if (isSelect) {
          Array.from(select.options).forEach(o => (o.selected = false));
        } else {
          Array.from(select.querySelectorAll('[data-value]')).forEach(li => (li.dataset.selected = 'false'));
        }
        filter.value = '';
        applyFilter();
        updateUI();
        filter.focus();
      });
    }

    // Form submission guard (needs ≥2)
    const form = (isSelect ? select.closest('form') : filter.closest('form'));
    if (form) {
      form.addEventListener('submit', (e) => {
        if (selectedValues().length < 2) {
          // If user typed, try auto-select first match once
          if (filter.value && selectFirstMatchFromQuery()) {
            updateUI();
            if (selectedValues().length >= 2) return; // allow submit now
          }
          e.preventDefault();
          if (noMatch) {
            noMatch.classList.remove('hidden');
            setTimeout(() => noMatch.classList.add('hidden'), 1800);
          }
          filter.focus();
        }

        // Defensive: if not a <select name="coaches">, inject hidden inputs
        if (!isSelect) {
          form.querySelectorAll('input[name="coaches"]').forEach(n => n.remove());
          selectedValues().forEach(v => {
            const hidden = document.createElement('input');
            hidden.type = 'hidden';
            hidden.name = 'coaches';
            hidden.value = v;
            form.appendChild(hidden);
          });
        }
      });
    }

    // MutationObserver (if options load async)
    if (isSelect) {
      const mo = new MutationObserver(() => {
        applyFilter();
        updateUI();
      });
      mo.observe(select, { childList: true, subtree: true });
    }

    // Initial paint
    updateUI();
    applyFilter();

    log('compare init OK');
    return true;
  }

  // Robust boot: try now, then retry until elements exist
  (function boot(attempt = 0) {
    if (init()) return;
    if (attempt < 40) {
      setTimeout(() => boot(attempt + 1), 50); // up to ~2s
    } else {
      window.addEventListener('load', () => init());
    }
  })();
})();

