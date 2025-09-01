(function () {
  const MAX = 10;
  const select = document.getElementById('coach-search');
  const filter = document.getElementById('coach-filter');
  const holder = document.getElementById('coach-selected');
  const counter = document.getElementById('coach-count');
  const btn = document.getElementById('compare-btn');

  if (!select || !holder) return;

  function selectedValues() {
    return Array.from(select.selectedOptions).map(o => (o.value || o.text).trim());
  }

  function updateCounter() {
    if (!counter) return;
    counter.textContent = `${selectedValues().length}/${MAX} selected`;
  }

  function updateButtonState() {
    if (!btn) return;
    const n = selectedValues().length;
    // Allow submit by default; gently discourage invalid counts
    btn.disabled = (n < 2 || n > MAX);
  }

  function syncBadges() {
    // Clear and rebuild from select
    holder.querySelectorAll('[data-coach-badge]').forEach(el => el.remove());
    selectedValues().forEach(val => {
      const badge = document.createElement('span');
      badge.className = 'inline-flex items-center gap-1 px-2 py-1 bg-gray-200 dark:bg-gray-700 rounded-full text-sm';
      badge.setAttribute('data-coach-badge', val.toLowerCase());
      badge.textContent = val;

      const x = document.createElement('button');
      x.type = 'button';
      x.className = 'ml-1 text-xs';
      x.textContent = '×';
      x.addEventListener('click', () => {
        // Deselect in the select element
        Array.from(select.options).forEach(o => {
          if ((o.value || o.text).trim() === val) o.selected = false;
        });
        syncBadges();
        updateCounter();
        updateButtonState();
        enforceMax();
      });

      badge.appendChild(x);
      holder.appendChild(badge);
    });
  }

  function enforceMax() {
    const n = selectedValues().length;
    const lock = n >= MAX;
    Array.from(select.options).forEach(o => {
      if (!o.selected) o.disabled = lock;
    });
  }

  // Select change (supports click and keyboard multi-select)
  select.addEventListener('change', () => {
    syncBadges();
    updateCounter();
    updateButtonState();
    enforceMax();
  });

  // Text filter
  if (filter) {
    filter.addEventListener('input', () => {
      const q = filter.value.toLowerCase();
      Array.from(select.options).forEach(o => {
        const show = (o.text || '').toLowerCase().includes(q);
        o.style.display = show ? '' : 'none';
      });
    });
  }

  // Initial
  syncBadges();
  updateCounter();
  updateButtonState();
  enforceMax();
})();
