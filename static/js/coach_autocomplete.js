(function () {
  const MAX = 10;
  const select = document.getElementById('coach-search');
  const filter = document.getElementById('coach-filter');
  const holder = document.getElementById('coach-selected');  // container for badges
  const hiddenHolder = document.getElementById('coach-hidden'); // where hidden inputs go
  const counter = document.getElementById('coach-count'); // a small counter element (add to template if missing)

  function updateCounter() {
    const count = holder.querySelectorAll('[data-coach-badge]').length;
    if (counter) counter.textContent = `${count}/${MAX} selected`;
  }

  function disableIfAtMax() {
    const count = holder.querySelectorAll('[data-coach-badge]').length;
    const disable = count >= MAX;
    [...select.options].forEach(opt => {
      if (!opt.selected) opt.disabled = disable;
    });
  }

  function addCoach(name, value) {
    // prevent dupes
    const key = value.toLowerCase();
    if (holder.querySelector(`[data-coach-badge="${key}"]`)) return;

    // create badge
    const badge = document.createElement('span');
    badge.className = 'inline-flex items-center gap-1 px-2 py-1 bg-gray-200 dark:bg-gray-700 rounded-full text-sm';
    badge.setAttribute('data-coach-badge', key);
    badge.textContent = name;

    // remove button
    const x = document.createElement('button');
    x.type = 'button';
    x.className = 'ml-1 text-xs';
    x.innerHTML = '×';
    x.addEventListener('click', () => {
      badge.remove();
      const hid = hiddenHolder.querySelector(`input[type="hidden"][value="${value}"]`);
      if (hid) hid.remove();
      // also unselect option
      [...select.options].forEach(o => { if ((o.value || o.text) === value) o.selected = false; });
      disableIfAtMax();
      updateCounter();
      updateCompareButton();
    });

    badge.appendChild(x);
    holder.appendChild(badge);

    // hidden input for form submit
    const hid = document.createElement('input');
    hid.type = 'hidden';
    hid.name = 'coaches';
    hid.value = value;
    hiddenHolder.appendChild(hid);

    disableIfAtMax();
    updateCounter();
    updateCompareButton();
  }

  function updateCompareButton() {
    const btn = document.getElementById('compare-btn');
    if (!btn) return;
    const count = holder.querySelectorAll('[data-coach-badge]').length;
    btn.disabled = count < 2 || count > MAX;
  }

  // select change
  if (select) {
    select.addEventListener('change', () => {
      const count = holder.querySelectorAll('[data-coach-badge]').length;
      if (count >= MAX) return;
      const opt = select.options[select.selectedIndex];
      if (!opt) return;
      const name = opt.text.trim();
      const value = (opt.value || name).trim();
      opt.selected = true;
      addCoach(name, value);
    });
  }

  // filter typing
  if (filter && select) {
    filter.addEventListener('input', () => {
      const q = filter.value.toLowerCase();
      [...select.options].forEach(o => {
        const show = (o.text || '').toLowerCase().includes(q);
        o.style.display = show ? '' : 'none';
      });
    });
  }

  // init
  updateCounter();
  disableIfAtMax();
  updateCompareButton();
})();

