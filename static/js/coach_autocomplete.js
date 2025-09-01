(function () {
  const MAX = 10;
  const options = document.getElementById('coach-options');
  const filter = document.getElementById('coach-filter');
  const holder = document.getElementById('coach-selected');
  const hidden = document.getElementById('coach-hidden');
  const counter = document.getElementById('coach-count');
  const btn = document.getElementById('compare-btn');

  if (!options || !holder || !hidden) return;

  function selectedValues() {
    return Array.from(hidden.querySelectorAll('input[name="coaches"]')).map(i => i.value);
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
    Array.from(options.querySelectorAll('li')).forEach(li => {
      if (lock) {
        li.classList.add('pointer-events-none', 'opacity-50');
      } else {
        li.classList.remove('pointer-events-none', 'opacity-50');
      }
    });
  }

  function addCoach(name, li) {
    // badge
    const badge = document.createElement('span');
    badge.className = 'inline-flex items-center gap-1 px-2 py-1 bg-gray-200 dark:bg-gray-700 rounded-full text-sm';
    badge.setAttribute('data-coach-badge', name.toLowerCase());
    badge.textContent = name;

    const x = document.createElement('button');
    x.type = 'button';
    x.className = 'ml-1 text-xs';
    x.textContent = '×';
    x.addEventListener('click', () => {
      badge.remove();
      const hid = hidden.querySelector(`input[data-coach-hidden='${name.toLowerCase()}']`);
      if (hid) hid.remove();
      options.appendChild(li);
      applyFilter();
      updateCounter();
      updateButtonState();
      enforceMax();
    });

    badge.appendChild(x);
    holder.appendChild(badge);

    const input = document.createElement('input');
    input.type = 'hidden';
    input.name = 'coaches';
    input.value = name;
    input.setAttribute('data-coach-hidden', name.toLowerCase());
    hidden.appendChild(input);

    li.remove();

    updateCounter();
    updateButtonState();
    enforceMax();
  }

  function applyFilter() {
    if (!filter) return;
    const q = filter.value.toLowerCase();
    Array.from(options.querySelectorAll('li')).forEach(li => {
      const show = (li.dataset.coach || '').toLowerCase().includes(q);
      li.style.display = show ? '' : 'none';
    });
  }

  options.addEventListener('click', e => {
    const li = e.target.closest('li[data-coach]');
    if (!li) return;
    if (selectedValues().length >= MAX) return;
    addCoach(li.dataset.coach, li);
  });

  if (filter) {
    filter.addEventListener('input', applyFilter);
  }

  // initial selections
  (function init() {
    const selected = options.dataset.selected ? JSON.parse(options.dataset.selected) : [];
    selected.forEach(name => {
      const li = options.querySelector(`li[data-coach="${name}"]`);
      if (li) addCoach(name, li);
    });
    applyFilter();
    updateCounter();
    updateButtonState();
    enforceMax();
  })();
})();

