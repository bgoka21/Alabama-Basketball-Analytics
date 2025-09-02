(function () {
  const KEY = 'compareSets:v1';
  const MAX_SETS = 50;
  const MAX_COACHES = 10;

  const container = document.getElementById('saved-sets');
  const list = document.getElementById('saved-sets-list');
  const saveBtn = document.getElementById('saved-sets-save');
  const applyBtn = document.getElementById('saved-sets-apply');
  const copyBtn = document.getElementById('saved-sets-copy');
  const renameBtn = document.getElementById('saved-sets-rename');
  const deleteBtn = document.getElementById('saved-sets-delete');
  const status = document.getElementById('saved-sets-status');

  if (!container || !list) return;

  function lsAvailable() {
    try {
      const x = '__test__';
      localStorage.setItem(x, x);
      localStorage.removeItem(x);
      return true;
    } catch (e) {
      return false;
    }
  }

  if (!lsAvailable()) {
    container.remove();
    return;
  }

  container.classList.remove('hidden');

  function notify(msg) {
    if (status) status.textContent = msg;
  }

  function loadSets() {
    try {
      return JSON.parse(localStorage.getItem(KEY)) || [];
    } catch (e) {
      return [];
    }
  }

  let sets = loadSets();

  function saveSets() {
    localStorage.setItem(KEY, JSON.stringify(sets));
  }

  function renderList() {
    list.innerHTML = '<option value="">Saved sets…</option>';
    sets
      .slice()
      .sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at))
      .forEach(s => {
        const opt = document.createElement('option');
        opt.value = s.id;
        const d = new Date(s.updated_at);
        opt.textContent = `${s.name} (${d.toLocaleDateString()})`;
        list.appendChild(opt);
      });
    updateButtons();
  }

  function updateButtons() {
    const has = !!list.value;
    applyBtn.disabled = !has;
    copyBtn.disabled = !has;
    renameBtn.disabled = !has;
    deleteBtn.disabled = !has;
  }

  list.addEventListener('change', updateButtons);

  function currentFilters() {
    const obj = {};
    ['year_min', 'year_max', 'sheet', 'conf', 'min_recruits', 'sort'].forEach(k => {
      const el = document.querySelector(`input[name="${k}"]`);
      if (el && el.value) obj[k] = el.value;
    });
    return obj;
  }

  function selectedCoaches() {
    const sel = document.getElementById('coach-search');
    return Array.from(sel.selectedOptions).map(o => o.value);
  }

  saveBtn.addEventListener('click', () => {
    const coaches = selectedCoaches();
    if (coaches.length < 2) {
      notify('Select at least two coaches');
      return;
    }
    if (coaches.length > MAX_COACHES) {
      notify(`Up to ${MAX_COACHES} coaches allowed`);
      return;
    }
    const hint = coaches.slice(0, 3).join(', ');
    const name = prompt('Name this set', hint);
    if (!name) return;
    const trimmed = name.trim().slice(0, 60);
    const now = new Date().toISOString();
    let existing = sets.find(s => s.name === trimmed);
    if (existing) {
      if (!confirm('Overwrite existing set?')) return;
      existing.coaches = coaches;
      existing.filters = currentFilters();
      existing.updated_at = now;
    } else {
      const id = String(Date.now());
      sets.push({
        id,
        name: trimmed,
        coaches,
        filters: currentFilters(),
        created_at: now,
        updated_at: now,
      });
      if (sets.length > MAX_SETS) {
        sets.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
        sets = sets.slice(-MAX_SETS);
      }
      list.value = id;
    }
    saveSets();
    renderList();
    notify('Set saved');
  });

  applyBtn.addEventListener('click', () => {
    const id = list.value;
    const set = sets.find(s => s.id === id);
    if (!set) return;
    const sel = document.getElementById('coach-search');
    Array.from(sel.options).forEach(opt => {
      opt.selected = false;
      opt.disabled = false;
    });
    let missing = [];
    let applied = 0;
    set.coaches.forEach(name => {
      const opt = Array.from(sel.options).find(o => o.value === name);
      if (opt && applied < MAX_COACHES) {
        opt.selected = true;
        applied++;
      } else if (!opt) {
        missing.push(name);
      }
    });
    sel.dispatchEvent(new Event('change'));
    ['year_min', 'year_max', 'sheet', 'conf', 'min_recruits', 'sort'].forEach(k => {
      const el = document.querySelector(`input[name="${k}"]`);
      if (el) el.value = set.filters[k] || '';
    });
    if (missing.length) {
      notify(`Missing: ${missing.join(', ')}`);
    } else {
      notify('Set applied');
    }
  });

  copyBtn.addEventListener('click', () => {
    const id = list.value;
    const set = sets.find(s => s.id === id);
    if (!set) return;
    const params = new URLSearchParams();
    set.coaches.forEach(c => params.append('coaches', c));
    Object.entries(set.filters).forEach(([k, v]) => params.set(k, v));
    params.set('set_name', set.name);
    const url = `${window.location.origin}${window.location.pathname}?${params.toString()}`;
    navigator.clipboard.writeText(url).then(
      () => notify('Link copied'),
      () => notify('Copy failed')
    );
  });

  renameBtn.addEventListener('click', () => {
    const id = list.value;
    const set = sets.find(s => s.id === id);
    if (!set) return;
    const name = prompt('Rename set', set.name);
    if (!name) return;
    const trimmed = name.trim().slice(0, 60);
    if (sets.some(s => s.name === trimmed && s.id !== id)) {
      alert('Name already exists');
      return;
    }
    set.name = trimmed;
    set.updated_at = new Date().toISOString();
    saveSets();
    renderList();
    list.value = id;
    notify('Set renamed');
  });

  deleteBtn.addEventListener('click', () => {
    const id = list.value;
    const set = sets.find(s => s.id === id);
    if (!set) return;
    if (!confirm(`Delete set "${set.name}"?`)) return;
    sets = sets.filter(s => s.id !== id);
    saveSets();
    renderList();
    notify('Set deleted');
  });

  function hydrateFromUrl() {
    const params = new URLSearchParams(window.location.search);
    const coachParams = params.getAll('coaches');
    if (coachParams.length) {
      const sel = document.getElementById('coach-search');
      Array.from(sel.options).forEach(opt => (opt.selected = false));
      coachParams.forEach(name => {
        const opt = Array.from(sel.options).find(o => o.value === name);
        if (opt) opt.selected = true;
      });
      sel.dispatchEvent(new Event('change'));
    }
    const setName = params.get('set_name');
    if (setName) {
      const match = sets.find(s => s.name === setName);
      if (match) {
        list.value = match.id;
        updateButtons();
      }
    }
  }

  renderList();
  hydrateFromUrl();
})();

