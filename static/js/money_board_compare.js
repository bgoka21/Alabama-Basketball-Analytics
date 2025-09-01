(document => {
  const checkboxes = Array.from(document.querySelectorAll('.coach-select'));
  const summary = document.getElementById('selected-summary');
  const compareBtn = document.getElementById('compare-btn');
  if (!checkboxes.length || !summary || !compareBtn) return;
  let selected = [];

  function update() {
    if (selected.length) {
      summary.textContent = `Selected (${selected.length}): ${selected.join(', ')}`;
      summary.classList.remove('hidden');
    } else {
      summary.textContent = '';
      summary.classList.add('hidden');
    }
    compareBtn.disabled = selected.length === 0;
  }

  checkboxes.forEach(cb => {
    const row = cb.closest('tr');
    cb.addEventListener('change', () => {
      const name = cb.dataset.coach;
      if (cb.checked) {
        if (selected.length >= 5) {
          cb.checked = false;
          return;
        }
        selected.push(name);
        row.classList.add('bg-blue-50', 'dark:bg-gray-700');
      } else {
        selected = selected.filter(c => c !== name);
        row.classList.remove('bg-blue-50', 'dark:bg-gray-700');
      }
      update();
    });
  });

  update();
})(document);
