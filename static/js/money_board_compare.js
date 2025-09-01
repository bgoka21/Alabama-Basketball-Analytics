(document => {
  const checkboxes = Array.from(document.querySelectorAll('.coach-select'));
  const summary = document.getElementById('selected-summary');
  const compareBtn = document.getElementById('compare-btn');
  if (!checkboxes.length || !summary || !compareBtn) return;
  let selected = [];

  function update() {
    summary.textContent = selected.length ? `Selected: ${selected.join(', ')}` : 'Selected: none';
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
        row.classList.add('bg-yellow-50');
      } else {
        selected = selected.filter(c => c !== name);
        row.classList.remove('bg-yellow-50');
      }
      update();
    });
  });

  compareBtn.addEventListener('click', evt => {
    evt.preventDefault();
    const form = compareBtn.closest('form');
    form.querySelectorAll('input[name="coaches"]').forEach(el => el.remove());
    selected.forEach(name => {
      const input = document.createElement('input');
      input.type = 'hidden';
      input.name = 'coaches';
      input.value = name;
      form.appendChild(input);
    });
    form.submit();
  });

  update();
})(document);
