(function () {
  const form = document.getElementById('stat-only-form');
  const statSelect = document.getElementById('stat-select');
  if (!form || !statSelect) return;

  const allowedNames = new Set(['stat', 'season_id']);

  statSelect.addEventListener('change', function () {
    const elements = Array.from(form.elements).filter((el) => el && el.name);
    const hasDisallowed = elements.some((el) => !allowedNames.has(el.name));
    if (!hasDisallowed) {
      return;
    }

    const params = new URLSearchParams();
    const seasonInput = form.querySelector('input[name="season_id"]');
    const seasonId = seasonInput ? seasonInput.value : '';
    if (seasonId) params.set('season_id', seasonId);
    if (this.value) params.set('stat', this.value);
    window.location.search = params.toString();
  });
})();
