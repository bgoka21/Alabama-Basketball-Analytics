(function () {
  // BEGIN Playcall Report
  function getSelectedSeries(selectEl) {
    if (!selectEl) {
      return new Set();
    }
    const selected = Array.from(selectEl.selectedOptions || []).map((option) => option.value);
    return new Set(selected);
  }

  function toggleCardVisibility(cards, selected) {
    cards.forEach((card) => {
      const series = card.getAttribute('data-playcall-series');
      const shouldShow = selected.size === 0 || selected.has(series);
      card.classList.toggle('hidden', !shouldShow);
    });
  }

  function applySearch(cards, query) {
    const normalized = (query || '').trim().toLowerCase();
    cards.forEach((card) => {
      if (card.classList.contains('hidden')) {
        return;
      }
      const rows = card.querySelectorAll('tbody tr');
      let anyVisible = false;
      rows.forEach((row) => {
        const labelCell = row.querySelector('td[data-key="playcall"]');
        if (!labelCell) {
          return;
        }
        const matches = !normalized || labelCell.textContent.trim().toLowerCase().includes(normalized);
        row.classList.toggle('hidden', !matches);
        if (matches) {
          anyVisible = true;
        }
      });
      const emptyState = card.querySelector('[data-no-matches]');
      if (emptyState) {
        emptyState.classList.toggle('hidden', anyVisible);
      }
      const totals = card.querySelector('tfoot');
      if (totals) {
        totals.classList.remove('hidden');
        totals.classList.toggle('opacity-50', !anyVisible);
      }
    });
  }

  document.addEventListener('DOMContentLoaded', () => {
    const seriesSelect = document.querySelector('[data-playcall-series-filter]');
    const searchInput = document.querySelector('[data-playcall-search]');
    const cards = Array.from(document.querySelectorAll('[data-playcall-card]'));

    if (!cards.length) {
      return;
    }

    cards.forEach((card) => {
      if (card.hasAttribute('data-initial-hidden')) {
        card.classList.add('hidden');
      }
    });

    function runFilters() {
      const selected = getSelectedSeries(seriesSelect);
      toggleCardVisibility(cards, selected);
      applySearch(cards, searchInput ? searchInput.value : '');
    }

    if (seriesSelect) {
      seriesSelect.addEventListener('change', runFilters);
    }

    if (searchInput) {
      searchInput.addEventListener('input', () => {
        applySearch(cards, searchInput.value);
      });
    }

    runFilters();
  });
  // END Playcall Report
})();
