(function () {
  'use strict';

  function ready(callback) {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', callback, { once: true });
    } else {
      callback();
    }
  }

  ready(initPlaycallReport);

  function initPlaycallReport() {
    const form = document.getElementById('playcall-report-form');
    const gameSelect = document.getElementById('playcall-game-select');
    const seriesSelect = document.getElementById('series-filter');
    const searchInput = document.getElementById('playcall-search');
    const familyCards = Array.from(document.querySelectorAll('[data-family-card]'));
    const flowCard = document.querySelector('[data-flow-card]');

    if (gameSelect && form) {
      gameSelect.addEventListener('change', () => {
        form.submit();
      });
    }

    if (seriesSelect) {
      seriesSelect.addEventListener('change', applyFilters);
    }

    if (searchInput) {
      searchInput.addEventListener('input', applyFilters);
    }

    applyFilters();

    function getSelectedSeries() {
      if (!seriesSelect) {
        return [];
      }
      return Array.from(seriesSelect.selectedOptions || []).map((option) => option.value);
    }

    function applyFilters() {
      const selectedSeries = getSelectedSeries();
      const query = (searchInput && searchInput.value ? searchInput.value : '').trim().toLowerCase();

      familyCards.forEach((card) => {
        const family = card.getAttribute('data-family-card') || '';
        const showCard = !selectedSeries.length || selectedSeries.includes(family);
        card.classList.toggle('hidden', !showCard);
        if (!showCard) {
          return;
        }

        const rows = Array.from(card.querySelectorAll('tbody tr[data-playcall]'));
        const totalsRow = card.querySelector('tbody tr[data-total-row="true"]');
        const emptyMessage = card.querySelector('[data-empty-message]');

        let visibleRows = 0;
        rows.forEach((row) => {
          const key = (row.getAttribute('data-playcall') || '').toLowerCase();
          const matchesSearch = !query || key.includes(query);
          row.classList.toggle('hidden', !matchesSearch);
          if (matchesSearch) {
            visibleRows += 1;
          }
        });

        if (totalsRow) {
          totalsRow.classList.toggle('hidden', query && visibleRows === 0);
        }
        if (emptyMessage) {
          emptyMessage.classList.toggle('hidden', !(query && visibleRows === 0));
        }
      });

      if (flowCard) {
        const rows = Array.from(flowCard.querySelectorAll('tbody tr[data-playcall]'));
        const totalsRow = flowCard.querySelector('tbody tr[data-total-row="true"]');
        const emptyMessage = flowCard.querySelector('[data-empty-message]');
        let visibleRows = 0;
        rows.forEach((row) => {
          const key = (row.getAttribute('data-playcall') || '').toLowerCase();
          const matchesSearch = !query || key.includes(query);
          row.classList.toggle('hidden', !matchesSearch);
          if (matchesSearch) {
            visibleRows += 1;
          }
        });
        if (totalsRow) {
          totalsRow.classList.toggle('hidden', query && visibleRows === 0);
        }
        if (emptyMessage) {
          emptyMessage.classList.toggle('hidden', !(query && visibleRows === 0));
        }
      }
    }
  }
})();
