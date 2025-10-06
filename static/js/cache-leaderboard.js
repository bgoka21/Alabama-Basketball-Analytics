(function () {
  'use strict';

  function ready(fn) {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', fn, { once: true });
    } else {
      fn();
    }
  }

  function getCsrfToken() {
    if (typeof window.__CSRF__ === 'string' && window.__CSRF__) {
      return window.__CSRF__;
    }
    return '';
  }

  function resolveSeason(button, container) {
    const direct = button.dataset.cacheSeason || container?.dataset.cacheSeason;
    if (direct) {
      return direct;
    }

    const selector = button.dataset.cacheSelect || container?.dataset.cacheSelect;
    if (selector) {
      const selectEl = document.querySelector(selector);
      if (selectEl && selectEl.value) {
        return selectEl.value;
      }
      return '';
    }

    if (container) {
      const select = container.querySelector('[data-cache-season-select]');
      if (select && select.value) {
        return select.value;
      }
    }

    return '';
  }

  function resolveEndpoint(button, container, seasonId) {
    const direct = button.dataset.cacheEndpoint || container?.dataset.cacheEndpoint;
    if (direct) {
      return direct;
    }

    const template =
      button.dataset.cacheEndpointTemplate || container?.dataset.cacheEndpointTemplate;
    if (template && seasonId) {
      return template.replace('__SEASON__', seasonId);
    }
    return '';
  }

  function toggleBusy(button, isBusy) {
    button.disabled = isBusy;
    const spinner = button.querySelector('.cache-spinner');
    const label = button.querySelector('.cache-label');
    if (spinner) {
      spinner.classList.toggle('hidden', !isBusy);
    }
    if (label) {
      label.classList.toggle('opacity-60', isBusy);
    }
  }

  function writeStatus(container, message, tone) {
    const statusEl = container.querySelector('.cache-status');
    if (!statusEl) {
      return;
    }
    statusEl.textContent = message;
    let baseClass = 'cache-status text-sm mt-3';
    if (tone === 'error') {
      baseClass += ' text-red-600';
    } else if (tone === 'success') {
      baseClass += ' text-green-600';
    } else {
      baseClass += ' text-gray-600';
    }
    statusEl.className = baseClass.trim();
  }

  async function handleClick(event) {
    const button = event.currentTarget;
    const container = button.closest('[data-cache-container]');
    if (!container) {
      return;
    }

    const seasonId = resolveSeason(button, container);
    if (!seasonId) {
      writeStatus(container, 'Please select a season to cache.', 'error');
      return;
    }

    const endpoint = resolveEndpoint(button, container, seasonId);
    if (!endpoint) {
      writeStatus(container, 'Unable to determine rebuild endpoint.', 'error');
      return;
    }

    toggleBusy(button, true);
    writeStatus(container, 'Rebuilding leaderboards…', 'info');

    try {
      const headers = { 'X-Requested-With': 'XMLHttpRequest' };
      const csrf = getCsrfToken();
      if (csrf) {
        headers['X-CSRFToken'] = csrf;
        headers['X-CSRF-Token'] = csrf;
      }

      const response = await fetch(endpoint, {
        method: 'POST',
        headers,
        credentials: 'same-origin',
      });

      if (!response.ok) {
        throw new Error(`Server responded with ${response.status}`);
      }

      const data = await response.json();
      if (data.status === 'ok') {
        writeStatus(
          container,
          `Leaderboard cache rebuilt for season ${data.season_id}.`,
          'success'
        );
      } else {
        const message = data.message || 'Rebuild failed.';
        writeStatus(container, message, 'error');
      }
    } catch (error) {
      writeStatus(container, error.message || 'Rebuild failed.', 'error');
    } finally {
      toggleBusy(button, false);
    }
  }

  ready(() => {
    const buttons = document.querySelectorAll('.cache-leaderboard-btn');
    buttons.forEach((button) => {
      button.addEventListener('click', handleClick);
    });
  });
})();

