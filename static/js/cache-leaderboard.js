(function () {
  'use strict';

  const POLL_INTERVAL_MS = 1000;
  const TOAST_DURATION_MS = 2500;

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

  let toastRoot = null;

  function getToastRoot() {
    if (toastRoot && document.body.contains(toastRoot)) {
      return toastRoot;
    }
    toastRoot = document.createElement('div');
    toastRoot.className = 'fixed inset-x-0 top-4 z-50 flex flex-col items-center gap-2 px-4 sm:items-end sm:px-6';
    document.body.appendChild(toastRoot);
    return toastRoot;
  }

  function showToast(type, message) {
    if (!message) {
      return;
    }
    const root = getToastRoot();
    const toast = document.createElement('div');
    const base = 'transition-opacity duration-200 ease-in-out px-4 py-2 rounded-lg shadow-lg text-sm font-medium';
    let variant = 'bg-gray-900 text-white';
    if (type === 'error') {
      variant = 'bg-red-600 text-white';
    } else if (type === 'success') {
      variant = 'bg-green-600 text-white';
    } else if (type === 'info') {
      variant = 'bg-blue-600 text-white';
    }
    toast.className = `${base} ${variant}`;
    toast.textContent = message;
    root.appendChild(toast);
    window.setTimeout(() => {
      toast.classList.add('opacity-0');
      window.setTimeout(() => {
        if (toast.parentNode === root) {
          root.removeChild(toast);
        }
      }, 200);
    }, TOAST_DURATION_MS);
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

  function resolveStatusEndpoint(button, container, seasonId) {
    const direct = button.dataset.cacheStatusEndpoint || container?.dataset.cacheStatusEndpoint;
    if (direct) {
      return direct;
    }

    const template =
      button.dataset.cacheStatusTemplate || container?.dataset.cacheStatusTemplate;
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
    statusEl.textContent = message || '';
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

  function getProgressElements(container) {
    const root = container.querySelector('#cache-progress');
    const bar = container.querySelector('#cache-progress-bar');
    const text = container.querySelector('#cache-progress-text');
    return { root, bar, text };
  }

  function updateProgressUI(elements, percent, message) {
    if (!elements) {
      return;
    }
    const numeric = Number.isFinite(percent) ? percent : Number(percent) || 0;
    const pct = Math.max(0, Math.min(100, Math.round(numeric)));
    if (elements.root) {
      elements.root.classList.remove('hidden');
    }
    if (elements.bar) {
      elements.bar.style.width = `${pct}%`;
    }
    if (elements.text) {
      const trimmed = message && message.trim ? message.trim() : '';
      elements.text.textContent = trimmed ? `${pct}% — ${trimmed}` : `${pct}%`;
    }
  }

  function startPolling(container, statusUrl) {
    const progressEls = getProgressElements(container);
    if (progressEls.root) {
      progressEls.root.classList.remove('hidden');
    }

    return new Promise((resolve) => {
      let stopped = false;
      let timerId = null;

      async function poll() {
        try {
          const response = await fetch(statusUrl, {
            method: 'GET',
            credentials: 'same-origin',
            headers: { 'Accept': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
          });
          if (!response.ok) {
            throw new Error(`Status ${response.status}`);
          }

          const data = await response.json();
          const percent = Number(data?.percent ?? 0);
          const message = typeof data?.message === 'string' ? data.message : '';
          const error = typeof data?.error === 'string' && data.error ? data.error : null;
          const done = Boolean(data?.done);

          updateProgressUI(progressEls, percent, message);

          const tone = error ? 'error' : done ? 'success' : 'info';
          writeStatus(container, error || message || 'Working…', tone);

          if (error || done) {
            stop({ done, error, message: message || (done && !error ? 'Complete' : ''), percent });
          }
        } catch (err) {
          const message = err && err.message ? err.message : 'Status check failed.';
          writeStatus(container, message, 'error');
          stop({ error: message });
        }
      }

      function stop(result) {
        if (stopped) {
          return;
        }
        stopped = true;
        if (timerId !== null) {
          window.clearInterval(timerId);
        }
        resolve(result || null);
      }

      poll();
      timerId = window.setInterval(poll, POLL_INTERVAL_MS);
    });
  }

  async function handleClick(event) {
    const button = event.currentTarget;
    const container = button.closest('[data-cache-container]');
    if (!container) {
      return;
    }

    const seasonId = resolveSeason(button, container);
    if (!seasonId) {
      writeStatus(container, 'Please select a season to rebuild.', 'error');
      return;
    }

    const endpoint = resolveEndpoint(button, container, seasonId);
    if (!endpoint) {
      writeStatus(container, 'Unable to determine rebuild endpoint.', 'error');
      return;
    }

    const statusUrl = resolveStatusEndpoint(button, container, seasonId);
    if (!statusUrl) {
      writeStatus(container, 'Unable to determine status endpoint.', 'error');
      return;
    }

    const progressEls = getProgressElements(container);
    if (progressEls.root) {
      progressEls.root.classList.add('hidden');
    }

    toggleBusy(button, true);
    writeStatus(container, 'Scheduling leaderboard rebuild…', 'info');

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

      let payload = null;
      try {
        payload = await response.json();
      } catch (error) {
        payload = null;
      }

      if (!payload || payload.ok !== true) {
        const message = payload?.error || payload?.message || 'Failed to schedule rebuild.';
        throw new Error(message);
      }

      updateProgressUI(progressEls, 0, 'Queued rebuild');
      writeStatus(container, 'Rebuild queued. Monitoring progress…', 'info');

      const result = await startPolling(container, statusUrl);
      if (result?.error) {
        showToast('error', result.error);
      } else {
        const successMessage = result?.message || 'Leaderboard cache rebuild complete.';
        showToast('success', successMessage);
        writeStatus(container, successMessage, 'success');
      }
    } catch (error) {
      const message = error && error.message ? error.message : 'Rebuild failed.';
      writeStatus(container, message, 'error');
      showToast('error', message);
      if (progressEls.root) {
        progressEls.root.classList.add('hidden');
      }
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
