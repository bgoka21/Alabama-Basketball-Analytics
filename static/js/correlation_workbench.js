(function () {
  'use strict';

  const VALUE_DELIMITER = '::';
  const DEFAULT_DECIMALS = 3;
  let chartInstance = null;

  function readJsonScript(id) {
    const node = document.getElementById(id);
    if (!node) {
      return null;
    }
    try {
      return JSON.parse(node.textContent || 'null');
    } catch (error) {
      console.warn('Failed to parse JSON from', id, error);
      return null;
    }
  }

  function buildHeaders() {
    const headers = { 'Content-Type': 'application/json' };
    if (window.__CSRF__) {
      headers['X-CSRFToken'] = window.__CSRF__;
    }
    return headers;
  }

  async function safeError(res) {
    try {
      const payload = await res.json();
      if (payload && typeof payload.error === 'string' && payload.error.trim()) {
        return payload.error.trim();
      }
    } catch (error) {
      // ignore
    }
    return res.statusText || 'Request failed';
  }

  function flattenPracticeCatalog(catalog) {
    const entries = [];
    if (!catalog || typeof catalog !== 'object') {
      return entries;
    }
    Object.entries(catalog).forEach(([groupOrKey, fields]) => {
      if (Array.isArray(fields)) {
        fields.forEach((field) => {
          if (!field || !field.key) {
            return;
          }
          entries.push({
            key: field.key,
            label: field.label || field.key,
            group: groupOrKey,
            source: 'practice'
          });
        });
        return;
      }

      if (fields && typeof fields === 'object' && fields.key) {
        entries.push({
          key: fields.key,
          label: fields.label || fields.key,
          group: fields.group || null,
          source: 'practice'
        });
      }
    });
    return entries;
  }

  function normalizeLeaderboardCatalog(catalog) {
    if (!catalog) {
      return [];
    }
    if (Array.isArray(catalog)) {
      return catalog
        .filter((entry) => entry && entry.key)
        .map((entry) => ({
          key: entry.key,
          label: entry.label || entry.key,
          source: 'game',
          hidden: Boolean(entry.hidden)
        }));
    }
    if (typeof catalog === 'object') {
      return Object.values(catalog)
        .filter((entry) => entry && entry.key)
        .map((entry) => ({
          key: entry.key,
          label: entry.label || entry.key,
          source: 'game',
          hidden: Boolean(entry.hidden)
        }));
    }
    return [];
  }

  function populateMetricSelect(select, practiceMetrics, leaderboardMetrics) {
    if (!select) {
      return;
    }
    select.innerHTML = '';

    const practiceGroups = new Map();
    practiceMetrics.forEach((metric) => {
      const groupLabel = metric.group ? `Practice · ${metric.group}` : 'Practice';
      if (!practiceGroups.has(groupLabel)) {
        const optgroup = document.createElement('optgroup');
        optgroup.label = groupLabel;
        practiceGroups.set(groupLabel, optgroup);
        select.appendChild(optgroup);
      }
      const option = document.createElement('option');
      option.value = `practice${VALUE_DELIMITER}${metric.key}`;
      option.textContent = metric.label;
      option.dataset.source = 'practice';
      option.dataset.key = metric.key;
      option.dataset.label = metric.label;
      practiceGroups.get(groupLabel).appendChild(option);
    });

    if (leaderboardMetrics.length) {
      const optgroup = document.createElement('optgroup');
      optgroup.label = 'Game Metrics';
      leaderboardMetrics.forEach((metric) => {
        const option = document.createElement('option');
        option.value = `game${VALUE_DELIMITER}${metric.key}`;
        option.textContent = metric.label;
        option.dataset.source = 'game';
        option.dataset.key = metric.key;
        option.dataset.label = metric.label;
        optgroup.appendChild(option);
      });
      select.appendChild(optgroup);
    }

    if (select.options.length) {
      select.selectedIndex = 0;
    }
  }

  function getSelectedMetric(select) {
    if (!select || !select.value) {
      return null;
    }
    const [source, key] = select.value.split(VALUE_DELIMITER);
    if (!source || !key) {
      return null;
    }
    const option = select.selectedOptions[0];
    const label = option ? option.dataset.label || option.textContent : key;
    return {
      source,
      key,
      label
    };
  }

  function formatCoefficient(value) {
    if (value === null || typeof value === 'undefined') {
      return '—';
    }
    const num = Number(value);
    if (Number.isNaN(num)) {
      return '—';
    }
    return num.toFixed(DEFAULT_DECIMALS);
  }

  function formatPoint(value) {
    if (value === null || typeof value === 'undefined') {
      return '—';
    }
    const num = Number(value);
    if (Number.isNaN(num)) {
      return '—';
    }
    return num.toFixed(DEFAULT_DECIMALS);
  }

  function clearChart(canvas) {
    if (chartInstance) {
      chartInstance.destroy();
      chartInstance = null;
    }
    if (canvas) {
      canvas.classList.add('hidden');
    }
  }

  function renderChart(canvas, studyLabel, scatter, xLabel, yLabel) {
    if (!canvas || typeof Chart === 'undefined') {
      return;
    }
    const context = canvas.getContext('2d');
    const data = scatter.map((point) => ({
      x: point.x,
      y: point.y,
      player: point.player
    }));

    if (chartInstance) {
      chartInstance.destroy();
    }

    chartInstance = new Chart(context, {
      type: 'scatter',
      data: {
        datasets: [
          {
            label: studyLabel,
            data,
            backgroundColor: '#9E1B32',
            borderColor: '#9E1B32',
            pointRadius: 5,
            hoverRadius: 7
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            display: false
          },
          tooltip: {
            callbacks: {
              label(context) {
                const player = context.raw.player || 'Unknown';
                const xVal = formatPoint(context.raw.x);
                const yVal = formatPoint(context.raw.y);
                return `${player}: (${xVal}, ${yVal})`;
              }
            }
          }
        },
        scales: {
          x: {
            title: {
              display: true,
              text: xLabel
            },
            ticks: {
              maxTicksLimit: 10
            }
          },
          y: {
            title: {
              display: true,
              text: yLabel
            },
            ticks: {
              maxTicksLimit: 10
            }
          }
        }
      }
    });
    canvas.classList.remove('hidden');
  }

  function renderPointsTable(pointsBody, emptyStateNode, points, xLabel, yLabel) {
    if (!pointsBody) {
      return;
    }
    pointsBody.innerHTML = '';

    if (!points || !points.length) {
      if (emptyStateNode) {
        emptyStateNode.classList.remove('hidden');
      }
      return;
    }

    if (emptyStateNode) {
      emptyStateNode.classList.add('hidden');
    }

    const fragment = document.createDocumentFragment();
    points
      .slice()
      .sort((a, b) => {
        return (a.player || '').localeCompare(b.player || '');
      })
      .forEach((point) => {
        const row = document.createElement('tr');
        row.className = 'hover:bg-gray-50';

        const playerCell = document.createElement('td');
        playerCell.className = 'px-4 py-2 text-left text-gray-900';
        playerCell.textContent = point.player || '—';

        const xCell = document.createElement('td');
        xCell.className = 'px-4 py-2 text-left text-gray-700';
        xCell.textContent = formatPoint(point.x);
        xCell.setAttribute('data-label', xLabel);

        const yCell = document.createElement('td');
        yCell.className = 'px-4 py-2 text-left text-gray-700';
        yCell.textContent = formatPoint(point.y);
        yCell.setAttribute('data-label', yLabel);

        row.appendChild(playerCell);
        row.appendChild(xCell);
        row.appendChild(yCell);
        fragment.appendChild(row);
      });

    pointsBody.appendChild(fragment);
  }

  function renderRosterOptions(select, roster, seasonId) {
    if (!select) {
      return;
    }
    select.innerHTML = '';

    const players = roster
      .filter((player) => {
        if (!seasonId) {
          return true;
        }
        return Number(player.season_id) === Number(seasonId);
      })
      .slice()
      .sort((a, b) => {
        const jerseyA = Number(a.jersey);
        const jerseyB = Number(b.jersey);
        const jerseyCompare = (Number.isNaN(jerseyA) ? 1 : 0) - (Number.isNaN(jerseyB) ? 1 : 0);
        if (jerseyCompare !== 0) {
          return jerseyCompare;
        }
        if (!Number.isNaN(jerseyA) && !Number.isNaN(jerseyB) && jerseyA !== jerseyB) {
          return jerseyA - jerseyB;
        }
        return (a.name || '').localeCompare(b.name || '');
      });

    if (!players.length) {
      const option = document.createElement('option');
      option.disabled = true;
      option.textContent = 'No players available for this season';
      select.appendChild(option);
      select.disabled = true;
      return;
    }

    select.disabled = false;

    players.forEach((player) => {
      const option = document.createElement('option');
      option.value = player.id;
      const jersey = Number(player.jersey);
      const jerseyLabel = !Number.isNaN(jersey) ? `#${String(jersey).padStart(2, '0')} ` : '';
      option.textContent = `${jerseyLabel}${player.name || player.label || 'Player'}`;
      select.appendChild(option);
    });
  }

  function updateHeaders(xHeader, yHeader, xMetric, yMetric) {
    if (xHeader) {
      xHeader.textContent = `${xMetric.label} (${xMetric.source === 'practice' ? 'Practice' : 'Game'})`;
    }
    if (yHeader) {
      yHeader.textContent = `${yMetric.label} (${yMetric.source === 'practice' ? 'Practice' : 'Game'})`;
    }
  }

  window.initCorrelationWorkbench = function initCorrelationWorkbench(config) {
    const root = document.querySelector('[data-correlation-root]');
    if (!root) {
      return;
    }

    const apiUrl = config && config.apiUrl;
    const rosterData = readJsonScript('correlation-roster-data') || [];
    const practiceCatalog = readJsonScript('correlation-practice-catalog') || {};
    const leaderboardCatalog = readJsonScript('correlation-leaderboard-catalog') || [];
    const seasons = readJsonScript('correlation-season-options') || [];

    const seasonSelect = document.getElementById('correlation-season');
    const rosterSelect = document.getElementById('correlation-roster');
    const xMetricSelect = document.getElementById('correlation-x-metric');
    const yMetricSelect = document.getElementById('correlation-y-metric');
    const labelInput = document.getElementById('correlation-study-label');
    const runButton = document.getElementById('correlation-run');
    const errorBox = document.getElementById('correlation-error');
    const chartCanvas = document.getElementById('correlation-chart');
    const emptyState = document.getElementById('correlation-empty-state');
    const summaryTitle = document.getElementById('correlation-study-title');
    const summarySubtitle = document.getElementById('correlation-study-subtitle');
    const sampleBadge = document.getElementById('correlation-sample-count');
    const pearsonCell = document.getElementById('correlation-pearson');
    const spearmanCell = document.getElementById('correlation-spearman');
    const samplesCell = document.getElementById('correlation-samples');
    const xHeader = document.getElementById('correlation-x-header');
    const yHeader = document.getElementById('correlation-y-header');
    const pointsBody = document.getElementById('correlation-points-body');
    const pointsEmpty = document.getElementById('correlation-points-empty');
    const dateFromInput = document.getElementById('correlation-date-from');
    const dateToInput = document.getElementById('correlation-date-to');

    if (!seasonSelect || !rosterSelect || !xMetricSelect || !yMetricSelect || !runButton) {
      return;
    }

    if (!apiUrl) {
      console.warn('Correlation workbench is missing an API URL.');
      runButton.disabled = true;
      return;
    }

    const practiceMetrics = flattenPracticeCatalog(practiceCatalog);
    const leaderboardMetrics = normalizeLeaderboardCatalog(leaderboardCatalog);
    populateMetricSelect(xMetricSelect, practiceMetrics, leaderboardMetrics);
    populateMetricSelect(yMetricSelect, practiceMetrics, leaderboardMetrics);

    if (!xMetricSelect.options.length || !yMetricSelect.options.length) {
      runButton.disabled = true;
    }

    const defaultSeasonId = root.dataset.selectedSeason;
    if (defaultSeasonId && seasonSelect.querySelector(`option[value="${defaultSeasonId}"]`)) {
      seasonSelect.value = defaultSeasonId;
    } else if (seasons.length && !seasonSelect.value) {
      seasonSelect.value = seasons[0].id;
    }

    renderRosterOptions(rosterSelect, rosterData, seasonSelect.value);

    seasonSelect.addEventListener('change', () => {
      renderRosterOptions(rosterSelect, rosterData, seasonSelect.value);
    });

    function clearError() {
      if (errorBox) {
        errorBox.classList.add('hidden');
        errorBox.textContent = '';
      }
    }

    function showError(message) {
      if (errorBox) {
        errorBox.textContent = message;
        errorBox.classList.remove('hidden');
      }
    }

    function setLoading(isLoading) {
      if (!runButton) {
        return;
      }
      if (isLoading) {
        runButton.dataset.loading = 'true';
        runButton.disabled = true;
        if (!runButton.dataset.originalText) {
          runButton.dataset.originalText = runButton.textContent;
        }
        runButton.textContent = 'Running…';
      } else {
        runButton.dataset.loading = 'false';
        runButton.disabled = false;
        if (runButton.dataset.originalText) {
          runButton.textContent = runButton.dataset.originalText;
        }
      }
    }

    function updateSummary(studyLabel, xMetric, yMetric, study) {
      if (summaryTitle) {
        summaryTitle.textContent = studyLabel;
      }
      if (summarySubtitle) {
        summarySubtitle.textContent = `${xMetric.label} vs ${yMetric.label}`;
      }
      if (sampleBadge) {
        const samples = study.samples || 0;
        sampleBadge.textContent = samples === 1 ? '1 sample' : `${samples} samples`;
      }
      if (pearsonCell) {
        pearsonCell.textContent = formatCoefficient(study.pearson);
      }
      if (spearmanCell) {
        spearmanCell.textContent = formatCoefficient(study.spearman);
      }
      if (samplesCell) {
        samplesCell.textContent = String(study.samples || 0);
      }
    }

    runButton.addEventListener('click', async () => {
      if (runButton.dataset.loading === 'true') {
        return;
      }
      clearError();

      const seasonId = Number(seasonSelect.value);
      if (!seasonId) {
        showError('Select a season before running a study.');
        return;
      }

      const xMetric = getSelectedMetric(xMetricSelect);
      const yMetric = getSelectedMetric(yMetricSelect);
      if (!xMetric || !yMetric) {
        showError('Choose both an X and Y metric.');
        return;
      }

      const rosterIds = Array.from(rosterSelect.selectedOptions)
        .map((option) => Number(option.value))
        .filter((value) => !Number.isNaN(value));

      const scope = { season_id: seasonId };
      if (rosterIds.length) {
        scope.roster_ids = rosterIds;
      }
      if (dateFromInput && dateFromInput.value) {
        scope.start_date = dateFromInput.value;
      }
      if (dateToInput && dateToInput.value) {
        scope.end_date = dateToInput.value;
      }

      const studyLabel = (labelInput && labelInput.value ? labelInput.value.trim() : '') || `${xMetric.label} vs ${yMetric.label}`;

      const payload = {
        studies: [
          {
            identifier: 'primary',
            label: studyLabel,
            x: {
              source: xMetric.source,
              key: xMetric.key,
              label: xMetric.label
            },
            y: {
              source: yMetric.source,
              key: yMetric.key,
              label: yMetric.label
            }
          }
        ],
        scope
      };

      setLoading(true);
      try {
        const response = await fetch(apiUrl, {
          method: 'POST',
          credentials: 'same-origin',
          headers: buildHeaders(),
          body: JSON.stringify(payload)
        });
        if (!response.ok) {
          throw new Error(await safeError(response));
        }
        const data = await response.json();
        const studies = data && Array.isArray(data.studies) ? data.studies : [];
        const study = studies[0];
        if (!study) {
          updateSummary(studyLabel, xMetric, yMetric, { pearson: null, spearman: null, samples: 0 });
          if (emptyState) {
            emptyState.classList.remove('hidden');
          }
          clearChart(chartCanvas);
          renderPointsTable(pointsBody, pointsEmpty, [], xMetric.label, yMetric.label);
          updateHeaders(xHeader, yHeader, xMetric, yMetric);
          return;
        }

        updateSummary(studyLabel, xMetric, yMetric, study);
        updateHeaders(xHeader, yHeader, xMetric, yMetric);

        const scatter = Array.isArray(study.scatter) ? study.scatter : [];
        if (scatter.length) {
          if (emptyState) {
            emptyState.classList.add('hidden');
          }
          renderChart(chartCanvas, studyLabel, scatter, xMetric.label, yMetric.label);
          renderPointsTable(pointsBody, pointsEmpty, scatter, xMetric.label, yMetric.label);
        } else {
          if (emptyState) {
            emptyState.classList.remove('hidden');
          }
          clearChart(chartCanvas);
          renderPointsTable(pointsBody, pointsEmpty, [], xMetric.label, yMetric.label);
        }
      } catch (error) {
        showError(error && error.message ? error.message : 'Failed to run correlation study.');
      } finally {
        setLoading(false);
      }
    });
  };
})();
