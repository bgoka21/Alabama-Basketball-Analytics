(function () {
  'use strict';

  const VALUE_DELIMITER = '::';
  const DEFAULT_DECIMALS = 3;
  const SESSION_PLAYER_LIMIT = 5;
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

  function normalizeGameCatalog(catalog) {
    if (!Array.isArray(catalog)) {
      return [];
    }
    return catalog
      .filter((entry) => entry && entry.key)
      .map((entry) => ({
        key: entry.key,
        label: entry.label || entry.key,
        source: 'game',
        group: entry.group || null,
        catalog: entry.catalog || 'leaderboard',
        hidden: Boolean(entry.hidden)
      }));
  }

  function populateMetricSelect(select, practiceMetrics, gameMetrics) {
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

    if (gameMetrics.length) {
      const gameGroups = new Map();
      gameMetrics.forEach((metric) => {
        const groupLabelBase = metric.catalog === 'practice'
          ? metric.group || 'Practice Metrics'
          : metric.group || 'Game Metrics';
        const groupLabel = `Game · ${groupLabelBase}`;
        if (!gameGroups.has(groupLabel)) {
          const optgroup = document.createElement('optgroup');
          optgroup.label = groupLabel;
          gameGroups.set(groupLabel, optgroup);
          select.appendChild(optgroup);
        }
        const option = document.createElement('option');
        option.value = `game${VALUE_DELIMITER}${metric.key}`;
        option.textContent = metric.label;
        option.dataset.source = 'game';
        option.dataset.key = metric.key;
        option.dataset.label = metric.label;
        gameGroups.get(groupLabel).appendChild(option);
      });
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

  function getSelectedGrouping(radios) {
    if (!radios || !radios.length) {
      return 'player';
    }
    const selected = Array.from(radios).find((radio) => radio.checked);
    return selected ? selected.value : 'player';
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

  function syncCanvasDimensions(canvas) {
    if (!canvas) {
      return;
    }
    const viewportHeight = window.innerHeight || 0;
    const targetHeight = Math.max(280, Math.min(520, Math.floor((viewportHeight || 0) * 0.6)));
    canvas.style.height = `${targetHeight}px`;
    canvas.height = targetHeight;
    const width = Math.floor(canvas.getBoundingClientRect().width);
    if (width > 0) {
      canvas.width = width;
    }
  }

  function computeTrendline(points) {
    const validPoints = points.filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y));
    if (validPoints.length < 2) {
      return null;
    }

    const n = validPoints.length;
    let sumX = 0;
    let sumY = 0;
    let sumXY = 0;
    let sumX2 = 0;

    validPoints.forEach((point) => {
      sumX += point.x;
      sumY += point.y;
      sumXY += point.x * point.y;
      sumX2 += point.x * point.x;
    });

    const denominator = n * sumX2 - sumX * sumX;
    if (Math.abs(denominator) <= Number.EPSILON) {
      return null;
    }

    const slope = (n * sumXY - sumX * sumY) / denominator;
    const intercept = (sumY - slope * sumX) / n;

    const sorted = validPoints.slice().sort((a, b) => a.x - b.x);
    const minX = sorted[0].x;
    const maxX = sorted[sorted.length - 1].x;

    return [
      { x: minX, y: intercept + slope * minX },
      { x: maxX, y: intercept + slope * maxX }
    ];
  }

  function renderChart(canvas, studyLabel, scatter, xLabel, yLabel, showTrendline) {
    if (!canvas || typeof Chart === 'undefined') {
      return;
    }
    canvas.classList.remove('hidden');
    syncCanvasDimensions(canvas);
    const context = canvas.getContext('2d');
    const data = scatter.map((point) => ({
      x: point.x,
      y: point.y,
      label: point.label || point.player,
      player: point.player_name || point.player,
      groupLabel: point.group_label || null
    }));

    if (chartInstance) {
      chartInstance.destroy();
    }

    const datasets = [
      {
        label: studyLabel,
        data,
        backgroundColor: '#9E1B32',
        borderColor: '#9E1B32',
        pointRadius: 5,
        hoverRadius: 7
      }
    ];

    if (showTrendline) {
      const trendlinePoints = computeTrendline(data);
      if (trendlinePoints) {
        datasets.push({
          label: `${studyLabel} Trendline`,
          data: trendlinePoints,
          type: 'line',
          fill: false,
          borderColor: '#2563eb',
          backgroundColor: '#2563eb',
          borderWidth: 2,
          pointRadius: 0,
          hitRadius: 0,
          hoverRadius: 0,
          tension: 0,
          borderDash: [6, 4]
        });
      }
    }

    chartInstance = new Chart(context, {
      type: 'scatter',
      data: {
        datasets
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
                const raw = context.raw || {};
                const title = raw.label || raw.player || 'Point';
                const xVal = formatPoint(raw.x);
                const yVal = formatPoint(raw.y);
                const lines = [`${title}: (${xVal}, ${yVal})`];
                if (raw.player && raw.player !== title) {
                  lines.push(`Player: ${raw.player}`);
                }
                if (raw.groupLabel && raw.groupLabel !== title) {
                  lines.push(raw.groupLabel);
                }
                return lines.join('\n');
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
  }

  function renderPointsTable(pointsBody, emptyStateNode, points, xLabel, yLabel, grouping) {
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
    const mode = grouping || 'player';

    points
      .slice()
      .sort((a, b) => {
        const labelA = (a.label || a.player || '').toLowerCase();
        const labelB = (b.label || b.player || '').toLowerCase();
        if (labelA < labelB) {
          return -1;
        }
        if (labelA > labelB) {
          return 1;
        }
        return 0;
      })
      .forEach((point) => {
        const row = document.createElement('tr');
        row.className = 'hover:bg-gray-50';

        const playerCell = document.createElement('td');
        playerCell.className = 'px-4 py-2 text-left text-gray-900';
        const label = point.label || point.player || '—';
        const playerName = point.player_name || point.player;
        const details = [];
        if (mode === 'practice' || mode === 'game') {
          if (playerName && playerName !== label) {
            details.push(playerName);
          }
          if (point.group_label && point.group_label !== label) {
            details.push(point.group_label);
          }
          if (point.session_date) {
            const parsed = new Date(point.session_date);
            if (!Number.isNaN(parsed.getTime())) {
              details.push(parsed.toLocaleDateString());
            }
          }
        } else if (playerName && playerName !== label) {
          details.push(playerName);
        }

        const container = document.createElement('div');
        const primary = document.createElement('div');
        primary.className = 'font-medium text-gray-900';
        primary.textContent = label;
        container.appendChild(primary);

        if (details.length) {
          const secondary = document.createElement('div');
          secondary.className = 'text-xs text-gray-500';
          secondary.textContent = details.join(' • ');
          container.appendChild(secondary);
        }

        playerCell.appendChild(container);

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
    const gameCatalog = readJsonScript('correlation-game-catalog') || [];
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
    const pointsLabel = document.getElementById('correlation-points-label');
    const pointHeader = document.getElementById('correlation-point-header');
    const dateFromInput = document.getElementById('correlation-date-from');
    const dateToInput = document.getElementById('correlation-date-to');
    const trendlineToggle = document.getElementById('correlation-trendline');
    const groupingRadios = document.querySelectorAll('input[name="correlation-group"]');
    const groupHelp = document.getElementById('correlation-group-help');
    const rosterHelp = document.getElementById('correlation-roster-help');
    const samplesHelp = document.getElementById('correlation-samples-help');
    let resizeFrame = null;
    let lastRender = null;

    if (!seasonSelect || !rosterSelect || !xMetricSelect || !yMetricSelect || !runButton || !chartCanvas) {
      return;
    }

    if (!apiUrl) {
      console.warn('Correlation workbench is missing an API URL.');
      runButton.disabled = true;
      return;
    }

    const practiceMetrics = flattenPracticeCatalog(practiceCatalog);
    const gameMetrics = normalizeGameCatalog(gameCatalog);
    populateMetricSelect(xMetricSelect, practiceMetrics, gameMetrics);
    populateMetricSelect(yMetricSelect, practiceMetrics, gameMetrics);

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

    scheduleResize();
    window.addEventListener('resize', scheduleResize);

    seasonSelect.addEventListener('change', () => {
      renderRosterOptions(rosterSelect, rosterData, seasonSelect.value);
    });

    function updateGroupingUI(grouping) {
      const copy = {
        player: {
          group: 'Each point represents a player.',
          roster: 'Hold Ctrl (Windows) or Command (Mac) to select multiple players.',
          label: 'Players',
          column: 'Player',
          empty: 'Run a study to list player-level values.',
          samples: 'Players included after filters.'
        },
        practice: {
          group: 'Each point represents an individual practice for the selected players.',
          roster: `Select up to ${SESSION_PLAYER_LIMIT} players when grouping by practice.`,
          label: 'Practices',
          column: 'Practice',
          empty: 'Run a study to list practice-level values.',
          samples: 'Practices included after filters.'
        },
        game: {
          group: 'Each point represents an individual game for the selected players.',
          roster: `Select up to ${SESSION_PLAYER_LIMIT} players when grouping by game.`,
          label: 'Games',
          column: 'Game',
          empty: 'Run a study to list game-level values.',
          samples: 'Games included after filters.'
        }
      };

      const messages = copy[grouping] || copy.player;
      if (groupHelp) {
        groupHelp.textContent = messages.group;
      }
      if (rosterHelp) {
        rosterHelp.textContent = messages.roster;
      }
      if (samplesHelp) {
        samplesHelp.textContent = messages.samples;
      }
      if (pointsLabel) {
        pointsLabel.textContent = messages.label;
      }
      if (pointHeader) {
        pointHeader.textContent = messages.column;
      }
      if (pointsEmpty) {
        pointsEmpty.textContent = messages.empty;
      }
    }

    const initialGrouping = getSelectedGrouping(groupingRadios);
    updateGroupingUI(initialGrouping);

    if (groupingRadios && groupingRadios.length) {
      groupingRadios.forEach((radio) => {
        radio.addEventListener('change', () => {
          updateGroupingUI(getSelectedGrouping(groupingRadios));
        });
      });
    }

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

    function scheduleResize() {
      if (!chartCanvas) {
        return;
      }
      if (resizeFrame) {
        cancelAnimationFrame(resizeFrame);
      }
      resizeFrame = requestAnimationFrame(() => {
        syncCanvasDimensions(chartCanvas);
        if (chartInstance && typeof chartInstance.resize === 'function') {
          chartInstance.resize();
        }
      });
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

    function updateSummary(studyLabel, xMetric, yMetric, study, grouping) {
      if (summaryTitle) {
        summaryTitle.textContent = studyLabel;
      }
      if (summarySubtitle) {
        summarySubtitle.textContent = `${xMetric.label} vs ${yMetric.label}`;
      }
      if (sampleBadge) {
        const samples = study.samples || 0;
        sampleBadge.textContent = samples === 1 ? '1 point' : `${samples} points`;
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

      const grouping = getSelectedGrouping(groupingRadios);

      if (grouping === 'practice') {
        if (xMetric.source !== 'practice' || yMetric.source !== 'practice') {
          showError('Practice grouping requires both metrics to use practice data.');
          return;
        }
      }

      if (grouping === 'game') {
        if (xMetric.source !== 'game' || yMetric.source !== 'game') {
          showError('Game grouping requires both metrics to use game data.');
          return;
        }
      }

      const rosterIds = Array.from(rosterSelect.selectedOptions)
        .map((option) => Number(option.value))
        .filter((value) => !Number.isNaN(value));

      if ((grouping === 'practice' || grouping === 'game') && !rosterIds.length) {
        showError('Select at least one player before running a per-session study.');
        return;
      }

      if ((grouping === 'practice' || grouping === 'game') && rosterIds.length > SESSION_PLAYER_LIMIT) {
        showError(`Per-session grouping supports up to ${SESSION_PLAYER_LIMIT} players at a time.`);
        return;
      }

      const scope = { season_id: seasonId, group_by: grouping };
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
          updateSummary(studyLabel, xMetric, yMetric, { pearson: null, spearman: null, samples: 0 }, grouping);
          if (emptyState) {
            emptyState.classList.remove('hidden');
          }
          clearChart(chartCanvas);
          lastRender = null;
          renderPointsTable(pointsBody, pointsEmpty, [], xMetric.label, yMetric.label, grouping);
          updateHeaders(xHeader, yHeader, xMetric, yMetric);
          return;
        }

        updateSummary(studyLabel, xMetric, yMetric, study, grouping);
        updateHeaders(xHeader, yHeader, xMetric, yMetric);

        const scatter = Array.isArray(study.scatter) ? study.scatter : [];
        if (scatter.length) {
          if (emptyState) {
            emptyState.classList.add('hidden');
          }
          const showTrendline = trendlineToggle ? trendlineToggle.checked : false;
          renderChart(chartCanvas, studyLabel, scatter, xMetric.label, yMetric.label, showTrendline);
          lastRender = {
            studyLabel,
            scatter,
            xLabel: xMetric.label,
            yLabel: yMetric.label
          };
          renderPointsTable(pointsBody, pointsEmpty, scatter, xMetric.label, yMetric.label, grouping);
        } else {
          if (emptyState) {
            emptyState.classList.remove('hidden');
          }
          clearChart(chartCanvas);
          lastRender = null;
          renderPointsTable(pointsBody, pointsEmpty, [], xMetric.label, yMetric.label, grouping);
        }
      } catch (error) {
        showError(error && error.message ? error.message : 'Failed to run correlation study.');
      } finally {
        setLoading(false);
      }
    });

    if (trendlineToggle) {
      trendlineToggle.addEventListener('change', () => {
        if (!lastRender || !chartCanvas) {
          return;
        }
        if (!lastRender.scatter || !lastRender.scatter.length) {
          clearChart(chartCanvas);
          lastRender = null;
          return;
        }
        renderChart(
          chartCanvas,
          lastRender.studyLabel,
          lastRender.scatter,
          lastRender.xLabel,
          lastRender.yLabel,
          trendlineToggle.checked
        );
      });
    }
  };
})();
