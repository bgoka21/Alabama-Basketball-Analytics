const SHOT_CLASS_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'atr', label: 'ATR' },
  { value: '2fg', label: '2FG' },
  { value: '3fg', label: '3FG' },
  { value: 'ft', label: 'FT' },
];

const POSSESSION_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'transition', label: 'Transition' },
  { value: 'halfcourt', label: 'Halfcourt' },
];

const ZONE_POSITION_RATIO = {
  rim: { x: 0.5, y: 0.88 },
  paint: { x: 0.5, y: 0.74 },
  corner_left: { x: 0.1, y: 0.82 },
  corner_right: { x: 0.9, y: 0.82 },
  corner: { x: 0.5, y: 0.82 },
  wing: { x: 0.2, y: 0.6 },
  wing_right: { x: 0.8, y: 0.6 },
  slot_left: { x: 0.35, y: 0.55 },
  slot_right: { x: 0.65, y: 0.55 },
  short_corner_left: { x: 0.16, y: 0.76 },
  short_corner_right: { x: 0.84, y: 0.76 },
  short_wing_left: { x: 0.25, y: 0.68 },
  short_wing_right: { x: 0.75, y: 0.68 },
  top: { x: 0.5, y: 0.48 },
  logo: { x: 0.5, y: 0.28 },
  unknown: { x: 0.5, y: 0.12 },
};

const SVG_NS = 'http://www.w3.org/2000/svg';

const toTitle = (text) => text.charAt(0).toUpperCase() + text.slice(1);

const normalizeShotClass = (shot) => {
  if (!shot || !shot.shot_class) {
    return '';
  }
  return String(shot.shot_class).trim().toLowerCase();
};

const normalizePossessionType = (shot) => {
  const raw = String(shot.possession_type || '').toLowerCase();
  if (raw.includes('trans')) {
    return 'transition';
  }
  if (raw.includes('half') || raw.includes('hc')) {
    return 'halfcourt';
  }
  return '';
};

const toPercent = (value) => `${(value * 100).toFixed(1)}%`;

const createSvgElement = (tag, attrs = {}) => {
  const el = document.createElementNS(SVG_NS, tag);
  Object.entries(attrs).forEach(([key, value]) => {
    el.setAttribute(key, value);
  });
  return el;
};

const buildCourtSvg = (width, height) => {
  const svg = createSvgElement('svg', {
    viewBox: `0 0 ${width} ${height}`,
    class: 'shot-chart-court',
    role: 'img',
    'aria-label': 'Shot chart court diagram',
  });

  const baselineY = height - 30;
  const hoopY = height - 80;
  const hoopRadius = 10;
  const paintWidth = 160;
  const paintHeight = 190;
  const paintX = (width - paintWidth) / 2;
  const paintY = height - paintHeight - 30;

  const courtOutline = createSvgElement('rect', {
    x: 20,
    y: 20,
    width: width - 40,
    height: height - 40,
    fill: '#f8fafc',
    stroke: '#9ca3af',
    'stroke-width': 2,
    rx: 8,
  });
  svg.appendChild(courtOutline);

  const baseline = createSvgElement('line', {
    x1: 20,
    y1: baselineY,
    x2: width - 20,
    y2: baselineY,
    stroke: '#9ca3af',
    'stroke-width': 2,
  });
  svg.appendChild(baseline);

  const hoop = createSvgElement('circle', {
    cx: width / 2,
    cy: hoopY,
    r: hoopRadius,
    fill: 'none',
    stroke: '#ef4444',
    'stroke-width': 2,
  });
  svg.appendChild(hoop);

  const paint = createSvgElement('rect', {
    x: paintX,
    y: paintY,
    width: paintWidth,
    height: paintHeight,
    fill: 'none',
    stroke: '#9ca3af',
    'stroke-width': 2,
  });
  svg.appendChild(paint);

  const freeThrow = createSvgElement('circle', {
    cx: width / 2,
    cy: paintY,
    r: 60,
    fill: 'none',
    stroke: '#9ca3af',
    'stroke-width': 2,
  });
  svg.appendChild(freeThrow);

  const arc = createSvgElement('path', {
    d: `M 60 ${paintY - 10} A ${width / 2 - 40} ${width / 2 - 40} 0 0 1 ${width - 60} ${paintY - 10}`,
    fill: 'none',
    stroke: '#9ca3af',
    'stroke-width': 2,
  });
  svg.appendChild(arc);

  return svg;
};

const interpolateColor = (start, end, factor) => {
  const s = start.match(/\w\w/g).map((c) => parseInt(c, 16));
  const e = end.match(/\w\w/g).map((c) => parseInt(c, 16));
  const rgb = s.map((channel, i) => {
    const value = Math.round(channel + (e[i] - channel) * factor);
    return value.toString(16).padStart(2, '0');
  });
  return `#${rgb.join('')}`;
};

const getFgColor = (fgPct) => {
  if (!Number.isFinite(fgPct)) {
    return '#94a3b8';
  }
  const clamped = Math.max(0, Math.min(fgPct, 1));
  return interpolateColor('dc2626', '16a34a', clamped);
};

class ShotChart {
  constructor(container) {
    this.container = container;
    this.playerId = container.dataset.playerId;
    this.seasonId = container.dataset.seasonId;
    this.endpoint = container.dataset.shotChartEndpoint;
    this.svgHost = container.querySelector('[data-shot-chart-graphic]');
    this.summaryEl = container.querySelector('[data-shot-chart-summary]');
    this.statusEl = container.querySelector('[data-shot-chart-status]');
    this.shotClassSelect = container.querySelector('[data-shot-class]');
    this.possessionSelect = container.querySelector('[data-possession-type]');
    this.rawShots = [];
    this.chartSvg = null;
    this.configureFilters();
  }

  configureFilters() {
    SHOT_CLASS_OPTIONS.forEach((opt) => {
      const option = document.createElement('option');
      option.value = opt.value;
      option.textContent = opt.label;
      this.shotClassSelect.appendChild(option);
    });

    POSSESSION_OPTIONS.forEach((opt) => {
      const option = document.createElement('option');
      option.value = opt.value;
      option.textContent = opt.label;
      this.possessionSelect.appendChild(option);
    });

    this.shotClassSelect.addEventListener('change', () => this.render());
    this.possessionSelect.addEventListener('change', () => this.render());
  }

  async load() {
    if (!this.playerId) {
      this.setStatus('Missing player id for shot chart.');
      return;
    }
    this.setStatus('Loading shot data...');
    const params = new URLSearchParams({ raw: '1' });
    if (this.seasonId) {
      params.set('season_id', this.seasonId);
    }

    const baseUrl = this.endpoint || `/api/players/${this.playerId}/shot-chart`;
    const response = await fetch(`${baseUrl}?${params.toString()}`);
    if (!response.ok) {
      this.setStatus('Unable to load shot data.');
      return;
    }
    const payload = await response.json();
    this.rawShots = Array.isArray(payload.raw) ? payload.raw : [];
    if (!this.rawShots.length && payload.zones) {
      this.rawShots = Object.entries(payload.zones).flatMap(([zone, attempts]) =>
        Array.from({ length: attempts }, () => ({ normalized_location: zone }))
      );
    }
    this.setStatus('');
    this.render();
  }

  setStatus(message) {
    if (this.statusEl) {
      this.statusEl.textContent = message;
      this.statusEl.classList.toggle('hidden', message.length === 0);
    }
  }

  getFilters() {
    return {
      shotClass: this.shotClassSelect.value,
      possession: this.possessionSelect.value,
    };
  }

  filterShots() {
    const { shotClass, possession } = this.getFilters();
    return this.rawShots.filter((shot) => {
      const shotClassValue = normalizeShotClass(shot);
      const possessionValue = normalizePossessionType(shot);
      if (shotClass !== 'all' && shotClassValue !== shotClass) {
        return false;
      }
      if (possession !== 'all' && possessionValue !== possession) {
        return false;
      }
      return true;
    });
  }

  aggregateShots(shots) {
    return shots.reduce((acc, shot) => {
      const zone = shot.normalized_location || 'unknown';
      if (!acc[zone]) {
        acc[zone] = { attempts: 0, makes: 0 };
      }
      acc[zone].attempts += 1;
      if (String(shot.result).toLowerCase() === 'made') {
        acc[zone].makes += 1;
      }
      return acc;
    }, {});
  }

  render() {
    const shots = this.filterShots();
    const totals = shots.reduce(
      (acc, shot) => {
        acc.attempts += 1;
        if (String(shot.result).toLowerCase() === 'made') {
          acc.makes += 1;
        }
        return acc;
      },
      { attempts: 0, makes: 0 }
    );

    if (this.summaryEl) {
      const fgPct = totals.attempts ? toPercent(totals.makes / totals.attempts) : '0.0%';
      this.summaryEl.textContent = `Attempts: ${totals.attempts} · Makes: ${totals.makes} · FG%: ${fgPct}`;
    }

    if (!shots.length) {
      this.setStatus('No shots found for the selected filters.');
    } else {
      this.setStatus('');
    }

    const width = 520;
    const height = 470;
    if (!this.chartSvg) {
      this.chartSvg = buildCourtSvg(width, height);
      this.svgHost.innerHTML = '';
      this.svgHost.appendChild(this.chartSvg);
    }

    const existingMarkers = this.chartSvg.querySelectorAll('.shot-marker');
    existingMarkers.forEach((marker) => marker.remove());

    const aggregates = this.aggregateShots(shots);
    const maxAttempts = Math.max(...Object.values(aggregates).map((zone) => zone.attempts), 0);
    Object.entries(aggregates).forEach(([zone, data]) => {
      const ratios = ZONE_POSITION_RATIO[zone] || ZONE_POSITION_RATIO.unknown;
      const radius = maxAttempts
        ? 6 + (data.attempts / maxAttempts) * 14
        : 6;
      const fgPct = data.attempts ? data.makes / data.attempts : 0;
      const color = getFgColor(fgPct);

      const marker = createSvgElement('circle', {
        class: 'shot-marker',
        cx: ratios.x * width,
        cy: ratios.y * height,
        r: radius,
        fill: color,
        'fill-opacity': 0.7,
        stroke: '#1f2937',
        'stroke-width': 1,
      });
      const title = createSvgElement('title');
      title.textContent = `${toTitle(zone.replace(/_/g, ' '))}: ${data.makes}/${data.attempts} (${toPercent(fgPct)})`;
      marker.appendChild(title);
      this.chartSvg.appendChild(marker);
    });
  }
}

document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('[data-shot-chart]').forEach((container) => {
    const chart = new ShotChart(container);
    chart.load();
  });
});
