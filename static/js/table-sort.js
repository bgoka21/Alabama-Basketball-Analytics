/**
 * This script attaches sorting to any table header with data-sortable=true unless the table has .table-exempt.
 * Columns are type-inferred; use data-value on <td> for precise control.
 * Updates aria-sort on headers and toggles .is-sorted-asc / .is-sorted-desc on the active header.
 */
(function () {
  'use strict';

  const MAX_SAMPLE_ROWS = 10;
  const SORTABLE_HEADER_SELECTOR = 'th[data-sortable="true"]';
  const SR_MESSAGES = {
    asc: 'sorted ascending',
    desc: 'sorted descending',
    none: 'unsorted'
  };
  const ARIA_SORT = {
    asc: 'ascending',
    desc: 'descending',
    none: 'none'
  };
  const MONTH_MAP = {
    jan: 0,
    january: 0,
    feb: 1,
    february: 1,
    mar: 2,
    march: 2,
    apr: 3,
    april: 3,
    may: 4,
    jun: 5,
    june: 5,
    jul: 6,
    july: 6,
    aug: 7,
    august: 7,
    sep: 8,
    sept: 8,
    september: 8,
    oct: 9,
    october: 9,
    nov: 10,
    november: 10,
    dec: 11,
    december: 11
  };
  const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
  const LIVE_REGION_SELECTORS = ['[data-table-sort-announcer]', '[data-table-sort-live]'];
  const headerGridCache = new WeakMap();

  function ready(callback) {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', callback, { once: true });
    } else {
      callback();
    }
  }

  ready(initTableSorting);

  function initTableSorting() {
    const tables = Array.from(document.querySelectorAll('table')).filter(
      (table) => !table.classList.contains('table-exempt')
    );

    tables.forEach((table) => {
      const sortableHeaders = Array.from(table.querySelectorAll(SORTABLE_HEADER_SELECTOR)).filter(
        (header) => header.getAttribute('data-sortable') === 'true'
      );

      if (!sortableHeaders.length) {
        return;
      }

      const primaryBody = table.tBodies[0];
      if (!primaryBody) {
        return;
      }

      sortableHeaders.forEach((header) => {
        if (!hasFocusableChild(header)) {
          header.setAttribute('tabindex', '0');
        }
        header.dataset.sortDirection = 'none';
        header.dataset.columnIndex = String(getColumnIndex(header));

        header.addEventListener('click', (event) => {
          event.preventDefault();
          activateSort(table, sortableHeaders, header);
        });

        header.addEventListener('keydown', (event) => {
          if (event.target !== header) {
            return;
          }
          if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            activateSort(table, sortableHeaders, header);
          }
        });

        header.addEventListener('mousedown', (event) => {
          if (event.detail > 1) {
            event.preventDefault();
          }
        });
      });

      applyDefaultSort(table, sortableHeaders);
    });
  }

  function hasFocusableChild(header) {
    return Boolean(
      header.querySelector(
        'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
      )
    );
  }

  function activateSort(table, headers, header) {
    if (header.getAttribute('data-sortable') !== 'true') {
      return;
    }

    const columnIndex = Number(header.dataset.columnIndex);
    if (Number.isNaN(columnIndex)) {
      return;
    }

    const direction = determineNextDirection(table, header);
    const type = header.dataset.sortType || inferColumnType(table, columnIndex);
    header.dataset.sortType = type;

    sortTableByColumn(table, columnIndex, type, direction);
    updateHeaderIndicators(headers, header, direction);
    table.dataset.sortColumnIndex = String(columnIndex);
    table.dataset.sortDirection = direction;
    announceSort(header, direction);
  }

  function determineNextDirection(table, header) {
    const columnIndex = header.dataset.columnIndex;
    const currentColumn = table.dataset.sortColumnIndex;
    const currentDirection = header.dataset.sortDirection || 'none';

    if (currentColumn === columnIndex && (currentDirection === 'asc' || currentDirection === 'desc')) {
      return currentDirection === 'asc' ? 'desc' : 'asc';
    }
    return 'asc';
  }

  function updateHeaderIndicators(headers, activeHeader, direction) {
    headers.forEach((header) => {
      const isActive = header === activeHeader;
      const srOnly = header.querySelector('.sr-only.sort-state');
      const caret = header.querySelector('.sort-caret');

      if (isActive) {
        header.dataset.sortDirection = direction;
        header.setAttribute('aria-sort', ARIA_SORT[direction]);
        header.classList.remove('is-sorted-asc', 'is-sorted-desc');
        if (direction === 'asc') {
          header.classList.add('is-sorted-asc');
        } else if (direction === 'desc') {
          header.classList.add('is-sorted-desc');
        }
        if (srOnly) {
          srOnly.textContent = SR_MESSAGES[direction];
        }
        if (caret) {
          caret.classList.remove('opacity-0');
          caret.classList.add('opacity-100');
          caret.setAttribute('aria-hidden', 'true');
        }
      } else {
        header.dataset.sortDirection = 'none';
        header.setAttribute('aria-sort', ARIA_SORT.none);
        header.classList.remove('is-sorted-asc', 'is-sorted-desc');
        if (srOnly) {
          srOnly.textContent = SR_MESSAGES.none;
        }
        if (caret) {
          caret.classList.add('opacity-0');
          caret.classList.remove('opacity-100');
          caret.setAttribute('aria-hidden', 'true');
        }
      }
    });
  }

  function sortTableByColumn(table, columnIndex, type, direction) {
    const tbody = table.tBodies[0];
    if (!tbody) {
      return;
    }

    const rows = Array.from(tbody.rows).map((row, index) => {
      const cell = getCellAt(row, columnIndex);
      const rawValue = cell ? getCellValue(cell) : '';
      const trimmed = rawValue.trim();
      const parsed = parseValueByType(trimmed, type);
      const isEmpty = trimmed === '' || parsed === null;
      return {
        row,
        index,
        sortKey: parsed,
        textKey: trimmed,
        isEmpty
      };
    });

    const multiplier = direction === 'asc' ? 1 : -1;

    rows.sort((a, b) => {
      if (a.isEmpty && b.isEmpty) {
        return a.index - b.index;
      }
      if (a.isEmpty) {
        return direction === 'asc' ? 1 : -1;
      }
      if (b.isEmpty) {
        return direction === 'asc' ? -1 : 1;
      }

      let comparison = 0;
      if (type === 'text') {
        comparison = collator.compare(a.textKey, b.textKey);
      } else {
        const aValue = a.sortKey;
        const bValue = b.sortKey;
        if (aValue < bValue) {
          comparison = -1;
        } else if (aValue > bValue) {
          comparison = 1;
        }
      }

      if (comparison === 0) {
        return a.index - b.index;
      }
      return comparison * multiplier;
    });

    const fragment = document.createDocumentFragment();
    rows.forEach((item) => {
      fragment.appendChild(item.row);
    });
    tbody.appendChild(fragment);
  }

  function getCellValue(cell) {
    const dataValue = cell.getAttribute('data-value');
    if (dataValue !== null) {
      return String(dataValue);
    }
    return cell.textContent || '';
  }

  function getColumnIndex(header) {
    const table = header.closest('table');
    if (!table) {
      return header.cellIndex || 0;
    }

    const grid = getHeaderGrid(table);
    for (let rowIndex = 0; rowIndex < grid.length; rowIndex += 1) {
      const row = grid[rowIndex];
      if (!row) {
        continue;
      }
      for (let columnIndex = 0; columnIndex < row.length; columnIndex += 1) {
        if (row[columnIndex] === header) {
          return columnIndex;
        }
      }
    }

    return header.cellIndex || 0;
  }

  function getHeaderGrid(table) {
    if (headerGridCache.has(table)) {
      return headerGridCache.get(table);
    }

    const grid = buildHeaderGrid(table);
    headerGridCache.set(table, grid);
    return grid;
  }

  function buildHeaderGrid(table) {
    const head = table.tHead;
    if (!head) {
      return [];
    }

    const grid = [];
    const rows = Array.from(head.rows);

    rows.forEach((row, rowIndex) => {
      grid[rowIndex] = grid[rowIndex] || [];
      let columnPointer = 0;

      Array.from(row.cells).forEach((cell) => {
        while (grid[rowIndex][columnPointer]) {
          columnPointer += 1;
        }

        const rowSpan = cell.rowSpan || 1;
        const colSpan = cell.colSpan || 1;

        for (let r = 0; r < rowSpan; r += 1) {
          const targetRow = rowIndex + r;
          grid[targetRow] = grid[targetRow] || [];
          for (let c = 0; c < colSpan; c += 1) {
            grid[targetRow][columnPointer + c] = cell;
          }
        }

        columnPointer += colSpan;
      });
    });

    return grid;
  }

  function getCellAt(row, columnIndex) {
    let position = 0;
    for (const cell of row.cells) {
      const span = cell.colSpan || 1;
      if (columnIndex < position + span) {
        return cell;
      }
      position += span;
    }
    return null;
  }

  function parseValueByType(value, type) {
    if (value === '') {
      return null;
    }

    switch (type) {
      case 'percent':
        return parsePercent(value);
      case 'currency':
        return parseCurrency(value);
      case 'number':
        return parseNumber(value);
      case 'date':
        return parseDate(value);
      case 'text':
      default:
        return value.trim();
    }
  }

  function inferColumnType(table, columnIndex) {
    const tbody = table.tBodies[0];
    if (!tbody) {
      return 'text';
    }

    const counts = {
      percent: 0,
      currency: 0,
      number: 0,
      date: 0,
      text: 0
    };
    let samples = 0;

    for (const row of tbody.rows) {
      if (samples >= MAX_SAMPLE_ROWS) {
        break;
      }
      const cell = getCellAt(row, columnIndex);
      if (!cell) {
        continue;
      }
      const rawValue = getCellValue(cell).trim();
      if (rawValue === '') {
        continue;
      }
      const detected = detectValueType(rawValue);
      counts[detected] += 1;
      samples += 1;
    }

    if (samples === 0) {
      return 'text';
    }

    const nonTextCount = counts.percent + counts.currency + counts.number + counts.date;
    if (counts.text > 0 && nonTextCount > 0) {
      return 'text';
    }

    const priority = ['percent', 'currency', 'number', 'date'];
    let chosen = 'text';
    let bestCount = 0;

    priority.forEach((type) => {
      if (counts[type] > bestCount) {
        chosen = type;
        bestCount = counts[type];
      }
    });

    if (bestCount === 0) {
      return 'text';
    }

    return chosen;
  }

  function detectValueType(value) {
    const trimmed = value.trim();
    if (!trimmed) {
      return 'text';
    }

    if (/%/.test(trimmed)) {
      const parsedPercent = parsePercent(trimmed);
      if (parsedPercent !== null) {
        return 'percent';
      }
    }

    if (isLikelyCurrency(trimmed)) {
      const parsedCurrency = parseCurrency(trimmed);
      if (parsedCurrency !== null) {
        return 'currency';
      }
    }

    if (isLikelyDate(trimmed)) {
      const parsedDate = parseDate(trimmed);
      if (parsedDate !== null) {
        return 'date';
      }
    }

    if (!/[A-Za-z]/.test(trimmed)) {
      const parsedNumber = parseNumber(trimmed);
      if (parsedNumber !== null) {
        return 'number';
      }
    }

    return 'text';
  }

  function isLikelyCurrency(value) {
    return /[$€£¥₹]|^(USD|EUR|GBP|AUD|CAD|CHF|JPY|CNY)\b/i.test(value);
  }

  function isLikelyDate(value) {
    return /\d{4}-\d{1,2}-\d{1,2}/.test(value) || /\d{1,2}\/\d{1,2}\/\d{2,4}/.test(value) || /[A-Za-z]{3,9}\s+\d{1,2}/.test(value);
  }

  function parseNumber(value) {
    if (value == null) {
      return null;
    }

    let sanitized = String(value).replace(/[\u00A0\s]/g, '').trim();
    if (!sanitized) {
      return null;
    }

    let negative = false;
    if (sanitized.startsWith('(') && sanitized.endsWith(')')) {
      negative = true;
      sanitized = sanitized.slice(1, -1);
    }

    sanitized = sanitized.replace(/,/g, '');
    sanitized = sanitized.replace(/[^0-9.+-]/g, '');

    if (!sanitized || sanitized === '.' || sanitized === '+' || sanitized === '-' || sanitized === '+.' || sanitized === '-.') {
      return null;
    }

    const number = Number(sanitized);
    if (Number.isNaN(number)) {
      return null;
    }

    return negative ? -number : number;
  }

  function parsePercent(value) {
    const withoutPercent = String(value).replace(/%/g, '');
    const number = parseNumber(withoutPercent);
    if (number === null) {
      return null;
    }
    return number / 100;
  }

  function parseCurrency(value) {
    return parseNumber(value);
  }

  function parseDate(value) {
    if (value == null) {
      return null;
    }

    const trimmed = String(value).trim();
    if (!trimmed) {
      return null;
    }

    if (/^\d+$/.test(trimmed)) {
      return null;
    }

    const isoMatch = trimmed.match(/^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2})(?::(\d{2}))?(?::(\d{2}))?)?$/);
    if (isoMatch) {
      const year = Number(isoMatch[1]);
      const month = Number(isoMatch[2]) - 1;
      const day = Number(isoMatch[3]);
      const hour = Number(isoMatch[4] || 0);
      const minute = Number(isoMatch[5] || 0);
      const second = Number(isoMatch[6] || 0);
      return new Date(year, month, day, hour, minute, second).getTime();
    }

    const mdyMatch = trimmed.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
    if (mdyMatch) {
      const month = Number(mdyMatch[1]) - 1;
      const day = Number(mdyMatch[2]);
      let year = Number(mdyMatch[3]);
      if (year < 100) {
        year += year >= 70 ? 1900 : 2000;
      }
      return new Date(year, month, day).getTime();
    }

    const textMatch = trimmed.match(/^([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})$/);
    if (textMatch) {
      const monthName = textMatch[1].toLowerCase();
      const month = MONTH_MAP[monthName];
      if (month !== undefined) {
        const day = Number(textMatch[2]);
        const year = Number(textMatch[3]);
        return new Date(year, month, day).getTime();
      }
    }

    const parsed = Date.parse(trimmed);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }

    return null;
  }

  function announceSort(header, direction) {
    const liveRegion = findLiveRegion();
    if (!liveRegion) {
      return;
    }

    const label = extractHeaderLabel(header);
    const directionText = direction === 'asc' ? 'ascending' : 'descending';
    liveRegion.textContent = `Sorted by ${label}, ${directionText}`;
  }

  function findLiveRegion() {
    for (const selector of LIVE_REGION_SELECTORS) {
      const region = document.querySelector(selector);
      if (region) {
        return region;
      }
    }
    return null;
  }

  function extractHeaderLabel(header) {
    const clone = header.cloneNode(true);
    clone.querySelectorAll('.sort-caret, .sr-only').forEach((node) => node.remove());
    return clone.textContent.trim().replace(/\s+/g, ' ') || header.textContent.trim();
  }

  function applyDefaultSort(table, headers) {
    if (!headers.length) {
      return;
    }

    const spec = parseDefaultSort(table.dataset.defaultSort || '');
    if (!spec.length) {
      return;
    }

    const resolved = spec
      .map((item) => {
        const key = item.key;
        const header = headers.find((candidate) => {
          return (candidate.dataset.key || candidate.getAttribute('data-key')) === key;
        });
        if (!header) {
          return null;
        }
        const columnIndex = Number(header.dataset.columnIndex);
        if (Number.isNaN(columnIndex)) {
          return null;
        }
        return { header, columnIndex, direction: item.direction };
      })
      .filter(Boolean);

    if (!resolved.length) {
      return;
    }

    let primary = null;

    for (let index = resolved.length - 1; index >= 0; index -= 1) {
      const entry = resolved[index];
      if (!entry) {
        continue;
      }
      const { header, columnIndex, direction } = entry;
      const type = header.dataset.sortType || inferColumnType(table, columnIndex);
      header.dataset.sortType = type;
      sortTableByColumn(table, columnIndex, type, direction);
      if (!primary) {
        primary = { header, direction };
      }
    }

    if (primary) {
      updateHeaderIndicators(headers, primary.header, primary.direction);
      table.dataset.sortColumnIndex = String(primary.header.dataset.columnIndex || '');
      table.dataset.sortDirection = primary.direction;
    }
  }

  function parseDefaultSort(value) {
    if (!value) {
      return [];
    }

    return value
      .split(';')
      .map((part) => part.trim())
      .filter(Boolean)
      .map((chunk) => {
        const pieces = chunk.split(':');
        const key = pieces[0] ? pieces[0].trim() : '';
        let direction = pieces[1] ? pieces[1].trim().toLowerCase() : 'desc';
        if (direction !== 'asc' && direction !== 'desc') {
          direction = 'desc';
        }
        return key ? { key, direction } : null;
      })
      .filter(Boolean);
  }
})();

