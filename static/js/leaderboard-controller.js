// static/js/leaderboard-controller.js
(function () {
  const state = {
    seasonId: null,
    snapshots: {}, // statKey -> payload
    practiceSections: {}, // statKey -> practice section payload
    columnsByKey: {}, // statKey -> columns_manifest
    initialized: false,
    currentKey: null,
  };

  function qs(sel, root = document) { return root.querySelector(sel); }
  function qsa(sel, root = document) { return Array.from(root.querySelectorAll(sel)); }

  function setText(el, txt) { if (el) el.textContent = txt; }

  function parseMaybeNumber(value) {
    if (value === null || typeof value === "undefined") return null;
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value !== "string") return null;
    const trimmed = value.trim();
    if (!trimmed) return null;
    const pct = trimmed.endsWith("%");
    const normalized = (pct ? trimmed.slice(0, -1) : trimmed).replace(/,/g, "");
    const num = Number(normalized);
    if (!Number.isFinite(num)) return null;
    return pct ? num / 100 : num;
  }

  const TABLE_CONTAINER_CLASS = "rounded-lg border border-gray-200 dark:border-gray-700 shadow-sm overflow-hidden";
  const TABLE_BODY_WRAPPER_CLASS = "overflow-x-auto table-card-body";
  const TABLE_BASE_CLASS = "min-w-full text-sm table-auto unified-table";
  const THEAD_BASE_CLASS = "bg-gray-50 dark:bg-gray-800/60 border-b border-gray-200 dark:border-gray-700";
  const TABLE_ROW_CLASS = "odd:bg-white even:bg-gray-50 dark:odd:bg-gray-900 dark:even:bg-gray-800 hover:bg-gray-100 dark:hover:bg-gray-700/60 transition-colors";
  const TOTAL_ROW_CLASS = "bg-gray-50 dark:bg-gray-800/60 font-semibold border-t border-gray-200 dark:border-gray-700";
  const CELL_PADDING_CLASS = "px-4 py-2";
  const ALIGN_CLASS_MAP = {
    left: "text-left",
    right: "text-right",
    center: "text-center",
  };
  const JUSTIFY_CLASS_MAP = {
    left: "justify-start",
    right: "justify-end",
    center: "justify-center",
  };
  const PRACTICE_EMPTY_MESSAGE = "No stats for the most recent practice.";

  function toClassList(value) {
    if (!value) return "";
    if (Array.isArray(value)) {
      return value.filter(Boolean).join(" ");
    }
    return String(value);
  }

  function normalizeTablePayload(payload) {
    if (!payload || typeof payload !== "object") {
      return payload;
    }

    const rawColumns = Array.isArray(payload.columns)
      ? payload.columns.filter(col => col && typeof col === "object")
      : [];
    const rawManifest = Array.isArray(payload.columns_manifest)
      ? payload.columns_manifest.filter(col => col && typeof col === "object")
      : [];

    const manifestByKey = new Map();
    rawManifest.forEach((col) => {
      const key = col.key || col.slug || "";
      if (!key) return;
      manifestByKey.set(key, col);
    });

    const columnSource = rawColumns.length ? rawColumns : rawManifest;
    const resolvedColumns = columnSource
      .map((entry) => {
        if (!entry || typeof entry !== "object") return null;
        const key = entry.key || entry.slug || "";
        if (!key) return null;
        const fromManifest = manifestByKey.get(key);
        const merged = { ...(fromManifest || {}), ...entry };
        merged.key = key;
        merged.label = merged.label || (fromManifest && fromManifest.label) || key;
        if (!merged.align && fromManifest && fromManifest.align) {
          merged.align = fromManifest.align;
        }
        if (fromManifest && fromManifest.value_key && !merged.value_key) {
          merged.value_key = fromManifest.value_key;
        }
        if (!Object.prototype.hasOwnProperty.call(merged, "sortable")) {
          const manifestSortable = fromManifest && fromManifest.sortable;
          merged.sortable = manifestSortable === false ? false : true;
        }
        return merged;
      })
      .filter(Boolean);

    const manifest = rawManifest.length
      ? rawManifest
      : resolvedColumns.map(col => ({
        key: col.key,
        label: col.label,
        align: col.align,
        value_key: col.value_key,
        sortable: col.sortable,
      }));

    payload.columns_manifest = manifest;

    const valueKeyMap = new Map();
    resolvedColumns.forEach(col => {
      if (col && col.key) {
        valueKeyMap.set(col.key, col.value_key || null);
      }
    });

    if (!Array.isArray(payload.rows)) {
      payload.rows = [];
    }

    payload.rows.forEach(row => {
      if (!row || typeof row !== "object") {
        return;
      }
      if (!row.display || typeof row.display !== "object") {
        row.display = {};
      }
      if (!row.metrics || typeof row.metrics !== "object") {
        row.metrics = {};
      }
      resolvedColumns.forEach((col, index) => {
        const key = col.key;
        if (!key) return;
        let textValue;
        if (Object.prototype.hasOwnProperty.call(row, key)) {
          textValue = row[key];
        } else if (Array.isArray(row.values)) {
          textValue = row.values[index];
        }
        const text = textValue === null || typeof textValue === "undefined" ? "" : String(textValue);
        row.display[key] = text;
        const rawKey = valueKeyMap.get(key);
        let raw = rawKey ? row[rawKey] : undefined;
        if (raw === undefined || raw === null) {
          raw = parseMaybeNumber(textValue);
        }
        row.metrics[key] = {
          raw: raw,
          text,
        };
      });
    });

    if (payload.totals && typeof payload.totals === "object") {
      const totals = payload.totals;
      if (!totals.display || typeof totals.display !== "object") {
        totals.display = {};
      }
      if (!totals.metrics || typeof totals.metrics !== "object") {
        totals.metrics = {};
      }
      resolvedColumns.forEach((col, index) => {
        const key = col.key;
        if (!key) return;
        let textValue;
        if (Object.prototype.hasOwnProperty.call(totals, key)) {
          textValue = totals[key];
        }
        const text = textValue === null || typeof textValue === "undefined" ? "" : String(textValue);
        const metric = totals.metrics[key] || {};
        let raw = metric.raw;
        if (raw === undefined || raw === null) {
          const rawKey = valueKeyMap.get(key);
          raw = rawKey ? totals[rawKey] : undefined;
        }
        if (raw === undefined || raw === null) {
          raw = parseMaybeNumber(textValue);
        }
        totals.metrics[key] = {
          raw: raw,
          text,
        };
      });
      payload.totals = totals;
    }

    if (payload.aux_table) {
      payload.aux_table = normalizeTablePayload(payload.aux_table);
    }

    payload.columns_resolved = resolvedColumns;

    return payload;
  }

  function normalizePracticeSection(section) {
    if (!section || typeof section !== "object") {
      return null;
    }
    const normalized = { ...section };
    normalized.title = typeof section.title === "string" ? section.title : "";
    const empty = section.empty_message;
    normalized.empty_message = typeof empty === "string" && empty.trim()
      ? empty
      : PRACTICE_EMPTY_MESSAGE;
    if (section.table && typeof section.table === "object") {
      normalized.table = normalizeTablePayload(section.table);
    } else {
      normalized.table = null;
    }
    return normalized;
  }

  function normalizePracticeSectionsPayload(data) {
    if (!data || typeof data !== "object") {
      return null;
    }
    const normalized = { ...data };
    const mainSections = Array.isArray(data.main)
      ? data.main.map(normalizePracticeSection).filter(Boolean)
      : [];
    const auxSections = Array.isArray(data.aux)
      ? data.aux.map(normalizePracticeSection).filter(Boolean)
      : [];
    normalized.main = mainSections;
    normalized.aux = auxSections;
    const empty = data.empty_message;
    normalized.empty_message = typeof empty === "string" && empty.trim()
      ? empty.trim()
      : PRACTICE_EMPTY_MESSAGE;
    if (normalized.note_display && typeof normalized.note_display === "string") {
      const trimmed = normalized.note_display.trim();
      normalized.note_display = trimmed || null;
    } else {
      normalized.note_display = null;
    }
    if (normalized.last_practice_date !== undefined && normalized.last_practice_date !== null) {
      normalized.last_practice_date = String(normalized.last_practice_date);
    } else {
      normalized.last_practice_date = null;
    }
    return normalized;
  }

  function hasPracticeSections(data) {
    if (!data || typeof data !== "object") {
      return false;
    }
    const mainCount = Array.isArray(data.main) ? data.main.length : 0;
    const auxCount = Array.isArray(data.aux) ? data.aux.length : 0;
    return mainCount > 0 || auxCount > 0;
  }

  function formatPracticeNote(data) {
    if (!data || typeof data !== "object") {
      return null;
    }
    if (data.note_display && typeof data.note_display === "string" && data.note_display.trim()) {
      return data.note_display.trim();
    }
    const iso = data.last_practice_date;
    if (!iso) {
      return null;
    }
    const parsed = new Date(iso);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    try {
      return parsed.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
    } catch (err) {
      return parsed.toISOString().split("T")[0];
    }
  }

  function buildPracticeSection(section, noteText) {
    if (!section || typeof section !== "object") {
      return null;
    }

    const wrapper = document.createElement("section");
    wrapper.className = "space-y-3";

    const header = document.createElement("div");
    header.className = "flex items-baseline justify-between";
    const heading = document.createElement("h3");
    heading.className = "text-lg font-semibold text-gray-900 dark:text-gray-100";
    heading.textContent = section.title || "";
    header.appendChild(heading);
    if (noteText) {
      const note = document.createElement("span");
      note.className = "text-xs opacity-70";
      note.textContent = `(${noteText})`;
      header.appendChild(note);
    }
    wrapper.appendChild(header);

    const srTotals = document.createElement("span");
    srTotals.className = "sr-only";
    srTotals.textContent = `${section.title || ""} — Practice Totals`;
    wrapper.appendChild(srTotals);

    const srLast = document.createElement("span");
    srLast.className = "sr-only";
    srLast.textContent = `${section.title || ""} — Last Practice`;
    wrapper.appendChild(srLast);

    const tableData = section.table && typeof section.table === "object" ? section.table : null;
    const hasContent = tableData && ((Array.isArray(tableData.rows) && tableData.rows.length > 0) || tableData.totals);

    if (hasContent) {
      const host = document.createElement("div");
      wrapper.appendChild(host);
      const columns = Array.isArray(tableData.columns_resolved) && tableData.columns_resolved.length
        ? tableData.columns_resolved
        : (Array.isArray(tableData.columns) && tableData.columns.length
          ? tableData.columns
          : tableData.columns_manifest || []);
      const options = {
        totals: tableData.totals,
        tableId: tableData.table_id || tableData.id,
        defaultSort: tableData.default_sort,
        caption: section.title || null,
      };
      buildTable(host, columns, Array.isArray(tableData.rows) ? tableData.rows : [], options);
    } else {
      const message = document.createElement("div");
      message.className = "text-sm opacity-70";
      message.textContent = section.empty_message || PRACTICE_EMPTY_MESSAGE;
      wrapper.appendChild(message);
    }

    return wrapper;
  }

  function renderPracticeSections(data) {
    const main = qs("#leaderboard-main");
    const aux = qs("#leaderboard-aux");
    if (!main) {
      return;
    }

    const noteText = formatPracticeNote(data);
    const emptyMessage = data && typeof data.empty_message === "string" && data.empty_message.trim()
      ? data.empty_message.trim()
      : PRACTICE_EMPTY_MESSAGE;
    const mainSections = Array.isArray(data && data.main) ? data.main : [];
    const auxSections = Array.isArray(data && data.aux) ? data.aux : [];

    main.innerHTML = "";
    if (mainSections.length) {
      mainSections.forEach((section) => {
        const node = buildPracticeSection(section, noteText);
        if (node) {
          main.appendChild(node);
        }
      });
    } else {
      const message = document.createElement("div");
      message.className = "text-sm opacity-70";
      message.textContent = emptyMessage;
      main.appendChild(message);
    }

    if (aux) {
      aux.innerHTML = "";
      if (auxSections.length) {
        aux.classList.remove("hidden");
        auxSections.forEach((section) => {
          const node = buildPracticeSection(section, noteText);
          if (node) {
            aux.appendChild(node);
          }
        });
      } else {
        aux.classList.add("hidden");
      }
    }
  }

  function createSortCaret(sortable) {
    const caret = document.createElement("span");
    caret.className = sortable ? "sort-caret opacity-0 group-hover:opacity-100 transition" : "sort-caret opacity-0 transition";
    caret.setAttribute("aria-hidden", "true");
    return caret;
  }

  function ensureAlign(value) {
    const normalized = typeof value === "string" ? value.toLowerCase() : "left";
    return ALIGN_CLASS_MAP[normalized] ? normalized : "left";
  }

  function extractSortValue(cell) {
    if (!cell) {
      return { text: "", value: 0, type: "text", empty: true };
    }
    const dataValue = cell.getAttribute("data-value");
    const datasetRaw = dataValue !== null ? dataValue : (cell.dataset.raw !== undefined ? cell.dataset.raw : null);
    const textContent = (cell.textContent || "").trim();
    let text = datasetRaw !== null && typeof datasetRaw !== "undefined" && String(datasetRaw).trim() !== "" ? String(datasetRaw).trim() : textContent;
    let numeric = parseMaybeNumber(datasetRaw);
    if (numeric === null) {
      numeric = parseMaybeNumber(textContent);
    }
    const isNumber = numeric !== null && numeric !== undefined && !Number.isNaN(numeric);
    if (!text) {
      text = isNumber ? String(numeric) : "";
    }
    return {
      text,
      value: isNumber ? numeric : text,
      type: isNumber ? "number" : "text",
      empty: !isNumber && !text,
    };
  }

  function getHeadersByKey(table, key) {
    return qsa("thead th[data-key]", table).filter(h => h.dataset.key === key);
  }

  function getPrimaryHeader(table, key) {
    const bottomRow = table.querySelector("thead tr:last-of-type");
    if (bottomRow) {
      const match = qsa("th[data-key]", bottomRow).find(th => th.dataset.key === key);
      if (match) {
        return match;
      }
    }
    const headers = getHeadersByKey(table, key);
    return headers.length ? headers[0] : null;
  }

  function updateSortIndicators(table, key, direction) {
    const allHeaders = qsa("thead th[data-key]", table);
    allHeaders.forEach((header) => {
      header.dataset.sortDir = "none";
      header.setAttribute("aria-sort", "none");
      header.classList.remove("is-sorted-asc", "is-sorted-desc");
      const caret = header.querySelector(".sort-caret");
      if (caret) {
        caret.classList.add("opacity-0");
        caret.classList.remove("opacity-100");
      }
    });

    const matching = getHeadersByKey(table, key);
    matching.forEach((header) => {
      header.dataset.sortDir = direction;
      header.setAttribute("aria-sort", direction === "asc" ? "ascending" : "descending");
      header.classList.remove("is-sorted-asc", "is-sorted-desc");
      header.classList.add(direction === "asc" ? "is-sorted-asc" : "is-sorted-desc");
      const caret = header.querySelector(".sort-caret");
      if (caret) {
        caret.classList.remove("opacity-0");
        caret.classList.add("opacity-100");
      }
    });
  }

  function performSort(table, columnIndex, direction, key) {
    const tbody = table.tBodies[0];
    if (!tbody) return;
    const multiplier = direction === "asc" ? 1 : -1;
    const rows = Array.from(tbody.rows).map((row, index) => {
      const cell = row.cells[columnIndex];
      const sortValue = extractSortValue(cell);
      return { row, index, sortValue };
    });

    rows.sort((a, b) => {
      const aVal = a.sortValue;
      const bVal = b.sortValue;
      if (aVal.empty && bVal.empty) {
        return a.index - b.index;
      }
      if (aVal.empty) {
        return 1;
      }
      if (bVal.empty) {
        return -1;
      }
      if (aVal.type === "number" && bVal.type === "number") {
        const diff = aVal.value - bVal.value;
        if (diff === 0) {
          return a.index - b.index;
        }
        return diff * multiplier;
      }
      const comparison = String(aVal.text).localeCompare(String(bVal.text), undefined, { numeric: true, sensitivity: "base" });
      if (comparison === 0) {
        return a.index - b.index;
      }
      return comparison * multiplier;
    });

    const fragment = document.createDocumentFragment();
    rows.forEach(({ row }) => fragment.appendChild(row));
    tbody.appendChild(fragment);

    table.dataset.sortKey = key;
    table.dataset.sortDir = direction;
  }

  function sortBy(table, key, directionOverride = null, columnIndexOverride = null, options = {}) {
    if (!table) return;
    const headerCandidates = getHeadersByKey(table, key);
    if (!headerCandidates.length) return;
    let header = headerCandidates[0];
    if (columnIndexOverride !== null && columnIndexOverride !== undefined) {
      const match = headerCandidates.find(h => Number(h.dataset.columnIndex) === Number(columnIndexOverride));
      if (match) {
        header = match;
      }
    } else {
      const primary = getPrimaryHeader(table, key);
      if (primary) {
        header = primary;
      }
    }

    const columnIndex = columnIndexOverride !== null && columnIndexOverride !== undefined
      ? Number(columnIndexOverride)
      : Number(header.dataset.columnIndex);
    if (Number.isNaN(columnIndex) || columnIndex < 0) return;

    const sortable = header.dataset.sortable !== "false";
    if (!sortable) return;

    const current = header.dataset.sortDir === "asc" || header.dataset.sortDir === "desc"
      ? header.dataset.sortDir
      : "none";
    const direction = directionOverride || (current === "asc" ? "desc" : "asc");

    performSort(table, columnIndex, direction, key);
    if (options.updateIndicators !== false) {
      updateSortIndicators(table, key, direction);
    } else {
      table.dataset.sortKey = key;
      table.dataset.sortDir = direction;
    }
  }

  function parseDefaultSort(spec) {
    if (!spec || typeof spec !== "string") {
      return [];
    }
    return spec
      .split(";")
      .map(part => part.trim())
      .filter(Boolean)
      .map(chunk => {
        const pieces = chunk.split(":");
        const key = pieces[0] ? pieces[0].trim() : "";
        if (!key) return null;
        let direction = pieces[1] ? pieces[1].trim().toLowerCase() : "desc";
        if (direction !== "asc" && direction !== "desc") {
          direction = "desc";
        }
        return { key, direction };
      })
      .filter(Boolean);
  }

  function applyDefaultSort(table, defaultSort) {
    const spec = parseDefaultSort(defaultSort);
    if (!table || !spec.length) return;
    let primary = null;
    for (let idx = spec.length - 1; idx >= 0; idx -= 1) {
      const item = spec[idx];
      if (!item) continue;
      const header = getPrimaryHeader(table, item.key);
      if (!header) continue;
      const columnIndex = Number(header.dataset.columnIndex);
      if (Number.isNaN(columnIndex)) continue;
      performSort(table, columnIndex, item.direction, item.key);
      primary = primary || item;
    }
    if (primary) {
      updateSortIndicators(table, primary.key, primary.direction);
    }
  }

  function createColumnHeader(table, column, columnIndex) {
    const key = column && column.key ? column.key : "";
    if (!key) return null;
    const alignKey = ensureAlign(column.align || "left");
    const alignClass = ALIGN_CLASS_MAP[alignKey];
    const justifyClass = JUSTIFY_CLASS_MAP[alignKey];
    const widthClass = toClassList(column.width);
    const headerExtraClass = toClassList(column.header_class || column.header_classes);
    const sortable = column.sortable !== false;
    const th = document.createElement("th");
    th.setAttribute("scope", "col");
    th.dataset.key = key;
    th.dataset.columnIndex = String(columnIndex);
    th.dataset.sortable = sortable ? "true" : "false";
    th.dataset.sortDir = "none";
    th.setAttribute("aria-sort", "none");
    if (column.format || column.sort_type) {
      th.dataset.sortType = column.sort_type || column.format;
    }
    const baseClasses = [
      CELL_PADDING_CLASS,
      "font-semibold",
      "text-gray-700",
      "dark:text-gray-200",
      "whitespace-nowrap",
      "select-none",
      alignClass,
    ];
    if (sortable) {
      baseClasses.push("sortable");
    }
    if (widthClass) {
      baseClasses.push(widthClass);
    }
    if (headerExtraClass) {
      baseClasses.push(headerExtraClass);
    }
    th.className = baseClasses.join(" ");

    const label = column.label || key;
    if (sortable) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `inline-flex items-center gap-1 w-full ${justifyClass} group`;
      const span = document.createElement("span");
      span.innerHTML = label;
      button.appendChild(span);
      button.appendChild(createSortCaret(true));
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        sortBy(table, key, null, columnIndex);
      });
      th.appendChild(button);
      th.addEventListener("click", (event) => {
        if (event.target && event.target.closest("button")) {
          return;
        }
        event.preventDefault();
        sortBy(table, key, null, columnIndex);
      });
    } else {
      const wrapper = document.createElement("div");
      wrapper.className = `inline-flex items-center gap-1 w-full ${justifyClass}`;
      const span = document.createElement("span");
      span.innerHTML = label;
      wrapper.appendChild(span);
      wrapper.appendChild(createSortCaret(false));
      th.appendChild(wrapper);
    }

    return th;
  }

  function createGroupHeader(label) {
    const th = document.createElement("th");
    th.setAttribute("scope", "col");
    th.className = `${CELL_PADDING_CLASS} font-semibold text-gray-700 dark:text-gray-200 whitespace-nowrap select-none text-center`;
    th.dataset.sortable = "false";
    th.dataset.sortDir = "none";
    th.setAttribute("aria-sort", "none");
    th.textContent = label || "";
    return th;
  }

  function buildHeader(table, columnsManifest) {
    const thead = document.createElement("thead");
    thead.className = THEAD_BASE_CLASS;
    const hasGroups = columnsManifest.some(col => col && col.group);
    if (hasGroups) {
      const headerItems = [];
      let currentGroup = null;
      columnsManifest.forEach((col, index) => {
        const group = col && col.group ? col.group : null;
        if (group) {
          if (currentGroup && currentGroup.label === group) {
            currentGroup.span += 1;
          } else {
            if (currentGroup && currentGroup.span > 0) {
              headerItems.push(currentGroup);
            }
            currentGroup = { type: "group", label: group, span: 1 };
          }
        } else {
          if (currentGroup && currentGroup.span > 0) {
            headerItems.push(currentGroup);
            currentGroup = null;
          }
          headerItems.push({ type: "column", column: col, index });
        }
      });
      if (currentGroup && currentGroup.span > 0) {
        headerItems.push(currentGroup);
      }

      const topRow = document.createElement("tr");
      headerItems.forEach((item) => {
        if (item.type === "group") {
          const th = createGroupHeader(item.label);
          th.colSpan = item.span;
          topRow.appendChild(th);
        } else if (item.type === "column") {
          const th = createColumnHeader(table, item.column, item.index);
          if (th) {
            th.rowSpan = 2;
            topRow.appendChild(th);
          }
        }
      });
      thead.appendChild(topRow);

      const bottomRow = document.createElement("tr");
      columnsManifest.forEach((col, index) => {
        if (!col || !col.group) return;
        const th = createColumnHeader(table, col, index);
        if (th) {
          bottomRow.appendChild(th);
        }
      });
      if (bottomRow.children.length) {
        thead.appendChild(bottomRow);
      }
    } else {
      const row = document.createElement("tr");
      columnsManifest.forEach((col, index) => {
        const th = createColumnHeader(table, col, index);
        if (th) {
          row.appendChild(th);
        }
      });
      thead.appendChild(row);
    }

    return thead;
  }

  function resolveCellClass(column) {
    const direct = toClassList(column.cell_class);
    if (direct) return direct;
    return toClassList(column.cell_classes);
  }

  function resolveMetric(source, key) {
    if (!source || typeof source !== "object") return null;
    if (source.metrics && typeof source.metrics === "object" && source.metrics[key]) {
      return source.metrics[key];
    }
    return null;
  }

  function resolveDisplay(source, key) {
    if (!source || typeof source !== "object") return "";
    if (source.display && typeof source.display === "object" && key in source.display) {
      return source.display[key];
    }
    if (key in source) {
      return source[key];
    }
    return "";
  }

  function assignCellValue(cell, column, entity) {
    const key = column.key;
    const metric = resolveMetric(entity, key);
    const displayValue = metric && typeof metric.text !== "undefined"
      ? metric.text
      : resolveDisplay(entity, key);
    const text = displayValue === null || typeof displayValue === "undefined"
      ? ""
      : String(displayValue).trim();
    cell.textContent = text;

    let rawValue = metric && metric.raw;
    if (rawValue === undefined || rawValue === null || rawValue === "") {
      const valueKey = column.value_key;
      if (valueKey && entity && typeof entity === "object" && valueKey in entity) {
        rawValue = entity[valueKey];
      }
    }
    if (rawValue === undefined || rawValue === null || rawValue === "") {
      rawValue = parseMaybeNumber(text);
    }

    if (rawValue !== undefined && rawValue !== null && rawValue !== "") {
      const valueString = String(rawValue);
      cell.dataset.raw = valueString;
      cell.setAttribute("data-value", valueString);
    } else {
      delete cell.dataset.raw;
      cell.removeAttribute("data-value");
    }
  }

  function buildTable(container, columnDefs, rows, options = {}) {
    container.innerHTML = "";
    const columns = Array.isArray(columnDefs) ? columnDefs : [];
    const tableWrapper = document.createElement("div");
    tableWrapper.className = TABLE_CONTAINER_CLASS;
    const accent = document.createElement("div");
    accent.className = "table-card-accent";
    accent.setAttribute("aria-hidden", "true");
    tableWrapper.appendChild(accent);

    const bodyWrapper = document.createElement("div");
    bodyWrapper.className = TABLE_BODY_WRAPPER_CLASS;
    tableWrapper.appendChild(bodyWrapper);

    const table = document.createElement("table");
    table.className = TABLE_BASE_CLASS;
    table.setAttribute("role", "table");
    const tableId = options.tableId || options.id;
    if (tableId) {
      table.id = tableId;
    }
    if (options.defaultSort) {
      table.dataset.defaultSort = options.defaultSort;
    }
    if (options.caption) {
      const caption = document.createElement("caption");
      caption.className = "sr-only";
      caption.textContent = options.caption;
      table.appendChild(caption);
    }

    const thead = buildHeader(table, columns);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    const safeRows = Array.isArray(rows) ? rows : [];
    safeRows.forEach((row) => {
      const tr = document.createElement("tr");
      tr.className = TABLE_ROW_CLASS;
      columns.forEach((column, index) => {
        if (!column || !column.key) return;
        const alignKey = ensureAlign(column.align || "left");
        const alignClass = ALIGN_CLASS_MAP[alignKey];
        const numericClass = alignKey === "right" ? "text-right tabular-nums" : "";
        const cellClass = resolveCellClass(column);
        const td = document.createElement("td");
        const classes = [CELL_PADDING_CLASS, alignClass];
        if (numericClass) classes.push(numericClass);
        if (cellClass) classes.push(cellClass);
        td.className = classes.join(" ");
        td.setAttribute("data-key", column.key);
        const dataType = column.format || "text";
        td.setAttribute("data-type", dataType);
        td.dataset.columnIndex = String(index);
        assignCellValue(td, column, row);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);

    const totals = options.totals && typeof options.totals === "object" ? options.totals : null;
    if (totals && totals.metrics) {
      const tfoot = document.createElement("tfoot");
      const tr = document.createElement("tr");
      tr.className = TOTAL_ROW_CLASS;
      tr.setAttribute("data-total", "true");
      columns.forEach((column, index) => {
        if (!column || !column.key) return;
        const alignKey = ensureAlign(column.align || "left");
        const alignClass = ALIGN_CLASS_MAP[alignKey];
        const numericClass = alignKey === "right" ? "text-right tabular-nums" : "";
        const cellClass = resolveCellClass(column);
        const cellClasses = [CELL_PADDING_CLASS, alignClass];
        if (numericClass) cellClasses.push(numericClass);
        if (cellClass) cellClasses.push(cellClass);
        const metric = totals.metrics[column.key];
        const cellTag = index === 0 ? "th" : "td";
        const cell = document.createElement(cellTag);
        if (cellTag === "th") {
          cell.setAttribute("scope", "row");
        }
        cell.className = cellClasses.join(" ");
        const display = metric && typeof metric.text !== "undefined"
          ? metric.text
          : resolveDisplay(totals, column.key);
        let text = display === null || typeof display === "undefined" ? "" : String(display).trim();
        if (index === 0 && !text) {
          text = totals.display && totals.display.player ? String(totals.display.player) : "Totals";
        }
        cell.textContent = text;
        let rawValue = metric && metric.raw;
        if (rawValue === undefined || rawValue === null || rawValue === "") {
          const valueKey = column.value_key;
          if (valueKey && valueKey in totals) {
            rawValue = totals[valueKey];
          }
        }
        if (rawValue === undefined || rawValue === null || rawValue === "") {
          rawValue = parseMaybeNumber(text);
        }
        if (rawValue !== undefined && rawValue !== null && rawValue !== "") {
          const valueString = String(rawValue);
          cell.dataset.raw = valueString;
          cell.setAttribute("data-value", valueString);
        }
        tr.appendChild(cell);
      });
      tfoot.appendChild(tr);
      table.appendChild(tfoot);
    }

    bodyWrapper.appendChild(table);
    container.appendChild(tableWrapper);

    if (options.defaultSort) {
      applyDefaultSort(table, options.defaultSort);
    }

    return table;
  }

  function renderPayload(payload) {
    const normalized = normalizeTablePayload(payload);
    const main = qs("#leaderboard-main");
    const aux = qs("#leaderboard-aux");
    if (!main) return;

    const tableOptions = normalized ? {
      totals: normalized.totals,
      tableId: normalized.table_id,
      defaultSort: normalized.default_sort,
      caption: normalized.caption || normalized.title || null,
    } : null;

    // Single table
    const resolvedColumns = normalized && Array.isArray(normalized.columns_resolved)
      ? normalized.columns_resolved
      : null;
    const columnsForMain = resolvedColumns && resolvedColumns.length
      ? resolvedColumns
      : (normalized && normalized.columns_manifest) || [];

    if (normalized && columnsForMain && Array.isArray(normalized.rows)) {
      buildTable(main, columnsForMain, normalized.rows, tableOptions || {});
    } else {
      main.innerHTML = "<div class='text-sm text-gray-500'>No data</div>";
    }

    // Dual table support (optional)
    if (aux) {
      if (normalized && normalized.aux_table) {
        const auxColumns = Array.isArray(normalized.aux_table.columns_resolved)
          ? normalized.aux_table.columns_resolved
          : normalized.aux_table.columns_manifest;
        if (auxColumns && Array.isArray(normalized.aux_table.rows)) {
          aux.classList.remove("hidden");
          const auxOptions = {
            totals: normalized.aux_table.totals,
            tableId: normalized.aux_table.table_id,
            defaultSort: normalized.aux_table.default_sort,
            caption: normalized.aux_table.caption || normalized.aux_table.title || null,
          };
          buildTable(aux, auxColumns, normalized.aux_table.rows, auxOptions);
        } else {
          aux.innerHTML = "";
          aux.classList.add("hidden");
        }
      } else {
        aux.innerHTML = "";
        aux.classList.add("hidden");
      }
    }
  }

  function renderStat(key) {
    if (!key) {
      return;
    }

    const practiceData = state.practiceSections[key];
    if (practiceData && hasPracticeSections(practiceData)) {
      const sectionCount = (Array.isArray(practiceData.main) ? practiceData.main.length : 0)
        + (Array.isArray(practiceData.aux) ? practiceData.aux.length : 0);
      console.info("[Leaderboards] Rendering practice stat", { statKey: key, sections: sectionCount });
      renderPracticeSections(practiceData);
      return;
    }

    const payload = state.snapshots[key];
    if (payload) {
      const rowCount = Array.isArray(payload.rows) ? payload.rows.length : 0;
      console.info("[Leaderboards] Rendering stat", { statKey: key, rows: rowCount });
      renderPayload(payload);
      return;
    }

    const main = qs("#leaderboard-main");
    const aux = qs("#leaderboard-aux");
    if (main) {
      main.innerHTML = "<div class='text-sm text-gray-500'>No data</div>";
    }
    if (aux) {
      aux.innerHTML = "";
      aux.classList.add("hidden");
    }
  }

  async function fetchAllSnapshots() {
    const url = `/admin/api/leaderboards/${state.seasonId}/all`;
    const res = await fetch(url, { credentials: "include" });
    if (!res.ok) throw new Error(`Snapshots fetch failed: ${res.status}`);
    const data = await res.json();
    // Expect { leaderboards: { [statKey]: payload }, filters_manifest?, columns_manifest? }
    const payloads = data.leaderboards || {};
    Object.keys(payloads).forEach((key) => {
      payloads[key] = normalizeTablePayload(payloads[key]);
    });
    state.snapshots = payloads;
    window.LEADERBOARDS = state.snapshots;
    state.missingKeys = Array.isArray(data.missing) ? data.missing : [];
    const practicePayloads = data.practice_sections || {};
    Object.keys(practicePayloads).forEach((key) => {
      const normalized = normalizePracticeSectionsPayload(practicePayloads[key]);
      if (normalized) {
        state.practiceSections[key] = normalized;
      }
    });
    window.LEADERBOARD_PRACTICE_SECTIONS = state.practiceSections;
    console.info("[Leaderboards] Loaded snapshots", {
      keys: Object.keys(payloads),
      missing: state.missingKeys,
    });
    state.missingKeys.forEach((key) => {
      console.info("[Leaderboards] Missing snapshot", key);
    });
    return state.snapshots;
  }

  function onStatChange(e) {
    const key = e.target.value;
    state.currentKey = key;
    renderStat(key);
  }

  function hydrateRawOnCells() {
    // Optional: if you want to store raw values on cells for faster sort; can be added when rendering
  }

  function init() {
    const root = qs("#leaderboard-root");
    if (!root) return;
    state.seasonId = root.dataset.seasonId;
    const statSelect = qs("#stat-select");
    if (!statSelect) return;

    // Initial payload from server-render (so first paint is instant)
    const initialKey = statSelect.value;
    state.currentKey = initialKey;
    try {
      const initialPayloadScript = qs("#initial-leaderboard-payload");
      if (initialPayloadScript) {
        const initialPayload = JSON.parse(initialPayloadScript.textContent || "{}");
        if (initialPayload && initialKey) {
          state.snapshots[initialKey] = normalizeTablePayload(initialPayload);
          const initial = state.snapshots[initialKey];
          const rowCount = initial && Array.isArray(initial.rows) ? initial.rows.length : 0;
          console.info("[Leaderboards] Initial payload loaded", {
            statKey: initialKey,
            rows: rowCount,
          });
        }
      }
    } catch (e) {
      console.warn("Failed to read initial payload", e);
    }

    try {
      const practiceScript = qs("#initial-practice-sections");
      if (practiceScript) {
        const practicePayload = JSON.parse(practiceScript.textContent || "null");
        if (practicePayload && initialKey) {
          const normalizedPractice = normalizePracticeSectionsPayload(practicePayload);
          if (normalizedPractice) {
            state.practiceSections[initialKey] = normalizedPractice;
            const sectionCount = (Array.isArray(normalizedPractice.main) ? normalizedPractice.main.length : 0)
              + (Array.isArray(normalizedPractice.aux) ? normalizedPractice.aux.length : 0);
            console.info("[Leaderboards] Initial practice sections loaded", {
              statKey: initialKey,
              sections: sectionCount,
            });
          }
        }
      }
    } catch (err) {
      console.warn("Failed to read initial practice sections", err);
    }

    window.LEADERBOARDS = state.snapshots;
    window.LEADERBOARD_PRACTICE_SECTIONS = state.practiceSections;

    // Disable any inline form submits
    const form = statSelect.closest("form");
    if (form) {
      form.addEventListener("submit", (ev) => ev.preventDefault());
    }
    statSelect.addEventListener("change", onStatChange);

    // First paint using whichever payload is available
    renderStat(initialKey);

    // Fetch all snapshots once, then stat switches are instant
    fetchAllSnapshots()
      .then((snapshots) => {
        const keys = Object.keys(snapshots || {});
        const missing = Array.isArray(state.missingKeys) ? state.missingKeys : [];
        console.info("[Leaderboards] /all fetch completed", { keys, missing });
        missing.forEach((key) => {
          console.info("[Leaderboards] Missing snapshot", key);
        });
        // If user has already changed stat before fetch completed, honor current selection
        const key = qs("#stat-select")?.value || initialKey;
        renderStat(key);
      })
      .catch(err => {
        console.error("All-snapshots fetch failed", err);
        // Fallback: keep initial payload; user can still switch once they exist
      });

    state.initialized = true;
  }

  document.addEventListener("DOMContentLoaded", init);
})();
