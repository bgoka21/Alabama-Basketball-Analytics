// static/js/leaderboard-controller.js
(function () {
  const state = {
    seasonId: null,
    snapshots: {}, // statKey -> payload
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

  function normalizeTablePayload(payload) {
    if (!payload || typeof payload !== "object") {
      return payload;
    }

    const columns = Array.isArray(payload.columns) ? payload.columns : [];
    if (!Array.isArray(payload.columns_manifest) || !payload.columns_manifest.length) {
      payload.columns_manifest = columns
        .map(col => ({
          key: col && col.key ? col.key : (col && col.slug ? col.slug : ""),
          label: col && col.label ? col.label : (col && col.key ? col.key : ""),
        }))
        .filter(col => col.key);
    }

    const manifest = Array.isArray(payload.columns_manifest) ? payload.columns_manifest : [];
    const valueKeyMap = new Map();
    columns.forEach(col => {
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
      manifest.forEach((col, index) => {
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

    if (payload.aux_table) {
      payload.aux_table = normalizeTablePayload(payload.aux_table);
    }

    return payload;
  }

  function buildTable(container, columnsManifest, rows) {
    container.innerHTML = "";
    const table = document.createElement("table");
    table.className = "table table-sm w-full";
    const thead = document.createElement("thead");
    const tr = document.createElement("tr");

    columnsManifest.forEach(col => {
      const th = document.createElement("th");
      th.textContent = col.label || col.key;
      th.dataset.key = col.key;
      th.className = "whitespace-nowrap";
      // Click to sort
      th.addEventListener("click", () => sortBy(table, col.key));
      tr.appendChild(th);
    });

    thead.appendChild(tr);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    rows.forEach(r => {
      const tr = document.createElement("tr");
      columnsManifest.forEach((col, idx) => {
        const td = document.createElement("td");
        const metric = r.metrics && r.metrics[col.key];
        if (metric && typeof metric.text !== "undefined") {
          td.textContent = metric.text;
          if (metric.raw !== undefined && metric.raw !== null) {
            td.dataset.raw = String(metric.raw);
          } else {
            delete td.dataset.raw;
          }
        } else if (metric && typeof metric.raw !== "undefined") {
          td.textContent = String(metric.raw);
          td.dataset.raw = String(metric.raw);
        } else {
          // Fallback to common display fields
          const displayValue = (r.display && r.display[col.key]) || "";
          td.textContent = displayValue;
          const rawFallback = parseMaybeNumber(displayValue);
          if (rawFallback !== null) {
            td.dataset.raw = String(rawFallback);
          } else {
            delete td.dataset.raw;
          }
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    container.appendChild(table);
  }

  function sortBy(table, key) {
    const thead = table.querySelector("thead");
    const headers = qsa("th", thead);
    const idx = headers.findIndex(h => h.dataset.key === key);
    if (idx === -1) return;

    const tbody = table.querySelector("tbody");
    const rows = qsa("tr", tbody);
    // Detect current direction
    const header = headers[idx];
    const dir = header.dataset.sortDir === "asc" ? "desc" : "asc";
    headers.forEach(h => { delete h.dataset.sortDir; });
    header.dataset.sortDir = dir;

    // We need raw values for sorting; store them on cells on render if available
    rows.sort((a, b) => {
      const A = a.children[idx].dataset.raw ?? a.children[idx].textContent;
      const B = b.children[idx].dataset.raw ?? b.children[idx].textContent;
      const aNum = Number(A); const bNum = Number(B);
      const bothNums = !Number.isNaN(aNum) && !Number.isNaN(bNum);
      if (bothNums) return dir === "asc" ? aNum - bNum : bNum - aNum;
      return dir === "asc" ? String(A).localeCompare(String(B)) : String(B).localeCompare(String(A));
    });

    // Re-attach
    rows.forEach(r => tbody.appendChild(r));
  }

  function renderPayload(payload) {
    const normalized = normalizeTablePayload(payload);
    const main = qs("#leaderboard-main");
    const aux = qs("#leaderboard-aux");
    if (!main) return;

    // Single table
    if (normalized && normalized.columns_manifest && normalized.rows) {
      buildTable(main, normalized.columns_manifest, normalized.rows);
    } else {
      main.innerHTML = "<div class='text-sm text-gray-500'>No data</div>";
    }

    // Dual table support (optional)
    if (aux) {
      if (normalized && normalized.aux_table && normalized.aux_table.columns_manifest && normalized.aux_table.rows) {
        aux.classList.remove("hidden");
        buildTable(aux, normalized.aux_table.columns_manifest, normalized.aux_table.rows);
      } else {
        aux.innerHTML = "";
        aux.classList.add("hidden");
      }
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
    return state.snapshots;
  }

  function onStatChange(e) {
    const key = e.target.value;
    state.currentKey = key;
    const payload = state.snapshots[key];
    if (!payload) {
      console.warn("No snapshot for key", key);
      return;
    }
    renderPayload(payload);
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
    try {
      const initialPayloadScript = qs("#initial-leaderboard-payload");
      if (initialPayloadScript) {
        const initialPayload = JSON.parse(initialPayloadScript.textContent || "{}");
        if (initialPayload && initialKey) {
          state.snapshots[initialKey] = normalizeTablePayload(initialPayload);
        }
      }
    } catch (e) {
      console.warn("Failed to read initial payload", e);
    }

    window.LEADERBOARDS = state.snapshots;

    // Disable any inline form submits
    const form = statSelect.closest("form");
    if (form) {
      form.addEventListener("submit", (ev) => ev.preventDefault());
    }
    statSelect.addEventListener("change", onStatChange);

    // First paint if initial payload exists
    if (state.snapshots[initialKey]) {
      renderPayload(state.snapshots[initialKey]);
    }

    // Fetch all snapshots once, then stat switches are instant
    fetchAllSnapshots()
      .then(() => {
        // If user has already changed stat before fetch completed, honor current selection
        const key = qs("#stat-select")?.value || initialKey;
        if (state.snapshots[key]) {
          renderPayload(state.snapshots[key]);
        }
      })
      .catch(err => {
        console.error("All-snapshots fetch failed", err);
        // Fallback: keep initial payload; user can still switch once they exist
      });

    state.initialized = true;
  }

  document.addEventListener("DOMContentLoaded", init);
})();
