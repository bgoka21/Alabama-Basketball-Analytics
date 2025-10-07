(function () {
  const root = document.getElementById("cache-controls");
  if (!root) return;

  const btn = document.getElementById("rebuild-cache-btn");
  const seasonSelect = document.getElementById("season-select");
  const progressWrap = document.getElementById("cache-progress");
  const bar = document.getElementById("cache-progress-bar");
  const text = document.getElementById("cache-progress-text");
  const msg = document.getElementById("cache-progress-msg");

  let pollTimer = null;

  function getNumericSeasonId() {
    // 1) prefer the select's numeric value
    let v = seasonSelect && seasonSelect.value ? seasonSelect.value : "";
    let n = parseInt(v, 10);
    if (Number.isFinite(n)) return n;

    // 2) fall back to current-season-id from HTML
    const fallback = parseInt(root.dataset.currentSeasonId || "", 10);
    if (Number.isFinite(fallback)) return fallback;

    // 3) last resort: first numeric option
    if (seasonSelect) {
      for (const opt of seasonSelect.options) {
        const n2 = parseInt(opt.value, 10);
        if (Number.isFinite(n2)) return n2;
      }
    }
    return NaN;
  }

  function makeUrls(seasonId) {
    const startTpl = root.dataset.cacheEndpointTemplate;
    const statusTpl = root.dataset.cacheStatusTemplate;
    if (!startTpl || !statusTpl) throw new Error("Missing data-* templates on #cache-controls");
    return {
      startUrl: startTpl.replace("__SEASON__", String(seasonId)),
      statusUrl: statusTpl.replace("__SEASON__", String(seasonId)),
    };
  }

  function setBar(pct, message) {
    const clamped = Math.max(0, Math.min(100, pct | 0));
    if (bar) bar.style.width = `${clamped}%`;
    if (text) text.textContent = `${clamped}%`;
    if (msg) msg.textContent = message || "";
  }

  async function pollStatus(statusUrl) {
    try {
      const res = await fetch(statusUrl, { credentials: "same-origin" });
      if (!res.ok) throw new Error(`Status ${res.status}`);
      const data = await res.json();
      setBar(data.percent || 0, data.message || "");
      if (data.done || data.error) {
        if (pollTimer) {
          clearInterval(pollTimer);
          pollTimer = null;
        }
        if (data.error && bar) {
          bar.classList.add("bg-red-500");
          msg && (msg.textContent = `Error: ${data.error}`);
        } else if (bar) {
          bar.classList.remove("bg-red-500");
          bar.classList.add("bg-green-500");
        }
      }
    } catch (e) {
      console.error("pollStatus failed:", e);
    }
  }

  async function startRebuild() {
    const seasonId = getNumericSeasonId();
    if (!Number.isFinite(seasonId)) {
      setBar(0, "Please select a season");
      return;
    }
    const { startUrl, statusUrl } = makeUrls(seasonId);

    setBar(0, "Queued rebuild");
    try {
      const res = await fetch(startUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: { "X-Requested-With": "XMLHttpRequest" },
      });
      if (!res.ok) throw new Error(`Start ${res.status}`);
    } catch (e) {
      console.error("startRebuild failed:", e);
      setBar(0, "Failed to queue");
      return;
    }

    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(() => pollStatus(statusUrl), 1000);
    await pollStatus(statusUrl);
  }

  // Resume polling if a job is already running when you load the page
  (async function resumeIfActive() {
    const seasonId = getNumericSeasonId();
    if (!Number.isFinite(seasonId)) return;
    const { statusUrl } = makeUrls(seasonId);
    try {
      const res = await fetch(statusUrl, { credentials: "same-origin" });
      if (!res.ok) return;
      const data = await res.json();
      if (data && data.percent > 0 && !data.done) {
        // show live progress
        pollTimer = setInterval(() => pollStatus(statusUrl), 1000);
        await pollStatus(statusUrl);
      } else {
        setBar(data.percent || 0, data.message || "Idle");
      }
    } catch {}
  })();

  btn && btn.addEventListener("click", startRebuild);
})();
