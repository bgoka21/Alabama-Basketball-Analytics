(function () {
  const container = document.querySelector("[data-cache-endpoint-template]");
  if (!container) return;

  const btn = document.querySelector("#rebuild-cache-btn");
  const seasonSelect = document.querySelector("#season-select");
  const progressWrap = document.getElementById("cache-progress");
  const bar = document.getElementById("cache-progress-bar");
  const text = document.getElementById("cache-progress-text");
  const msg = document.getElementById("cache-progress-msg");

  let pollTimer = null;

  function setBar(pct, message) {
    const percent = Math.max(0, Math.min(100, Math.round(pct || 0)));
    if (bar) bar.style.width = `${percent}%`;
    if (text) text.textContent = `${percent}%`;
    if (msg) msg.textContent = message || "";
  }

  async function pollStatus(url) {
    try {
      const res = await fetch(url, { credentials: "same-origin" });
      if (!res.ok) throw new Error(`Status ${res.status}`);
      const data = await res.json();
      setBar(data.percent || 0, data.message || "");
      if (data.done || data.error) {
        clearInterval(pollTimer);
        pollTimer = null;
        if (data.error) {
          console.error("Rebuild error:", data.error);
          bar?.classList.add("bg-red-500");
          if (msg) msg.textContent = `Error: ${data.error}`;
        }
      }
    } catch (e) {
      console.error("Poll failed", e);
    }
  }

  async function startRebuild() {
    const seasonId = seasonSelect?.value || container.dataset.defaultSeason || "1";
    const startUrl = container.dataset.cacheEndpointTemplate.replace("__SEASON__", seasonId);
    const statusUrl = container.dataset.cacheStatusTemplate.replace("__SEASON__", seasonId);

    if (progressWrap) progressWrap.classList.remove("hidden");
    bar?.classList.remove("bg-red-500");
    setBar(0, "Queued rebuild");

    try {
      await fetch(startUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: { "X-Requested-With": "XMLHttpRequest" },
      });
    } catch (e) {
      setBar(0, "Failed to queue");
      return;
    }

    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(() => pollStatus(statusUrl), 1000);
    await pollStatus(statusUrl);
  }

  if (btn) btn.addEventListener("click", startRebuild);
})();
