// Content script: injects a "Take Notes" button into the YouTube player toolbar.
// YouTube is a single-page app, so we re-check on its navigation events and when the player re-renders.
(() => {
  const BTN_CLASS = "vnt-take-notes";
  const ICON = `<svg viewBox="0 0 36 36" width="100%" height="100%" aria-hidden="true">
    <path fill="#fff" d="M11 9h11l5 5v13a1 1 0 0 1-1 1H11a1 1 0 0 1-1-1V10a1 1 0 0 1 1-1zm10 1.5V15h4.5L21 10.5z
      M13 18h10v1.6H13zm0 3.4h10V23H13zm0-6.8h6v1.6h-6z"/></svg>`;

  function currentVideoId() {
    if (location.pathname !== "/watch") return null;
    const id = new URLSearchParams(location.search).get("v");
    return id && /^[A-Za-z0-9_-]{11}$/.test(id) ? id : null;
  }

  function toast(text, kind = "info") {
    const player = document.querySelector("#movie_player") || document.body;
    let el = player.querySelector(".vnt-toast");
    if (!el) {
      el = document.createElement("div");
      el.className = "vnt-toast";
      player.appendChild(el);
    }
    el.textContent = text;
    el.dataset.kind = kind;
    el.classList.add("vnt-show");
    clearTimeout(el._t);
    el._t = setTimeout(() => el.classList.remove("vnt-show"), 4500);
  }

  let lastClick = { id: null, at: 0 };

  async function onClick(ev) {
    ev.preventDefault();
    ev.stopPropagation();
    const videoId = currentVideoId(); // read at click time, never cached (SPA navigation)
    if (!videoId) return;
    if (lastClick.id === videoId && Date.now() - lastClick.at < 5000) {
      toast("Already submitted. Open the extension to track it.");
      return;
    }
    lastClick = { id: videoId, at: Date.now() };
    const player = document.querySelector("#movie_player");
    const video = document.querySelector("video.html5-main-video");
    const adShowing = player?.classList.contains("ad-showing");
    const durationSec = !adShowing && video && Number.isFinite(video.duration) ? video.duration : null;
    if (durationSec && durationSec > 20 * 60) {
      toast("This video is longer than 20 minutes, which isn't supported.", "error");
      return;
    }
    const title = (document.title || "").replace(/^\(\d+\)\s*/, "").replace(/ - YouTube$/, "").trim();
    const btn = ev.currentTarget;
    btn.classList.add("vnt-busy");
    try {
      const resp = await chrome.runtime.sendMessage({
        type: "SUBMIT_JOB", videoId, url: `https://www.youtube.com/watch?v=${videoId}`, title, durationSec,
      });
      if (resp?.ok) toast("✓ Queued! Click the Video Notes icon to follow progress.", "ok");
      else {
        lastClick = { id: null, at: 0 };
        toast(resp?.error?.message || "Couldn't submit this video.", "error");
      }
    } catch (e) {
      lastClick = { id: null, at: 0 };
      toast("The extension was updated. Reload this page and try again.", "error");
    } finally {
      btn.classList.remove("vnt-busy");
    }
  }

  function ensureButton() {
    const existing = document.querySelectorAll(`.${BTN_CLASS}`);
    if (!currentVideoId()) {
      existing.forEach((b) => b.remove());
      return;
    }
    const controls = document.querySelector("#movie_player .ytp-right-controls");
    if (!controls) return;
    if (controls.querySelector(`.${BTN_CLASS}`)) return;
    existing.forEach((b) => b.remove());
    const btn = document.createElement("button");
    btn.className = `ytp-button ${BTN_CLASS}`;
    btn.title = "Take Notes";
    btn.setAttribute("aria-label", "Take Notes");
    btn.setAttribute("data-tooltip-title", "Take Notes");
    btn.innerHTML = ICON;
    btn.addEventListener("click", onClick);
    // Newer players nest buttons inside .ytp-right-controls-left/-right, so insert next to the settings
    // button within ITS parent; fall back to the start of the toolbar.
    const settingsBtn = controls.querySelector(".ytp-settings-button");
    if (settingsBtn?.parentNode) settingsBtn.parentNode.insertBefore(btn, settingsBtn);
    else {
      const group = controls.querySelector(".ytp-right-controls-left") || controls;
      group.insertBefore(btn, group.firstChild);
    }
  }

  let scheduled = false;
  function schedule() {
    if (scheduled) return;
    scheduled = true;
    requestAnimationFrame(() => { scheduled = false; ensureButton(); });
  }

  document.addEventListener("yt-navigate-finish", schedule);
  document.addEventListener("yt-page-data-updated", schedule);
  window.addEventListener("popstate", schedule);
  new MutationObserver(schedule).observe(document.documentElement, { childList: true, subtree: true });
  schedule();
})();
