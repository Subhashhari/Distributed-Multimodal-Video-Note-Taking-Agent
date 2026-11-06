// Background service worker: Notion OAuth (once), job submission, status polling, badge, notifications.
import { ApiError, gateway, notionSearchPages } from "./lib/api.js";
import { TERMINAL } from "./lib/status.js";
import { clearAuth, getAuth, getJobs, getSettings, saveAuth, saveJobs, saveSettings, upsertJob } from "./lib/store.js";

const POPUP_POLL_MS = 1500;
const popupPorts = new Set();
let popupTimer = null;

// ------------------------------------------------------------------ Notion OAuth (one time)
async function connectNotion() {
  const { gatewayUrl } = await getSettings();
  const cfg = await gateway.config(gatewayUrl);
  if (!cfg.notion_oauth_client_id || cfg.notion_oauth_client_id.startsWith("__FILL_ME")) {
    throw new ApiError(0, "NOTION_OAUTH_NOT_CONFIGURED",
      "Notion sign-in isn't configured on the server. Use Options → paste an integration token instead.");
  }
  const redirectUri = chrome.identity.getRedirectURL("notion");
  const state = crypto.randomUUID();
  const url = new URL("https://api.notion.com/v1/oauth/authorize");
  url.search = new URLSearchParams({
    client_id: cfg.notion_oauth_client_id, response_type: "code", owner: "user",
    redirect_uri: redirectUri, state,
  }).toString();

  const finalUrl = await chrome.identity.launchWebAuthFlow({ url: url.toString(), interactive: true });
  const params = new URL(finalUrl).searchParams;
  if (params.get("error")) throw new ApiError(0, "NOTION_OAUTH_DENIED", "Notion sign-in was cancelled.");
  if (params.get("state") !== state) throw new ApiError(0, "NOTION_OAUTH_STATE", "Sign-in check failed, try again.");

  const tok = await gateway.exchange(gatewayUrl, params.get("code"), redirectUri);
  await saveAuth({
    accessToken: tok.access_token, refreshToken: tok.refresh_token || null,
    workspaceName: tok.workspace_name || "Notion", workspaceIcon: tok.workspace_icon || null,
    botId: tok.bot_id, mode: "oauth", connectedAt: Date.now(),
  });
  await pickDefaultParent(tok.access_token);
  return getAuth();
}

async function connectWithToken(token) {
  const pages = await notionSearchPages(token); // validates the token
  await saveAuth({ accessToken: token, workspaceName: "Notion (integration token)", mode: "internal",
                   connectedAt: Date.now() });
  await pickDefaultParent(token, pages);
  return getAuth();
}

async function pickDefaultParent(token, pages) {
  try {
    pages = pages || (await notionSearchPages(token));
    const settings = await getSettings();
    if (settings.defaultParentId && pages.some((p) => p.id === settings.defaultParentId)) return;
    const top = pages.find((p) => p.topLevel) || pages[0];
    if (top) await saveSettings({ defaultParentId: top.id, defaultParentTitle: top.title });
  } catch (e) {
    console.warn("Couldn't list Notion pages", e);
  }
}

// ------------------------------------------------------------------ jobs
async function submitJob({ videoId, url, title, durationSec }) {
  let auth = await getAuth();
  if (!auth?.accessToken) auth = await connectNotion();
  const settings = await getSettings();
  const body = {
    youtube_url: url || `https://www.youtube.com/watch?v=${videoId}`,
    notion_token: auth.accessToken,
    client_title: title || null,
    client_duration_sec: Number.isFinite(durationSec) && durationSec > 0 ? durationSec : null,
  };
  if (settings.defaultParentId) body.notion_parent_id = settings.defaultParentId;
  const failFrames = (settings.devFailFrames || "").split(",").map((s) => parseInt(s, 10)).filter(Number.isFinite);
  if (failFrames.length) body.debug = { fail_frames: failFrames };

  const res = await gateway.submit(settings.gatewayUrl, body, crypto.randomUUID());
  await upsertJob({
    job_id: res.job_id, video_id: res.video_id, status: res.status, progress: 0,
    video_title: title || null, submitted_at: Date.now(), warnings: [], error: null, retry: null, result: null,
  });
  await refreshBadge();
  ensureAlarm();
  startPopupPolling();
  return res;
}

async function pollJobs({ onlyActive = true } = {}) {
  const settings = await getSettings();
  const jobs = await getJobs();
  let changed = false;
  for (const job of jobs) {
    if (onlyActive && TERMINAL.has(job.status) && job.result_fetched) continue;
    try {
      const s = await gateway.status(settings.gatewayUrl, job.job_id);
      const wasTerminal = TERMINAL.has(job.status);
      Object.assign(job, s, { video_title: s.video_title || job.video_title, gateway_error: null });
      if (TERMINAL.has(s.status) && !job.result_fetched) {
        const r = await gateway.results(settings.gatewayUrl, job.job_id);
        Object.assign(job, { result: r.result, warnings: r.warnings, error: r.error || job.error,
                             result_fetched: true });
        if (!wasTerminal) notifyDone(job);
      }
      changed = true;
    } catch (e) {
      if (e.status === 404) { job.status = "FAILED"; job.error = { code: "JOB_NOT_FOUND", message: "The server no longer knows this job." }; job.result_fetched = true; }
      else job.gateway_error = { code: e.code, message: e.message };
      changed = true;
    }
  }
  if (changed) await saveJobs(jobs);
  await refreshBadge(jobs);
  broadcast();
  if (!jobs.some((j) => !TERMINAL.has(j.status))) chrome.alarms.clear("poll");
  return jobs;
}

function notifyDone(job) {
  const ok = job.status === "COMPLETED" || job.status === "COMPLETED_WITH_WARNINGS";
  const title = ok ? (job.status === "COMPLETED" ? "Notes ready" : "Notes ready (with warnings)")
                   : "Couldn't create notes";
  chrome.notifications.create(`job:${job.job_id}`, {
    type: "basic", iconUrl: "icons/128.png", title,
    message: job.video_title || job.video_id,
    buttons: ok && job.result?.notion_page_url ? [{ title: "View in Notion" }] : [],
    priority: 1,
  });
}

async function openJob(notificationId) {
  const jobId = notificationId.replace(/^job:/, "");
  const job = (await getJobs()).find((j) => j.job_id === jobId);
  if (job?.result?.notion_page_url) chrome.tabs.create({ url: job.result.notion_page_url });
}

async function refreshBadge(jobs) {
  jobs = jobs || (await getJobs());
  const active = jobs.filter((j) => !TERMINAL.has(j.status)).length;
  await chrome.action.setBadgeBackgroundColor({ color: "#2563eb" });
  await chrome.action.setBadgeText({ text: active ? String(active) : "" });
}

function ensureAlarm() {
  chrome.alarms.create("poll", { periodInMinutes: 0.5 });
}

// ------------------------------------------------------------------ popup live updates
async function broadcast() {
  if (!popupPorts.size) return;
  const state = await getState();
  for (const p of popupPorts) {
    try { p.postMessage({ type: "STATE", state }); } catch { popupPorts.delete(p); }
  }
}

function startPopupPolling() {
  if (popupTimer || !popupPorts.size) return;
  popupTimer = setInterval(async () => {
    if (!popupPorts.size) { clearInterval(popupTimer); popupTimer = null; return; }
    await pollJobs();
  }, POPUP_POLL_MS);
}

async function getState() {
  const [auth, jobs, settings] = await Promise.all([getAuth(), getJobs(), getSettings()]);
  return {
    connected: !!auth?.accessToken,
    workspaceName: auth?.workspaceName || null,
    destination: settings.defaultParentTitle || null,
    jobs,
  };
}

chrome.runtime.onConnect.addListener((port) => {
  if (port.name !== "popup") return;
  popupPorts.add(port);
  port.onDisconnect.addListener(() => popupPorts.delete(port));
  broadcast();
  pollJobs();
  startPopupPolling();
});

// ------------------------------------------------------------------ messages
const handlers = {
  SUBMIT_JOB: (msg) => submitJob(msg),
  GET_STATE: () => getState(),
  CONNECT_NOTION: () => connectNotion(),
  CONNECT_TOKEN: (msg) => connectWithToken(msg.token),
  DISCONNECT_NOTION: async () => { await clearAuth(); await saveSettings({ defaultParentId: "", defaultParentTitle: "" }); },
  LIST_PAGES: async () => notionSearchPages((await getAuth())?.accessToken || ""),
  REFRESH: () => pollJobs({ onlyActive: true }),
  DISMISS_JOB: async (msg) => { await saveJobs((await getJobs()).filter((j) => j.job_id !== msg.jobId)); await refreshBadge(); broadcast(); },
  CLEAR_FINISHED: async () => { await saveJobs((await getJobs()).filter((j) => !TERMINAL.has(j.status))); broadcast(); },
  RETRY_JOB: async (msg) => {
    const job = (await getJobs()).find((j) => j.job_id === msg.jobId);
    if (!job) return null;
    await saveJobs((await getJobs()).filter((j) => j.job_id !== msg.jobId));
    return submitJob({ videoId: job.video_id, title: job.video_title, durationSec: job.duration_sec });
  },
};

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  const fn = handlers[msg?.type];
  if (!fn) return false;
  Promise.resolve(fn(msg))
    .then((data) => sendResponse({ ok: true, data }))
    .catch((e) => {
      if (e?.code === "NOTION_UNAUTHORIZED" || e?.code === "MISSING_NOTION_TOKEN") clearAuth();
      sendResponse({ ok: false, error: { code: e?.code || "ERROR", message: e?.message || String(e) } });
    });
  return true; // async response
});

chrome.alarms.onAlarm.addListener((a) => { if (a.name === "poll") pollJobs(); });
chrome.notifications.onButtonClicked.addListener((id) => openJob(id));
chrome.notifications.onClicked.addListener((id) => openJob(id));
chrome.runtime.onStartup.addListener(() => { ensureAlarm(); refreshBadge(); });
chrome.runtime.onInstalled.addListener(() => { refreshBadge(); });
