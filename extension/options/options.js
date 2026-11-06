import { gateway } from "../lib/api.js";
import { getAuth, getSettings, saveSettings } from "../lib/store.js";

const $ = (id) => document.getElementById(id);

function msg(id, text, ok = true) {
  const el = $(id);
  el.textContent = text;
  el.className = `msg ${ok ? "ok" : "err"}`;
}

const send = (type, extra = {}) => chrome.runtime.sendMessage({ type, ...extra });

async function ensureHostPermission(url) {
  const u = new URL(url);
  if (["localhost", "127.0.0.1"].includes(u.hostname)) return true;
  return chrome.permissions.request({ origins: [`${u.protocol}//${u.host}/*`] });
}

async function renderNotion() {
  const auth = await getAuth();
  $("notionStatus").textContent = auth?.accessToken
    ? `Connected: ${auth.workspaceName}${auth.mode === "internal" ? "" : " (OAuth)"}`
    : "Not connected.";
  $("disconnect").hidden = !auth?.accessToken;
}

async function loadPages() {
  const settings = await getSettings();
  const sel = $("parent");
  sel.innerHTML = '<option value="">Server default</option>';
  const r = await send("LIST_PAGES");
  if (!r?.ok) { msg("notionMsg", r?.error?.message || "Couldn't list pages.", false); return; }
  for (const p of r.data) {
    const o = document.createElement("option");
    o.value = p.id;
    o.textContent = `${p.topLevel ? "" : "↳ "}${p.title}`;
    o.dataset.title = p.title;
    if (p.id === settings.defaultParentId) o.selected = true;
    sel.appendChild(o);
  }
  if (!r.data.length) msg("notionMsg", "No pages are shared with the integration yet.", false);
}

async function init() {
  const s = await getSettings();
  $("gateway").value = s.gatewayUrl;
  $("failFrames").value = s.devFailFrames || "";
  await renderNotion();
  if ((await getAuth())?.accessToken) loadPages();

  $("saveGateway").addEventListener("click", async () => {
    const url = $("gateway").value.trim() || "http://localhost:8080";
    try {
      if (!(await ensureHostPermission(url))) return msg("gatewayMsg", "Permission denied for that host.", false);
      const saved = await saveSettings({ gatewayUrl: url });
      const h = await gateway.health(saved.gatewayUrl);
      msg("gatewayMsg", `OK: ${h.workers?.alive ?? 0} worker(s) alive, mode ${h.pipeline_mode}.`);
    } catch (e) {
      msg("gatewayMsg", e.message || "Couldn't reach the server.", false);
    }
  });

  $("connect").addEventListener("click", async () => {
    const r = await send("CONNECT_NOTION");
    if (r?.ok) { msg("notionMsg", "Connected to Notion."); await renderNotion(); loadPages(); }
    else msg("notionMsg", r?.error?.message || "Couldn't connect.", false);
  });
  $("disconnect").addEventListener("click", async () => {
    await send("DISCONNECT_NOTION");
    await renderNotion();
    $("parent").innerHTML = '<option value="">Server default</option>';
    msg("notionMsg", "Disconnected.");
  });
  $("useToken").addEventListener("click", async () => {
    const token = $("token").value.trim();
    if (token.length < 20) return msg("notionMsg", "That token looks too short.", false);
    const r = await send("CONNECT_TOKEN", { token });
    if (r?.ok) { $("token").value = ""; msg("notionMsg", "Token saved."); await renderNotion(); loadPages(); }
    else msg("notionMsg", r?.error?.message || "Token was rejected by Notion.", false);
  });
  $("refreshPages").addEventListener("click", loadPages);
  $("saveParent").addEventListener("click", async () => {
    const opt = $("parent").selectedOptions[0];
    await saveSettings({ defaultParentId: opt?.value || "", defaultParentTitle: opt?.dataset.title || "" });
    msg("notionMsg", opt?.value ? `Notes will be published under "${opt.dataset.title}".` : "Using server default.");
  });
  $("saveDev").addEventListener("click", async () => {
    await saveSettings({ devFailFrames: $("failFrames").value.trim() });
    msg("devMsg", "Saved.");
  });
}

init();
