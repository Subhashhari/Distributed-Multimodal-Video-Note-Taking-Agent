import { STEPS, TERMINAL, errorText, failedStepIndex, mmss, retryText, statusLine, stepIndex, warningLines }
  from "../lib/status.js";

const $ = (id) => document.getElementById(id);
let state = null;

function send(type, extra = {}) {
  return chrome.runtime.sendMessage({ type, ...extra });
}

function kindOf(job) {
  switch (job.status) {
    case "COMPLETED": return "ok";
    case "COMPLETED_WITH_WARNINGS": return "warn";
    case "FAILED": return "fail";
    case "DEAD_LETTERED": return "dead";
    default: return "active";
  }
}

function renderSteps(ol, job) {
  ol.innerHTML = "";
  const failed = job.status === "FAILED" || job.status === "DEAD_LETTERED";
  const cur = failed ? failedStepIndex(job) : stepIndex(job.status);
  const waiting = job.retry?.waiting;
  STEPS.forEach((s, i) => {
    const li = document.createElement("li");
    li.textContent = s.label;
    if (failed) {
      if (i < cur) li.className = "done";
      else if (i === cur) li.className = "failed";
    } else if (i < cur || (i === cur && cur === STEPS.length - 1)) li.className = "done";
    else if (i === cur) li.className = waiting ? "active waiting" : "active";
    ol.appendChild(li);
  });
}

function button(label, cls, onClick) {
  const b = document.createElement(cls === "link" ? "button" : "button");
  b.className = `btn ${cls || ""}`;
  b.textContent = label;
  b.addEventListener("click", onClick);
  return b;
}

function renderJob(job) {
  const el = $("jobTpl").content.firstElementChild.cloneNode(true);
  const kind = kindOf(job);
  el.dataset.kind = kind;
  el.querySelector(".thumb").src = `https://i.ytimg.com/vi/${job.video_id}/mqdefault.jpg`;
  el.querySelector(".title").textContent = job.video_title || `YouTube video ${job.video_id}`;
  el.querySelector(".title").title = job.video_title || "";
  el.querySelector(".sub").textContent = job.duration_sec ? `${mmss(job.duration_sec)} · ${job.video_id}` : job.video_id;
  el.querySelector(".x").addEventListener("click", () => send("DISMISS_JOB", { jobId: job.job_id }));
  renderSteps(el.querySelector(".steps"), job);
  const pct = kind === "active" ? job.progress || 0 : 100;
  el.querySelector(".fill").style.width = `${pct}%`;

  const line = el.querySelector(".line");
  const waitMsg = retryText(job);
  if (job.gateway_error && kind === "active") {
    line.textContent = errorText(job.gateway_error);
    line.classList.add("waiting");
  } else if (waitMsg && kind === "active") {
    line.textContent = `⏳ ${waitMsg}`;
    line.classList.add("waiting");
  } else if (kind === "fail" || kind === "dead") {
    line.textContent = `✗ ${statusLine(job)}: ${errorText(job.error)}`;
  } else if (kind === "ok") {
    line.textContent = `✓ ${statusLine(job)}`;
  } else if (kind === "warn") {
    line.textContent = `⚠ ${statusLine(job)}`;
  } else {
    line.textContent = `${statusLine(job)} ${job.progress ? `${job.progress}%` : ""}`;
  }

  const warns = el.querySelector(".warns");
  if (kind === "warn" || kind === "ok") {
    for (const w of warningLines(job)) {
      const li = document.createElement("li");
      li.textContent = w;
      warns.appendChild(li);
    }
  }

  const actions = el.querySelector(".actions");
  const url = job.result?.notion_page_url;
  if (url && (kind === "ok" || kind === "warn")) {
    const a = document.createElement("a");
    a.className = "btn primary";
    a.href = url;
    a.target = "_blank";
    a.rel = "noopener";
    a.textContent = "View in Notion ↗";
    actions.appendChild(a);
  }
  if (kind === "fail" || kind === "dead") {
    actions.appendChild(button("Try again", "", () => send("RETRY_JOB", { jobId: job.job_id })));
    if (job.error?.code === "NOTION_UNAUTHORIZED") {
      actions.appendChild(button("Reconnect Notion", "", () => send("CONNECT_NOTION")));
    }
  }
  return el;
}

function render() {
  if (!state) return;
  const conn = $("conn");
  conn.innerHTML = "";
  const dot = document.createElement("span");
  dot.className = state.connected ? "dot" : "dot off";
  const label = document.createElement("span");
  label.textContent = state.connected
    ? `${state.workspaceName || "Notion"}${state.destination ? ` → ${state.destination}` : ""}`
    : "Not connected";
  conn.append(dot, label);

  const list = $("list");
  list.innerHTML = "";
  state.jobs.forEach((j) => list.appendChild(renderJob(j)));
  $("empty").hidden = !(state.connected && state.jobs.length === 0);
  $("connect").hidden = state.connected || state.jobs.length > 0;
  $("clearBtn").hidden = !state.jobs.some((j) => TERMINAL.has(j.status));
}

function showBanner(text) {
  const b = $("banner");
  b.textContent = text || "";
  b.hidden = !text;
}

async function init() {
  const port = chrome.runtime.connect({ name: "popup" });
  port.onMessage.addListener((msg) => {
    if (msg.type === "STATE") { state = msg.state; render(); }
  });
  const resp = await send("GET_STATE");
  if (resp?.ok) { state = resp.data; render(); }
  setInterval(render, 1000); // keep "retrying in Ns" countdowns fresh

  $("connectBtn").addEventListener("click", async () => {
    showBanner("");
    const r = await send("CONNECT_NOTION");
    if (!r?.ok) showBanner(r?.error?.message || "Couldn't connect to Notion.");
    const s = await send("GET_STATE");
    if (s?.ok) { state = s.data; render(); }
  });
  $("clearBtn").addEventListener("click", () => send("CLEAR_FINISHED"));
  for (const id of ["optionsLink", "optionsLink2"]) {
    $(id).addEventListener("click", (e) => { e.preventDefault(); chrome.runtime.openOptionsPage(); });
  }
}

init();
