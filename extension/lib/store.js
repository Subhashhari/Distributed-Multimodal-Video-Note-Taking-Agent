// chrome.storage.local wrapper. Tokens stay in local (not sync) storage.

export const DEFAULT_SETTINGS = {
  gatewayUrl: "http://localhost:8080",
  defaultParentId: "",
  defaultParentTitle: "",
  devFailFrames: "",
};

const MAX_JOBS = 20;

export async function getSettings() {
  const { settings } = await chrome.storage.local.get("settings");
  return { ...DEFAULT_SETTINGS, ...(settings || {}) };
}

export async function saveSettings(patch) {
  const next = { ...(await getSettings()), ...patch };
  next.gatewayUrl = (next.gatewayUrl || DEFAULT_SETTINGS.gatewayUrl).replace(/\/+$/, "");
  await chrome.storage.local.set({ settings: next });
  return next;
}

export async function getAuth() {
  const { auth } = await chrome.storage.local.get("auth");
  return auth || null;
}

export async function saveAuth(auth) {
  await chrome.storage.local.set({ auth });
}

export async function clearAuth() {
  await chrome.storage.local.remove("auth");
}

export async function getJobs() {
  const { jobs } = await chrome.storage.local.get("jobs");
  return jobs || [];
}

export async function saveJobs(jobs) {
  await chrome.storage.local.set({ jobs: jobs.slice(0, MAX_JOBS) });
}

export async function upsertJob(job) {
  const jobs = await getJobs();
  const i = jobs.findIndex((j) => j.job_id === job.job_id);
  if (i >= 0) jobs[i] = { ...jobs[i], ...job };
  else jobs.unshift(job);
  await saveJobs(jobs);
  return jobs;
}
