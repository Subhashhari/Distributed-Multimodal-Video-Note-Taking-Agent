// Gateway + Notion HTTP helpers (used by the background service worker and options page).

export class ApiError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

async function request(base, path, { method = "GET", body, headers = {} } = {}) {
  let resp;
  try {
    resp = await fetch(`${base}${path}`, {
      method,
      headers: { ...(body ? { "Content-Type": "application/json" } : {}), ...headers },
      body: body ? JSON.stringify(body) : undefined,
    });
  } catch (e) {
    throw new ApiError(0, "GATEWAY_UNREACHABLE", `Can't reach the notes server at ${base}. Is it running?`);
  }
  let data = null;
  try { data = await resp.json(); } catch { /* non-JSON */ }
  if (!resp.ok) {
    const err = data?.error || {};
    throw new ApiError(resp.status, err.code || `HTTP_${resp.status}`, err.message || `Server error (${resp.status}).`);
  }
  return data;
}

export const gateway = {
  config: (base) => request(base, "/config/public"),
  health: (base) => request(base, "/healthz"),
  submit: (base, body, idemKey) =>
    request(base, "/jobs", { method: "POST", body, headers: { "Idempotency-Key": idemKey } }),
  status: (base, jobId) => request(base, `/jobs/${jobId}`),
  results: (base, jobId) => request(base, `/jobs/${jobId}/results`),
  exchange: (base, code, redirectUri) =>
    request(base, "/oauth/notion/exchange", { method: "POST", body: { code, redirect_uri: redirectUri } }),
};

export async function notionSearchPages(token) {
  const resp = await fetch("https://api.notion.com/v1/search", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Notion-Version": "2022-06-28",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      filter: { property: "object", value: "page" },
      sort: { direction: "descending", timestamp: "last_edited_time" },
      page_size: 50,
    }),
  });
  if (resp.status === 401) throw new ApiError(401, "NOTION_UNAUTHORIZED", "Notion token is invalid or revoked.");
  if (!resp.ok) throw new ApiError(resp.status, "NOTION_ERROR", `Notion error (${resp.status}).`);
  const data = await resp.json();
  return (data.results || []).map((p) => ({
    id: p.id,
    title: pageTitle(p) || "Untitled",
    topLevel: p.parent?.type === "workspace",
  }));
}

function pageTitle(page) {
  for (const prop of Object.values(page.properties || {})) {
    if (prop.type === "title") return (prop.title || []).map((t) => t.plain_text).join("");
  }
  return "";
}
