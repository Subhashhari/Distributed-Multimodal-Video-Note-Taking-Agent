// Turns API statuses / error codes into human-readable text. The UI never shows raw JSON.

export const STEPS = [
  { key: "QUEUED", label: "Queued" },
  { key: "TRANSCRIBING", label: "Transcribing" },
  { key: "SUMMARIZING", label: "Summarizing" },
  { key: "EXTRACTING_FRAMES", label: "Extracting Frames" },
  { key: "PUBLISHING", label: "Publishing" },
  { key: "DONE", label: "Done" },
];

export const TERMINAL = new Set(["COMPLETED", "COMPLETED_WITH_WARNINGS", "FAILED", "DEAD_LETTERED"]);

const ACTIVE_TEXT = {
  QUEUED: "Queued, waiting for a free worker…",
  TRANSCRIBING: "Fetching the transcript…",
  SUMMARIZING: "Writing your notes…",
  EXTRACTING_FRAMES: "Capturing key frames from the video…",
  PUBLISHING: "Publishing to Notion…",
};

const API_NAMES = { llm: "The AI service", notion: "Notion", youtube: "YouTube" };

const ERRORS = {
  VIDEO_TOO_LONG: "This video is longer than 20 minutes, which isn't supported.",
  LIVE_STREAM_UNSUPPORTED: "Live streams and premieres aren't supported.",
  VIDEO_UNAVAILABLE: "This video is private, removed, or region-locked.",
  TRANSCRIPT_UNAVAILABLE: "This video has no captions we could use.",
  YOUTUBE_BOT_CHECK: "YouTube blocked the request (bot check). Try again later.",
  YOUTUBE_BLOCKED: "YouTube is blocking requests from the server right now.",
  NOTION_UNAUTHORIZED: "Notion access was revoked or expired. Reconnect Notion.",
  NOTION_FORBIDDEN: "The Notion integration can't write to that page. Share the page with it.",
  NOTION_PARENT_NOT_FOUND: "The Notion page wasn't found. Share it with the integration in Notion.",
  NOTION_NO_DESTINATION: "No Notion page is shared with the integration. Pick one in Options.",
  LLM_NOT_CONFIGURED: "The server's AI model isn't configured (LLM_API_KEY / LLM_MODEL).",
  LLM_AUTH_FAILED: "The server's AI API key was rejected.",
  LLM_INVALID_OUTPUT: "The AI model returned unusable output. Try again.",
  LLM_DAILY_QUOTA_EXHAUSTED: "The AI service's daily free quota is used up. Try again tomorrow.",
  LLM_DAILY_QUOTA: "The AI service's daily free quota is used up. Try again tomorrow.",
  RATE_LIMIT_BUDGET_EXHAUSTED: "An external service kept rate-limiting us. Try again in a few minutes.",
  RETRY_BUDGET_EXHAUSTED: "Something kept failing, so we stopped retrying.",
  CRASH_LOOP: "This video repeatedly crashed the workers, so it was set aside for inspection.",
  ENQUEUE_FAILED: "The server couldn't queue the job. Try again.",
  INVALID_YOUTUBE_URL: "That doesn't look like a YouTube video link.",
  UNSUPPORTED_HOST: "Only YouTube videos are supported.",
  MISSING_NOTION_TOKEN: "Connect Notion first.",
  QUEUE_UNAVAILABLE: "The job queue is unavailable. Try again shortly.",
  GATEWAY_UNREACHABLE: "Can't reach the notes server. Is it running?",
  STAGE_TIMEOUT: "A step took too long.",
};

const WARNINGS = {
  FRAME_EXTRACTION_FAILED: "couldn't be captured",
  FRAME_BLANK: "was blank",
  FRAME_UPLOAD_FAILED: "couldn't be uploaded to Notion",
};

export function mmss(sec) {
  const s = Math.max(0, Math.floor(sec || 0));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60;
  const mm = String(m).padStart(h ? 2 : 2, "0"), ss = String(r).padStart(2, "0");
  return h ? `${h}:${mm}:${ss}` : `${mm}:${ss}`;
}

export function stepIndex(status) {
  if (status === "COMPLETED" || status === "COMPLETED_WITH_WARNINGS") return 5;
  return Math.max(0, STEPS.findIndex((s) => s.key === status));
}

export function failedStepIndex(job) {
  const stage = job.error?.stage;
  const map = { transcribe: 1, summarize: 2, frames: 3, publish: 4 };
  return stage in map ? map[stage] : 0;
}

export function errorText(err) {
  if (!err) return "Something went wrong.";
  return ERRORS[err.code] || err.message || `Something went wrong (${err.code}).`;
}

export function retryText(job, now = Date.now()) {
  const r = job.retry;
  if (!r || !r.waiting) return null;
  const secs = Math.max(0, Math.ceil((Date.parse(r.next_retry_at) - now) / 1000));
  const when = secs > 0 ? `retrying in ${secs}s` : "retrying now";
  if (r.reason === "RATE_LIMITED") {
    const who = API_NAMES[r.api] || "A service";
    return `${who} is busy, ${when} (attempt ${r.attempt} of ${r.max_attempts})`;
  }
  return `Hit a temporary problem, ${when} (attempt ${r.attempt} of ${r.max_attempts})`;
}

export function statusLine(job) {
  switch (job.status) {
    case "COMPLETED": return "Done";
    case "COMPLETED_WITH_WARNINGS": return "Done, with warnings";
    case "FAILED": return "Couldn't create notes";
    case "DEAD_LETTERED": return `Couldn't process after ${job.error?.attempts || "several"} attempts`;
    default: return ACTIVE_TEXT[job.status] || job.status;
  }
}

export function warningLines(job) {
  const ws = job.warnings || [];
  const frameWs = ws.filter((w) => w.timestamp_sec !== undefined && w.timestamp_sec !== null);
  const lines = [];
  if (frameWs.length) {
    const req = job.result?.frames_requested;
    const head = req ? `${frameWs.length} of ${req} frames` : `${frameWs.length} frame(s)`;
    const detail = frameWs.map((w) => `${mmss(w.timestamp_sec)} ${WARNINGS[w.code] || "was skipped"}`).join(", ");
    lines.push(`${head} couldn't be added (${detail}). The page was published without them.`);
  }
  for (const w of ws.filter((x) => x.timestamp_sec === undefined || x.timestamp_sec === null)) {
    lines.push(w.message || w.code);
  }
  return lines;
}
