# Distributed Multimodal Video Note-Taking Agent — Implementation Plan

> **Status:** Planning only. No code yet.
> **Covers:** (Part A) Cloud "Project 2" specification, every requirement, (Part B) reuse of the same system for the **CSE 816 SPE Capstone** (end-to-end DevOps on local Kubernetes).
> **Date:** 06-Nov-2026

---

## Table of Contents

- [0. TL;DR](#0-tldr)
- [Part A — Cloud Project (Note-Taking Agent)](#part-a--cloud-project-note-taking-agent)
  - [1. Requirements Traceability Matrix](#1-requirements-traceability-matrix)
  - [2. Architecture](#2-architecture)
  - [3. Design Decisions ("Left to Your Design", §10)](#3-design-decisions-left-to-your-design-10)
  - [4. Data Model & State Store](#4-data-model--state-store)
  - [5. Job State Machine](#5-job-state-machine)
  - [6. Messaging Topology (RabbitMQ)](#6-messaging-topology-rabbitmq)
  - [7. API Gateway](#7-api-gateway)
  - [8. Worker & Pipeline Stages](#8-worker--pipeline-stages)
  - [9. Rate Limiting (Global, Cross-Worker)](#9-rate-limiting-global-cross-worker)
  - [10. Failure Handling (§6) — Detailed Design](#10-failure-handling-6--detailed-design)
  - [11. Idempotency & Exactly-Once *Effects*](#11-idempotency--exactly-once-effects)
  - [12. Browser Extension](#12-browser-extension)
  - [13. Hard Constraints A & B](#13-hard-constraints-a--b)
  - [14. Configuration / Environment Variables](#14-configuration--environment-variables)
  - [15. Docker Compose Deployment](#15-docker-compose-deployment)
  - [16. Observability](#16-observability)
  - [17. Security](#17-security)
  - [18. Testing Strategy](#18-testing-strategy)
  - [19. Demo Runbook (what the grader will do)](#19-demo-runbook-what-the-grader-will-do)
  - [20. Performance & Latency Budget](#20-performance--latency-budget)
  - [21. Repository Layout](#21-repository-layout)
  - [22. Build Phases & Milestones](#22-build-phases--milestones)
  - [23. Risks & Mitigations](#23-risks--mitigations)
- [Part B — Reusing This for the SPE Capstone](#part-b--reusing-this-for-the-spe-capstone)
  - [24. Can We Use This Project? (Answer)](#24-can-we-use-this-project-answer)
  - [25. SPE Rubric Mapping](#25-spe-rubric-mapping)
  - [26. DevOps Pipeline Design](#26-devops-pipeline-design)
  - [27. Kubernetes Design](#27-kubernetes-design)
  - [28. IaC / Configuration Management](#28-iac--configuration-management)
  - [29. Monitoring & Observability (SPE)](#29-monitoring--observability-spe)
  - [30. Cutting-Edge Extensions](#30-cutting-edge-extensions)
  - [31. Laptop Resource Budget](#31-laptop-resource-budget)
  - [32. Team-of-Two Work Split](#32-team-of-two-work-split)
  - [33. Combined Timeline (Both Courses)](#33-combined-timeline-both-courses)
  - [34. SPE Deliverables Checklist](#34-spe-deliverables-checklist)
  - [35. Draft SPE Abstract & Proposal Outline](#35-draft-spe-abstract--proposal-outline)
- [Appendix](#appendix)

---

## 0. TL;DR

| Decision | Choice | Why (one line) |
|---|---|---|
| Language | Python 3.12 for gateway and workers | Best ecosystem for yt-dlp, transcripts, ffmpeg, LLM SDKs |
| Gateway | FastAPI + Uvicorn | Async, Pydantic validation, OpenAPI for free |
| Broker | **RabbitMQ 4.x** (quorum queues) | Native manual acks, redelivery on connection loss, DLX, delivery-limit |
| State store | **PostgreSQL 16** | Durable job state, compare-and-set transitions, stage artifacts |
| Coordination | **Redis 7** | Global token-bucket rate limiters (Lua, atomic), worker heartbeats |
| Worker topology | **Generic workers with one message per stage** ("stage chaining") | Crash only re-runs the interrupted stage; any worker can do any stage; can still specialise via env |
| LLM | **Free by default:** Google Gemini API free tier (Flash model) behind a provider adapter. **Ollama** (local, offline) for SPE/laptop demos. **Mock** for CI. Paid providers (Claude/OpenAI) are optional drop-ins | Zero cost; structured JSON output; vision for frame-grounded captions; global limiter keeps us inside free-tier caps |
| Transcript | `youtube-transcript-api` → fallback `yt-dlp` subtitles → optional `faster-whisper` | Robust across caption-less videos |
| Metadata/duration | `yt-dlp --dump-json` (no download) | True duration, title, availability |
| Frames | `yt-dlp -g` (stream URL) + `ffmpeg -ss T -i URL -frames:v 1` | Real frame captured directly from the stream, no full download |
| Notion | Notion REST API + **File Upload API** (`/v1/file_uploads`) | Images uploaded straight to Notion; no public image host needed |
| Extension | Chrome MV3, **vanilla JS, no build step** | Satisfies "loadable unpacked, no build" |
| Retry/backoff | TTL "delay queues" 10s → 30s → 90s (demo profile 2s → 4s → 8s) | Native RabbitMQ, no plugin |
| Deploy | `docker-compose.yml` (Cloud) **and** Helm chart on kind/K3s (SPE) | Same images, two targets |

---

# Part A — Cloud Project (Note-Taking Agent)

## 1. Requirements Traceability Matrix

Every "must" in the spec, where it is satisfied, and how it is demonstrated.

| # | Spec § | Requirement | Where in this plan | How it's demonstrated / verified |
|---|---|---|---|---|
| R1 | 1.1 | 4 independent processes/containers: extension, gateway, broker, worker pool | §2, §15 | `docker compose ps`; kill any one without touching others |
| R2 | 1.1 | Gateway does **not** perform pipeline work | §7 | Gateway image has no ffmpeg/yt-dlp/LLM SDK; code review; returns 202 in <50 ms |
| R3 | 1.1 | Broker: durable queue, at-least-once, redelivery on crash | §6 | Quorum durable queues, persistent messages, manual ack; SIGKILL demo |
| R4 | 1.1 | ≥ 2 independent worker processes | §15 | `deploy.replicas: 2`, `--scale worker=N` |
| R5 | 2 | Content script injects "Take Notes" button in player toolbar on watch pages | §12.2 | Manual + Playwright e2e |
| R6 | 2 | Captures current video ID, sends to background | §12.2 | e2e |
| R7 | 2 | Keeps working across SPA navigation video→video | §12.2 | `yt-navigate-finish` + MutationObserver; e2e test navigates 3 videos |
| R8 | 2 | Background: one-time Notion OAuth, token stored, no re-prompt | §12.3 | `chrome.storage.local`; second job doesn't prompt |
| R9 | 2 | Background submits jobs, relays status to popup | §12.3 | |
| R10 | 2 | Popup shows Queued → Transcribing → Summarizing → Extracting Frames → Publishing → Done | §12.4 | Stepper UI |
| R11 | 2 | One-click "View in Notion" link on success | §12.4 | |
| R12 | 2 | Failures and "completed with warnings" shown clearly; never raw JSON | §12.4 | Error-code → human text map |
| R13 | 3.1 | Timestamped transcript; learn true duration; enforce Constraint A | §8.2, §13.1 | |
| R14 | 3.2 | LLM: summary bullets + timestamps of visual moments + caption per timestamp | §8.3 | Pydantic schema enforcement |
| R15 | 3.2 | Chunk long transcripts; merge keeping section order | §8.3.4 | Unit test with tiny chunk size |
| R16 | 3.3 | Seek to each timestamp, capture one frame from the video stream | §8.4 | |
| R17 | 3.4 | Upload frames, assemble Notion page (headings, bullets, images with captions) | §8.5 | |
| R18 | 4 | All 8 statuses + allowed transitions | §5 | State-machine unit tests (illegal transitions rejected) |
| R19 | 5 | `POST /jobs` → 202 + `Location`, body `{job_id,status,video_id}`; no pipeline work | §7.2 | |
| R20 | 5 | Invalid input → 4xx `{"error":{"code","message"}}` | §7.2 | Contract tests |
| R21 | 5 | `notion_token` supplied per request; gateway never stores it | §7.2, §17 | Token only travels encrypted in the message; DB has no token column |
| R22 | 5 | Default Notion destination when `notion_parent_id` omitted, stated in README | §8.5.1 | README section |
| R23 | 5 | `GET /jobs/{id}` read-only, idempotent; 404 unknown | §7.3 | Test: 100 polls leave `updated_at` unchanged |
| R24 | 5 | `GET /jobs/{id}/results` → 409 before terminal; absolute working Notion URL on success | §7.4 | |
| R25 | 6 | HTTP 429 from **any** of 3 external APIs → re-queue with exponential backoff; user sees "waiting" | §10.1 | Fault-injection demo |
| R26 | 6 | Worker SIGKILL mid-job → reassigned; no lost job; no duplicate Notion page | §10.2, §11 | Kill demo during EXTRACTING_FRAMES |
| R27 | 6 | Ack only after stage work durably complete; stage redelivery idempotent | §6.4, §11 | |
| R28 | 6 | Graceful degradation: partial frames → COMPLETED_WITH_WARNINGS; all fail → text-only page | §10.3 | Fault-injection demo |
| R29 | 6 | DLQ after bounded, configurable attempts; demonstrable quickly | §10.4 | Private video + demo backoff profile (<1 min) |
| R30 | 7A | Support ≤ 20 min; reject longer deterministically, don't process | §13.1 | Reject at submit (client duration hint) + authoritative check at TRANSCRIBING |
| R31 | 7B | Only real frames; omit on failure, never substitute; each frame paired with LLM caption (what + why) | §13.2 | Automated checker + LLM-as-judge |
| R32 | 8 | Unpacked extension, no build step | §12.1 | |
| R33 | 8 | One compose file for gateway, broker, ≥2 workers; `--scale worker=N` | §15 | |
| R34 | 8 | README: base URL, default Notion destination, every env var (LLM key/model, rate limit, Notion OAuth creds, broker URL, retry/backoff params) | §14, §22 | README checklist |
| R35 | 8 | Health endpoint (`GET /healthz`) checking gateway, broker, state store | §7.5 | |
| R36 | 9 | Burst of jobs drains without any LLM 429 | §9 | Burst script + metric `llm_http_429_total == 0` |
| R37 | 9 | Low E2E latency near 20 min | §20 | Target < 90 s |

---

## 2. Architecture

### 2.1 Component Diagram

```
 ┌──────────────────────────── Chrome ─────────────────────────────┐
 │  YouTube tab                                                    │
 │  ┌───────────────────┐  runtime   ┌───────────────────────────┐ │
 │  │ Content script    │──message──▶│ Background service worker │ │
 │  │ "Take Notes" btn  │            │ - Notion OAuth (once)     │ │
 │  └───────────────────┘            │ - submit / poll jobs      │ │
 │  ┌───────────────────┐◀──message──│ - chrome.storage jobs list│ │
 │  │ Popup UI (stepper)│            └────────────┬──────────────┘ │
 │  └───────────────────┘                         │ HTTPS/HTTP     │
 └────────────────────────────────────────────────┼────────────────┘
                                                  ▼
                                  ┌───────────────────────────────┐
                                  │  API Gateway (FastAPI)        │
                                  │  POST /jobs  GET /jobs/{id}   │
                                  │  GET /jobs/{id}/results       │
                                  │  GET /healthz  /metrics       │
                                  │  POST /oauth/notion/exchange  │
                                  └──────┬───────────────┬────────┘
                         insert job row  │               │ publish stage msg
                                         ▼               ▼
                              ┌──────────────┐   ┌──────────────────────────┐
                              │ PostgreSQL   │   │ RabbitMQ                  │
                              │ jobs, events │   │ ex: jobs.work (direct)    │
                              │ artifacts,   │   │ q: stage.transcribe       │
                              │ frames       │   │ q: stage.summarize        │
                              └──────▲───────┘   │ q: stage.frames           │
                                     │           │ q: stage.publish          │
                                     │           │ q: retry.10s/30s/90s (TTL)│
                                     │           │ q: jobs.dlq  (+ events)   │
                                     │           └───────────┬───────────────┘
                                     │                       │ consume (prefetch=1, manual ack)
                     ┌───────────────┴───────────────────────▼──────────────┐
                     │ Worker pool (N ≥ 2 identical containers)             │
                     │  transcribe → summarize → frames → publish            │
                     │  clients: YouTube(yt-dlp) · LLM · Notion · ffmpeg     │
                     └───────────────┬──────────────────────────────────────┘
                                     │ token buckets, heartbeats
                                     ▼
                              ┌──────────────┐
                              │ Redis        │
                              └──────────────┘
          External: YouTube  ·  LLM provider (Anthropic)  ·  Notion API
```

### 2.2 Request Flow (happy path)

1. User clicks **Take Notes** on `youtube.com/watch?v=X`. Content script sends `{type:"SUBMIT", videoId, url, durationHint, title}` to background.
2. Background makes sure a Notion token exists (runs OAuth once if missing), then `POST /jobs` with `Idempotency-Key`.
3. Gateway validates, inserts `jobs` row (`QUEUED`), publishes `stage.transcribe` message **with publisher confirms**, returns `202` + `Location: /jobs/{id}`. If the publish fails, the row is marked `FAILED` (`ENQUEUE_FAILED`) and the gateway returns `503`. Alternatively we use a transactional outbox (§7.2.3).
4. Worker A consumes `transcribe`: CAS `QUEUED→TRANSCRIBING`, fetches metadata and transcript, stores artifact, publishes `summarize` (confirmed), acks.
5. Worker B (any worker) consumes `summarize`: CAS `TRANSCRIBING→SUMMARIZING`, chunked LLM calls through the global limiter, stores synthesis, publishes `frames`, acks.
6. `frames`: CAS → `EXTRACTING_FRAMES`, ffmpeg per timestamp (bounded parallelism), optional vision-grounded caption check, stores JPEG bytes, publishes `publish`, acks.
7. `publish`: CAS → `PUBLISHING`, uploads images (File Upload API), creates the page, appends blocks, stores `notion_page_url`, sets `COMPLETED` or `COMPLETED_WITH_WARNINGS`, acks.
8. Popup/background poll `GET /jobs/{id}` and render progress; on terminal, `GET /jobs/{id}/results` gives the Notion URL.

---

## 3. Design Decisions ("Left to Your Design", §10)

Each becomes a short ADR in `docs/decisions/` so we can justify it in the viva/demo.

### ADR-1 Broker: RabbitMQ (vs Redis Streams, Kafka, SQS)
- **Manual ack + automatic redelivery when the consumer's TCP connection drops.** A SIGKILLed worker's unacked message goes back to the queue immediately. Redis Streams needs a custom `XAUTOCLAIM` reaper with idle timeouts, which makes recovery slower.
- **Quorum queues** are replicated and durable, and they have `x-delivery-limit`, which protects against *crash loops* (a message that kills every worker it touches). Our app-level attempt counter can't count those, because the worker dies before it can increment anything.
- **Dead-letter exchanges + per-queue TTL** give us exponential backoff with no plugin.
- Management UI on `:15672` shows queues and the DLQ live during the demo.
- Kafka is overkill (no per-message ack, no per-message delay). SQS isn't local.

### ADR-2 State store: PostgreSQL (+ Redis for ephemeral coordination)
- Job state needs durability, **conditional updates (CAS)** for safe transitions, JSONB for warnings/artifacts, and an audit trail (`job_events`).
- Redis holds only rebuildable, ephemeral data: rate-limit buckets, worker heartbeats, a hot status cache (optional).
- Frame JPEGs (~50–200 KB × ≤ 8) go in Postgres `bytea`. That's simple and needs no extra service. If needed, we could switch to MinIO/S3 by changing one storage interface.

### ADR-3 Internal state machine
- Public statuses exactly as in the spec (§5). Internally we also track `stage_attempt`, `retry_state` (`next_retry_at`, `reason`) and `publish_state` sub-states (§11.4).
- All transitions go through one function, `transition(job_id, from, to)`. It runs `UPDATE … WHERE status = :from`, so an illegal or duplicate transition is a no-op that we detect.

### ADR-4 Global LLM rate limit
- **Redis token buckets** (requests/min **and** tokens/min), shared by every worker and checked atomically by a Lua script, plus a distributed **concurrency semaphore** that caps in-flight LLM calls.
- They're set to **≈ 80 % of the provider's tier limit**, so a burst never reaches the provider's limit and we never get a 429 in the first place. Separate buckets for Notion (≈ 2.5 req/s, below Notion's 3 req/s average) and YouTube (concurrency cap + req/min).

### ADR-5 Worker topology: generic workers, one message per stage
- Each stage is its own message. A worker finishes a stage, **persists its output durably**, publishes the next-stage message (publisher-confirmed), and **only then acks** the current message.
- Why: a crash during EXTRACTING_FRAMES re-runs only frame extraction, not the transcript or LLM work (which saves money and tokens). It also directly satisfies "ack only after a stage's work is durably complete".
- Every worker subscribes to all four stage queues by default (`WORKER_STAGES=all`), so 2 workers cover everything. Setting `WORKER_STAGES=frames` creates a specialised pool **without code changes**. That matters for SPE/KEDA per-queue autoscaling.
- Alternative considered: one message per job with in-process checkpointing. It's simpler, but a crash makes the whole job wait for redelivery, and you can't scale per stage.

### ADR-6 LLM provider/model: free by default, paid is optional
**No paid LLM API is required.** The spec leaves the provider open, and SPE prefers no paid cloud. All providers sit behind one adapter interface:
```python
class LLMClient(Protocol):
    async def synthesize(self, chunk: TranscriptChunk, ctx: ChunkContext) -> ChunkSynthesis: ...
    async def reduce(self, sections: list[Section]) -> GlobalSummary: ...
    async def verify_frames(self, frames: list[FrameCandidate]) -> list[FrameVerdict]: ...  # batched, see §8.4
    def classify_error(self, exc: Exception) -> ErrorClass: ...   # maps provider errors → RateLimited / Transient / Fatal
```

| `LLM_PROVIDER` | Cost | Vision | Structured output mechanism | Role in this project |
|---|---|---|---|---|
| `gemini` (**default**) | Free tier (API key from Google AI Studio) | Yes | `response_mime_type=application/json` + `response_schema` | Default for the Cloud project and SPE "online" demo |
| `ollama` | Free, local, offline | Yes, with a vision model (e.g. `gemma3`, `qwen2.5vl`, `llama3.2-vision`) | `format: <JSON schema>` | SPE laptop-only demo; fallback if a free-tier daily cap runs out |
| `openai_compat` | Free tiers exist (Groq, OpenRouter `:free` models, GitHub Models, Mistral, Cerebras) | Model-dependent | `response_format: json_schema` / JSON mode | Backup provider; one adapter covers all OpenAI-compatible endpoints via `LLM_BASE_URL` |
| `anthropic` / `openai` | Paid (optional) | Yes | Forced tool call / `json_schema` | Optional quality upgrade only |
| `mock` | Free | Fake | Canned deterministic JSON | CI, unit/integration tests, rehearsals |

Notes:
- **Free tiers have low per-minute and per-day caps, and they change.** Look up the current limits for the chosen model and set `LLM_RPM`/`LLM_TPM`/`LLM_RPD` to ~80 % of them (§9). Tight limits actually *strengthen* the demo: the global limiter is what lets an 8-video burst drain with zero 429s.
- **Pin a specific model version** in `.env` (e.g. a dated Gemini Flash model, not just "latest") so behaviour doesn't change mid-grading.
- **Privacy:** free tiers may use inputs to improve the provider's models. That's acceptable here (public YouTube transcripts and frames) and is noted in the README.
- **Call budget per job** (matters on free tiers): 1 synthesis call for a ≤ 20-min video (≈ 5k tokens), +1 reduce call only when chunked, +**1 batched** vision call for all frames (§8.4). That's **2–3 LLM calls per job**, so even a ~10 RPM free tier handles several jobs per minute.
- **Fallback chain** (`LLM_FALLBACK_PROVIDER`, optional): if the primary returns a *daily-quota-exhausted* error (not a per-minute 429), the stage can switch to the fallback (e.g. `gemini` → `ollama`) instead of dead-lettering. Ordinary per-minute 429s still go through the normal backoff path (§10.1). The choice is recorded on the job as an informational note.

### ADR-7 Transcript fetching
1. `youtube-transcript-api`: prefer manual captions in `TRANSCRIPT_LANGS` (default `en`), otherwise auto-generated.
2. Fallback: `yt-dlp --write-auto-subs --skip-download` (json3/vtt) → parsed.
3. Optional fallback (`WHISPER_FALLBACK=true`): download audio only (`-f ba`) and transcribe with `faster-whisper` (`base`/`small`, CPU int8). For 20 min of audio that takes about 1–3 min.
4. None available → `TRANSCRIPT_UNAVAILABLE` (non-retryable → FAILED).

### ADR-8 Frame extraction
- `yt-dlp -f "bv*[height<=720][ext=mp4]/bv*[height<=720]/best[height<=720]" -g` gives a direct, signed googlevideo URL (resolved once per stage, valid for hours).
- `ffmpeg -ss {t} -i {url} -frames:v 1 -q:v 2 -y out.jpg`, using **input seeking**. ffmpeg uses HTTP range requests, so only a few MB are fetched per frame and the whole video is never downloaded.
- Bounded parallelism (`FRAME_CONCURRENCY=3`), per-frame timeout (`FRAME_TIMEOUT_SEC=30`).
- Sanity checks only (no substitution): file decodes, dimensions > 0, not near-uniform black/white (mean/stddev luma). If a check fails, the frame is omitted with a warning.

### ADR-9 Extension tooling
- Manifest V3, plain JS/HTML/CSS, ES modules, **zero build**. Hand-written CSS with a YouTube-like look and dark mode support.

---

## 4. Data Model & State Store

### 4.1 PostgreSQL schema (managed by Alembic migrations)

```sql
-- jobs: one row per submission
CREATE TABLE jobs (
  id               UUID PRIMARY KEY,
  video_id         TEXT NOT NULL,           -- 11-char YouTube id
  youtube_url      TEXT NOT NULL,           -- normalized https://www.youtube.com/watch?v=ID
  status           TEXT NOT NULL,           -- QUEUED | TRANSCRIBING | ... | DEAD_LETTERED
  progress         SMALLINT NOT NULL DEFAULT 0,  -- 0..100
  video_title      TEXT,
  duration_sec     INTEGER,
  notion_parent_id TEXT,                    -- nullable → default destination
  idempotency_key  TEXT UNIQUE,             -- from extension, dedupes double-clicks
  attempt          SMALLINT NOT NULL DEFAULT 0,   -- failure attempts in current stage
  rl_attempt       SMALLINT NOT NULL DEFAULT 0,   -- rate-limit retries in current stage
  retry_reason     TEXT,                    -- RATE_LIMITED | TRANSIENT_ERROR | null
  next_retry_at    TIMESTAMPTZ,             -- for "waiting… retrying in 27 s"
  warnings         JSONB NOT NULL DEFAULT '[]',  -- [{code, message, timestamp_sec?}]
  error            JSONB,                   -- {code, message, stage}
  result           JSONB,                   -- {notion_page_url, notion_page_id, frames_requested, frames_published}
  publish_state    TEXT,                    -- NULL | PAGE_CREATING | PAGE_CREATED | CONTENT_APPENDED
  worker_id        TEXT,                    -- last claimant (for debugging/demo)
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at     TIMESTAMPTZ
);
CREATE INDEX ON jobs (status);
CREATE INDEX ON jobs (created_at DESC);

-- job_events: append-only audit log of transitions (great for demo + debugging)
CREATE TABLE job_events (
  id         BIGSERIAL PRIMARY KEY,
  job_id     UUID REFERENCES jobs(id),
  from_status TEXT, to_status TEXT,
  kind       TEXT,       -- TRANSITION | RETRY_SCHEDULED | WARNING | REDELIVERED | DLQ
  detail     JSONB,
  worker_id  TEXT,
  at         TIMESTAMPTZ DEFAULT now()
);

-- stage_artifacts: durable outputs of each stage (makes redelivery cheap + idempotent)
CREATE TABLE stage_artifacts (
  job_id   UUID REFERENCES jobs(id),
  stage    TEXT,           -- transcribe | summarize
  data     JSONB NOT NULL, -- transcript segments / synthesis result
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (job_id, stage)
);

-- frames: one row per LLM-flagged timestamp
CREATE TABLE frames (
  job_id         UUID REFERENCES jobs(id),
  timestamp_sec  INTEGER,
  caption        TEXT NOT NULL,      -- LLM caption (what is shown + why flagged)
  status         TEXT NOT NULL,      -- PENDING | CAPTURED | FAILED | REJECTED
  failure_code   TEXT,
  image_jpeg     BYTEA,
  width INT, height INT, sha256 TEXT,
  notion_file_upload_id TEXT,
  notion_upload_at TIMESTAMPTZ,      -- uploads expire if unattached (~1 h) → re-upload
  PRIMARY KEY (job_id, timestamp_sec)
);
```

**The Notion token is never stored** in any table (R21).

### 4.2 Redis keys

| Key | Type | Purpose | TTL |
|---|---|---|---|
| `rl:{api}:req` | hash (tokens, ts) | Token bucket: requests | none |
| `rl:llm:tok` | hash | Token bucket: LLM tokens/min | none |
| `rl:llm:day:{YYYYMMDD}` | counter | LLM requests used today (free-tier daily cap) | 48 h |
| `sem:llm` | zset (holder→expiry) | Distributed concurrency semaphore | per-entry lease |
| `worker:{id}:hb` | string | Heartbeat (for `/healthz` worker count) | 15 s |
| `faults` | hash | Demo-mode fault injection switches (§10.5) | none |

---

## 5. Job State Machine

```
           ┌────────┐ claim  ┌─────────────┐ transcript ┌─────────────┐  LLM done ┌──────────────────┐ frames done ┌────────────┐
 submit ──▶│ QUEUED │──────▶│ TRANSCRIBING│──────────▶│ SUMMARIZING │─────────▶│ EXTRACTING_FRAMES│───────────▶│ PUBLISHING │
           └───┬────┘        └──────┬──────┘            └──────┬──────┘           └────────┬─────────┘             └─────┬──────┘
               │                    │                          │                           │                             │
               │      any non-terminal ──▶ FAILED (non-retryable)   /   DEAD_LETTERED (budget exhausted)                  │
               ▼                                                                                                         ▼
                                                                                    COMPLETED  |  COMPLETED_WITH_WARNINGS
```

### 5.1 Transition table (enforced in code)

```python
ALLOWED = {
  None:               {"QUEUED"},
  "QUEUED":           {"TRANSCRIBING", "FAILED", "DEAD_LETTERED"},
  "TRANSCRIBING":     {"SUMMARIZING", "FAILED", "DEAD_LETTERED"},
  "SUMMARIZING":      {"EXTRACTING_FRAMES", "FAILED", "DEAD_LETTERED"},
  "EXTRACTING_FRAMES":{"PUBLISHING", "FAILED", "DEAD_LETTERED"},
  "PUBLISHING":       {"COMPLETED", "COMPLETED_WITH_WARNINGS", "FAILED", "DEAD_LETTERED"},
  # terminal: COMPLETED, COMPLETED_WITH_WARNINGS, FAILED, DEAD_LETTERED → {}
}
STAGE_STATUS = {"transcribe":"TRANSCRIBING","summarize":"SUMMARIZING",
                "frames":"EXTRACTING_FRAMES","publish":"PUBLISHING"}
```

### 5.2 Rules
- **Claiming a stage** = `UPDATE jobs SET status=:stage_status, worker_id=:w WHERE id=:id AND status IN (:prev_status, :stage_status)`.
  The `:stage_status` case covers **redelivery**: a re-delivered `frames` message finds the job already in `EXTRACTING_FRAMES` and resumes.
  If the job is already *past* this stage (a duplicate message after a crash between publish-next and ack), the worker **acks and drops** the message and logs a `DUPLICATE_STAGE_MESSAGE` event.
- **Retries don't change the public status.** The job stays in `SUMMARIZING` while waiting on a 429, and `retry_reason`/`next_retry_at` are set so the UI can show "waiting" (R25).
- Terminal states are immutable. Any later message for a terminal job is acked and dropped.
- `progress` is monotonic (`GREATEST(progress, :new)`).

### 5.3 Progress mapping

| Stage | Range | Granularity |
|---|---|---|
| QUEUED | 0 | |
| TRANSCRIBING | 5 → 20 | metadata fetched (10), transcript fetched (20) |
| SUMMARIZING | 20 → 55 | per chunk: `20 + 35 * done/total` |
| EXTRACTING_FRAMES | 55 → 85 | per frame |
| PUBLISHING | 85 → 99 | uploads, page create, block batches |
| COMPLETED* | 100 | |

---

## 6. Messaging Topology (RabbitMQ)

### 6.1 Exchanges & queues

| Name | Type | Args | Purpose |
|---|---|---|---|
| `jobs.work` | direct exchange | durable | Routes by stage key |
| `stage.transcribe` / `stage.summarize` / `stage.frames` / `stage.publish` | **quorum** queue | `x-delivery-limit=5`, `x-dead-letter-exchange=jobs.dlx`, `x-dead-letter-strategy=at-least-once`, `x-overflow=reject-publish` | Work queues |
| `jobs.retry` | direct exchange | | Routes to delay tiers |
| `retry.t1` / `retry.t2` / `retry.t3` | classic durable queue | `x-message-ttl={BACKOFF_1..3}`, `x-dead-letter-exchange=jobs.work` (keeps original routing key) | Backoff "parking" queues with no consumers |
| `jobs.dlx` | **fanout** exchange | | Dead-letter fan-out |
| `jobs.dlq` | quorum queue | | **Inspection DLQ**, never auto-consumed |
| `jobs.dlq.events` | quorum queue | | Consumed by `dlq-watcher` to mark DB `DEAD_LETTERED` (covers crash-loop dead-letters) |

> Retry tier routing (as built): the worker publishes directly, through the default exchange, to `retry.<stage>.<N>s` (e.g. `retry.frames.30s`, `x-message-ttl=30000`, `x-dead-letter-exchange=jobs.work`, `x-dead-letter-routing-key=frames`). When the TTL expires, the message goes back to its stage queue. That's 4 stages × 3 tiers = 12 small queues, declared idempotently at startup. The delay is **in the queue name**, so switching `RETRY_PROFILE` declares new queues instead of conflicting with existing TTL arguments.

### 6.2 Message envelope (JSON, persistent `delivery_mode=2`)

```json
{
  "v": 1,
  "job_id": "3f0c…",
  "stage": "frames",
  "attempt": 2,          // failure attempts for this stage
  "rl_attempt": 1,       // rate-limit retries for this stage
  "enc_notion_token": "gAAAAAB…",   // Fernet/AES-GCM, key = MESSAGE_ENC_KEY
  "notion_parent_id": null,
  "trace": {"traceparent": "00-…"}, // OpenTelemetry context propagation
  "debug": {"fail_frames": [461]}   // only honored when DEMO_FAULTS_ENABLED=true
}
```
Headers: `message_id` = `{job_id}:{stage}:{attempt}:{rl_attempt}` (useful for dedupe logs).

### 6.3 Consumer settings
- **Pull model (as built):** each worker concurrency slot does `basic.get` across its stage queues, **later stages first** (publish → frames → summarize → transcribe), so in-flight jobs finish before new ones start. Each slot holds exactly one unacked message. A busy worker never hoards messages, and a SIGKILL returns at most one message per slot. (`basic.qos` prefetch can't give one message across several quorum queues, because quorum queues don't support global QoS. That's why we pull.) Idle polling interval: `WORKER_POLL_INTERVAL_SEC=0.5`.
- `aio-pika` robust connection with `heartbeat=30`. **All blocking work (ffmpeg, yt-dlp, whisper) runs in `asyncio.to_thread`/subprocess**, so the heartbeat never starves. (This is the classic pika pitfall where long CPU work leads to a missed heartbeat, the broker closes the connection, and you get spurious redelivery.)
- Broker `consumer_timeout` stays at its default (30 min), which is far above any stage duration. Each stage also has its own deadline (`STAGE_TIMEOUT_SEC`).

### 6.4 Ack protocol (the core of correctness)

```
on message(stage, job):
  1. claim stage (CAS)                    → if job past stage: ack & drop
  2. do work, skipping parts already persisted (idempotent resume)
  3. COMMIT outputs to Postgres           (durable)
  4. publish next-stage msg, wait for publisher confirm
  5. basic_ack(current msg)
on retryable error:  publish to retry tier (confirm) → update retry fields → ack
on fatal error:      set FAILED (CAS)  → ack
on budget exhausted: set DEAD_LETTERED → publish to jobs.dlx (confirm) → ack
on crash anywhere:   no ack → broker redelivers to another worker (R26)
```

What a crash at each point leads to:
- Before 3: the work is redone (idempotent; partially saved frames are skipped).
- Between 3 and 4: on redelivery, the artifacts already exist, so we skip straight to publishing the next message.
- Between 4 and 5: the next stage receives a duplicate message, but the claim CAS sees the job already advanced. The duplicate is acked and dropped (or resumed if that stage is mid-flight on the same stage, which is safe because stages are idempotent).

---

## 7. API Gateway

### 7.1 Stack
FastAPI, Uvicorn (2 workers), SQLAlchemy 2 async + asyncpg, aio-pika (publisher only), redis-py asyncio, Pydantic v2 settings, structlog, prometheus-fastapi-instrumentator, OpenTelemetry. **No ffmpeg, yt-dlp, or LLM SDK in this image** (R2).

### 7.2 `POST /jobs`

**Request** (exactly per spec + optional extras):
```json
{ "youtube_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  "notion_token": "secret_… / ntn_…",
  "notion_parent_id": "optional-page-or-db-id",
  "client_duration_sec": 212,     // optional hint from <video>.duration (Constraint A early reject)
  "client_title": "…" }           // optional, shown in popup before transcript stage
```
Header: `Idempotency-Key: <uuid generated by extension per click>`.

**Validation (no network calls; pure, fast):**
| Check | Error code | HTTP |
|---|---|---|
| Body is JSON + schema | `INVALID_REQUEST` | 400 |
| `youtube_url` parses to an 11-char ID. Accepted forms: `youtube.com/watch?v=`, `m.youtube.com/watch`, `youtu.be/`, `youtube.com/shorts/`, `youtube.com/embed/`, `youtube.com/live/`. The ID regex is `^[A-Za-z0-9_-]{11}$` | `INVALID_YOUTUBE_URL` | 422 |
| Non-YouTube host | `UNSUPPORTED_HOST` | 422 |
| `notion_token` present, non-empty, plausible prefix/length | `MISSING_NOTION_TOKEN` | 401 |
| `notion_parent_id` if present: 32-hex or UUID (dashes optional) | `INVALID_NOTION_PARENT_ID` | 422 |
| `client_duration_sec > MAX_VIDEO_SECONDS` (1200) | `VIDEO_TOO_LONG` | 422 |
| Payload too large / per-client submit rate exceeded | `PAYLOAD_TOO_LARGE` / `TOO_MANY_REQUESTS` | 413 / 429 |
| Broker unreachable at enqueue | `QUEUE_UNAVAILABLE` | 503 |

Error body always: `{"error": {"code": "INVALID_YOUTUBE_URL", "message": "That doesn't look like a YouTube video link."}}`

**Behaviour:**
1. If `Idempotency-Key` was seen before, return the existing job (202, same body). This dedupes double-clicks.
2. Insert job row `QUEUED`, publish `transcribe` message (with confirm), return **202** + `Location: /jobs/{id}`:
   `{"job_id": "...", "status": "QUEUED", "video_id": "dQw4w9WgXcQ"}`

#### 7.2.3 DB ↔ broker consistency
Simple approach: insert the row, then publish with confirm. If the publish fails, mark the row `FAILED/ENQUEUE_FAILED` and return 503.
Robust approach (stretch goal): a **transactional outbox**. The insert and an `outbox` row commit together, and a relay loop in the gateway publishes pending outbox rows. This is "messaging plumbing", not pipeline work, so it's allowed in the gateway. We start with the simple approach and switch to the outbox only if time permits.

### 7.3 `GET /jobs/{job_id}`
- **Pure SELECT**, no writes, no side effects (R23). Tests assert that `updated_at` doesn't change across polls.
- 404 `{"error":{"code":"JOB_NOT_FOUND",...}}` for unknown or malformed IDs.
- Response (spec fields + additive fields the popup uses):
```json
{ "job_id":"uuid","status":"EXTRACTING_FRAMES","progress":68,
  "video_id":"…","video_title":"…","duration_sec":1147,
  "warnings":[], "error":null, "result":null,
  "retry": {"waiting": true, "reason":"RATE_LIMITED", "api":"llm",
            "attempt":2, "max_attempts":6, "next_retry_at":"2026-11-…Z"},
  "created_at":"…","updated_at":"…" }
```
- `Cache-Control: no-store`.

### 7.4 `GET /jobs/{job_id}/results`
- 404 unknown; **409** `{"error":{"code":"JOB_NOT_FINISHED","message":"Job is still SUMMARIZING"}}` if non-terminal (R24).
- `COMPLETED` / `COMPLETED_WITH_WARNINGS` → 200:
```json
{ "status":"COMPLETED_WITH_WARNINGS",
  "result":{"notion_page_url":"https://www.notion.so/Title-<32hex>",
            "frames_requested":6,"frames_published":5},
  "warnings":[{"code":"FRAME_EXTRACTION_FAILED","timestamp_sec":461,"message":"…"}] }
```
- `FAILED` / `DEAD_LETTERED` → 200 with `result:null` and `error:{code,message,stage}`. The request was valid, the outcome is a failure.
- The URL is the absolute `url` field returned by Notion's create-page response, so it always works.

### 7.5 `GET /healthz` (R35)
Checks run concurrently with a 1 s timeout each:
- Postgres: `SELECT 1`
- RabbitMQ: open a channel + passive-declare `stage.transcribe`
- Redis: `PING`
- Workers: count of live heartbeat keys (informational; `degraded` if 0)

```json
200 {"status":"ok","checks":{"gateway":"ok","postgres":"ok","rabbitmq":"ok","redis":"ok"},
     "workers":{"alive":2,"ids":["worker-7f3a","worker-c21b"]},"version":"1.0.0"}
503 {"status":"unavailable","checks":{"rabbitmq":"error: connection refused", ...}}
```
Plus `GET /livez` (process up, for container healthchecks) and `GET /metrics` (Prometheus).

### 7.6 Other endpoints
| Endpoint | Purpose |
|---|---|
| `POST /oauth/notion/exchange` | Exchanges an OAuth `code` for a token using the server-held client secret; **returns the token to the extension, never stores it** (§12.3) |
| `GET /config/public` | Returns `NOTION_OAUTH_CLIENT_ID`, `max_video_seconds` for the extension |
| `GET /admin/dlq` | Lists DEAD_LETTERED jobs and their last error (protected by `ADMIN_TOKEN`) |
| `POST /admin/dlq/{job_id}/replay` | Manually re-enqueue from the first incomplete stage (resets attempts) |
| `POST /admin/faults` | Demo-only fault toggles (§10.5); disabled unless `DEMO_FAULTS_ENABLED=true` |
| `GET /docs` | OpenAPI UI |

CORS: allow `chrome-extension://{EXTENSION_ID}` (configurable) and `http://localhost:*` for dev.

---

## 8. Worker & Pipeline Stages

### 8.1 Worker process structure
```
worker/main.py
  ├─ load settings, init OTel, logging, metrics server (:9100/metrics)
  ├─ connect Postgres, Redis, RabbitMQ (robust)
  ├─ declare topology (idempotent)
  ├─ heartbeat task (SET worker:{id}:hb EX 15 every 5 s)
  ├─ consumers for WORKER_STAGES (default all four) — prefetch 1 per channel
  ├─ dlq-watcher consumer (jobs.dlq.events) — marks DEAD_LETTERED for crash-loop cases
  └─ graceful shutdown on SIGTERM: stop consuming, finish current stage (≤ STAGE_TIMEOUT), ack, exit
     (SIGKILL obviously skips this → broker redelivers; that's the demo)
```
One worker container handles one stage message at a time (prefetch=1 per stage channel, plus a global in-process semaphore `WORKER_CONCURRENCY=1`), which keeps resource use predictable. Scale by adding containers.

**Error taxonomy** (drives retry decisions):

| Class | Examples | Action |
|---|---|---|
| `RateLimited(api, retry_after)` | HTTP 429 from LLM / Notion / YouTube (yt-dlp "HTTP Error 429") | Backoff retry; separate budget `RL_MAX_RETRIES` |
| `Transient` | 5xx, timeouts, connection reset, LLM `overloaded` (529), yt-dlp extractor hiccup, **video unavailable/private** | Backoff retry; budget `MAX_ATTEMPTS` → DLQ |
| `Fatal` | `VIDEO_TOO_LONG`, `LIVE_STREAM_UNSUPPORTED`, `TRANSCRIPT_UNAVAILABLE`, Notion 401 `unauthorized`, Notion 404 parent not shared, LLM invalid output twice | FAILED immediately |
| `FrameError` | per-timestamp ffmpeg failure | Not a job error; → warning (graceful degradation) |

> Private/unavailable videos are deliberately treated as **Transient** so they go through the full retry budget and land in the DLQ, exactly as the spec suggests for demoing the DLQ. (They could also be classified as fatal. We document this choice: "YouTube availability can be temporary, e.g. premieres or region glitches, so we retry with a bound.")

### 8.2 Stage 1 — TRANSCRIBING (`transcribe`)
1. Claim (CAS `QUEUED→TRANSCRIBING`). If the `transcribe` artifact already exists, jump to step 6.
2. **Metadata:** `yt-dlp --dump-single-json --skip-download --no-playlist URL` (through the YouTube limiter) → `title`, `duration`, `is_live`, `availability`, `chapters` (bonus: chapters can seed section headings).
   - Errors "Private video", "Video unavailable", "Sign in to confirm your age" → `Transient(VIDEO_UNAVAILABLE)`.
3. **Constraint A (authoritative):** `duration > MAX_VIDEO_SECONDS` → `Fatal(VIDEO_TOO_LONG)` → FAILED. `is_live` or `duration` missing → `Fatal(LIVE_STREAM_UNSUPPORTED)`. Update `video_title`, `duration_sec`, progress 10.
4. **Transcript:** follow the ADR-7 cascade. Normalise to `[{start: float, dur: float, text: str}]`, strip `[Music]`-style noise, merge tiny segments into ~5–10 s windows to save tokens.
5. Persist `stage_artifacts(job, 'transcribe', {segments, language, source, chapters})`. Progress 20.
6. Publish `summarize` (confirm) → ack.

### 8.3 Stage 2 — SUMMARIZING (`summarize`)

#### 8.3.1 Output schema (Pydantic, also sent to the LLM as a tool schema)
```json
{
  "sections": [
    { "heading": "Gradient Descent Intuition",
      "start_sec": 0, "end_sec": 184,
      "bullets": ["…", "…"] }
  ],
  "visual_moments": [
    { "timestamp_sec": 461,
      "caption": "Architecture diagram of the transformer encoder stack…",   // what is shown
      "why_flagged": "Speaker says 'as you can see in this diagram' and walks through each block.", // why
      "importance": 4 }   // 1..5
  ],
  "chunk_summary": "One-paragraph summary of this chunk"
}
```
Final merged synthesis adds `title`, `tldr: [3–5 bullets]`, `key_terms: [{term, definition}]` (bonus).

#### 8.3.2 Prompt design
- **System prompt** (cached via prompt caching): role = expert note-taker. Rules:
  - Bullets must be faithful to the transcript, with no invented facts.
  - Flag a `visual_moment` only when the transcript gives *evidence* that something visual is on screen: deictic cues ("as you can see", "this slide", "on the screen", "this diagram/graph/equation/code"), explicit slide transitions, whiteboard work, demos.
  - The timestamp must be inside this chunk's `[start,end]`, ideally 1–3 s *after* the cue (once the slide is shown).
  - Each moment needs a `caption` (what is likely shown) **and** `why_flagged`.
  - Return at most `MAX_FRAMES_PER_CHUNK`.
- **User content:** the chunk as lines `[MM:SS] text`, plus context: video title, chapter titles in range, the previous chunk's last heading (for continuity).
- Structured output with the JSON schema, using each provider's native mechanism (Gemini `response_schema`, Ollama `format`, OpenAI-compatible `json_schema`, Anthropic forced tool call), so the response is always parseable. Smaller local models are more likely to drift, so we always validate with Pydantic. If validation fails, re-prompt once with the validation errors. A second failure → `Fatal(LLM_INVALID_OUTPUT)`.

#### 8.3.3 Calls go through the limiter
`await limiter.acquire("llm", requests=1, tokens=est_in + max_out)` (§9). After the response, reconcile with the actual `usage` (refund or charge the difference).

#### 8.3.4 Chunking & merge (R15)
- Token-count the transcript (`len(text)/4` heuristic or the provider's token counter). If it's ≤ `LLM_CHUNK_TOKENS` (default 12k), make **one call**. A 20-min talk is roughly 3–4k words, about 5k tokens, so usually one call.
- Otherwise split **on segment boundaries** into chunks of ≤ `LLM_CHUNK_TOKENS` with `LLM_CHUNK_OVERLAP_SEC` (default 15 s) of overlap. Chunk *i* covers `[s_i, e_i]`.
- Map: process chunks (sequentially or in parallel, bounded by the limiter). Persist each chunk result as it finishes (`stage_artifacts` key `summarize:chunk:{i}`), so a crash or redelivery resumes at the first missing chunk.
- Merge:
  1. Concatenate `sections` **in chunk order** (sort key `(chunk_index, start_sec)`). Section order stays intact.
  2. If the last section of chunk *i* and the first of chunk *i+1* have the same/similar heading (case-insensitive and normalized Levenshtein ≥ 0.8) or overlap in time, merge them and dedupe their bullets.
  3. `visual_moments`: drop out-of-range timestamps (`<0` or `>duration-1`) with a warning. Sort by time. Within `MIN_FRAME_GAP_SEC` (20 s), keep the higher `importance`. Cap at `MAX_FRAMES` (default 8) by importance, then re-sort chronologically.
  4. **Reduce call** (small; only headings + bullets): produce `title`, `tldr`, `key_terms`. With a single chunk, do this in the same call.
- Persist the final `stage_artifacts(job,'summarize', synthesis)` and insert `frames` rows (`PENDING`) for every selected moment (upsert on `(job_id,timestamp_sec)`).
- Publish `frames` → ack.

### 8.4 Stage 3 — EXTRACTING_FRAMES (`frames`)
1. Claim. Load `frames` rows; **skip rows already `CAPTURED`/`FAILED`/`REJECTED`** (idempotent resume after SIGKILL, R26/R27).
2. If there are 0 pending frames, go straight to step 6.
3. Resolve the stream URL once: `yt-dlp -g -f <720p format>` (YouTube limiter). Failure → `Transient` (retry the stage), *unless* attempts are exhausted for this stage. In that case we degrade instead: mark all pending frames `FAILED(STREAM_UNAVAILABLE)` and continue to publish a text-only page (R28).
4. For each pending timestamp, with concurrency `FRAME_CONCURRENCY`:
   - `ffmpeg -nostdin -hide_banner -loglevel error -ss {t} -i "{url}" -frames:v 1 -vf "scale='min(1280,iw)':-2" -q:v 3 -y /tmp/{job}_{t}.jpg` with a timeout.
   - Validate: decodes (Pillow), size > 1 KB, luma mean ∈ (8, 247) **or** stddev > 6 (not a flat black/white frame).
   - Success → `UPDATE frames SET status='CAPTURED', image_jpeg=…, sha256=…` (committed per frame, so progress is durable frame-by-frame).
   - Failure → `status='FAILED', failure_code='FRAME_EXTRACTION_FAILED'` (or `FRAME_BLANK`). **No substitution** (Constraint B).
   - Progress update after each frame.
   - **Demo hook:** if `DEMO_FAULTS_ENABLED` and the timestamp is in `debug.fail_frames` or the fault `frame_fail_rate` triggers, raise a `FrameError`. `FRAME_STAGE_DELAY_SEC` slows the stage so the grader has time to SIGKILL mid-stage.
5. **Optional vision verification** (`VISION_VERIFY=true`, default on; "multimodal" in the title). Send the captured frames plus each one's transcript caption/why to the LLM → per frame `{"timestamp_sec": 461, "shows_relevant_visual": bool, "caption": "<grounded caption: what the image actually shows + why it matters>"}`.
   - **Batched by default** (`VISION_BATCH_SIZE=8`): all frames go in **one** multimodal call (images downscaled to ≤ 768 px for this call only; the published image stays full-size), which returns an array of verdicts keyed by timestamp. That's 1 call instead of 8, which matters on free tiers. Set `VISION_BATCH_SIZE=1` for per-frame calls (better for small local models). Missing or malformed verdicts for a frame → keep the transcript-based caption (still LLM-written, still satisfies Constraint B).
   - `VISION_VERIFY=false` skips this step entirely. Captions then come from the transcript-based synthesis alone, which still meets Constraint B (an LLM-written caption of what is shown + why). Recommended when the free-tier quota is tight or the local model has no vision.
   - `true` → replace the caption with the grounded one (still LLM-written, now grounded in the actual pixels → stronger for LLM-as-judge).
   - `false` (e.g. a talking head with no slide) → `status='REJECTED'`, warning `FRAME_NOT_RELEVANT`. The frame is omitted, never substituted.
   - Rate-limited like any LLM call. A 429 here retries the stage (captured frames are already persisted, so no work is lost).
6. Publish `publish` → ack. (Transition to PUBLISHING happens on claim in the next stage. The spec allows "0 or more frames captured".)

### 8.5 Stage 4 — PUBLISHING (`publish`)

#### 8.5.1 Destination resolution (R22, documented in README)
1. If `notion_parent_id` is in the request → use it. Detect whether it's a page or a database by trying `GET /v1/pages/{id}` and then `GET /v1/databases/{id}`.
2. Else, if the `NOTION_DEFAULT_PARENT_ID` env var is set → use it. This applies to internal-integration setups.
3. Else (OAuth default) → the **page the user selected in Notion's OAuth page picker**. We find it with `POST /v1/search` (filter `object=page`, sort by `last_edited_time` desc) and take the first top-level page shared with the integration. The extension also caches this as `defaultParentId` after OAuth and sends it explicitly, so step 3 is only the fallback.
4. Nothing accessible → `Fatal(NOTION_NO_DESTINATION)` with message "Share a page with the integration".

#### 8.5.2 Page layout
```
📄 <Video Title> — Notes                                   (page title; suffix "· [a1b2c3]" short job id for idempotent lookup)
  [callout ℹ️] Source: <YouTube link> · Duration 18:42 · Generated 06-Nov-2026 · Job a1b2c3
  ## TL;DR
    • …  • …  • …
  ## <Section 1 heading>   (00:00–03:04, linked to youtube …&t=0s)
    • bullet …
    • bullet …
    [image: frame @ 02:41]   caption: "02:41 — <what it shows>. Why: <why flagged>."
  ## <Section 2 heading> …
  ## Key terms (toggle)
  ---
  [paragraph, grey] Frames are real stills captured from the video at the timestamps shown. N of M frames published.
```
- Each frame goes under the section whose `[start,end]` contains its timestamp, placed after the bullets.
- Image caption (Notion `image.caption` rich text) = `"[MM:SS](youtube link &t=) — {caption} Why flagged: {why}"`. This satisfies Constraint B pairing.
- Respect Notion limits: rich text ≤ 2000 chars per text object (split if longer), ≤ 100 children per request (batch appends), nesting depth ≤ 2 per request.

#### 8.5.3 Upload frames (Notion File Upload API)
For each `CAPTURED` frame without a valid `notion_file_upload_id` (or whose upload is older than `NOTION_UPLOAD_TTL_MIN`, default 50 min):
1. `POST /v1/file_uploads` `{"mode":"single_part","filename":"frame_461.jpg","content_type":"image/jpeg"}` → `{id, upload_url}`
2. `POST /v1/file_uploads/{id}/send` (multipart `file`) → `status: uploaded`
3. Store the `notion_file_upload_id` and timestamp.
All through the Notion limiter. An upload failure after retries → that frame is omitted with warning `FRAME_UPLOAD_FAILED` (graceful degradation still applies).

#### 8.5.4 Create page + append (idempotent; see §11.4)
- Image block: `{"type":"image","image":{"type":"file_upload","file_upload":{"id":"…"},"caption":[…]}}`.
- `frames_requested` = number of LLM-selected moments. `frames_published` = image blocks actually appended.
- Final status: `COMPLETED` if `frames_published == frames_requested` and there are no warnings; else `COMPLETED_WITH_WARNINGS` with one warning per omitted frame (`{code, timestamp_sec, message}`) plus any non-fatal issues (`TRANSCRIPT_AUTO_GENERATED`, `LLM_TIMESTAMP_OUT_OF_RANGE`, etc.).
  > Decision: `TRANSCRIPT_AUTO_GENERATED` is informational, **not** a warning that downgrades the status. Only real omissions or issues downgrade it. This is configurable.
- 0 frames requested (e.g. a podcast) → text-only page, `COMPLETED` (nothing was omitted).
- All frames failed → text-only page, `COMPLETED_WITH_WARNINGS` (R28).
- Set `result`, `completed_at`, progress 100 → ack.

---

## 9. Rate Limiting (Global, Cross-Worker)

### 9.1 Token bucket in Redis (atomic Lua)
```
KEYS[1] = bucket key, ARGV = capacity, refill_per_sec, requested, now_ms
→ returns {granted(0/1), wait_ms}
```
`limiter.acquire(api, requests=1, tokens=0, max_wait=LIMITER_MAX_WAIT_SEC)`:
- Try every applicable bucket (LLM: `req` and `tok`). If any bucket denies, sleep `min(wait_ms, 1000)` with jitter, then retry.
- If the cumulative wait would exceed `LIMITER_MAX_WAIT_SEC` (default 20 s), raise `SelfThrottled` → treated like `RateLimited` → the message is parked in a retry tier. This frees the worker for other stages (e.g. frame extraction for another job) instead of blocking it.
- **Concurrency semaphore** for the LLM (`LLM_MAX_CONCURRENCY`, default 2), implemented as a Redis ZSET of leases with expiry. A crashed worker's lease expires, so there's no deadlock.
- **Daily-quota bucket** (`LLM_RPD`, for free tiers): a Redis counter keyed by the provider's quota day (`rl:llm:day:{YYYYMMDD}`, expires after 48 h). When it's exhausted, the stage either switches to `LLM_FALLBACK_PROVIDER` (if set) or parks the job with `retry_reason='LLM_DAILY_QUOTA'`. Waiting hours makes no sense for a demo, so after `RL_MAX_RETRIES` the job is dead-lettered with `LLM_DAILY_QUOTA_EXHAUSTED` and a clear popup message. `/healthz` also reports `llm_quota_remaining_today`, so graders can see it before starting.

### 9.2 Defaults (all env-configurable)

| API | Limits we enforce | Provider limit (for context) |
|---|---|---|
| LLM (Gemini free tier, default) | `LLM_RPM=8`, `LLM_TPM=200000`, `LLM_RPD=200`, `LLM_MAX_CONCURRENCY=2`: **placeholders; set to ≈80 % of the model's current free-tier limits** | Free tiers: low RPM plus a per-day cap; check the provider's rate-limit page |
| LLM (Ollama, local) | `LLM_RPM=0` (unlimited), `LLM_MAX_CONCURRENCY=1` | Bounded by laptop CPU/GPU, not by quota; concurrency 1 avoids thrashing RAM |
| Notion | `NOTION_RPS=2.5` (burst 3) | Avg 3 req/s per integration |
| YouTube | `YOUTUBE_RPM=30`, `YOUTUBE_MAX_CONCURRENCY=3` | Undocumented; avoid bot detection |

### 9.3 Never hitting a provider 429 in a burst (R36)
- The workers' combined demand is capped by the shared buckets, not by worker count. 2 workers or 20 workers make no difference to the provider.
- Token estimates are conservative (input + `max_tokens`), reconciled after each call.
- `prefetch=1`, so jobs wait in the durable queue. The queue grows, then drains at the permitted rate. Grafana shows `queue_depth` rising then falling and `llm_http_429_total` flat at 0.
- Still handle 429 if it happens anyway (shared key used elsewhere): honour the `retry-after` header (`delay = max(backoff_tier, retry_after)`, rounded up to the next tier) **and** drain the local bucket (set its tokens to 0 for `retry_after` seconds) so other workers back off too.

---

## 10. Failure Handling (§6) — Detailed Design

### 10.1 HTTP 429 from any external API (R25)
- Detection: LLM HTTP 429 (Gemini `RESOURCE_EXHAUSTED`, OpenAI-compatible `429`, SDK `RateLimitError`; the per-minute vs per-day distinction is handled by the provider adapter's `classify_error`); Notion HTTP 429 (`code: rate_limited`) with `Retry-After`; YouTube: yt-dlp `HTTP Error 429` / "Too Many Requests" in stderr, or the transcript API's `TooManyRequests`/`RequestBlocked`.
- Action: `rl_attempt += 1`. If `rl_attempt ≤ RL_MAX_RETRIES` (default 6), publish the same stage message to tier `min(rl_attempt, 3)` (10 s → 30 s → 90 s → 90 s …), set `retry_reason='RATE_LIMITED'`, `next_retry_at`, write a `job_events` row, ack.
- **The public status remains the current stage.** The popup shows "⏳ Summarizing: the AI service is busy, retrying in 27 s (attempt 2 of 6)". It's never shown as an error.
- If the rate-limit retry budget is exhausted → DEAD_LETTERED with `RATE_LIMIT_BUDGET_EXHAUSTED`.
- On the next successful claim of that stage: clear `retry_reason`/`next_retry_at`.
- Within-call micro-retries: the SDK's own automatic retries are **disabled** (`max_retries=0`), so that all backoff is visible and queue-driven, as the spec asks ("re-queued").

### 10.2 Worker crash (R26, R27)
- SIGKILL closes the TCP connection, RabbitMQ requeues the unacked message at once, and another worker with spare prefetch gets it within milliseconds. Total recovery time is roughly the time to redeliver plus the time to redo the interrupted stage's remaining work.
- The resumed stage skips persisted work (frames already `CAPTURED`, chunks already summarised).
- `job_events` records `REDELIVERED` (the `redelivered` flag on the message + a different `worker_id`), which the demo can show: "picked up by worker-c21b after worker-7f3a died".
- No duplicate Notion page; see §11.4.
- **Crash loop protection:** if a message kills the worker every time (e.g. an ffmpeg segfault on a pathological video), the quorum queue's `x-delivery-limit=5` dead-letters it to `jobs.dlx`. `dlq-watcher` marks it `DEAD_LETTERED` with `CRASH_LOOP`.

### 10.3 Graceful degradation (R28)
- Per-frame isolation: one bad timestamp never fails the stage.
- Stream URL failure after retries means all frames fail, and we still publish a text-only page.
- Notion image-upload failure per frame → that frame is omitted.
- If the page create succeeds but an image block append fails (e.g. an expired upload), re-upload once. If it fails again, append the rest without it and add a warning.
- The result is always a published page with `COMPLETED_WITH_WARNINGS` and a precise warning list.

### 10.4 Dead-letter queue (R29)
- Budget: `MAX_ATTEMPTS` (default 5) for transient failures **per job** (summed across stages, so a flaky job can't get 5 × 4 attempts). Plus `RL_MAX_RETRIES` for rate limits. Plus the broker-level `x-delivery-limit` for crash loops.
- Backoff: `BACKOFF_SCHEDULE=10,30,90` (prod) / `2,4,8` (`RETRY_PROFILE=demo`). With demo + 5 attempts, a private video reaches the DLQ in about 2+4+8+8 ≈ 22 s plus processing time, so **under 1 minute**.
- On exhaustion: `status=DEAD_LETTERED`, `error={code:'VIDEO_UNAVAILABLE', message:'…', stage:'transcribe', attempts:5}`, publish the original message + headers `x-last-error`, `x-attempts` to `jobs.dlx`, ack.
- Inspection: the RabbitMQ UI shows the message in `jobs.dlq`, `GET /admin/dlq` lists jobs, and `POST /admin/dlq/{id}/replay` re-runs a job.
- Popup: "❌ Couldn't process this video after 5 attempts. It may be private, removed, or region-locked."

### 10.5 Fault injection (for a reliable live demo)
Enabled only with `DEMO_FAULTS_ENABLED=true`. Two ways to trigger:
- **Runtime** (no restart): `POST /admin/faults {"llm_429": 2, "notion_429": 1, "youtube_429": 1, "frame_fail_rate": 0.5, "frame_stage_delay_sec": 20}`. These are stored in the Redis `faults` hash. Counters decrement as they fire, so for example the next 2 LLM calls raise a synthetic 429 **at the HTTP client boundary** and go through exactly the same code path as a real one.
- **Per job:** `debug.fail_frames: [461]` in the POST body (also available from the extension's "Developer options").
- `scripts/demo.sh` wraps each scenario (§19).
Note: real 429s from a provider are hard to trigger on demand. Fault injection is how we "demonstrate live". We also show the limiter preventing real ones.

---

## 11. Idempotency & Exactly-Once *Effects*

At-least-once delivery means every stage must be safe to **repeat**.

| Stage | Side effects | Idempotency mechanism |
|---|---|---|
| transcribe | DB artifact | `INSERT … ON CONFLICT (job_id,stage) DO NOTHING`; skip if exists |
| summarize | DB artifacts per chunk, frames rows | Per-chunk artifacts; `frames` upsert on `(job_id,timestamp_sec)`; skip finished chunks |
| frames | DB frame rows | Only process `PENDING`; per-frame commit |
| publish | **External: Notion page** | `publish_state` sub-state machine + deterministic lookup (§11.4) |
| transitions | DB status | CAS `WHERE status IN (…)` |
| POST /jobs | DB row + message | `Idempotency-Key` unique |

### 11.4 No duplicate Notion pages (the hardest part)
The danger window: Notion creates the page, then the worker dies before recording `page_id`. On redelivery, a naive retry creates a second page.

Protocol:
1. Before creating: `UPDATE jobs SET publish_state='PAGE_CREATING'` (commit).
2. Create the page with a **deterministic marker**:
   - Page parent: title `"{video_title} — Notes · [{job_id[:6]}]"`.
   - Database parent: also set a `Job ID` rich-text property if the DB has one (or create it on first use; documented).
   The page is created **with only the header callout** (small, fast).
3. Immediately: `UPDATE jobs SET publish_state='PAGE_CREATED', result.notion_page_id=…, url=…` (commit).
4. Append content in batches of ≤ 100 blocks. After all of them succeed: `publish_state='CONTENT_APPENDED'`.
5. Finalize status → ack.

On redelivery:
- `CONTENT_APPENDED` → just finalize.
- `PAGE_CREATED` → the page exists but may be partially filled. **List its children and archive them all (except the header), then re-append everything.** That rebuilds it deterministically and avoids both duplicates and half-pages.
- `PAGE_CREATING` (crash inside the window) → **look the page up instead of searching the index**:
  - Page parent: `GET /v1/blocks/{parent_id}/children` (strongly consistent, unlike `/search`) → find a `child_page` whose title ends with `[{job_id[:6]}]`. Found → adopt its id as in `PAGE_CREATED`. Not found → create.
  - Database parent: `POST /v1/databases/{id}/query` filter `Job ID equals {job_id}`.
- Result: exactly **one** page per job, whatever the crash point. We test this by killing the worker at every step (test hooks `CRASH_AT=publish.after_create` etc.).

### 11.5 Concurrent duplicates
Can two workers run the same stage at once? Only if the broker redelivers while the first worker is still alive (a network partition beyond the heartbeat timeout). For safety:
- Stage claim writes `worker_id` + `lease_until = now()+STAGE_TIMEOUT`.
- A redelivered message whose job has an unexpired lease held by a worker with a live heartbeat is parked in retry tier 1 instead of run in parallel.
- Every per-frame and per-chunk write is an upsert, so even a true overlap is harmless.

---

## 12. Browser Extension

### 12.1 Structure (MV3, no build step) — R32
```
extension/
  manifest.json
  background.js          (service worker, type: module)
  content/youtube.js     (content script)
  content/youtube.css
  popup/popup.html, popup.js, popup.css
  options/options.html, options.js       (gateway URL, Notion connect/disconnect, default page, dev options)
  lib/api.js             (gateway client), lib/status.js (status → text/icon map), lib/storage.js
  icons/16.png 32.png 48.png 128.png
```
**manifest.json essentials:**
```json
{
  "manifest_version": 3,
  "name": "Video Notes → Notion",
  "version": "1.0.0",
  "permissions": ["storage", "identity", "alarms", "notifications"],
  "host_permissions": ["https://www.youtube.com/*", "http://localhost:8080/*"],
  "optional_host_permissions": ["https://*/*", "http://*/*"],
  "background": { "service_worker": "background.js", "type": "module" },
  "content_scripts": [{ "matches": ["https://www.youtube.com/*"],
                        "js": ["content/youtube.js"], "css": ["content/youtube.css"],
                        "run_at": "document_idle" }],
  "action": { "default_popup": "popup/popup.html" },
  "options_page": "options/options.html"
}
```
The content script matches all of `youtube.com/*` (not only `/watch`) **because YouTube is a SPA**: landing on the homepage and clicking a video never reloads the page. The script decides when to show the button.

### 12.2 Content script — R5, R6, R7
- `ensureButton()`:
  - Only if `location.pathname === '/watch'` and `new URLSearchParams(location.search).get('v')` is a valid ID.
  - Find the toolbar `#movie_player .ytp-right-controls`. Insert `<button class="ytp-button vnt-take-notes" aria-label="Take Notes" title="Take Notes">` with an inline SVG notepad icon, before the settings button. Idempotent: no-op if `.vnt-take-notes` already exists.
  - Not on a watch page → remove the button.
- **SPA navigation handling:**
  - `document.addEventListener('yt-navigate-finish', ensureButton)` (YouTube's own event).
  - `window.addEventListener('popstate', ensureButton)`.
  - A `MutationObserver` on `#movie_player` (debounced) handles the player re-rendering its controls (theatre mode, miniplayer, ads).
  - The click handler reads the **current** `v` param at click time (never a cached one), so after navigating A→B it submits B.
- On click:
  - `videoId`, canonical URL, `title` (`document.title` minus " - YouTube"), `durationHint = document.querySelector('video.html5-main-video')?.duration` (ignored during ads: if `.ad-showing` is present, omit the hint).
  - `chrome.runtime.sendMessage({type:'SUBMIT_JOB', …})`.
  - Button state: spinner → ✓ "Queued, open the extension to track" toast (small in-page toast, auto-hide 4 s) or ✗ toast with a human-readable error ("This video is longer than 20 minutes").
  - Debounce: ignore repeat clicks on the same video within 5 s (plus the backend `Idempotency-Key`).
- Edge cases: YouTube `/shorts/{id}` (optional: inject into the shorts action bar), embedded players (out of scope), miniplayer (button still in the toolbar).

### 12.3 Background service worker — R8, R9
**Notion OAuth (one-time):**
1. Public integration created at notion.so/my-integrations with redirect URI `https://<EXTENSION_ID>.chromiumapp.org/notion`. A stable extension ID comes from a `"key"` in manifest.json (documented), so the redirect URI never changes between machines.
2. `chrome.identity.launchWebAuthFlow({url: "https://api.notion.com/v1/oauth/authorize?client_id=…&response_type=code&owner=user&redirect_uri=…&state=<random>", interactive: true})`.
3. Verify `state`, extract `code`, then `POST {gateway}/oauth/notion/exchange {code, redirect_uri}`. The gateway calls `POST https://api.notion.com/v1/oauth/token` with Basic auth (client_id:client_secret; **the secret stays on the server and never ships in the extension**), returns `{access_token, workspace_name, bot_id, duplicated_template_id?}` and **stores nothing**.
4. Store `{access_token, workspace_name, refresh_token?}` in `chrome.storage.local` (not `sync`; tokens shouldn't roam). If Notion returns a refresh token, refresh it on a 401 before re-prompting.
5. After OAuth: `POST /v1/search` (directly from the extension with the token) to list pages the user granted. If there's exactly one, it becomes `defaultParentId`; with several, the options page lets the user choose.
6. Fallback for graders: the options page has "Use internal integration token" (paste `ntn_…`), handy if OAuth app review isn't set up.

**Job submission & status relay:**
- `SUBMIT_JOB` handler → get the token (trigger OAuth if missing, then continue the same submission) → `POST /jobs` with `Idempotency-Key: crypto.randomUUID()` → save `{job_id, video_id, title, thumb, submitted_at, last_status}` at the front of `jobs` in `chrome.storage.local` (keep the last 20).
- **Polling** in an MV3-friendly way (service workers are ephemeral):
  - While the popup is open: the popup connects with `chrome.runtime.connect({name:'popup'})`. Background polls active jobs every 1.5 s and pushes updates on the port.
  - While the popup is closed: `chrome.alarms` (`periodInMinutes: 0.5`, the minimum) polls active jobs, updates the **badge** (number of active jobs; green ✓ / orange ! / red ✗ briefly on completion), and fires a **notification** on terminal states ("Notes ready: <title>", with a [View in Notion] button → `chrome.tabs.create`).
  - Exponential polling backoff on network errors; stop polling at terminal states.
- On terminal: call `GET /jobs/{id}/results` once to get `notion_page_url`, then cache it.
- Errors talking to the gateway: "Can't reach the notes server at http://localhost:8080. Is it running?"

### 12.4 Popup UI — R10, R11, R12
Layout (≈ 380 × 520 px, light/dark theme):
```
┌──────────────────────────────────────┐
│ Video Notes            ● Connected to │
│                          "My Workspace"│
├──────────────────────────────────────┤
│ [thumb] Intro to Transformers   18:42 │
│ ● Queued ─ ● Transcribing ─ ◉ Summarizing ─ ○ Frames ─ ○ Publishing ─ ○ Done │
│ ▓▓▓▓▓▓▓▓▓░░░░░░░░ 42%                 │
│ ⏳ AI service busy, retrying in 27 s (2/6)│
├──────────────────────────────────────┤
│ [thumb] Linear Algebra Ep. 3    ✓ Done │
│ [ View in Notion ↗ ]                  │
├──────────────────────────────────────┤
│ [thumb] Lecture 7          ⚠ Done with warnings │
│ 1 of 6 frames couldn't be captured (at 7:41). │
│ [ View in Notion ↗ ]  [details ▾]     │
├──────────────────────────────────────┤
│ [thumb] Private vid       ✗ Couldn't process │
│ We tried 5 times. The video may be private or removed. [Retry] [Dismiss] │
└──────────────────────────────────────┘
```
- Stepper labels exactly: **Queued → Transcribing → Summarizing → Extracting Frames → Publishing → Done**. Each step shows done ✓, active (pulsing), pending, or failed (red at the stage where it failed, taken from `error.stage`).
- Status chip colours: blue active, amber waiting/warnings, green done, red failed, purple dead-lettered.
- `lib/status.js` maps every `error.code` and `warnings[].code` to a friendly sentence, e.g.
  `VIDEO_TOO_LONG` → "This video is longer than 20 minutes, which isn't supported."
  `NOTION_UNAUTHORIZED` → "Notion access expired. Click Reconnect." (button)
  `NOTION_NO_DESTINATION` → "Share a Notion page with the integration (Options → Choose page)."
  `FRAME_EXTRACTION_FAILED` → "Frame at 7:41 couldn't be captured, so the page was published without it."
  Unknown codes → "Something went wrong (code XYZ)." **The popup never renders raw JSON.** A "Copy diagnostics" link copies JSON to the clipboard for debugging, but doesn't display it.
- Empty state: "Open a YouTube video and click 📝 Take Notes in the player."
- Not connected: a big "Connect Notion" button.
- Accessibility: buttons with labels, `aria-live="polite"` for status changes, keyboard navigable.

---

## 13. Hard Constraints A & B

### 13.1 Constraint A — video length ≤ 20 min (R30)
Defence in depth, all deterministic and driven by `MAX_VIDEO_SECONDS=1200`:
1. **Extension:** if `<video>.duration > 1200`, show an immediate inline toast and don't submit (best UX). The hint is still sent when it's valid.
2. **Gateway:** `client_duration_sec > 1200` → `422 VIDEO_TOO_LONG` (rejected at submission, no job is created).
3. **Worker TRANSCRIBING (authoritative):** the yt-dlp `duration > 1200` → `FAILED` with `VIDEO_TOO_LONG` **before** any transcript/LLM/frame work. Never retried (non-retryable).
- Exactly 1200 s is allowed (`>`, not `>=`). Live streams / unknown duration → rejected (`LIVE_STREAM_UNSUPPORTED`).
- Test matrix: 19:59 ✓, 20:00 ✓, 20:01 ✗, live ✗, missing hint but long ✗ (worker).

### 13.2 Constraint B — real frames only, captioned (R31)
- **Only one code path produces images:** ffmpeg decoding of the source stream at `timestamp_sec`. There's no image-generation dependency, no placeholder images, and no fallback image.
- On failure the frame is **omitted** with a warning, never substituted. The vision step can only *reject* a frame, never alter it.
- The JPEG may be resized (a downscale to ≤ 1280 px wide preserves content). No crops, filters, or overlays.
- **Every image block carries a caption.** In code, the publish step constructs `ImageBlock(file_upload_id, caption)` where `caption` is a required non-empty field. Frames lacking a caption are dropped (with a warning). This should never happen, because the schema requires a caption.
- **Provenance** stored per frame: `sha256`, timestamp, source stream format id. Our eval script re-extracts frames and compares perceptual hashes (pHash distance ≤ 8) against the images downloaded from the Notion page, which proves the image came from the video.
- Caption content: what is shown (grounded by vision) + why it was flagged (transcript evidence) + timestamp link.

### 13.3 LLM-as-judge self-evaluation (§9)
`tests/eval/judge.py` on a held-out set of 6+ videos (lecture, slide talk, coding tutorial, whiteboard, podcast with no visuals, one 19:xx video):
- Fetch the published Notion page via the API. For each image + caption, ask a judge LLM: "Does the caption accurately describe this image? Does it explain why it matters? Score 1–5", plus a pHash real-frame check.
- Check the summary against the transcript for faithfulness and coverage, score 1–5.
- Check Constraint A by submitting a 25-min video and expecting an immediate 422 or FAILED.
- Output a markdown report `eval_report.md`. Use it to tune prompts.

---

## 14. Configuration / Environment Variables

All documented in README and `.env.example` (R34). Pydantic Settings validates at startup (fail fast with clear messages).

| Variable | Default | Used by | Description |
|---|---|---|---|
| `GATEWAY_BASE_URL` | `http://localhost:8080` | ext/README | Base URL of the API |
| `DATABASE_URL` | `postgresql+asyncpg://notes:notes@postgres:5432/notes` | gw, worker | State store |
| `BROKER_URL` | `amqp://notes:notes@rabbitmq:5672/` | gw, worker | **Broker connection string** |
| `REDIS_URL` | `redis://redis:6379/0` | gw, worker | Rate limiter / heartbeats |
| `LLM_PROVIDER` | `gemini` | worker | `gemini` (free tier) \| `ollama` (local, free) \| `openai_compat` (Groq/OpenRouter/GitHub Models/Mistral…) \| `anthropic` \| `openai` \| `mock` |
| `LLM_API_KEY` (aliases `GEMINI_API_KEY`, …) | — (free key from Google AI Studio; not needed for `ollama`/`mock`) | worker | **LLM API key** |
| `LLM_MODEL` | a pinned Gemini Flash model id | worker | **Model name** (pin an exact version) |
| `LLM_VISION_MODEL` | = `LLM_MODEL` | worker | Model for frame verification (for Ollama, a vision model, e.g. `gemma3:4b`) |
| `LLM_BASE_URL` | provider default; `http://ollama:11434` for Ollama | worker | Endpoint for `ollama` / `openai_compat` |
| `LLM_FALLBACK_PROVIDER` / `LLM_FALLBACK_MODEL` | empty | worker | Used only when the primary's daily quota is exhausted (e.g. `ollama`) |
| `LLM_RPM` | `8` (placeholder: ~80 % of the free-tier limit; `0` = unlimited) | worker | **Configured LLM rate limit** (requests/min, global) |
| `LLM_TPM` | `200000` (placeholder) | worker | Tokens/min, global |
| `LLM_RPD` | `200` (placeholder; `0` = unlimited) | worker | Requests/day, global (free-tier daily cap) |
| `VISION_BATCH_SIZE` | `8` | worker | Frames per vision call (8 = one call per job) |
| `LLM_MAX_CONCURRENCY` | `2` | worker | Max in-flight LLM calls cluster-wide |
| `LLM_CHUNK_TOKENS` | `12000` | worker | Chunk size |
| `LLM_CHUNK_OVERLAP_SEC` | `15` | worker | Chunk overlap |
| `LLM_MAX_OUTPUT_TOKENS` | `4000` | worker | |
| `NOTION_OAUTH_CLIENT_ID` | — | gw | **Notion OAuth client id** |
| `NOTION_OAUTH_CLIENT_SECRET` | — | gw | **Notion OAuth client secret** |
| `NOTION_OAUTH_REDIRECT_URI` | `https://<ext-id>.chromiumapp.org/notion` | gw/ext | |
| `NOTION_DEFAULT_PARENT_ID` | empty | worker | Default destination override (§8.5.1) |
| `NOTION_VERSION` | pinned API version string | worker | `Notion-Version` header |
| `NOTION_RPS` | `2.5` | worker | Notion global rate |
| `YOUTUBE_RPM` / `YOUTUBE_MAX_CONCURRENCY` | `30` / `3` | worker | |
| `YTDLP_COOKIES_FILE` | empty | worker | Optional cookies if YouTube bot-checks |
| `TRANSCRIPT_LANGS` | `en,en-US,en-GB` | worker | |
| `WHISPER_FALLBACK` / `WHISPER_MODEL` | `false` / `base` | worker | |
| `MAX_VIDEO_SECONDS` | `1200` | all | Constraint A |
| `MAX_FRAMES` / `MIN_FRAME_GAP_SEC` | `8` / `20` | worker | |
| `FRAME_CONCURRENCY` / `FRAME_TIMEOUT_SEC` | `3` / `30` | worker | |
| `VISION_VERIFY` | `true` | worker | |
| `MAX_ATTEMPTS` | `5` | worker | **Retry budget** (transient), per job |
| `RL_MAX_RETRIES` | `6` | worker | Rate-limit retry budget per stage |
| `RETRY_PROFILE` | `prod` | worker | `prod` \| `demo` |
| `BACKOFF_SCHEDULE_SEC` | `10,30,90` | worker | **Backoff parameters** (tier TTLs), used when `RETRY_PROFILE=prod` |
| `DEMO_BACKOFF_SCHEDULE_SEC` | `2,4,8` | worker | Tier TTLs used when `RETRY_PROFILE=demo` |
| `BROKER_DELIVERY_LIMIT` | `5` | worker | Quorum queue crash-loop limit |
| `STAGE_TIMEOUT_SEC` | `600` | worker | Per-stage hard deadline |
| `LIMITER_MAX_WAIT_SEC` | `20` | worker | Max in-place wait before parking |
| `WORKER_STAGES` | `all` | worker | Stage specialisation |
| `MESSAGE_ENC_KEY` | — (required) | gw, worker | Fernet key for the token in messages |
| `ADMIN_TOKEN` | — | gw | Protects `/admin/*` |
| `DEMO_FAULTS_ENABLED` | `false` | gw, worker | Enables fault injection |
| `CORS_ALLOWED_ORIGINS` | `chrome-extension://<id>` | gw | |
| `LOG_LEVEL` / `LOG_FORMAT` | `INFO` / `json` | all | |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty (disabled) | all | Tracing |

---

## 15. Docker Compose Deployment

`docker-compose.yml` (R1, R4, R33):

```yaml
name: video-notes
services:
  gateway:
    build: { context: ., dockerfile: services/gateway/Dockerfile }
    ports: ["8080:8080"]
    env_file: .env
    depends_on:
      postgres: { condition: service_healthy }
      rabbitmq: { condition: service_healthy }
      redis:    { condition: service_healthy }
      migrate:  { condition: service_completed_successfully }
    healthcheck: { test: ["CMD", "curl", "-fsS", "localhost:8080/livez"], interval: 10s }
    restart: unless-stopped

  worker:
    build: { context: ., dockerfile: services/worker/Dockerfile }
    env_file: .env
    deploy: { replicas: 2 }          # ≥ 2 workers; `docker compose up --scale worker=N`
    stop_grace_period: 30s
    depends_on: { same as gateway }
    restart: unless-stopped
    # no container_name → scaling works; names are video-notes-worker-1, -2, …

  migrate:                            # one-shot Alembic migration
    build: { context: ., dockerfile: services/gateway/Dockerfile }
    command: ["alembic", "upgrade", "head"]
    depends_on: { postgres: { condition: service_healthy } }

  rabbitmq:
    image: rabbitmq:4-management
    ports: ["5672:5672", "15672:15672"]   # 15672 = management UI for demo
    volumes: [rabbitmq-data:/var/lib/rabbitmq]
    healthcheck: { test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"] }

  postgres:
    image: postgres:16-alpine
    volumes: [pg-data:/var/lib/postgresql/data]
    healthcheck: { test: ["CMD-SHELL", "pg_isready -U notes"] }

  redis:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes"]
    healthcheck: { test: ["CMD", "redis-cli", "ping"] }

  # optional observability profile: `docker compose --profile obs up`
  prometheus: { profiles: ["obs"], ... }
  grafana:    { profiles: ["obs"], ports: ["3000:3000"], ... }
  jaeger:     { profiles: ["obs"], ports: ["16686:16686"], ... }

volumes: { rabbitmq-data: {}, pg-data: {} }
```

**Dockerfiles**
- `gateway`: `python:3.12-slim`, multi-stage (uv/pip wheels), non-root user, `uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 2`.
- `worker`: `python:3.12-slim` + `apt-get install ffmpeg` (or a static ffmpeg build), `yt-dlp` pinned via pip (updatable without a rebuild: `pip install -U yt-dlp` at start, controlled by `YTDLP_AUTO_UPDATE`), non-root, `tini` as PID 1 so SIGTERM is forwarded properly.

**Make targets:** `make up`, `make down`, `make logs`, `make scale N=4`, `make kill-worker`, `make demo-429`, `make demo-crash`, `make demo-degrade`, `make demo-dlq`, `make burst N=8`, `make health`, `make test`, `make eval`, `make ext-zip`.

---

## 16. Observability

(Required lightly for Cloud. It's the centrepiece for SPE, §29.)
- **Logs:** structlog JSON, always with `job_id`, `stage`, `worker_id`, `attempt`, `trace_id`. Tokens/keys redacted by a processor.
- **Metrics (Prometheus):**
  - `jobs_submitted_total`, `jobs_terminal_total{status}`
  - `stage_duration_seconds{stage}` (histogram), `job_e2e_seconds` (histogram)
  - `external_requests_total{api,status_code}`, `external_429_total{api}` (**must stay 0 for llm in a burst**)
  - `retries_scheduled_total{reason,stage,tier}`, `dlq_messages_total`
  - `limiter_wait_seconds{api}` (histogram), `llm_tokens_total{direction=in|out}`, `llm_cost_usd_total` (estimated)
  - `frames_total{outcome=captured|failed|rejected|published}`
  - `redeliveries_total{stage}`; `worker_alive` gauge
  - RabbitMQ built-in Prometheus plugin (`:15692`): queue depth, unacked, consumers
- **Traces:** OpenTelemetry. The gateway's span context is injected into message headers, so each worker's stage span continues the same trace. LLM calls are spans with GenAI semantic-convention attributes (model, tokens). The whole job shows as **one trace across services**.
- **Grafana dashboard "Video Notes":** jobs by status, queue depth per stage, stage latency p50/p95, 429s by API, limiter wait, tokens/min vs limit, DLQ size, worker count.

---

## 17. Security
- **Notion token handling:** sent from the extension over HTTP(S) per job. Inside the system it exists only **encrypted** (Fernet, `MESSAGE_ENC_KEY`) inside broker messages. It's decrypted in worker memory only, never written to Postgres or logs. Redaction filter on log processors. Messages in the DLQ still contain the encrypted token, and replay requires it.
- **OAuth client secret** lives only in the gateway env. The extension uses an auth-code flow with a server-side exchange and a `state` param to prevent CSRF.
- **CORS** locked to the extension origin. Optional `X-Client-Key` shared header.
- **Input validation** strict (URL allow-list). We never pass user strings to a shell. ffmpeg/yt-dlp are invoked with argument arrays and the validated video ID only.
- **Rate limit on POST /jobs** per client IP (e.g. 30/min) to protect the queue.
- Containers non-root, read-only root FS where possible, pinned image tags. (For SPE: Trivy, SBOM, cosign; §30.)
- Prompt-injection hardening: the transcript is treated as data inside delimiters. The output is schema-validated. The LLM has no tools besides the output schema.

---

## 18. Testing Strategy

| Level | Tooling | What |
|---|---|---|
| Unit | pytest, hypothesis | URL parsing (all forms, malformed), state machine (all legal/illegal transitions), progress mapping, chunker (boundaries, overlap, order), merger (section order, dedupe), visual-moment selection (gap/cap), token bucket Lua (via fakeredis/real redis), error classification (yt-dlp stderr samples), Notion block builder (limits: 2000 chars, 100 blocks), status→text map in extension (vitest-free: plain node test) |
| Contract | pytest + httpx | Every endpoint and status code per spec: 202+Location, 4xx error body shape, 404, 409, idempotent GET (no `updated_at` change) |
| Integration | testcontainers (Postgres, RabbitMQ, Redis) + **mock servers** for LLM/Notion/YouTube (respx or a FastAPI stub) | Full pipeline with mocks; 429 injection → backoff → success; crash injection at each `CRASH_AT` point → exactly one Notion page in the stub; DLQ after N attempts; graceful degradation |
| Chaos | pytest + docker SDK | Start compose, submit job, SIGKILL a worker during frames, assert COMPLETED and exactly one page |
| E2E extension | Playwright (Chromium with `--load-extension`) | Button appears; SPA navigation A→B→C keeps working and submits the right ID; popup renders stepper and human-readable text (never `{`) |
| Load | `scripts/burst.py` / k6 | 10–20 jobs in 30 s → all complete, `external_429_total{api="llm"} == 0` |
| Eval | `tests/eval/judge.py` | Constraints A/B + caption quality (LLM-as-judge) on 6+ videos |

CI runs unit + contract + integration (with mocks, no real API keys). Real-API smoke tests run manually or nightly with secrets.

---

## 19. Demo Runbook (what the grader will do)

Prereqs: `.env` filled, `docker compose up -d --build`, extension loaded unpacked, Notion connected.

0. **Sanity:** `curl localhost:8080/healthz` → all `ok`, `workers.alive: 2`.
1. **Happy path:** open a ~19-min lecture and click Take Notes. The popup walks the stepper; Done; View in Notion opens the page with sections, bullets, and captioned frames. Show `docker compose logs -f worker` to see the stages bouncing between workers.
2. **Navigation:** click another video in the sidebar (SPA). The button is still there and submits the new ID.
3. **429 handling:** `make demo-429` (`POST /admin/faults {"llm_429":2,"notion_429":1}`). Submit a video. The popup shows "AI service busy, retrying in 10 s (1/6)", then 30 s, then continues and completes. Grafana/logs show `RETRY_SCHEDULED tier=t1,t2`.
   - Use `RETRY_PROFILE=demo` for 2 s/4 s/8 s if time is short. That's a one-line `.env` change + `docker compose up -d worker`.
4. **Worker crash:** `make demo-crash` sets `frame_stage_delay_sec=20`, submits a job, waits for EXTRACTING_FRAMES, then runs `docker kill -s KILL $(docker compose ps -q worker | head -1)`. The other worker resumes frames within ~1 s (`job_events` shows REDELIVERED, worker_id changed). The job completes and **Notion has exactly one page** (show the parent page's children).
5. **Graceful degradation:** `POST /admin/faults {"frame_fail_rate":0.5}` (or the `debug.fail_frames` dev option). The result is ⚠ "Done with warnings, 2 of 6 frames couldn't be captured" and the page is published with 4 images. With `frame_fail_rate:1.0`, the page is text-only, still published, with warnings.
6. **DLQ:** with `RETRY_PROFILE=demo`, submit a private/removed video. The popup shows retrying 1/5 … 5/5, then ✗ "Couldn't process after 5 attempts". The RabbitMQ UI (`:15672`) → `jobs.dlq` has the message; `GET /admin/dlq` lists it.
7. **Constraint A:** click Take Notes on a 45-min video. You get an instant toast "longer than 20 minutes" (and `curl` without the hint → job FAILED `VIDEO_TOO_LONG` at TRANSCRIBING, with no LLM calls in the metrics).
8. **Burst:** click Take Notes on 8 videos quickly (or `make burst N=8`). The queue grows, then drains; `external_429_total{api="llm"}` = 0. `docker compose up -d --scale worker=4` shows faster draining, still 0 LLM 429s (the limiter is global).
9. **Scaling/kill other components:** `docker compose stop gateway` makes the workers keep processing; restart it and polling resumes. `docker compose restart rabbitmq` makes workers/gateway reconnect (robust connections), and queued messages persist (durable quorum queues).

---

## 20. Performance & Latency Budget

Target for a 19–20 min video: **< 90 s end-to-end** (p50), < 150 s p95.

| Stage | Estimate | Optimisations |
|---|---|---|
| Queue wait | ~0 s (idle) | prefetch 1, ≥ 2 workers |
| Metadata + transcript | 2–5 s | single yt-dlp JSON call; transcript API is fast |
| LLM synthesis | 15–40 s | usually a single call (≈ 5–6 k input tokens); prompt caching for the system prompt; parallel chunks if > 1 |
| Frames (8) | 6–15 s | input-seeking over HTTP range; 3-way parallel; stream URL resolved once |
| Vision verify (8 frames) | 4–10 s | **one batched call** (images ≤ 768 px for this call); Ollama on CPU: 30–90 s, so consider `VISION_VERIFY=false` there |
| Notion publish | 5–12 s | ≤ 2.5 rps: 8 uploads × 2 calls + 1 create + 1–2 appends ≈ 20 calls ≈ 8 s |
| **Total** | **≈ 40–90 s** | |

Option to save time: begin Notion uploads during the frames stage. We won't do this, because it would blur stage idempotency boundaries; we'll only consider it if latency is graded harshly.

---

## 21. Repository Layout

```
.
├── README.md                    # quickstart, base URL, default Notion destination, env vars, demo
├── PLAN.md                      # this file
├── .env.example
├── Makefile
├── docker-compose.yml
├── pyproject.toml               # one Python package "notetaker" (uv/poetry), two entrypoints
├── src/notetaker/
│   ├── common/                  # settings, db models, state machine, messaging, errors, crypto, telemetry, logging
│   ├── gateway/                 # FastAPI app: routes/jobs.py, routes/health.py, routes/oauth.py, routes/admin.py, schemas.py
│   ├── worker/
│   │   ├── main.py, consumer.py, retry.py, dlq_watcher.py, heartbeat.py
│   │   ├── stages/transcribe.py, summarize.py, frames.py, publish.py
│   │   ├── clients/youtube.py, llm/{base,gemini,ollama,openai_compat,anthropic,mock}.py, notion.py, ffmpeg.py
│   │   ├── ratelimit/{bucket.lua, limiter.py, semaphore.py}
│   │   └── faults.py
│   └── prompts/synthesize.md, reduce.md, verify_frame.md
├── migrations/                  # Alembic
├── services/gateway/Dockerfile
├── services/worker/Dockerfile
├── extension/                   # MV3, no build
├── tests/{unit,contract,integration,chaos,e2e,eval}/
├── scripts/{demo.sh, burst.py, kill_worker.sh, judge.py}
├── observability/{prometheus.yml, alerts.yml, grafana/dashboards/*.json}
├── deploy/                      # (SPE) helm/, argocd/, ansible/, keda/, chaos/
├── .github/workflows/           # (SPE) ci.yml, cd.yml, security.yml
└── docs/{architecture.md, decisions/ADR-*.md, demo-script.md}
```

---

## 22. Build Phases & Milestones

Each phase ends in something demonstrable. (Dates are in §33.)

| Phase | Scope | Exit criteria |
|---|---|---|
| **P0 Skeleton** (1 day) | Repo, pyproject, compose with infra, settings, logging, Alembic, `/healthz` | `docker compose up` → healthz OK |
| **P1 Gateway + state** (1–2 days) | POST/GET/results, validation, error bodies, idempotency key, state machine module, publish to RabbitMQ | Contract tests green; job sits QUEUED |
| **P2 Worker spine** (2 days) | Topology declaration, consumer, ack protocol, stage chaining with **stub stages**, retry tiers, DLQ, dlq-watcher, heartbeat | Stub job flows through all statuses; kill test passes with stubs |
| **P3 Real stages** (3–4 days) | yt-dlp metadata, transcript cascade, Constraint A, LLM adapter + schema + chunk/merge, ffmpeg frames, Notion uploads/page builder, publish idempotency | Real video → real Notion page |
| **P4 Rate limiting & faults** (1–2 days) | Redis buckets, semaphore, 429 classification for 3 APIs, fault injection, demo profile | Burst test 0 LLM 429s; 429 demo works |
| **P5 Extension** (2–3 days, parallel with P3) | Content script + SPA, OAuth + exchange endpoint, background polling/alarms/notifications, popup stepper + friendly errors, options page | Playwright e2e green |
| **P6 Hardening** (2 days) | Vision verification, graceful degradation edge cases, crash-at-every-point tests, observability dashboard | All four failure demos rehearsed |
| **P7 Docs & eval** (1–2 days) | README (all required items), ADRs, eval report via LLM-as-judge, demo script, recording | Fresh clone → running stack from README only (have someone else try it) |

**README must contain** (R34): base URL; quickstart; Notion integration setup (OAuth app + redirect URI + stable extension ID key; internal-token alternative); **default destination behaviour** (§8.5.1); **full env var table** (§14); retry/backoff parameters + demo profile; how to load the extension; how to run each failure demo; health check; architecture diagram; design choices summary.

---

## 23. Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| YouTube bot detection / transcript API blocked (`RequestBlocked`) | Pipeline can't start | Run from a laptop (residential IP); `YTDLP_COOKIES_FILE`; keep yt-dlp updated; yt-dlp subtitle fallback; whisper fallback |
| yt-dlp format/signature changes | Frames fail | Pin + auto-update option; graceful degradation still publishes text |
| Notion File Upload API quirks (size ≤ 20 MB single-part, expiry if unattached) | Images missing | JPEG ≤ 1 MB; re-upload if older than TTL; omit + warn |
| Notion OAuth needs public integration setup | Grader friction | Documented step-by-step; internal integration token fallback in options |
| MV3 service worker suspended | Missed status updates | Alarms + storage-backed job list; popup-driven fast polling |
| LLM flags timestamps with no visual (talking head) | Weak frames | Prompt with evidence rules; vision verification rejects irrelevant frames |
| Free-tier LLM caps (per-minute and per-day) | 429s, or quota gone mid-demo | Global RPM/TPM/RPD limiter set at ~80 %; 2–3 calls per job (batched vision); `/healthz` shows remaining daily quota; `LLM_FALLBACK_PROVIDER=ollama`; don't burn quota in rehearsals (use `mock`) |
| Free-tier terms or limits change | Config suddenly wrong | Limits are env vars, not code; adapter makes swapping provider a `.env` change; `openai_compat` covers several free providers |
| Local model (Ollama) output quality / JSON drift | Weaker notes, invalid JSON | Schema-constrained `format`; Pydantic validation + one repair re-prompt; use a 7–8B model if RAM allows |
| Duplicate Notion page on crash | Correctness hit in grading | §11.4 protocol + crash-at-every-point tests |
| Demo timing (backoffs too slow) | Grader impatience | `RETRY_PROFILE=demo`, runtime fault toggles, `frame_stage_delay_sec` |
| Heartbeat starvation causing spurious redelivery | Duplicate work | Blocking work off the event loop; idempotent stages anyway |

---

# Part B — Reusing This for the SPE Capstone

## 24. Can We Use This Project? (Answer)

**Yes, technically it's a very strong fit. Check two policy points first.**

### Why it fits
- The SPE brief says: *"We assess DevOps engineering, not application complexity."* That cuts both ways. The Note-Taking Agent already **is** a real multi-service distributed app (gateway, workers, broker, DB, cache), which is exactly what makes Kubernetes, autoscaling, observability, and chaos engineering *meaningful* rather than toy-like. Since the app gets built anyway for the Cloud course, nearly all of the SPE effort can go into the pipeline, which is what's graded.
- It lines up with the brief's own example topics:
  - **#25 Voice-to-Text Transcription:** "fed by an asynchronous job queue. Cutting-edge: **KEDA queue-based autoscaling**" (almost identical architecture)
  - **#23 Summarization Microservice:** "LLM summarizer with **rate limiting** and observability of **token usage and cost**" (we already have both)
  - **#26 AI Agent Orchestration:** "multi-step agent workflow with monitoring of each agent action. Cutting-edge: **OpenTelemetry agent tracing**"
  - Track **AI / LLMOps** ("LLM-backed app with CI/CD, model/version registry, request tracing").
- The Cloud project's grader scenarios (kill a worker, scale workers, burst load) translate directly into SPE cutting-edge items: **Chaos Mesh pod-kill**, **KEDA autoscaling**, and **SLO dashboards**.

### ⚠️ Things to check or adapt before committing
1. **Academic-integrity / dual-submission policy.** Some institutes treat submitting the same work for two courses as self-plagiarism unless both instructors approve. **Ask Prof. Thangaraju (SPE) and the Cloud course instructor** whether one codebase can serve both courses, with each course graded on its own layer: the Cloud course on the distributed app, SPE on the DevOps pipeline. Mention it explicitly in the SPE proposal (due ~11-Nov). Being transparent turns a risk into a non-issue.
2. **Team of exactly two.** SPE requires two members, both with visible Git contributions to *code and DevOps*, plus individual vivas. If the Cloud project is individual or has a different team, the SPE partner must genuinely own parts of it (see §32 split) and both must be able to explain everything.
3. **"Runs on a laptop, free-tier cloud optional."** Our app calls three external services (YouTube, an LLM API, Notion). All three can be **free**: YouTube and Notion cost nothing, and the LLM defaults to the **Gemini free tier** (ADR-6). No paid API is needed anywhere. To also meet "the free-tier service must be optional":
   - **Ollama in-cluster** (`LLM_PROVIDER=ollama`; a Deployment + PVC for model weights, in the Helm chart behind `ollama.enabled=true`). A small instruct model plus a small vision model (or `VISION_VERIFY=false`) means the **entire demo runs offline on a laptop**.
   - `LLM_PROVIDER=mock` in CI (no keys, deterministic, no quota used).
   - Gemini free tier is the optional "better quality" mode. Switching between them is a Helm value / `.env` change.
   - Total LLM cost for both courses: **₹0**.
4. **Kubernetes, not Compose.** The Cloud project requires `docker-compose.yml`. SPE requires a local K8s cluster (kind/Minikube/K3s). We keep **both**, built from the same images: Compose for the Cloud grader, Helm on kind for SPE.
5. **Scope discipline.** SPE demo is 07–12 Dec, so the app must be feature-complete by ~25 Nov to leave time for the pipeline. The Cloud deadline (fill in: ________) should drive app priorities.

### Bottom line
Use **one monorepo**: the application code (Part A) and the DevOps layer (`.github/workflows`, `deploy/helm`, `deploy/argocd`, `deploy/ansible`, `deploy/keda`, `deploy/chaos`, `observability/`). After policy approval, the Cloud project is graded on `src/`, `extension/`, and `docker-compose.yml`. SPE is graded on the automated path from `git push` → build → test → scan → image → GitOps deploy → observable on a local cluster.

---

## 25. SPE Rubric Mapping

| SPE Criterion (marks) | What we deliver | Evidence in demo/viva |
|---|---|---|
| Proposal & architecture (3) | 1-page proposal (§35), architecture diagram (app + platform), ADRs | Proposal PDF; `docs/architecture.md` |
| VCS & collaboration (2) | GitHub, protected `main`, PRs with required review **from the other member**, Conventional Commits, CODEOWNERS, issues/project board, progressive commits from both | GitHub Insights → contributors graph; PR history |
| CI/CD (4) | GitHub Actions: lint → unit → integration (service containers) → build → scan → push to GHCR → bump GitOps manifest → **Argo CD auto-sync** to kind | Live: push a change → it's deployed and visible within minutes |
| Containers & K8s (5) | Multi-stage non-root images; Helm chart: Deployments, StatefulSets, Services, Ingress, ConfigMaps, Secrets, probes, resources, HPA (gateway), **KEDA** (workers), PDBs, NetworkPolicies | `kubectl get all`, scale under load, self-healing |
| Config mgmt / IaC (2) | **Ansible** playbook bootstraps a laptop: installs kind/kubectl/helm, creates the cluster (config-as-code), installs ingress, Argo CD, KEDA, monitoring, Chaos Mesh. **Helm** for app config; Argo CD `Application` manifests | `ansible-playbook site.yml` → whole platform up from zero |
| Monitoring & observability (3) | kube-prometheus-stack (Prometheus, Grafana, Alertmanager), Loki (logs), Tempo (traces), custom app metrics + RabbitMQ metrics, dashboards-as-code, alert rules, SLOs | Grafana dashboards live during the demo; one trace across gateway → worker → LLM |
| Cutting-edge extension (4) | **Primary:** KEDA queue-driven autoscaling + Chaos Mesh fault experiments + OpenTelemetry LLM tracing. **Optional:** Argo Rollouts canary, Trivy/SBOM/Cosign supply chain | Burst → pods scale 1→N→1; chaos pod-kill → no lost jobs; trace view |
| Report, demo, viva (2) | 4–6 page report, live demo, backup recording, contribution statement | |

---

## 26. DevOps Pipeline Design

### 26.1 Source control & collaboration
- GitHub repo, `main` protected: PR required, 1 approval (the other teammate), status checks required, linear history.
- Branches: `feat/*`, `fix/*`, `ci/*`, `infra/*`. Conventional Commits (`feat(worker): …`), enforced by a commitlint action.
- CODEOWNERS split by area (§32) so reviews naturally cross between members.
- GitHub Projects board with issues per phase. Each PR links an issue.
- Pre-commit hooks: ruff, ruff-format, mypy (light), yamllint, hadolint, `helm lint`, detect-secrets.

### 26.2 CI (GitHub Actions) — `.github/workflows/ci.yml`
Trigger: PR and push to `main`.

```
jobs:
  lint:        ruff, mypy, yamllint, hadolint, helm lint, kubeconform (render chart → validate), eslint (extension)
  unit:        pytest -m unit --cov (coverage gate ≥ 70%), upload coverage
  integration: services: postgres, rabbitmq, redis (GH service containers); LLM_PROVIDER=mock; Notion/YouTube stubs
  extension:   web-ext lint / manifest validation; zip artifact
  build:       (needs above) docker buildx → images gateway & worker, tags: sha-<short>, branch; cache-to GHA
  scan:        Trivy image scan (fail on CRITICAL), Trivy fs/config (IaC misconfig), Syft SBOM (SPDX) upload as artifact
  push:        (main only) push to ghcr.io/<org>/video-notes-{gateway,worker}:sha-<short>
  sign:        (main only) cosign sign --keyless (GitHub OIDC) + attest SBOM   [cutting-edge, optional]
  e2e-kind:    (main only, optional) spin up kind in the runner (helm/kind-action), helm install with mock LLM, run smoke test (submit job → COMPLETED against Notion stub)
```

### 26.3 CD: GitOps with Argo CD (pull-based, which suits a laptop cluster)
GitHub can't reach a laptop cluster to push to it, so Argo CD **inside the cluster pulls** from Git. That's why GitOps fits here.
- `cd.yml` (on main after push + sign): update `deploy/helm/video-notes/values-local.yaml` → `image.tag: sha-<short>` (via `yq`), then commit with `[skip ci]` as a bot (or open an auto-merged PR). Alternative: **Argo CD Image Updater** watching GHCR, so no commit-back is needed.
- Argo CD `Application` `video-notes` → path `deploy/helm/video-notes`, `syncPolicy: automated {prune: true, selfHeal: true}`, poll interval 1 min (or a webhook via smee/ngrok, optional).
- The flow: **push → CI (build/test/scan) → image in GHCR → tag bump in Git → Argo CD sync → new pods roll out → Grafana shows the new `version` label**, fully automated, as required by the brief.
- Jenkins alternative (course default): a `Jenkinsfile` with the same stages running in-cluster (Jenkins Helm chart) for teams who prefer it. We pick GitHub Actions + Argo CD and justify that choice in the report.

### 26.4 Environments
- `local` (kind on the laptop) is the graded environment. Optional: `staging` namespace in the same cluster, deployed first by an Argo CD ApplicationSet, then promoted.

---

## 27. Kubernetes Design

### 27.1 Cluster
`kind` config-as-code (`deploy/kind/cluster.yaml`): 1 control-plane + 2 worker nodes (so pod-kill and node-drain demos are meaningful), port-mappings 80/443 for ingress-nginx. K3s is an alternative for lower RAM.

### 27.2 Helm chart `deploy/helm/video-notes`
| Resource | Details |
|---|---|
| `Deployment gateway` | 2 replicas, readiness `/healthz`, liveness `/livez`, requests 100m/128Mi, limits 500m/256Mi, HPA on CPU (2→5) |
| `Deployment worker` | Managed by **KEDA ScaledObject** (min 1/2, max 6) on RabbitMQ queue length; `terminationGracePeriodSeconds: 60` (graceful SIGTERM → finish stage); requests 250m/256Mi, limits 1 CPU/1Gi (ffmpeg); liveness via a heartbeat file or `/healthz` on the metrics port |
| `Job migrate` | Helm pre-install/upgrade hook running Alembic |
| RabbitMQ | Bitnami chart / RabbitMQ Cluster Operator (1 replica locally, quorum queues), PVC, Prometheus plugin |
| PostgreSQL | Bitnami chart or CloudNativePG (1 instance), PVC |
| Redis | Bitnami chart, AOF on |
| Ollama (optional, `ollama.enabled`) | `Deployment` + `Service ollama:11434` + PVC (~5–10 Gi) for model weights; init Job runs `ollama pull <model>`; requests 2 CPU/4–6Gi; workers point `LLM_BASE_URL` at it. This is the laptop-only, no-cloud LLM for the SPE demo |
| `Ingress` | `notes.localtest.me` → gateway (extension `GATEWAY_BASE_URL` configurable). `localtest.me` resolves to 127.0.0.1 |
| `ConfigMap` | Non-secret env (§14) |
| `Secret` | LLM key, Notion OAuth creds, `MESSAGE_ENC_KEY`, `ADMIN_TOKEN`, created via **Sealed Secrets** (encrypted in Git) or SOPS, or **Vault** as a stretch goal |
| `PodDisruptionBudget` | gateway minAvailable 1, worker minAvailable 1 |
| `NetworkPolicy` | Only gateway/workers may reach postgres/redis/rabbitmq; egress for workers to the internet (YouTube/LLM/Notion) |
| `ServiceMonitor` | gateway, worker, rabbitmq scraped by Prometheus |
| `PrometheusRule` | Alerts: DLQ > 0, job failure rate > 20 %, `external_429_total{api="llm"}` increase, no workers alive, queue depth > 20 for 5 min |

### 27.3 Mapping the Cloud graders' scenarios to K8s
- Worker crash → `kubectl delete pod --grace-period=0 --force <worker>` or a **Chaos Mesh PodChaos** experiment. The broker redelivers and the Deployment/KEDA recreates the pod.
- Scaling → KEDA automatically, or `kubectl scale`.
- Health → readiness gates, `/healthz` via the ingress.

---

## 28. IaC / Configuration Management

- **Ansible** (course default) `deploy/ansible/site.yml`, idempotent roles:
  - `prereqs`: docker, kind, kubectl, helm, (argocd CLI) on Ubuntu/macOS/WSL2
  - `cluster`: `kind create cluster --config …` (skipped if it exists)
  - `platform`: helm installs ingress-nginx, Argo CD, KEDA, kube-prometheus-stack, Loki, Tempo, Chaos Mesh, Sealed Secrets (versions pinned in `group_vars`)
  - `app`: apply Argo CD `Application`s (app-of-apps), which pull everything else from Git
  - `secrets`: generate/seal secrets from a local vault file (`ansible-vault`)
- **Helm** values per environment hold the app configuration.
- Optional: **Terraform** with `tehcyx/kind` + `helm` providers as an alternative; mention the trade-off in the report.
- Result: `make platform` (= `ansible-playbook -i inventory/local.ini site.yml`) takes a fresh laptop to the full platform plus the app, which is a reproducible viva demo.

---

## 29. Monitoring & Observability (SPE)

- **Metrics:** kube-prometheus-stack + app metrics (§16) + RabbitMQ plugin + KEDA metrics.
- **Logs:** Loki + Promtail/Alloy. Grafana Explore by `job_id` shows every log line of one job across pods.
- **Traces:** OpenTelemetry SDK in gateway and worker → OTel Collector → **Tempo**. Trace context travels **through RabbitMQ headers**, so one trace spans `POST /jobs` → transcribe (pod A) → summarize (pod B, with LLM spans: model, tokens, latency) → frames → publish (Notion spans). Exemplars link the metrics histograms to traces.
- **Dashboards as code** (`observability/grafana/dashboards/*.json`, loaded via ConfigMap sidecar):
  1. *Pipeline overview*: jobs/min by status, success rate, e2e p50/p95, per-stage latency heatmap
  2. *Queue & scaling*: queue depth per stage, unacked, consumers, **KEDA replica count overlaid on queue depth**
  3. *External APIs & LLMOps*: requests/429s by API, limiter wait, tokens/min vs configured limit, estimated cost/job
  4. *Reliability*: retries by reason, DLQ size, redeliveries, chaos experiment annotations
  5. *Platform*: node/pod CPU/mem (stock dashboards)
- **SLOs:** 95 % of ≤ 20-min jobs complete within 3 min; 99 % of jobs never lost (DLQ or terminal). Burn-rate alerts via PrometheusRule (Sloth/Pyrra, optional).
- Demo moment: push a deliberately slower build. The Grafana `version` panel switches and the latency panel moves. (With Argo Rollouts, an analysis rolls it back automatically; see §30.)

---

## 30. Cutting-Edge Extensions

The brief requires ≥ 1. We plan **3 core + 2 optional**, each tied to something real in the app rather than bolted on:

| # | Extension | Why it's natural here | Demo |
|---|---|---|---|
| ★1 | **KEDA event-driven autoscaling** (`ScaledObject` on RabbitMQ `stage.*` queue lengths; optionally per-stage pools via `WORKER_STAGES`) | Queue-backed worker pool is the core of the app; brief topic #25 names exactly this | Burst 10 jobs → workers 1 → 6 → back to 1; Grafana overlay. The global Redis limiter keeps LLM 429s at 0 even at 6 pods, which is a nice story ("autoscale compute, not API quota") |
| ★2 | **Chaos engineering with Chaos Mesh** (PodChaos kill worker mid-stage, NetworkChaos delay to RabbitMQ, StressChaos on worker) | The Cloud grader literally kills workers; we make it repeatable, scheduled, and observable | Run `chaos/worker-pod-kill.yaml` while jobs flow → zero lost jobs, one Notion page per job, recovery time on dashboard |
| ★3 | **OpenTelemetry end-to-end + LLM (GenAI) tracing** to Tempo, logs to Loki | Async multi-hop pipeline is a perfect showcase for context propagation through a broker; LLMOps token/cost observability | Click a job → full trace across pods incl. LLM calls with token counts |
| ☆4 | **Argo Rollouts canary** for the worker (or gateway) with Prometheus `AnalysisTemplate` (job failure rate, stage latency) | Prompt/model changes are risky, and a canary release of a new prompt version = LLMOps "prompt versioning + safe rollout" (topic #20) | Push a "bad prompt" version → analysis fails → automatic rollback |
| ☆5 | **DevSecOps supply chain**: Trivy gates, Syft SBOM, Cosign keyless signing, **Kyverno** policy verifying signatures + non-root at admission | Cheap to add in CI; strong report material | Unsigned image is rejected by the cluster |

If time is short: ★1 + ★3 are enough for full innovation marks. ★2 is low effort once the app is idempotent (it already is).

---

## 31. Laptop Resource Budget

| Component | Approx. RAM |
|---|---|
| kind nodes overhead (3 nodes) | 1.5 GB |
| App: gateway ×2, worker ×2 (idle) | 0.8 GB (workers spike ~300 MB each during ffmpeg) |
| RabbitMQ, Postgres, Redis | 0.8 GB |
| Argo CD | 0.6 GB |
| kube-prometheus-stack | 1.2 GB |
| Loki + Tempo + OTel collector | 0.8 GB |
| KEDA, Chaos Mesh, ingress | 0.5 GB |
| **Total** | **~6.5–7 GB** (+ Ollama model 2–5 GB if running a local LLM) |

**Minimum spec (documented in report):** 4 CPU cores, 16 GB RAM, 25 GB free disk; Docker Desktop/WSL2 or native Linux. A "lite" Ansible profile (no Loki/Tempo, 1 kind node) fits 8 GB.

---

## 32. Team-of-Two Work Split

Both members touch **both code and DevOps** (required). Example split. Swap freely, but keep it balanced in Git history.

| Area | Member A | Member B |
|---|---|---|
| App | Gateway, state machine, DB/migrations, broker topology, retry/DLQ, idempotent publish | Worker stages (transcript, LLM chunk/merge, frames, Notion), rate limiter, browser extension |
| DevOps | CI workflows (lint/test/build/scan), Helm chart, Argo CD GitOps, Argo Rollouts | Ansible bootstrap, observability stack + dashboards + alerts, KEDA, Chaos Mesh |
| Docs | Report: architecture, CI/CD, K8s | Report: IaC, observability, cutting-edge, challenges |
| Cross-review | Reviews B's PRs | Reviews A's PRs |

Practices: small PRs, at least one meaningful commit per person every couple of days, a pair-programming session per week (both understand everything for the viva), and a signed contribution statement listing PRs per person.

---

## 33. Combined Timeline (Both Courses)

Today = **Fri 06-Nov-2026**. SPE milestones from the brief; Cloud deadline TBD (adjust).

| Week | Dates | Cloud app (Part A) | SPE DevOps (Part B) |
|---|---|---|---|
| W1 | 06–12 Nov | P0–P2: skeleton, gateway, state machine, worker spine with stub stages, retry/DLQ | Repo + branch protection + CODEOWNERS; CI lint/unit/build; **proposal submitted ~11-Nov** (confirm dual-use with instructors) |
| W2 | 13–19 Nov | P3: real transcript/LLM/frames/Notion; P5 extension in parallel | Dockerfiles hardened; kind config; Helm chart v1; Ansible bootstrap v1; images to GHCR |
| W3 | 20–26 Nov | P4 rate limiting + fault injection; P6 hardening (crash-at-every-point) | Argo CD GitOps, push→deploy automated. **Milestone "pipeline working" ~25-Nov** ✅ |
| W4 | 27 Nov–03 Dec | P7 README, eval report, Cloud demo rehearsal (submit if due) | kube-prometheus-stack, Loki, Tempo, OTel, dashboards, alerts. **"Deployed + observable" ~02-Dec** ✅ KEDA |
| W5 | 04–10 Dec | Bug fixes only | Chaos Mesh, (Argo Rollouts / supply chain if time), report 4–6 pages, backup screen recording, contribution statement, viva prep. **Final demo 07–12 Dec** |

Critical path: worker spine (W1) → real stages (W2) → images on GHCR (W2) → GitOps (W3). Start the Helm chart against stub images in W2 so DevOps work doesn't wait on app features.

---

## 34. SPE Deliverables Checklist

- [ ] Topic + abstract registered on the shared sheet (kickoff)
- [ ] 1-page proposal (~11-Nov): app, architecture diagram, tools per stage, chosen extension(s), dual-use note
- [ ] Git repo: source, pipeline definitions, IaC (Ansible), Helm/K8s manifests, Argo CD apps, dashboards, chaos experiments
- [ ] Automated flow: single push → build → test → image → deploy to local cluster → observable
- [ ] Final report (4–6 pages PDF): architecture diagram, tool choices + rationale, cutting-edge extension, challenges, min laptop specs
- [ ] Live demo script (push a visible change, e.g. a popup version label or a Notion page footer version; watch it flow; show dashboards; chaos kill; KEDA scale)
- [ ] Screen-recorded backup of the full pipeline run
- [ ] Individual contribution statement signed by both
- [ ] Both members rehearsed for individual viva (each can explain every stage)

---

## 35. Draft SPE Abstract & Proposal Outline

**Abstract (for the registration sheet):**
> *Video Notes Agent: an LLMOps platform on local Kubernetes.* We build a distributed, queue-driven application. A Chrome extension submits YouTube videos. Stateless workers transcribe them, summarise with an LLM, capture real video frames of key slides, and publish structured study notes to Notion. Around it we engineer a fully automated DevOps lifecycle: GitHub-based collaboration, GitHub Actions CI (tests, image builds, Trivy scanning, SBOM), GitOps continuous delivery with Argo CD onto a kind cluster provisioned by Ansible and Helm, and observability with Prometheus, Grafana, Loki and OpenTelemetry/Tempo, including LLM token and cost tracing. The cutting-edge extensions are KEDA event-driven autoscaling of workers on RabbitMQ queue depth and Chaos Mesh experiments that prove no job is lost when worker pods are killed mid-task. Everything runs on a laptop at zero cost: the LLM is the Gemini free tier, or a local Ollama model served inside the cluster for a fully offline demo.

**Proposal outline (1 page):** Problem & app (3 lines) · Architecture diagram (app + platform) · Tool per DevOps stage (table from §25) · Cutting-edge: KEDA + Chaos Mesh + OTel (and why) · Laptop constraints & local LLM fallback · Milestone plan (§33) · Team split (§32) · Note on shared codebase with the Cloud course (approved by …).

---

# Appendix

### A. Status → popup text map (excerpt)

| status / code | Popup text |
|---|---|
| QUEUED | "Queued, waiting for a free worker…" |
| TRANSCRIBING | "Fetching the transcript…" |
| SUMMARIZING | "Writing your notes…" |
| EXTRACTING_FRAMES | "Capturing key frames (3 of 6)…" |
| PUBLISHING | "Publishing to Notion…" |
| `retry.reason=RATE_LIMITED` | "⏳ {API} is busy, retrying in {n}s (attempt {a} of {max})" |
| COMPLETED | "✓ Done" + View in Notion |
| COMPLETED_WITH_WARNINGS | "⚠ Done, {k} of {m} frames couldn't be captured" + details |
| FAILED / VIDEO_TOO_LONG | "This video is longer than 20 minutes, which isn't supported." |
| FAILED / TRANSCRIPT_UNAVAILABLE | "This video has no captions we could use." |
| FAILED / NOTION_UNAUTHORIZED | "Notion access was revoked. Reconnect." |
| DEAD_LETTERED | "✗ We tried {n} times and couldn't process this video. It may be private or removed." |

### B. Warning & error code catalogue
`VIDEO_TOO_LONG`, `LIVE_STREAM_UNSUPPORTED`, `VIDEO_UNAVAILABLE`, `TRANSCRIPT_UNAVAILABLE`, `LLM_INVALID_OUTPUT`, `LLM_TIMESTAMP_OUT_OF_RANGE` (warning), `FRAME_EXTRACTION_FAILED` (warning), `FRAME_BLANK` (warning), `FRAME_NOT_RELEVANT` (warning), `FRAME_UPLOAD_FAILED` (warning), `STREAM_UNAVAILABLE` (warning), `NOTION_UNAUTHORIZED`, `NOTION_NO_DESTINATION`, `NOTION_PARENT_NOT_FOUND`, `RATE_LIMIT_BUDGET_EXHAUSTED`, `RETRY_BUDGET_EXHAUSTED`, `CRASH_LOOP`, `ENQUEUE_FAILED`, `INVALID_REQUEST`, `INVALID_YOUTUBE_URL`, `UNSUPPORTED_HOST`, `MISSING_NOTION_TOKEN`, `INVALID_NOTION_PARENT_ID`, `JOB_NOT_FOUND`, `JOB_NOT_FINISHED`.

### C. Synthesis prompt skeleton (to be refined during P3)
```
SYSTEM:
You turn lecture/video transcripts into accurate study notes.
Rules:
1. Use only information in the transcript. No outside facts.
2. Group content into sections in chronological order with concise headings.
3. Bullets: 3–7 per section, each ≤ 25 words, information-dense.
4. visual_moments: flag a timestamp ONLY when the transcript shows evidence that
   something visual and informative is on screen (slide, diagram, chart, equation, code,
   whiteboard, demo). Pick a time 1–3 s after the cue. Must lie in [{chunk_start},{chunk_end}].
5. For each moment give `caption` (what is most likely shown) and `why_flagged`
   (quote or paraphrase the transcript evidence).
6. At most {max_frames} moments; importance 1–5.
Return by calling the `record_notes` tool.

USER:
Video: "{title}"   Chunk {i}/{n} covering {start}–{end}
Chapters in range: {chapters}
Previous section heading: {prev_heading}
<transcript>
[00:00] ...
[00:07] ...
</transcript>
```

### D. Open questions to settle early
1. Cloud project deadline and team composition. Does it match the SPE team?
2. Instructor approval for shared codebase across both courses.
3. Current Gemini free-tier limits for the chosen Flash model (set `LLM_RPM`/`LLM_TPM`/`LLM_RPD` at ~80 %), and which Ollama models fit our laptops' RAM for the offline/SPE demo (benchmark a 4B vs 7–8B model on a 20-min transcript).
4. Notion: public OAuth integration (needs a redirect URI and a stable extension ID key) vs internal-token fallback for graders.
5. Whether graders run the extension against `localhost:8080` (Compose) or the kind ingress, so both should be supported via the options page.
