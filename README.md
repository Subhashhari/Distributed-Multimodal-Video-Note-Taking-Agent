# Distributed Multimodal Video Note-Taking Agent

Click **Take Notes** on a YouTube video. A Notion page appears with structured study notes and **real
frames captured from the video**. Each frame has an AI-written caption explaining what it shows and why it
matters.

```
Chrome extension ──HTTP──▶ API Gateway ──▶ RabbitMQ ──▶ Worker × N ──▶ YouTube · LLM · ffmpeg · Notion
   (button, popup)          (FastAPI)      (durable       (pull one stage at a time,
                               │            queues, DLQ)    ack only after durable commit)
                               └──── PostgreSQL (job state) ──── Redis (global rate limits, heartbeats)
```

| Component | Container | Role |
|---|---|---|
| Browser extension | (Chrome) | Take Notes button, one-time Notion login, progress popup |
| API Gateway | `gateway` | Validates and enqueues jobs, serves status/results. **No pipeline work** |
| Message broker | `rabbitmq` | Durable quorum queues, at-least-once delivery, retry delay queues, dead-letter queue |
| Worker pool | `worker` × 2+ | Transcribe → Summarize → Extract frames → Publish |
| State / coordination | `postgres`, `redis` | Job state machine and audit log; shared rate limiter |

---

## 1. Quick start (5 minutes, no API keys)

Prerequisites: **Docker Desktop** (Compose v2), **Chrome**, Python 3.10+ (only for the demo script).

```bash
cp .env.example .env            # defaults work for stub mode
docker compose up -d --build    # gateway, rabbitmq, postgres, redis, migrate, 2 workers
python scripts/demo.py health   # or: curl http://localhost:8080/healthz
python scripts/demo.py submit https://youtu.be/dQw4w9WgXcQ --watch
```

In `.env`, `MESSAGE_ENC_KEY` and `ADMIN_TOKEN` must be real values. Generate them with the commands shown in `.env.example`.

`PIPELINE_MODE=stub` runs the whole distributed system (queues, retries, crash recovery, DLQ) with fake
transcript, LLM and Notion steps. Switch to `PIPELINE_MODE=real` once the keys below are set.

- **Base URL:** `http://localhost:8080` (port via `GATEWAY_PORT`)
- **API docs:** <http://localhost:8080/docs>
- **RabbitMQ UI:** <http://localhost:15672> (`notes` / `notes`): queues, unacked messages, `jobs.dlq`
- **Grafana (optional):** `docker compose --profile obs up -d` then <http://localhost:3000>

## 2. Real mode setup

### 2.1 LLM (free: Gemini API free tier)
1. <https://aistudio.google.com> → **Get API key** → create a key.
2. In `.env`: `LLM_PROVIDER=gemini`, `LLM_API_KEY=<key>`, `LLM_MODEL=<a current Flash model id>`.
3. Set `LLM_RPM`, `LLM_TPM`, `LLM_RPD` to about 80 % of that model's free-tier limits (shown in AI Studio).
   All workers share these limits, so a burst of jobs queues up instead of causing provider 429s.

No paid API is needed. Alternatives: `LLM_PROVIDER=ollama` (local and offline, see §6),
`openai_compat` (Groq, OpenRouter, GitHub Models and similar, via `LLM_BASE_URL`), or `mock`.

### 2.2 Notion
1. In Notion, create a page called **Video Notes**.
2. <https://www.notion.so/my-integrations> → **New integration** → type **Public**.
   - Redirect URI: `https://cfaokfnedgejbfoelphincfdjinidhpb.chromiumapp.org/notion`
     (the extension ships with a fixed key, so this ID is the same on every machine)
   - Capabilities: Read, Update, Insert content.
3. Copy the OAuth **client ID** and **client secret** into `NOTION_OAUTH_CLIENT_ID` and `NOTION_OAUTH_CLIENT_SECRET`.
4. Set `PIPELINE_MODE=real`, then run `docker compose up -d` (this recreates the containers with the new values).

*Quicker alternative for graders:* create an **Internal** integration, share the Video Notes page with it
(page `•••` → Connections), and paste its token in the extension's **Options** page.

### 2.3 Load the extension (no build step)
1. Open `chrome://extensions` and enable **Developer mode**.
2. Click **Load unpacked** and choose the `extension/` folder.
3. Pin **Video Notes** and click it, then click **Connect Notion** (one time) and pick your **Video Notes** page.
4. Open any YouTube video and click the 📝 **Take Notes** button in the player toolbar.

If the gateway isn't on `localhost:8080`, set its URL in the extension's Options page.

### 2.4 Default Notion destination
If a job has no `notion_parent_id`, the page is created under the first of these that applies:
1. the page chosen in the extension (Options → *Publish notes under*), sent with each job;
2. `NOTION_DEFAULT_PARENT_ID` on the server, if set;
3. otherwise, the most recently edited **top-level page shared with the integration**. That is the page
   you picked in Notion's OAuth page picker.

## 3. API

| Method & path | Description |
|---|---|
| `POST /jobs` | `{youtube_url, notion_token, notion_parent_id?}` returns **202** with a `Location` header and `{job_id, status:"QUEUED", video_id}`. Optional: `client_duration_sec`, `client_title`, header `Idempotency-Key` |
| `GET /jobs/{id}` | `{job_id, status, progress, video_title, duration_sec, warnings, error, result, retry}`. Read-only; **404** if unknown |
| `GET /jobs/{id}/results` | Final outcome with an absolute `notion_page_url`, `frames_requested`, `frames_published`, warnings. **409** until the job is terminal |
| `GET /healthz` | Checks gateway, Postgres, RabbitMQ and Redis; lists live workers and remaining daily LLM quota. **503** if a dependency is down |
| `GET /metrics` | Prometheus metrics (workers expose theirs on `:9100`) |
| `POST /oauth/notion/exchange` | Used by the extension: swaps an OAuth code for a token. The client secret stays on the server and **nothing is stored** |
| `GET /admin/dlq` · `POST /admin/dlq/{id}/replay` | Inspect and replay dead-lettered jobs (`Authorization: Bearer $ADMIN_TOKEN`) |
| `GET /admin/jobs/{id}/events` | Audit log: every transition, retry, redelivery and the worker that handled it |
| `GET/POST/DELETE /admin/faults` | Demo fault injection (`DEMO_FAULTS_ENABLED=true`) |

Errors always look like `{"error": {"code": "INVALID_YOUTUBE_URL", "message": "..."}}`.

Statuses: `QUEUED → TRANSCRIBING → SUMMARIZING → EXTRACTING_FRAMES → PUBLISHING → COMPLETED |
COMPLETED_WITH_WARNINGS`. Any non-terminal status can go to `FAILED` (non-retryable) or `DEAD_LETTERED`
(retry budget exhausted).

## 4. Demo runbook: the four required failure modes

Use `python scripts/demo.py ...` (standard library only; it reads `ADMIN_TOKEN` from `.env`). Clear faults
afterwards with `python scripts/demo.py faults clear`.

| Scenario | Command | What you should see |
|---|---|---|
| **HTTP 429** from any API | `demo.py faults set llm_429=2` (or `notion_429`, `youtube_429`), then submit a video | Popup shows *"The AI service is busy, retrying in 10s (attempt 1 of 6)"*, then 30 s, then the job completes. It is never shown as an error |
| **Worker crash** | `demo.py crash https://youtu.be/<id>` | Script waits for EXTRACTING_FRAMES and SIGKILLs that worker container. Another worker resumes within ~1 s (`REDELIVERED` event) and finishes. Exactly **one** Notion page. Restart with `docker compose up -d worker` |
| **Graceful degradation** | `demo.py faults set frame_fail_rate=0.5` (1.0 = all frames) or Options → *force frame timestamps to fail* | `COMPLETED_WITH_WARNINGS` with a page containing the frames that succeeded (text-only if all failed), and the popup lists which timestamps were omitted |
| **Dead-letter queue** | Submit a private/removed video, or `demo.py submit <url> --unavailable` | Retries 1/5 … 5/5, then `DEAD_LETTERED`. The message sits in `jobs.dlq` (RabbitMQ UI); `demo.py dlq` lists it |

Set `RETRY_PROFILE=demo` in `.env` for a 2 s / 4 s / 8 s backoff, which takes the DLQ demo to about 25 s.
Production backoff is 10 s / 30 s / 90 s.

Other checks:
- **Scaling:** `docker compose up -d --scale worker=4`. Work spreads evenly, and the global LLM limit still holds.
- **Burst:** click Take Notes on several videos in a row, or `demo.py burst <url1> <url2> ...`. The queue
  fills and drains, and Grafana's *HTTP 429s* panel stays at 0 for `llm`.
- **Constraint A (≤ 20 min):** a longer video is rejected immediately by the extension or gateway (`VIDEO_TOO_LONG`).
  The worker re-checks the true duration before any LLM or frame work.
- **Constraint B (real frames):** images come only from `ffmpeg` decoding the source stream at the
  timestamp. Failed frames are omitted, never substituted. Every image has a caption saying what it shows
  and why it was flagged.
- **Kill other components:** `docker compose stop gateway` (workers keep going), `docker compose restart rabbitmq`
  (messages persist and clients reconnect).

## 5. Configuration (all environment variables)

Everything is documented inline in [.env.example](.env.example). The important ones:

| Variable | Default | Purpose |
|---|---|---|
| `PIPELINE_MODE` | `stub` | `stub` (no keys) or `real` |
| `LLM_PROVIDER` / `LLM_API_KEY` / `LLM_MODEL` | `gemini` / – / – | **LLM provider, API key, model name** |
| `LLM_RPM` / `LLM_TPM` / `LLM_RPD` | `8` / `200000` / `200` | **Configured LLM rate limit**, global across workers (per min / tokens per min / per day) |
| `LLM_MAX_CONCURRENCY` | `2` | Max in-flight LLM calls cluster-wide |
| `LLM_BASE_URL`, `LLM_FALLBACK_PROVIDER` | – | For ollama/openai_compat; fallback when the daily quota runs out |
| `VISION_VERIFY` / `VISION_BATCH_SIZE` | `true` / `8` | Ground frame captions with one batched vision call per job |
| `NOTION_OAUTH_CLIENT_ID` / `NOTION_OAUTH_CLIENT_SECRET` / `NOTION_OAUTH_REDIRECT_URI` | – | **Notion OAuth client credentials** |
| `NOTION_DEFAULT_PARENT_ID` | – | Optional default destination (§2.4) |
| `NOTION_RPS` | `2.5` | Notion rate limit (Notion allows ~3 req/s) |
| `BROKER_URL` | `amqp://notes:notes@rabbitmq:5672/` | **Broker connection string** |
| `DATABASE_URL` / `REDIS_URL` | compose defaults | State store / coordination store |
| `MAX_ATTEMPTS` | `5` | **Retry budget** for transient failures, then DLQ |
| `RL_MAX_RETRIES` | `6` | Retry budget for HTTP 429s per stage |
| `RETRY_PROFILE` | `prod` | `prod` uses `BACKOFF_SCHEDULE_SEC`, `demo` uses `DEMO_BACKOFF_SCHEDULE_SEC` |
| `BACKOFF_SCHEDULE_SEC` / `DEMO_BACKOFF_SCHEDULE_SEC` | `10,30,90` / `2,4,8` | **Backoff parameters** (exponential tiers) |
| `BROKER_DELIVERY_LIMIT` | `5` | Crash-loop protection (quorum queue delivery limit) |
| `MAX_VIDEO_SECONDS` | `1200` | Constraint A |
| `MAX_FRAMES` / `MIN_FRAME_GAP_SEC` | `8` / `20` | Frame selection |
| `MESSAGE_ENC_KEY` | – (required) | Encrypts the Notion token inside queue messages |
| `ADMIN_TOKEN` / `DEMO_FAULTS_ENABLED` | – / `true` | Admin endpoints / fault injection |
| `YTDLP_COOKIES_FILE` | – | Only if YouTube asks to "confirm you're not a bot" |

## 6. Optional extras

- **Monitoring:** `docker compose --profile obs up -d` starts Prometheus (`:9090`) and Grafana (`:3000`).
  The *Video Notes Pipeline* dashboard shows queue depth per stage, 429s per API, retries, stage latency,
  limiter wait, LLM tokens/min, workers up/busy and redeliveries.
- **Local LLM (offline, free):** `docker compose --profile local-llm up -d`, then
  `docker compose exec ollama ollama pull <model>` and set `LLM_PROVIDER=ollama`, `LLM_MODEL=<model>`,
  `LLM_BASE_URL=http://ollama:11434`. Use a vision-capable model or set `VISION_VERIFY=false`.

## 7. Design choices (details in [PLAN.md](PLAN.md))

- **RabbitMQ quorum queues:** an unacked message is redelivered as soon as a worker's connection drops.
  `x-delivery-limit` catches crash loops. TTL queues give backoff with no plugins.
- **One message per stage.** A worker commits the stage's output to Postgres, publishes the next message
  (publisher-confirmed), and **only then acks**. A crash re-runs only the interrupted stage, and every stage
  skips work already saved (frames, chunks, uploads).
- **No duplicate Notion pages:** the page title carries a short job marker. If a worker dies right after
  Notion created the page, the retry finds that page instead of creating another. Partially filled pages are
  rebuilt.
- **Global rate limits:** Redis sliding windows (atomic Lua) shared by all workers for LLM (RPM, TPM, per
  day, concurrency), Notion and YouTube. When capacity is full, a stage is parked in a delay queue rather
  than sent to the provider to get a 429.
- **Workers pull** one message at a time, later stages first, so in-flight jobs finish before new ones start.
- **Notion token** is sent by the extension with each job, travels encrypted inside queue messages, and is
  never stored or logged.

## 8. Development

```bash
python -m venv .venv && .venv/Scripts/activate     # macOS/Linux: source .venv/bin/activate
pip install -e ".[dev]"
pytest                       # 90+ tests: unit, API contract, worker integration, real stages with fakes
node --test tests/extension  # extension status-text tests
ruff check src tests
```

## 9. Troubleshooting

| Symptom | Fix |
|---|---|
| `/healthz` shows `workers.alive: 0` | `docker compose logs worker`. Usually `MESSAGE_ENC_KEY` is missing or invalid |
| "Notion page wasn't found" | Share the page with the integration (page `•••` → Connections) |
| "YouTube blocked the request (bot check)" | Export browser cookies to a file, mount it, and set `YTDLP_COOKIES_FILE` |
| Jobs wait with "AI service is busy" | Expected under load: the shared limiter is pacing calls. Raise `LLM_RPM` if your quota allows |
| "daily free quota is used up" | Wait for the reset, set `LLM_FALLBACK_PROVIDER=ollama`, or use another key |
