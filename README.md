# Distributed Multimodal Video Note-Taking Agent

Turns a YouTube video into structured study notes (summary plus real, captioned video frames) and publishes
them to Notion. A browser extension drives it. Behind the extension is an API gateway, a RabbitMQ queue, and a pool of workers that
respect every external API's rate limits.

> **Status:** phases P0–P2 are done: gateway, job state machine, worker pool, retries/backoff, DLQ,
> crash recovery, and fault injection, running end-to-end in **stub mode** (fake transcript/LLM/Notion).
> Real YouTube/LLM/ffmpeg/Notion stages (P3) and the browser extension (P5) come next. See [PLAN.md](PLAN.md).

## Architecture

```
Chrome extension ──HTTP──▶ Gateway (FastAPI) ──▶ RabbitMQ ──▶ Worker × N ──▶ YouTube / LLM / Notion
                               │                                │
                               └──────── PostgreSQL (job state) ┘── Redis (rate limits, heartbeats)
```

Each pipeline stage (`transcribe → summarize → frames → publish`) is its own queue message. A worker
acknowledges a message only after the stage's output is committed and the next message is confirmed by the
broker. So if a worker dies, the broker redelivers its message to another worker, which resumes the stage.

## Quick start

Prerequisites: Docker Desktop (with Compose v2).

```bash
cp .env.example .env          # then fill in every __FILL_ME__ value (see table below)
docker compose up -d --build  # gateway + rabbitmq + postgres + redis + migrate + 2 workers
curl http://localhost:8080/healthz
```

- **Base URL:** `http://localhost:8080` (change the port with `GATEWAY_PORT`)
- **RabbitMQ UI:** <http://localhost:15672> (user/pass from `.env`, default `notes`/`notes`)
- **API docs:** <http://localhost:8080/docs>

In `PIPELINE_MODE=stub` (the current default) no API keys are needed. Any `notion_token` of 20+ characters is accepted.

### Scaling and crash recovery

```bash
docker compose up -d --scale worker=4          # more workers
docker compose ps                               # find a worker container name
docker kill -s KILL video-notes-worker-1        # SIGKILL mid-job → another worker resumes it
docker compose up -d worker                     # bring it back
```

## API

| Method & path | Description |
|---|---|
| `POST /jobs` | Submit `{youtube_url, notion_token, notion_parent_id?}` → `202` + `Location`. Optional header `Idempotency-Key` |
| `GET /jobs/{id}` | Status, progress, warnings, error, `retry` (when waiting on backoff). Read-only |
| `GET /jobs/{id}/results` | Final outcome; `409` until the job is terminal |
| `GET /healthz` | Checks gateway, Postgres, RabbitMQ, Redis; lists live workers |
| `POST /oauth/notion/exchange` | Exchanges a Notion OAuth code for a token (used by the extension; nothing stored) |
| `GET /admin/dlq`, `POST /admin/dlq/{id}/replay` | Inspect / replay dead-lettered jobs (`Authorization: Bearer $ADMIN_TOKEN`) |
| `GET /admin/jobs/{id}/events` | Full audit log of a job (transitions, retries, redeliveries, which worker) |
| `GET/POST/DELETE /admin/faults` | Demo fault injection (requires `DEMO_FAULTS_ENABLED=true`) |

Errors always look like `{"error": {"code": "...", "message": "..."}}`.

## Default Notion destination

If a job has no `notion_parent_id`, the page is created under, in order:
1. `NOTION_DEFAULT_PARENT_ID` if set;
2. otherwise the page the user shared with the integration during the OAuth flow (the most recently edited
   top-level page the integration can access).

(Implemented with the real Notion stage in phase P3.)

## Failure handling: demo commands

```bash
ADMIN="Authorization: Bearer <ADMIN_TOKEN from .env>"

# 429 from an external API: next 2 LLM calls return HTTP 429, so the job waits 10 s then 30 s and completes
curl -X POST localhost:8080/admin/faults -H "$ADMIN" -H 'Content-Type: application/json' -d '{"llm_429":2}'

# Worker crash: slow each stage down, then SIGKILL the worker processing it
curl -X POST localhost:8080/admin/faults -H "$ADMIN" -H 'Content-Type: application/json' -d '{"stage_delay_sec":5}'

# Graceful degradation: half the frames fail, giving COMPLETED_WITH_WARNINGS (1.0 = text-only page)
curl -X POST localhost:8080/admin/faults -H "$ADMIN" -H 'Content-Type: application/json' -d '{"frame_fail_rate":0.5}'

# Dead-letter queue: per-job debug flag simulates an unavailable video (use RETRY_PROFILE=demo for speed)
curl -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
  -d '{"youtube_url":"https://youtu.be/dQw4w9WgXcQ","notion_token":"ntn_xxxxxxxxxxxxxxxxxxxx","debug":{"unavailable":true}}'

curl -X DELETE localhost:8080/admin/faults -H "$ADMIN"   # clear all faults
```

Backoff schedule: `BACKOFF_SCHEDULE_SEC=10,30,90` (prod) or `DEMO_BACKOFF_SCHEDULE_SEC=2,4,8` with
`RETRY_PROFILE=demo`. Transient failures dead-letter after `MAX_ATTEMPTS=5`. Rate-limit retries have their own
budget, `RL_MAX_RETRIES=6`.

## Environment variables

All variables are documented inline in [.env.example](.env.example). The ones you must set:

| Variable | Purpose |
|---|---|
| `MESSAGE_ENC_KEY` | Encrypts the Notion token inside queue messages (generate with the command in `.env.example`) |
| `ADMIN_TOKEN` | Protects `/admin/*` |
| `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL` | LLM provider, key and model (default: Gemini free tier) |
| `LLM_RPM`, `LLM_TPM`, `LLM_RPD` | Global LLM rate limit shared by all workers (≈80 % of provider limits) |
| `NOTION_OAUTH_CLIENT_ID`, `NOTION_OAUTH_CLIENT_SECRET`, `NOTION_OAUTH_REDIRECT_URI` | Notion OAuth app |
| `BROKER_URL` | RabbitMQ connection string |
| `DATABASE_URL`, `REDIS_URL` | State store and coordination store |
| `MAX_ATTEMPTS`, `RL_MAX_RETRIES`, `RETRY_PROFILE`, `BACKOFF_SCHEDULE_SEC`, `DEMO_BACKOFF_SCHEDULE_SEC` | Retry / backoff parameters |
| `MAX_VIDEO_SECONDS` | Video length cap (1200 = 20 min) |

## Development

```bash
python -m venv .venv && .venv/Scripts/activate      # Windows (use bin/activate on macOS/Linux)
pip install -e ".[dev]"
pytest                                               # unit + API contract + worker integration tests
ruff check src tests
```

Tests use SQLite, fakeredis and an in-memory broker, so no Docker or API keys are needed.
