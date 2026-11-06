"""Prometheus metrics for the worker (served on :9100/metrics)."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

STAGE_SECONDS = Histogram(
    "notes_stage_duration_seconds", "Time spent running a stage", ["stage", "outcome"],
    buckets=(0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600),
)
STAGE_OUTCOMES = Counter("notes_stage_outcomes_total", "Stage message outcomes", ["stage", "outcome"])
JOBS_TERMINAL = Counter("notes_jobs_terminal_total", "Jobs reaching a terminal status", ["status"])
RETRIES = Counter("notes_retries_scheduled_total", "Retries scheduled", ["stage", "reason", "api"])
REDELIVERIES = Counter("notes_redeliveries_total", "Messages redelivered after a worker died", ["stage"])

EXTERNAL_REQUESTS = Counter("notes_external_requests_total", "Calls to external APIs", ["api", "status"])
EXTERNAL_429 = Counter("notes_external_429_total", "HTTP 429 responses from external APIs", ["api"])
LIMITER_WAIT = Histogram(
    "notes_limiter_wait_seconds", "Time spent waiting for shared rate-limit capacity", ["api"],
    buckets=(0, 0.1, 0.5, 1, 2, 5, 10, 20, 30),
)
SELF_THROTTLED = Counter("notes_self_throttled_total", "Stages parked because shared capacity was full", ["api"])
LLM_TOKENS = Counter("notes_llm_tokens_total", "LLM tokens used", ["direction"])
FRAMES = Counter("notes_frames_total", "Frame outcomes", ["outcome"])
WORKER_BUSY = Gauge("notes_worker_busy", "1 while this worker is processing a message")
