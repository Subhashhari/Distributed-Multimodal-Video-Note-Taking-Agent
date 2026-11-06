"""Serialize jobs into the response shapes required by the spec (§5)."""

from __future__ import annotations

from datetime import datetime

from ..common.config import Settings
from ..common.db import Job
from ..common.repo import as_utc
from ..common.states import SUCCESS, TERMINAL

PUBLIC_RESULT_KEYS = ("notion_page_url", "frames_requested", "frames_published")


def iso(dt: datetime | None) -> str | None:
    dt = as_utc(dt)
    return dt.isoformat().replace("+00:00", "Z") if dt else None


def public_result(job: Job) -> dict | None:
    if job.status not in SUCCESS or not job.result:
        return None
    return {k: job.result[k] for k in PUBLIC_RESULT_KEYS if k in job.result}


def retry_view(job: Job, settings: Settings) -> dict | None:
    if job.status in TERMINAL or job.next_retry_at is None:
        return None
    rate_limited = job.retry_reason == "RATE_LIMITED"
    return {
        "waiting": True,
        "reason": job.retry_reason,
        "api": job.retry_api,
        "attempt": job.rl_attempt if rate_limited else job.attempt,
        "max_attempts": settings.rl_max_retries if rate_limited else settings.max_attempts,
        "next_retry_at": iso(job.next_retry_at),
    }


def status_view(job: Job, settings: Settings) -> dict:
    return {
        "job_id": str(job.id),
        "status": job.status,
        "progress": job.progress,
        "video_id": job.video_id,
        "video_title": job.video_title,
        "duration_sec": job.duration_sec,
        "warnings": job.warnings or [],
        "error": job.error,
        "result": public_result(job),
        "retry": retry_view(job, settings),
        "created_at": iso(job.created_at),
        "updated_at": iso(job.updated_at),
    }


def results_view(job: Job) -> dict:
    body: dict = {"status": job.status, "result": public_result(job), "warnings": job.warnings or []}
    if job.status not in SUCCESS:
        body["error"] = job.error
    return body
