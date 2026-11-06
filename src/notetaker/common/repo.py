"""All job-state reads/writes. Every state change goes through here.

Correctness rules (see PLAN.md §5, §11):
  * Transitions are checked against the state machine under a row lock (SELECT ... FOR UPDATE).
  * Terminal jobs are immutable (except an explicit admin replay).
  * Progress is monotonic.
  * Reads never write.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from .db import Database, Frame, Job, JobEvent, StageArtifact, utcnow
from .states import (
    PROGRESS,
    STAGE_STATUS,
    STAGES,
    TERMINAL,
    Status,
    pipeline_index,
    prev_status,
)


class ClaimOutcome(StrEnum):
    CLAIMED = "CLAIMED"  # first time this stage runs for the job
    RESUMED = "RESUMED"  # redelivery / retry of a stage already in progress
    ALREADY_PAST = "ALREADY_PAST"  # duplicate message; job already moved on
    TERMINAL = "TERMINAL"
    NOT_FOUND = "NOT_FOUND"
    NOT_READY = "NOT_READY"  # out-of-order message (should not happen)


def as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


# --------------------------------------------------------------------------- gateway side
async def create_job(
    db: Database,
    *,
    video_id: str,
    youtube_url: str,
    notion_parent_id: str | None,
    idempotency_key: str | None,
    video_title: str | None = None,
    duration_sec: int | None = None,
) -> tuple[Job, bool]:
    """Insert a QUEUED job. Returns (job, created). Same idempotency key → existing job."""
    if idempotency_key:
        existing = await get_job_by_idempotency_key(db, idempotency_key)
        if existing:
            return existing, False
    job = Job(
        id=uuid.uuid4(),
        video_id=video_id,
        youtube_url=youtube_url,
        status=Status.QUEUED,
        progress=0,
        video_title=video_title,
        duration_sec=duration_sec,
        notion_parent_id=notion_parent_id,
        idempotency_key=idempotency_key,
        warnings=[],
    )
    try:
        async with db.session() as s, s.begin():
            s.add(job)
            await s.flush()
            s.add(JobEvent(job_id=job.id, kind="TRANSITION", from_status=None, to_status=Status.QUEUED))
    except IntegrityError:
        if idempotency_key:
            existing = await get_job_by_idempotency_key(db, idempotency_key)
            if existing:
                return existing, False
        raise
    return job, True


async def get_job(db: Database, job_id: uuid.UUID) -> Job | None:
    async with db.session() as s:
        return await s.get(Job, job_id)


async def get_job_by_idempotency_key(db: Database, key: str) -> Job | None:
    async with db.session() as s:
        return (await s.execute(select(Job).where(Job.idempotency_key == key))).scalar_one_or_none()


async def list_jobs_by_status(db: Database, status: str, limit: int = 100) -> list[Job]:
    async with db.session() as s:
        rows = await s.execute(
            select(Job).where(Job.status == status).order_by(Job.updated_at.desc()).limit(limit)
        )
        return list(rows.scalars())


async def list_events(db: Database, job_id: uuid.UUID) -> list[JobEvent]:
    async with db.session() as s:
        rows = await s.execute(select(JobEvent).where(JobEvent.job_id == job_id).order_by(JobEvent.id))
        return list(rows.scalars())


# --------------------------------------------------------------------------- worker side
async def claim_stage(
    db: Database, job_id: uuid.UUID, stage: str, worker_id: str, *, redelivered: bool = False
) -> tuple[ClaimOutcome, Job | None]:
    target = STAGE_STATUS[stage]
    before = prev_status(stage)
    async with db.session() as s, s.begin():
        job = await s.get(Job, job_id, with_for_update=True)
        if job is None:
            return ClaimOutcome.NOT_FOUND, None
        if job.status in TERMINAL:
            return ClaimOutcome.TERMINAL, job
        cur, tgt = pipeline_index(job.status), pipeline_index(target)
        if cur > tgt:
            return ClaimOutcome.ALREADY_PAST, job
        if job.status not in (before, target):
            return ClaimOutcome.NOT_READY, job

        first_time = job.status == before
        previous_worker = job.worker_id
        job.status = target
        job.worker_id = worker_id
        job.retry_reason = None
        job.retry_api = None
        job.next_retry_at = None
        if first_time:
            job.rl_attempt = 0
            job.progress = max(job.progress, PROGRESS[stage][0])
            s.add(JobEvent(job_id=job.id, kind="TRANSITION", from_status=before, to_status=target,
                           worker_id=worker_id))
        else:
            s.add(JobEvent(job_id=job.id, kind="REDELIVERED" if redelivered else "RESUMED",
                           from_status=target, to_status=target, worker_id=worker_id,
                           detail={"previous_worker": previous_worker}))
        return (ClaimOutcome.CLAIMED if first_time else ClaimOutcome.RESUMED), job


async def set_progress(db: Database, job_id: uuid.UUID, value: int) -> None:
    async with db.session() as s, s.begin():
        job = await s.get(Job, job_id, with_for_update=True)
        if job and job.status not in TERMINAL and value > job.progress:
            job.progress = min(value, 99)


async def update_job(db: Database, job_id: uuid.UUID, **fields: Any) -> None:
    allowed = {"video_title", "duration_sec", "publish_state", "result"}
    bad = set(fields) - allowed
    if bad:
        raise ValueError(f"update_job cannot set {bad}")
    async with db.session() as s, s.begin():
        job = await s.get(Job, job_id, with_for_update=True)
        if job and job.status not in TERMINAL:
            for k, v in fields.items():
                setattr(job, k, v)


async def add_warning(db: Database, job_id: uuid.UUID, warning: dict) -> None:
    async with db.session() as s, s.begin():
        job = await s.get(Job, job_id, with_for_update=True)
        if job and job.status not in TERMINAL:
            _append_warning(s, job, warning)


def _append_warning(s, job: Job, warning: dict) -> None:
    job.warnings = [*(job.warnings or []), warning]
    s.add(JobEvent(job_id=job.id, kind="WARNING", detail=warning, worker_id=job.worker_id))


async def schedule_retry(
    db: Database,
    job_id: uuid.UUID,
    *,
    stage: str,
    reason: str,
    api: str | None,
    attempt: int,
    rl_attempt: int,
    delay_sec: int,
    code: str,
    message: str,
) -> None:
    async with db.session() as s, s.begin():
        job = await s.get(Job, job_id, with_for_update=True)
        if job is None or job.status in TERMINAL:
            return
        job.attempt = attempt
        job.rl_attempt = rl_attempt
        job.retry_reason = reason
        job.retry_api = api
        job.next_retry_at = utcnow() + timedelta(seconds=delay_sec)
        s.add(JobEvent(job_id=job.id, kind="RETRY_SCHEDULED", from_status=job.status, to_status=job.status,
                       worker_id=job.worker_id,
                       detail={"stage": stage, "reason": reason, "api": api, "attempt": attempt,
                               "rl_attempt": rl_attempt, "delay_sec": delay_sec, "code": code,
                               "message": message[:500]}))


async def _terminate(db: Database, job_id: uuid.UUID, status: Status, error: dict) -> bool:
    async with db.session() as s, s.begin():
        job = await s.get(Job, job_id, with_for_update=True)
        if job is None or job.status in TERMINAL:
            return False
        frm = job.status
        job.status = status
        job.error = error
        job.retry_reason = None
        job.retry_api = None
        job.next_retry_at = None
        job.completed_at = utcnow()
        s.add(JobEvent(job_id=job.id, kind="TRANSITION", from_status=frm, to_status=status,
                       worker_id=job.worker_id, detail=error))
        return True


async def mark_failed(db: Database, job_id: uuid.UUID, *, code: str, message: str, stage: str | None) -> bool:
    return await _terminate(db, job_id, Status.FAILED, {"code": code, "message": message, "stage": stage})


async def mark_dead_lettered(
    db: Database, job_id: uuid.UUID, *, code: str, message: str, stage: str | None, attempts: int
) -> bool:
    return await _terminate(
        db, job_id, Status.DEAD_LETTERED,
        {"code": code, "message": message, "stage": stage, "attempts": attempts},
    )


async def complete_job(db: Database, job_id: uuid.UUID, result: dict) -> str | None:
    """PUBLISHING → COMPLETED, or COMPLETED_WITH_WARNINGS if any warning was recorded."""
    async with db.session() as s, s.begin():
        job = await s.get(Job, job_id, with_for_update=True)
        if job is None:
            return None
        if job.status in TERMINAL:
            return job.status
        status = Status.COMPLETED_WITH_WARNINGS if job.warnings else Status.COMPLETED
        frm = job.status
        job.status = status
        job.result = result
        job.progress = 100
        job.completed_at = utcnow()
        job.retry_reason = None
        job.retry_api = None
        job.next_retry_at = None
        s.add(JobEvent(job_id=job.id, kind="TRANSITION", from_status=frm, to_status=status,
                       worker_id=job.worker_id))
        return status


async def reset_for_replay(db: Database, job_id: uuid.UUID) -> str | None:
    """Admin replay of a FAILED / DEAD_LETTERED job from the stage where it stopped."""
    async with db.session() as s, s.begin():
        job = await s.get(Job, job_id, with_for_update=True)
        if job is None or job.status not in (Status.FAILED, Status.DEAD_LETTERED):
            return None
        stage = (job.error or {}).get("stage") or "transcribe"
        if stage not in STAGES:
            stage = "transcribe"
        frm = job.status
        job.status = prev_status(stage)
        job.attempt = 0
        job.rl_attempt = 0
        job.error = None
        job.completed_at = None
        s.add(JobEvent(job_id=job.id, kind="REPLAYED", from_status=frm, to_status=job.status,
                       detail={"stage": stage}))
        return stage


# --------------------------------------------------------------------------- artifacts
async def get_artifact(db: Database, job_id: uuid.UUID, key: str) -> dict | None:
    async with db.session() as s:
        art = await s.get(StageArtifact, (job_id, key))
        return art.data if art else None


async def put_artifact(db: Database, job_id: uuid.UUID, key: str, data: dict) -> None:
    async with db.session() as s, s.begin():
        art = await s.get(StageArtifact, (job_id, key))
        if art is None:
            s.add(StageArtifact(job_id=job_id, key=key, data=data))
        else:
            art.data = data


# --------------------------------------------------------------------------- frames
async def upsert_frames(db: Database, job_id: uuid.UUID, moments: list[dict]) -> None:
    """Insert PENDING frame rows for new timestamps; never touches existing rows (idempotent)."""
    async with db.session() as s, s.begin():
        existing = set(
            (await s.execute(select(Frame.timestamp_sec).where(Frame.job_id == job_id))).scalars()
        )
        for m in moments:
            ts = int(m["timestamp_sec"])
            if ts in existing:
                continue
            existing.add(ts)
            s.add(Frame(job_id=job_id, timestamp_sec=ts, caption=m["caption"],
                        why_flagged=m.get("why_flagged", ""), importance=int(m.get("importance", 3)),
                        status="PENDING"))


async def list_frames(db: Database, job_id: uuid.UUID) -> list[Frame]:
    async with db.session() as s:
        rows = await s.execute(select(Frame).where(Frame.job_id == job_id).order_by(Frame.timestamp_sec))
        return list(rows.scalars())


async def mark_frame_captured(
    db: Database, job_id: uuid.UUID, ts: int, *, image: bytes | None, width: int | None,
    height: int | None, sha256: str | None, caption: str | None = None,
) -> None:
    async with db.session() as s, s.begin():
        f = await s.get(Frame, (job_id, ts), with_for_update=True)
        if f is None or f.status != "PENDING":
            return
        f.status = "CAPTURED"
        f.image_jpeg, f.width, f.height, f.sha256 = image, width, height, sha256
        if caption:
            f.caption = caption


async def mark_frame_failed(
    db: Database, job_id: uuid.UUID, ts: int, *, code: str, message: str, status: str = "FAILED"
) -> None:
    """Mark a frame FAILED/REJECTED and record the job warning in the same transaction."""
    async with db.session() as s, s.begin():
        f = await s.get(Frame, (job_id, ts), with_for_update=True)
        if f is None or f.status not in ("PENDING", "CAPTURED"):
            return
        f.status = status
        f.failure_code = code
        job = await s.get(Job, job_id, with_for_update=True)
        if job and job.status not in TERMINAL:
            _append_warning(s, job, {"code": code, "timestamp_sec": ts, "message": message})
