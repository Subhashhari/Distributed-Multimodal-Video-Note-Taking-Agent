"""POST /jobs, GET /jobs/{id}, GET /jobs/{id}/results (spec §5).

The gateway validates and enqueues only; it never does pipeline work.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from ..common import repo
from ..common.logging import get_logger
from ..common.messaging import Envelope
from ..common.states import TERMINAL
from ..common.youtube_urls import InvalidInput, canonical_watch_url, normalize_notion_id, parse_youtube_url
from .api_errors import ApiError
from .views import results_view, status_view

log = get_logger(__name__)
router = APIRouter()


class CreateJobRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    youtube_url: str = Field(max_length=2048)
    notion_token: str | None = Field(default=None, max_length=1024)
    notion_parent_id: str | None = Field(default=None, max_length=512)
    # Optional hints from the extension (never trusted for correctness):
    client_duration_sec: float | None = Field(default=None, ge=0)
    client_title: str | None = Field(default=None, max_length=500)
    # Demo-only options (ignored unless DEMO_FAULTS_ENABLED=true), e.g. {"fail_frames": [461]}
    debug: dict[str, Any] | None = None


def _parse_job_id(raw: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except ValueError as exc:
        raise ApiError(404, "JOB_NOT_FOUND", "No job with that id.") from exc


@router.post("/jobs", status_code=202)
async def create_job(
    request: Request,
    body: CreateJobRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
):
    st = request.app.state
    settings = st.settings

    try:
        video_id = parse_youtube_url(body.youtube_url)
        parent_id = normalize_notion_id(body.notion_parent_id) if body.notion_parent_id else None
    except InvalidInput as exc:
        raise ApiError(422, exc.code, exc.message) from exc

    token = (body.notion_token or "").strip()
    if len(token) < 20:
        raise ApiError(401, "MISSING_NOTION_TOKEN", "Connect Notion first: a valid notion_token is required.")

    # Constraint A, early and deterministic (authoritative check happens again in the worker).
    if body.client_duration_sec is not None and body.client_duration_sec > settings.max_video_seconds:
        mins = settings.max_video_seconds // 60
        raise ApiError(422, "VIDEO_TOO_LONG", f"This video is longer than {mins} minutes, which isn't supported.")

    job, created = await repo.create_job(
        st.db,
        video_id=video_id,
        youtube_url=canonical_watch_url(video_id),
        notion_parent_id=parent_id,
        idempotency_key=idempotency_key,
        video_title=(body.client_title or None),
        duration_sec=int(body.client_duration_sec) if body.client_duration_sec else None,
    )

    if created:
        env = Envelope(
            job_id=job.id,
            stage="transcribe",
            enc_notion_token=st.cipher.encrypt(token),
            notion_parent_id=parent_id,
            debug=(body.debug or {}) if settings.demo_faults_enabled else {},
        )
        try:
            await st.broker.publish_stage(env)
        except Exception as exc:
            log.error("enqueue_failed", job_id=str(job.id), error=str(exc))
            await repo.mark_failed(st.db, job.id, code="ENQUEUE_FAILED",
                                   message="Could not enqueue the job.", stage=None)
            raise ApiError(503, "QUEUE_UNAVAILABLE", "The job queue is unavailable. Try again shortly.") from exc
        log.info("job_created", job_id=str(job.id), video_id=video_id)

    return JSONResponse(
        {"job_id": str(job.id), "status": job.status, "video_id": job.video_id},
        status_code=202,
        headers={"Location": f"/jobs/{job.id}"},
    )


@router.get("/jobs/{job_id}")
async def get_job(request: Request, job_id: str):
    job = await repo.get_job(request.app.state.db, _parse_job_id(job_id))
    if job is None:
        raise ApiError(404, "JOB_NOT_FOUND", "No job with that id.")
    return JSONResponse(status_view(job, request.app.state.settings), headers={"Cache-Control": "no-store"})


@router.get("/jobs/{job_id}/results")
async def get_results(request: Request, job_id: str):
    job = await repo.get_job(request.app.state.db, _parse_job_id(job_id))
    if job is None:
        raise ApiError(404, "JOB_NOT_FOUND", "No job with that id.")
    if job.status not in TERMINAL:
        raise ApiError(409, "JOB_NOT_FINISHED", f"Job is still {job.status}.")
    return JSONResponse(results_view(job), headers={"Cache-Control": "no-store"})
