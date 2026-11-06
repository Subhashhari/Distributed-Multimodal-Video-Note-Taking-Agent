"""Admin endpoints: DLQ inspection/replay, job event log, demo fault injection."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, Request
from pydantic import BaseModel, Field

from ..common import repo
from ..common.faults import FAULTS_KEY, validate_faults
from ..common.messaging import Envelope
from ..common.states import Status
from ..common.youtube_urls import InvalidInput, normalize_notion_id
from .api_errors import ApiError
from .routes_jobs import _parse_job_id
from .views import iso

router = APIRouter(prefix="/admin")


def require_admin(request: Request, authorization: str | None = Header(default=None)) -> None:
    token = request.app.state.settings.admin_token
    if not token or token.startswith("__FILL_ME"):
        raise ApiError(403, "ADMIN_DISABLED", "Set ADMIN_TOKEN to enable admin endpoints.")
    if authorization != f"Bearer {token}":
        raise ApiError(401, "UNAUTHORIZED", "Missing or wrong admin token.")


def require_faults(request: Request) -> None:
    if not request.app.state.settings.demo_faults_enabled:
        raise ApiError(403, "FAULTS_DISABLED", "Set DEMO_FAULTS_ENABLED=true to use fault injection.")


@router.get("/dlq", dependencies=[Depends(require_admin)])
async def list_dlq(request: Request, limit: int = 100):
    jobs = await repo.list_jobs_by_status(request.app.state.db, Status.DEAD_LETTERED, min(limit, 500))
    return {
        "count": len(jobs),
        "jobs": [
            {"job_id": str(j.id), "video_id": j.video_id, "video_title": j.video_title,
             "error": j.error, "updated_at": iso(j.updated_at)}
            for j in jobs
        ],
    }


class ReplayRequest(BaseModel):
    notion_token: str = Field(min_length=20, max_length=1024)
    notion_parent_id: str | None = None


@router.post("/dlq/{job_id}/replay", status_code=202, dependencies=[Depends(require_admin)])
async def replay(request: Request, job_id: str, body: ReplayRequest):
    st = request.app.state
    jid = _parse_job_id(job_id)
    job = await repo.get_job(st.db, jid)
    if job is None:
        raise ApiError(404, "JOB_NOT_FOUND", "No job with that id.")
    try:
        parent = normalize_notion_id(body.notion_parent_id) if body.notion_parent_id else job.notion_parent_id
    except InvalidInput as exc:
        raise ApiError(422, exc.code, exc.message) from exc
    stage = await repo.reset_for_replay(st.db, jid)
    if stage is None:
        raise ApiError(409, "NOT_REPLAYABLE", "Only FAILED or DEAD_LETTERED jobs can be replayed.")
    await st.broker.publish_stage(
        Envelope(job_id=jid, stage=stage, enc_notion_token=st.cipher.encrypt(body.notion_token),
                 notion_parent_id=parent)
    )
    return {"job_id": str(jid), "replayed_from_stage": stage}


@router.get("/jobs/{job_id}/events", dependencies=[Depends(require_admin)])
async def job_events(request: Request, job_id: str):
    events = await repo.list_events(request.app.state.db, _parse_job_id(job_id))
    return {
        "events": [
            {"at": iso(e.at), "kind": e.kind, "from": e.from_status, "to": e.to_status,
             "worker_id": e.worker_id, "detail": e.detail}
            for e in events
        ]
    }


@router.get("/faults", dependencies=[Depends(require_admin), Depends(require_faults)])
async def get_faults(request: Request):
    return {"faults": await request.app.state.redis.hgetall(FAULTS_KEY)}


@router.post("/faults", dependencies=[Depends(require_admin), Depends(require_faults)])
async def set_faults(request: Request, body: dict):
    try:
        values = validate_faults(body)
    except (ValueError, TypeError) as exc:
        raise ApiError(422, "INVALID_FAULTS", str(exc)) from exc
    if values:
        await request.app.state.redis.hset(FAULTS_KEY, mapping=values)
    return {"faults": await request.app.state.redis.hgetall(FAULTS_KEY)}


@router.delete("/faults", dependencies=[Depends(require_admin), Depends(require_faults)])
async def clear_faults(request: Request):
    await request.app.state.redis.delete(FAULTS_KEY)
    return {"faults": {}}
