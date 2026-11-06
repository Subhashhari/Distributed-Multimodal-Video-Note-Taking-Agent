"""Contract tests for the REST API (spec §5)."""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from notetaker.common import repo
from notetaker.gateway.main import create_app
from tests.conftest import NOTION_TOKEN

VID = "dQw4w9WgXcQ"
URL = f"https://www.youtube.com/watch?v={VID}"


@pytest.fixture
def client(settings, broker, redis):
    app = create_app(settings, broker=broker, redis=redis)
    with TestClient(app) as c:
        c.fake_broker = broker
        yield c


def submit(client, **overrides):
    body = {"youtube_url": URL, "notion_token": NOTION_TOKEN, **overrides}
    return client.post("/jobs", json=body)


def assert_error(resp, status, code):
    assert resp.status_code == status, resp.text
    body = resp.json()
    assert set(body) == {"error"}
    assert body["error"]["code"] == code
    assert isinstance(body["error"]["message"], str) and body["error"]["message"]


# ----------------------------------------------------------------- POST /jobs
def test_submit_returns_202_location_and_enqueues(client):
    r = submit(client)
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "QUEUED" and body["video_id"] == VID
    uuid.UUID(body["job_id"])
    assert r.headers["Location"] == f"/jobs/{body['job_id']}"
    [env] = client.fake_broker.stage
    assert env.stage == "transcribe" and str(env.job_id) == body["job_id"]
    assert NOTION_TOKEN not in env.model_dump_json()  # token travels encrypted only


@pytest.mark.parametrize(
    ("overrides", "status", "code"),
    [
        ({"youtube_url": "https://vimeo.com/123"}, 422, "UNSUPPORTED_HOST"),
        ({"youtube_url": "https://www.youtube.com/watch?v=bad"}, 422, "INVALID_YOUTUBE_URL"),
        ({"notion_token": None}, 401, "MISSING_NOTION_TOKEN"),
        ({"notion_token": "short"}, 401, "MISSING_NOTION_TOKEN"),
        ({"notion_parent_id": "not-an-id"}, 422, "INVALID_NOTION_PARENT_ID"),
        ({"client_duration_sec": 1201}, 422, "VIDEO_TOO_LONG"),
    ],
)
def test_submit_validation_errors(client, overrides, status, code):
    assert_error(submit(client, **overrides), status, code)
    assert client.fake_broker.stage == []  # nothing enqueued


def test_submit_exactly_20_minutes_is_allowed(client):
    assert submit(client, client_duration_sec=1200).status_code == 202


def test_submit_malformed_body(client):
    assert_error(client.post("/jobs", json={"nope": 1}), 400, "INVALID_REQUEST")
    assert_error(client.post("/jobs", content=b"{not json", headers={"Content-Type": "application/json"}),
                 400, "INVALID_REQUEST")


def test_idempotency_key_dedupes_double_clicks(client):
    h = {"Idempotency-Key": "click-123"}
    a = client.post("/jobs", json={"youtube_url": URL, "notion_token": NOTION_TOKEN}, headers=h)
    b = client.post("/jobs", json={"youtube_url": URL, "notion_token": NOTION_TOKEN}, headers=h)
    assert a.json()["job_id"] == b.json()["job_id"]
    assert len(client.fake_broker.stage) == 1


def test_broker_down_returns_503_and_marks_failed(client):
    client.fake_broker.fail_publish = True
    assert_error(submit(client), 503, "QUEUE_UNAVAILABLE")


# ----------------------------------------------------------------- GET /jobs/{id}
def test_status_shape_and_read_only(client):
    job_id = submit(client).json()["job_id"]
    first = client.get(f"/jobs/{job_id}")
    assert first.status_code == 200
    body = first.json()
    for key in ("job_id", "status", "progress", "video_title", "duration_sec", "warnings", "error", "result"):
        assert key in body
    assert body["status"] == "QUEUED" and body["result"] is None and body["warnings"] == []
    for _ in range(20):
        again = client.get(f"/jobs/{job_id}").json()
    assert again == body  # polling never mutates state (incl. updated_at)


def test_status_unknown_job_404(client):
    assert_error(client.get(f"/jobs/{uuid.uuid4()}"), 404, "JOB_NOT_FOUND")
    assert_error(client.get("/jobs/not-a-uuid"), 404, "JOB_NOT_FOUND")


# ----------------------------------------------------------------- GET /jobs/{id}/results
def test_results_409_before_terminal(client):
    job_id = submit(client).json()["job_id"]
    assert_error(client.get(f"/jobs/{job_id}/results"), 409, "JOB_NOT_FINISHED")


def test_results_after_completion(client):
    job_id = uuid.UUID(submit(client).json()["job_id"])
    db = client.app.state.db
    for stage in ("transcribe", "summarize", "frames", "publish"):
        client.portal.call(repo.claim_stage, db, job_id, stage, "w1")
    client.portal.call(repo.add_warning, db, job_id,
                       {"code": "FRAME_EXTRACTION_FAILED", "timestamp_sec": 461, "message": "x"})
    client.portal.call(repo.complete_job, db, job_id,
                       {"notion_page_url": "https://www.notion.so/abc", "frames_requested": 6,
                        "frames_published": 5, "notion_page_id": "secret-internal"})
    r = client.get(f"/jobs/{job_id}/results")
    assert r.status_code == 200
    assert r.json() == {
        "status": "COMPLETED_WITH_WARNINGS",
        "result": {"notion_page_url": "https://www.notion.so/abc", "frames_requested": 6, "frames_published": 5},
        "warnings": [{"code": "FRAME_EXTRACTION_FAILED", "timestamp_sec": 461, "message": "x"}],
    }


def test_results_unknown_404(client):
    assert_error(client.get(f"/jobs/{uuid.uuid4()}/results"), 404, "JOB_NOT_FOUND")


# ----------------------------------------------------------------- system
def test_healthz(client, redis):
    r = client.get("/healthz")
    assert r.status_code == 200
    body = r.json()
    assert body["checks"] == {"gateway": "ok", "postgres": "ok", "rabbitmq": "ok", "redis": "ok"}
    assert body["status"] == "degraded" and body["workers"]["alive"] == 0  # no worker heartbeats yet
    client.portal.call(redis.set, "worker:w-1:hb", "{}")
    assert client.get("/healthz").json()["workers"] == {"alive": 1, "ids": ["w-1"]}


def test_healthz_503_when_broker_down(client):
    client.fake_broker.healthy = False
    r = client.get("/healthz")
    assert r.status_code == 503
    assert r.json()["checks"]["rabbitmq"].startswith("error")


def test_unknown_route_uses_error_shape(client):
    assert_error(client.get("/nope"), 404, "NOT_FOUND")


# ----------------------------------------------------------------- admin
ADMIN = {"Authorization": "Bearer test-admin-token"}


def test_admin_requires_token(client):
    assert_error(client.get("/admin/dlq"), 401, "UNAUTHORIZED")


def test_admin_faults_roundtrip(client):
    r = client.post("/admin/faults", json={"llm_429": 2}, headers=ADMIN)
    assert r.status_code == 200 and r.json()["faults"]["llm_429"] == "2"
    assert_error(client.post("/admin/faults", json={"bogus": 1}, headers=ADMIN), 422, "INVALID_FAULTS")
    assert client.delete("/admin/faults", headers=ADMIN).json() == {"faults": {}}


def test_admin_dlq_list_and_replay(client):
    job_id = uuid.UUID(submit(client).json()["job_id"])
    db = client.app.state.db
    client.portal.call(repo.claim_stage, db, job_id, "transcribe", "w1")
    client.portal.call(lambda: repo.mark_dead_lettered(db, job_id, code="VIDEO_UNAVAILABLE", message="gone",
                                                       stage="transcribe", attempts=5))
    listed = client.get("/admin/dlq", headers=ADMIN).json()
    assert listed["count"] == 1 and listed["jobs"][0]["error"]["code"] == "VIDEO_UNAVAILABLE"
    r = client.post(f"/admin/dlq/{job_id}/replay", json={"notion_token": NOTION_TOKEN}, headers=ADMIN)
    assert r.status_code == 202 and r.json()["replayed_from_stage"] == "transcribe"
    assert client.get(f"/jobs/{job_id}").json()["status"] == "QUEUED"
