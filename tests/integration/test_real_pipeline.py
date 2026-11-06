"""Real stage code end to end with fake YouTube/ffmpeg/Notion and the mock LLM (no network)."""

from __future__ import annotations

import httpx
import pytest

from notetaker.common import repo
from notetaker.worker.llm.providers import MockProvider
from notetaker.worker.llm.service import LLMService
from notetaker.worker.ratelimit import RateLimiter
from notetaker.worker.runner import Outcome, StageRunner
from notetaker.worker.stages import real
from tests.helpers import PARENT, FakeNotion, FakeYouTube, fake_capture
from tests.integration.test_runner import drain, new_job


@pytest.fixture
def notion():
    return FakeNotion()


@pytest.fixture
def youtube():
    return FakeYouTube(duration=600)


@pytest.fixture
def make_runner(settings, db, broker, faults, cipher, redis, notion, youtube, monkeypatch):
    monkeypatch.setattr(real, "capture_frame", fake_capture)
    settings.pipeline_mode = "real"
    settings.llm_chunk_tokens = 400  # force several chunks to exercise chunk/merge/reduce
    settings.notion_rps = 1000  # the limiter itself is covered in test_ratelimit.py

    def build(worker_id="worker-A"):
        limiter = RateLimiter(redis, settings)
        llm = LLMService(settings, limiter, MockProvider(), faults=faults)
        handlers = {"transcribe": real.Transcribe(youtube), "summarize": real.Summarize(llm),
                    "frames": real.Frames(youtube, llm), "publish": real.Publish(limiter, notion.transport())}
        return StageRunner(settings, db, broker, faults, cipher, handlers, worker_id)
    return build


async def test_real_pipeline_publishes_one_captioned_page(make_runner, db, broker, cipher, notion):
    runner = make_runner()
    job_id = await new_job(db, broker, cipher)
    outcomes = await drain(runner, broker)
    assert outcomes[-1] == Outcome.COMPLETED, outcomes
    job = await repo.get_job(db, job_id)
    assert job.status == "COMPLETED", (job.warnings, job.error)
    assert job.video_title == "Intro to Testing" and job.duration_sec == 600
    assert notion.created == 1
    [page_id] = notion.pages
    blocks = notion.page_blocks(page_id)
    images = [b for b in blocks if b["type"] == "image"]
    assert job.result["frames_published"] == len(images) == job.result["frames_requested"] > 0
    assert job.result["notion_page_url"].startswith("https://www.notion.so/")
    assert [b["type"] for b in blocks][:3] == ["callout", "heading_2", "bulleted_list_item"]  # header, TL;DR
    art = await repo.get_artifact(db, job_id, "summarize")
    assert art["chunks"] > 1  # chunked + merged + reduced
    starts = [s["start_sec"] for s in art["sections"]]
    assert starts == sorted(starts)  # section order intact


async def test_too_long_is_rejected_before_any_llm_or_frame_work(make_runner, db, broker, cipher, youtube):
    youtube.duration = 1500
    job_id = await new_job(db, broker, cipher, duration=None)
    assert await drain(make_runner(), broker) == [Outcome.FAILED]
    job = await repo.get_job(db, job_id)
    assert job.error["code"] == "VIDEO_TOO_LONG" and youtube.calls == ["metadata"]


async def test_partial_frame_failure_publishes_with_warnings(make_runner, db, broker, cipher, notion, redis):
    await redis.hset("faults", "frame_fail_rate", 1.0)
    job_id = await new_job(db, broker, cipher)
    await drain(make_runner(), broker)
    job = await repo.get_job(db, job_id)
    assert job.status == "COMPLETED_WITH_WARNINGS"
    assert job.result["frames_published"] == 0 and notion.created == 1  # text-only page still published
    assert {w["code"] for w in job.warnings} == {"FRAME_EXTRACTION_FAILED"}


async def test_publish_retry_rebuilds_same_page_no_duplicates(make_runner, db, broker, cipher, notion):
    notion.fail_next_append = 1  # page created, then appending content fails (5xx) → retried
    job_id = await new_job(db, broker, cipher)
    outcomes = await drain(make_runner(), broker)
    assert Outcome.RETRY_SCHEDULED in outcomes and outcomes[-1] == Outcome.COMPLETED
    assert notion.created == 1
    [page_id] = notion.pages
    assert sum(1 for b in notion.page_blocks(page_id) if b["type"] == "callout") == 1  # not duplicated
    assert (await repo.get_job(db, job_id)).status == "COMPLETED"


async def test_crash_inside_page_create_window_adopts_existing_page(make_runner, db, broker, cipher, notion):
    """Worker died right after Notion created the page but before we saved its id."""
    job_id = await new_job(db, broker, cipher)
    runner = make_runner()
    for _ in range(3):  # transcribe, summarize, frames
        await runner.process(broker.pop_stage().model_dump_json().encode())
    publish_msg = broker.pop_stage().model_dump_json().encode()
    # Simulate: page exists in Notion with our marker, DB says PAGE_CREATING with no page id.
    await repo.claim_stage(db, job_id, "publish", "worker-A")
    await repo.update_job(db, job_id, publish_state="PAGE_CREATING")
    marker = f"[{job_id.hex[:6]}]"
    notion.handle(httpx.Request("POST", "https://api.notion.com/v1/pages", json={
        "parent": {"page_id": PARENT},
        "properties": {"title": {"title": [{"text": {"content": f"Intro — Notes {marker}"}}]}}}))
    assert notion.created == 1
    assert await make_runner("worker-B").process(publish_msg, redelivered=True) == Outcome.COMPLETED
    assert notion.created == 1  # adopted, not duplicated
    job = await repo.get_job(db, job_id)
    assert job.status == "COMPLETED" and job.result["frames_published"] > 0


async def test_llm_429_retries_then_completes(make_runner, db, broker, cipher, redis):
    await redis.hset("faults", "llm_429", 1)
    job_id = await new_job(db, broker, cipher)
    outcomes = await drain(make_runner(), broker)
    assert outcomes.count(Outcome.RETRY_SCHEDULED) == 1 and outcomes[-1] == Outcome.COMPLETED
    events = await repo.list_events(db, job_id)
    assert any(e.kind == "RETRY_SCHEDULED" and e.detail["api"] == "llm" for e in events)


async def test_notion_401_fails_job_clearly(make_runner, db, broker, cipher, notion):
    orig = notion.handle

    def deny(request):
        if request.url.path.startswith("/v1/search") or "/pages" in request.url.path:
            return httpx.Response(401, json={"message": "unauthorized"})
        return orig(request)
    notion.handle = deny
    job_id = await new_job(db, broker, cipher)
    await drain(make_runner(), broker)
    job = await repo.get_job(db, job_id)
    assert job.status == "FAILED" and job.error["code"] == "NOTION_UNAUTHORIZED"
