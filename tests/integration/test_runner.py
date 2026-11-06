"""Worker runner: full stub pipeline, retries, DLQ, failures, redelivery, degradation (spec §4, §6)."""

from __future__ import annotations

import pytest

from notetaker.common import repo
from notetaker.common.messaging import Envelope
from notetaker.worker.runner import Outcome, StageRunner, handle_dlq_event
from notetaker.worker.stages import build_handlers
from tests.conftest import NOTION_TOKEN

VID = "dQw4w9WgXcQ"


@pytest.fixture
def runner(settings, db, broker, faults, cipher):
    return StageRunner(settings, db, broker, faults, cipher, build_handlers(settings), "worker-A")


async def new_job(db, broker, cipher, *, duration=600, debug=None):
    job, _ = await repo.create_job(db, video_id=VID, youtube_url=f"https://www.youtube.com/watch?v={VID}",
                                   notion_parent_id=None, idempotency_key=None, duration_sec=duration)
    await broker.publish_stage(Envelope(job_id=job.id, stage="transcribe",
                                        enc_notion_token=cipher.encrypt(NOTION_TOKEN), debug=debug or {}))
    return job.id


async def drain(runner, broker, max_steps=50, *, release_retries=True):
    outcomes = []
    for _ in range(max_steps):
        env = broker.pop_stage()
        if env is None:
            if release_retries and broker.retries:
                broker.release_retries()
                continue
            break
        outcomes.append(await runner.process(env.model_dump_json().encode()))
    return outcomes


async def test_happy_path_goes_through_every_status(runner, db, broker, cipher):
    job_id = await new_job(db, broker, cipher)
    outcomes = await drain(runner, broker)
    assert outcomes == [Outcome.ADVANCED] * 3 + [Outcome.COMPLETED]
    job = await repo.get_job(db, job_id)
    assert job.status == "COMPLETED" and job.progress == 100
    assert job.result["frames_requested"] == job.result["frames_published"] == 3
    transitions = [e.to_status for e in await repo.list_events(db, job_id) if e.kind == "TRANSITION"]
    assert transitions == ["QUEUED", "TRANSCRIBING", "SUMMARIZING", "EXTRACTING_FRAMES", "PUBLISHING",
                           "COMPLETED"]


async def test_rate_limit_is_retried_with_backoff_not_failed(runner, db, broker, cipher, redis):
    await redis.hset("faults", "llm_429", 2)
    job_id = await new_job(db, broker, cipher)
    await runner.process(broker.pop_stage().model_dump_json().encode())  # transcribe ok
    out = await runner.process(broker.pop_stage().model_dump_json().encode())  # summarize → 429
    assert out == Outcome.RETRY_SCHEDULED
    env, delay = broker.retries[-1]
    assert (env.stage, env.rl_attempt, delay) == ("summarize", 1, 10)
    job = await repo.get_job(db, job_id)
    assert job.status == "SUMMARIZING"  # user sees "waiting", not an error
    assert (job.retry_reason, job.retry_api) == ("RATE_LIMITED", "llm") and job.next_retry_at
    broker.release_retries()
    assert await runner.process(broker.pop_stage().model_dump_json().encode()) == Outcome.RETRY_SCHEDULED
    assert broker.retries[-1][1] == 30  # exponential: 10 → 30
    assert (await drain(runner, broker))[-1] == Outcome.COMPLETED
    assert (await repo.get_job(db, job_id)).status == "COMPLETED"


async def test_rate_limit_budget_exhausted_dead_letters(runner, db, broker, cipher, redis, settings):
    await redis.hset("faults", "notion_429", 100)
    job_id = await new_job(db, broker, cipher)
    outcomes = await drain(runner, broker)
    assert outcomes[-1] == Outcome.DEAD_LETTERED
    assert outcomes.count(Outcome.RETRY_SCHEDULED) == settings.rl_max_retries
    job = await repo.get_job(db, job_id)
    assert job.status == "DEAD_LETTERED" and job.error["code"] == "RATE_LIMIT_BUDGET_EXHAUSTED"
    assert len(broker.dead) == 1


async def test_unavailable_video_retries_then_dead_letters(runner, db, broker, cipher, settings):
    job_id = await new_job(db, broker, cipher, debug={"unavailable": True})
    outcomes = await drain(runner, broker)
    assert outcomes == [Outcome.RETRY_SCHEDULED] * (settings.max_attempts - 1) + [Outcome.DEAD_LETTERED]
    job = await repo.get_job(db, job_id)
    assert job.status == "DEAD_LETTERED"
    assert job.error == {"code": "VIDEO_UNAVAILABLE", "message": "Video unavailable (stub)",
                         "stage": "transcribe", "attempts": settings.max_attempts}
    [(env, err)] = broker.dead
    assert env.job_id == job_id and err["code"] == "VIDEO_UNAVAILABLE"


async def test_too_long_video_fails_without_retry(runner, db, broker, cipher):
    job_id = await new_job(db, broker, cipher, duration=1201)
    assert await drain(runner, broker) == [Outcome.FAILED]
    job = await repo.get_job(db, job_id)
    assert job.status == "FAILED" and job.error["code"] == "VIDEO_TOO_LONG"
    assert broker.retries == [] and broker.dead == []


async def test_partial_frame_failure_completes_with_warnings(runner, db, broker, cipher):
    job_id = await new_job(db, broker, cipher, debug={"fail_frames": [100]})  # 600 s → frames 100,300,500
    await drain(runner, broker)
    job = await repo.get_job(db, job_id)
    assert job.status == "COMPLETED_WITH_WARNINGS"
    assert job.result["frames_requested"] == 3 and job.result["frames_published"] == 2
    assert job.warnings == [{"code": "FRAME_EXTRACTION_FAILED", "timestamp_sec": 100,
                             "message": "Frame couldn't be captured (stub)."}]


async def test_all_frames_fail_still_publishes_text_only(runner, db, broker, cipher, redis):
    await redis.hset("faults", "frame_fail_rate", 1.0)
    job_id = await new_job(db, broker, cipher)
    await drain(runner, broker)
    job = await repo.get_job(db, job_id)
    assert job.status == "COMPLETED_WITH_WARNINGS"
    assert job.result["frames_published"] == 0 and job.result["notion_page_url"]
    assert len(job.warnings) == 3


async def test_redelivery_resumes_and_duplicates_are_dropped(settings, db, broker, faults, cipher):
    """Simulate a SIGKILL mid-frames: the message is redelivered to another worker, which resumes;
    a duplicate of an already-finished stage is dropped (no double work, no second page)."""
    handlers = build_handlers(settings)
    a = StageRunner(settings, db, broker, faults, cipher, handlers, "worker-A")
    b = StageRunner(settings, db, broker, faults, cipher, handlers, "worker-B")
    job_id = await new_job(db, broker, cipher)
    await a.process(broker.pop_stage().model_dump_json().encode())  # transcribe
    await a.process(broker.pop_stage().model_dump_json().encode())  # summarize
    frames_msg = broker.pop_stage().model_dump_json().encode()

    # Worker A claims frames and captures the first frame, then "dies" (no ack, no next message).
    await repo.claim_stage(db, job_id, "frames", "worker-A")
    first = (await repo.list_frames(db, job_id))[0]
    await repo.mark_frame_captured(db, job_id, first.timestamp_sec, image=None, width=None, height=None,
                                   sha256=None)

    # Broker redelivers the same message to worker B.
    assert await b.process(frames_msg, redelivered=True) == Outcome.ADVANCED
    kinds = [(e.kind, e.worker_id) for e in await repo.list_events(db, job_id)]
    assert ("REDELIVERED", "worker-B") in kinds
    publish_msg = broker.pop_stage().model_dump_json().encode()
    assert await b.process(publish_msg) == Outcome.COMPLETED

    # A late duplicate (e.g. crash between publish-next and ack) is dropped.
    assert await a.process(frames_msg, redelivered=True) == Outcome.DROPPED
    assert await a.process(publish_msg, redelivered=True) == Outcome.DROPPED
    job = await repo.get_job(db, job_id)
    assert job.status == "COMPLETED" and job.result["frames_published"] == 3


async def test_transient_error_recovers_within_budget(runner, db, broker, cipher, redis):
    await redis.hset("faults", "transient_error", 2)
    job_id = await new_job(db, broker, cipher)
    outcomes = await drain(runner, broker)
    assert outcomes.count(Outcome.RETRY_SCHEDULED) == 2 and outcomes[-1] == Outcome.COMPLETED
    assert (await repo.get_job(db, job_id)).attempt == 2


async def test_crash_loop_dead_letter_event_marks_job(db, broker, cipher):
    job_id = await new_job(db, broker, cipher)
    env = broker.pop_stage()
    assert await handle_dlq_event(db, env.model_dump_json().encode()) is True
    job = await repo.get_job(db, job_id)
    assert job.status == "DEAD_LETTERED" and job.error["code"] == "CRASH_LOOP"
    assert await handle_dlq_event(db, env.model_dump_json().encode()) is False  # idempotent


async def test_bad_message_is_dropped(runner):
    assert await runner.process(b"not json") == Outcome.DROPPED
