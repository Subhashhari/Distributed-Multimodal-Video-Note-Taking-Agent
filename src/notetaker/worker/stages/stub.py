"""Stub stages: exercise the whole distributed machinery (queue, acks, retries, DLQ, crash recovery,
graceful degradation) with no external APIs. They follow the same idempotency rules as the real ones.

Stub results are clearly fake (notion_page_url points at notion.so/stub-...); never used in real mode.
"""

from __future__ import annotations

import asyncio

from ...common import repo
from ...common.errors import FatalError, TransientError
from ..context import StageContext


async def _work(ctx: StageContext, seconds: float | None = None) -> None:
    await asyncio.sleep(ctx.settings.stub_stage_delay_sec if seconds is None else seconds)
    await ctx.fault_delay()


class StubTranscribe:
    async def run(self, ctx: StageContext) -> None:
        await ctx.faults.check_rate_limit("youtube")
        await ctx.faults.check_transient()
        if await repo.get_artifact(ctx.db, ctx.job.id, "transcribe"):
            return  # already done (redelivery)
        if ctx.debug.get("unavailable"):
            raise TransientError("Video unavailable (stub)", code="VIDEO_UNAVAILABLE")
        await _work(ctx)
        duration = int(ctx.debug.get("duration_sec") or ctx.job.duration_sec or 600)
        if duration > ctx.settings.max_video_seconds:
            raise FatalError(
                f"This video is longer than {ctx.settings.max_video_seconds // 60} minutes.",
                code="VIDEO_TOO_LONG",
            )
        segments = [{"start": float(t), "dur": 30.0, "text": f"Stub transcript segment at {t}s."}
                    for t in range(0, duration, 30)]
        await repo.update_job(ctx.db, ctx.job.id, video_title=ctx.job.video_title or
                              f"Stub video {ctx.job.video_id}", duration_sec=duration)
        await repo.put_artifact(ctx.db, ctx.job.id, "transcribe",
                                {"segments": segments, "language": "en", "source": "stub"})
        await ctx.progress(1.0)


class StubSummarize:
    async def run(self, ctx: StageContext) -> None:
        await ctx.faults.check_rate_limit("llm")
        await ctx.faults.check_transient()
        if await repo.get_artifact(ctx.db, ctx.job.id, "summarize"):
            return
        transcript = await repo.get_artifact(ctx.db, ctx.job.id, "transcribe")
        if transcript is None:
            raise TransientError("Transcript artifact missing", code="ARTIFACT_MISSING")
        await _work(ctx)
        duration = int(transcript["segments"][-1]["start"] + 30) if transcript["segments"] else 60
        n_sections = 3
        span = max(duration // n_sections, 1)
        sections = [
            {"heading": f"Section {i + 1}", "start_sec": i * span, "end_sec": (i + 1) * span,
             "bullets": [f"Stub point {i + 1}.{j + 1}" for j in range(3)]}
            for i in range(n_sections)
        ]
        moments = [
            {"timestamp_sec": i * span + span // 2, "caption": f"Stub slide in section {i + 1}.",
             "why_flagged": "Stub: speaker refers to a slide.", "importance": 3}
            for i in range(n_sections)
        ]
        await repo.upsert_frames(ctx.db, ctx.job.id, moments)
        await repo.put_artifact(ctx.db, ctx.job.id, "summarize",
                                {"title": ctx.job.video_title, "tldr": ["Stub summary."],
                                 "sections": sections, "visual_moments": moments})
        await ctx.progress(1.0)


class StubFrames:
    async def run(self, ctx: StageContext) -> None:
        await ctx.faults.check_transient()
        frames = await repo.list_frames(ctx.db, ctx.job.id)
        pending = [f for f in frames if f.status == "PENDING"]
        fail_set = {int(t) for t in ctx.debug.get("fail_frames", [])}
        done = len(frames) - len(pending)
        for f in pending:  # per-frame commit → a crash resumes at the next pending frame
            await _work(ctx, ctx.settings.stub_stage_delay_sec / 2)
            if f.timestamp_sec in fail_set or await ctx.faults.frame_should_fail():
                await repo.mark_frame_failed(ctx.db, ctx.job.id, f.timestamp_sec,
                                             code="FRAME_EXTRACTION_FAILED",
                                             message="Frame couldn't be captured (stub).")
            else:
                await repo.mark_frame_captured(ctx.db, ctx.job.id, f.timestamp_sec, image=None,
                                               width=None, height=None, sha256=None)
            done += 1
            await ctx.progress(done / max(len(frames), 1))
        await ctx.progress(1.0)


class StubPublish:
    async def run(self, ctx: StageContext) -> dict:
        await ctx.faults.check_rate_limit("notion")
        await ctx.faults.check_transient()
        await _work(ctx)
        frames = await repo.list_frames(ctx.db, ctx.job.id)
        published = [f for f in frames if f.status == "CAPTURED"]
        return {
            "notion_page_url": f"https://www.notion.so/stub-{ctx.job.id.hex}",
            "notion_page_id": None,
            "frames_requested": len(frames),
            "frames_published": len(published),
            "stub": True,
        }
