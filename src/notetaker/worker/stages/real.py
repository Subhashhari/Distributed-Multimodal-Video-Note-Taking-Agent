"""Real pipeline stages (spec §3). Each is idempotent: on redelivery it skips work already persisted.

  transcribe  yt-dlp metadata (true duration → Constraint A) + timestamped transcript
  summarize   chunked LLM synthesis → sections + visual moments (each with caption + why_flagged)
  frames      ffmpeg grabs one real frame per timestamp; optional batched vision check
  publish     Notion file uploads + page assembly, with exactly-once page creation
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from ...common import repo
from ...common.db import utcnow
from ...common.errors import FatalError, FrameError, RateLimited, TransientError
from ...common.repo import as_utc
from ..clients.ffmpeg import capture_frame, downscale_for_vision
from ..clients.notion import NotionClient, NotionValidationError
from ..clients.youtube import YouTubeClient, clean_segments
from ..context import StageContext
from ..llm import LLMService
from ..pipeline.notion_blocks import PublishedFrame, build_page_blocks
from ..pipeline.synthesis import ChunkSynthesis, merge_chunks, select_moments
from ..pipeline.transcript import chunk_transcript
from ..ratelimit import RateLimiter

NOTION_UPLOAD_TTL = timedelta(minutes=50)  # Notion expires unattached uploads after ~1 h


class Transcribe:
    def __init__(self, youtube: YouTubeClient) -> None:
        self.yt = youtube

    async def run(self, ctx: StageContext) -> None:
        await ctx.faults.check_transient()
        await ctx.fault_delay()
        if await repo.get_artifact(ctx.db, ctx.job.id, "transcribe"):
            return
        if ctx.debug.get("unavailable"):
            raise TransientError("Video unavailable (debug flag)", code="VIDEO_UNAVAILABLE")
        info = await self.yt.metadata(ctx.job.video_id)
        if info.is_live:
            raise FatalError("Live streams aren't supported.", code="LIVE_STREAM_UNSUPPORTED")
        if info.duration is None:
            raise FatalError("Couldn't determine the video's length.", code="LIVE_STREAM_UNSUPPORTED")
        await repo.update_job(ctx.db, ctx.job.id, video_title=info.title, duration_sec=info.duration)
        if info.duration > ctx.settings.max_video_seconds:  # Constraint A: reject before any real work
            raise FatalError(f"This video is longer than {ctx.settings.max_video_seconds // 60} minutes, "
                             "which isn't supported.", code="VIDEO_TOO_LONG")
        await ctx.progress(0.4)
        segments, meta = await self.yt.transcript(info)
        segments = clean_segments(segments)
        if not segments:
            raise FatalError("This video has no captions we could use.", code="TRANSCRIPT_UNAVAILABLE")
        await repo.put_artifact(ctx.db, ctx.job.id, "transcribe", {
            "segments": segments, "title": info.title, "duration": info.duration, "chapters": info.chapters, **meta})
        await ctx.progress(1.0)


class Summarize:
    def __init__(self, llm: LLMService) -> None:
        self.llm = llm

    async def run(self, ctx: StageContext) -> None:
        await ctx.faults.check_transient()
        await ctx.fault_delay()
        if await repo.get_artifact(ctx.db, ctx.job.id, "summarize"):
            return
        tr = await repo.get_artifact(ctx.db, ctx.job.id, "transcribe")
        if tr is None:
            raise TransientError("Transcript missing", code="ARTIFACT_MISSING")
        s = ctx.settings
        duration = float(tr["duration"])
        chunks = chunk_transcript(tr["segments"], s.llm_chunk_tokens, s.llm_chunk_overlap_sec, duration)
        results: list[ChunkSynthesis] = []
        prev_heading = None
        for ch in chunks:  # sequential: keeps section order and passes context forward
            key = f"summarize:chunk:{ch.index}"
            saved = await repo.get_artifact(ctx.db, ctx.job.id, key)
            if saved is None:
                res = await self.llm.synthesize(ch, len(chunks), tr["title"], tr.get("chapters", []), prev_heading)
                lo, hi = ch.start - 5, ch.end + 5  # keep only moments inside this chunk
                res.visual_moments = [m for m in res.visual_moments if lo <= m.timestamp_sec <= hi]
                await repo.put_artifact(ctx.db, ctx.job.id, key, res.model_dump())
            else:
                res = ChunkSynthesis.model_validate(saved)
            results.append(res)
            prev_heading = res.sections[-1].heading if res.sections else prev_heading
            await ctx.progress((ch.index + 1) / (len(chunks) + (1 if len(chunks) > 1 else 0)))

        sections, moments = merge_chunks(results)
        if len(results) > 1:
            tldr = (await self.llm.reduce(tr["title"], sections)).tldr
        else:
            tldr = results[0].tldr if results else []
        chosen = select_moments(moments, duration, s.max_frames, s.min_frame_gap_sec)
        frames = [{"timestamp_sec": int(m.timestamp_sec), "caption": m.caption, "why_flagged": m.why_flagged,
                   "importance": m.importance} for m in chosen]
        await repo.upsert_frames(ctx.db, ctx.job.id, frames)
        await repo.put_artifact(ctx.db, ctx.job.id, "summarize", {
            "tldr": tldr, "sections": [x.model_dump() for x in sections], "visual_moments": frames,
            "chunks": len(chunks)})
        await ctx.progress(1.0)


class Frames:
    def __init__(self, youtube: YouTubeClient, llm: LLMService) -> None:
        self.yt = youtube
        self.llm = llm

    async def run(self, ctx: StageContext) -> None:
        await ctx.faults.check_transient()
        frames = await repo.list_frames(ctx.db, ctx.job.id)
        pending = [f for f in frames if f.status == "PENDING"]
        if pending:
            await self._capture(ctx, frames, pending)
        if ctx.settings.vision_verify and not await repo.get_artifact(ctx.db, ctx.job.id, "frames:verified"):
            await self._verify(ctx)
        await ctx.progress(1.0)

    async def _capture(self, ctx, frames, pending) -> None:
        try:
            stream = await self.yt.stream(ctx.job.video_id)
        except RateLimited:
            raise
        except Exception as exc:
            if ctx.env.attempt + 1 < ctx.settings.max_attempts:
                raise TransientError(f"Couldn't open the video stream: {exc}", code="STREAM_UNAVAILABLE") from exc
            for f in pending:  # out of retries: degrade gracefully, publish text-only
                await repo.mark_frame_failed(ctx.db, ctx.job.id, f.timestamp_sec, code="FRAME_EXTRACTION_FAILED",
                                             message="The video stream couldn't be opened.")
            return
        fail_set = {int(t) for t in ctx.debug.get("fail_frames", [])}
        sem = asyncio.Semaphore(max(ctx.settings.frame_concurrency, 1))
        done = len(frames) - len(pending)

        async def one(f) -> None:
            nonlocal done
            async with sem:
                await ctx.fault_delay()
                try:
                    if f.timestamp_sec in fail_set or await ctx.faults.frame_should_fail():
                        raise FrameError("Injected frame failure (demo fault)")
                    cap = await capture_frame(stream.url, stream.headers, f.timestamp_sec,
                                              ctx.settings.frame_timeout_sec)
                    await repo.mark_frame_captured(ctx.db, ctx.job.id, f.timestamp_sec, image=cap.jpeg,
                                                   width=cap.width, height=cap.height, sha256=cap.sha256)
                except FrameError as exc:  # never fails the job; never substituted (Constraint B)
                    await repo.mark_frame_failed(ctx.db, ctx.job.id, f.timestamp_sec, code=exc.code,
                                                 message=f"Frame couldn't be captured: {exc.message[:150]}")
                done += 1
                await ctx.progress(done / max(len(frames), 1) * (0.8 if ctx.settings.vision_verify else 1.0))

        await asyncio.gather(*(one(f) for f in pending))

    async def _verify(self, ctx) -> None:
        captured = [f for f in await repo.list_frames(ctx.db, ctx.job.id) if f.status == "CAPTURED"]
        size = max(ctx.settings.vision_batch_size, 1)
        for i in range(0, len(captured), size):
            batch = captured[i:i + size]
            try:
                verdicts = await self.llm.verify_frames(batch, [downscale_for_vision(f.image_jpeg) for f in batch])
            except RateLimited:
                raise  # captured frames are persisted; the retry only redoes verification
            except Exception as exc:  # vision is an enhancement: keep transcript captions
                ctx.log.warning("vision_verify_skipped", error=str(exc)[:200])
                continue
            by_ts = {int(round(v.timestamp_sec)): v for v in verdicts.frames}
            for idx, f in enumerate(batch):
                v = by_ts.get(f.timestamp_sec) or (verdicts.frames[idx] if idx < len(verdicts.frames) else None)
                if v is None:
                    continue
                if not v.shows_relevant_visual:
                    await repo.mark_frame_failed(ctx.db, ctx.job.id, f.timestamp_sec, code="FRAME_NOT_RELEVANT",
                                                 message="No informative visual at this moment.", status="REJECTED",
                                                 warn=False)
                elif v.caption.strip():
                    await repo.update_frame(ctx.db, ctx.job.id, f.timestamp_sec, caption=v.caption.strip()[:600])
        await repo.put_artifact(ctx.db, ctx.job.id, "frames:verified", {"done": True})


class Publish:
    def __init__(self, limiter: RateLimiter, transport=None) -> None:
        self.limiter = limiter
        self.transport = transport  # tests inject a fake Notion

    async def run(self, ctx: StageContext) -> dict:
        await ctx.faults.check_transient()
        await ctx.fault_delay()
        job = await repo.get_job(ctx.db, ctx.job.id)
        synth = await repo.get_artifact(ctx.db, job.id, "summarize")
        tr = await repo.get_artifact(ctx.db, job.id, "transcribe")
        if synth is None or tr is None:
            raise TransientError("Earlier stage output missing", code="ARTIFACT_MISSING")
        marker = f"[{job.id.hex[:6]}]"
        title = f"{(job.video_title or tr['title'])[:180]} — Notes {marker}"
        result = dict(job.result or {})

        async with NotionClient(ctx.notion_token, ctx.settings, self.limiter, ctx.faults, self.transport) as notion:
            parent = await notion.resolve_parent(ctx.env.notion_parent_id or job.notion_parent_id)
            page = None
            if result.get("notion_page_id"):
                page = {"id": result["notion_page_id"], "url": result["notion_page_url"]}
            elif job.publish_state == "PAGE_CREATING":  # crashed inside the create window → look it up
                page = await notion.find_child_page(parent, marker)
            if page is None:
                await repo.update_job(ctx.db, job.id, publish_state="PAGE_CREATING")
                page = await notion.create_page(parent, title)
            if not result.get("notion_page_id"):
                result.update(notion_page_id=page["id"], notion_page_url=page["url"])
                await repo.update_job(ctx.db, job.id, publish_state="PAGE_CREATED", result=result)
            await ctx.progress(0.2)

            if job.publish_state != "CONTENT_APPENDED":
                # A redelivery may find a half-filled page: clear it and rebuild deterministically.
                for b in await notion.list_children(page["id"]):
                    await notion.delete_block(b["id"])
                published = await self._upload_frames(ctx, notion)
                await ctx.progress(0.7)
                frames = await repo.list_frames(ctx.db, job.id)
                requested = len([f for f in frames if f.status != "REJECTED"])
                blocks = build_page_blocks(
                    video_id=job.video_id, title=tr["title"], duration=tr.get("duration"), job_short=marker,
                    tldr=synth.get("tldr", []), sections=synth["sections"], frames=published,
                    frames_requested=requested,
                    source_note="auto-generated captions" if tr.get("auto_generated") else "")
                try:
                    await notion.append_children(page["id"], blocks)
                except NotionValidationError as exc:
                    if "file_upload" in exc.message.lower() or "upload" in exc.message.lower():
                        for f in published:  # an upload expired: force fresh uploads on retry
                            await repo.update_frame(ctx.db, job.id, f.timestamp_sec, notion_file_upload_id=None)
                        raise TransientError("Notion upload expired; re-uploading", code="NOTION_UPLOAD_EXPIRED") \
                            from exc
                    raise
                result.update(frames_requested=requested, frames_published=len(published))
                await repo.update_job(ctx.db, job.id, publish_state="CONTENT_APPENDED", result=result)
        return {"notion_page_url": result["notion_page_url"], "notion_page_id": result["notion_page_id"],
                "frames_requested": result.get("frames_requested", 0),
                "frames_published": result.get("frames_published", 0)}

    async def _upload_frames(self, ctx, notion: NotionClient) -> list[PublishedFrame]:
        out = []
        for f in await repo.list_frames(ctx.db, ctx.job.id):
            if f.status != "CAPTURED" or not f.image_jpeg:
                continue
            fresh = f.notion_upload_at and utcnow() - as_utc(f.notion_upload_at) < NOTION_UPLOAD_TTL
            upload_id = f.notion_file_upload_id if (f.notion_file_upload_id and fresh) else None
            if upload_id is None:
                try:
                    upload_id = await notion.upload_image(f"frame_{f.timestamp_sec}s.jpg", f.image_jpeg)
                except RateLimited:
                    raise
                except (FatalError, TransientError) as exc:
                    if isinstance(exc, FatalError) and exc.code in ("NOTION_UNAUTHORIZED", "NOTION_FORBIDDEN"):
                        raise
                    await repo.mark_frame_failed(ctx.db, ctx.job.id, f.timestamp_sec, code="FRAME_UPLOAD_FAILED",
                                                 message=f"Frame couldn't be uploaded to Notion: {exc.message[:120]}")
                    continue
                await repo.update_frame(ctx.db, ctx.job.id, f.timestamp_sec, notion_file_upload_id=upload_id,
                                        notion_upload_at=utcnow())
            out.append(PublishedFrame(f.timestamp_sec, f.caption, f.why_flagged, upload_id))
        return out
