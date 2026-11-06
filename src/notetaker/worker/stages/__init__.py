"""Stage handlers. PIPELINE_MODE=stub uses fakes; PIPELINE_MODE=real uses YouTube/LLM/ffmpeg/Notion."""

from __future__ import annotations

from ...common.config import Settings
from ..context import StageHandler


def build_handlers(settings: Settings, *, redis=None, faults=None) -> dict[str, StageHandler]:
    if settings.pipeline_mode == "stub":
        from . import stub

        return {
            "transcribe": stub.StubTranscribe(),
            "summarize": stub.StubSummarize(),
            "frames": stub.StubFrames(),
            "publish": stub.StubPublish(),
        }

    from ..clients.youtube import YouTubeClient
    from ..llm import build_llm_service
    from ..ratelimit import RateLimiter
    from . import real

    if redis is None:
        raise ValueError("real pipeline needs redis for the shared rate limiter")
    limiter = RateLimiter(redis, settings)
    youtube = YouTubeClient(settings, limiter, faults)
    llm = build_llm_service(settings, limiter, faults)
    return {
        "transcribe": real.Transcribe(youtube),
        "summarize": real.Summarize(llm),
        "frames": real.Frames(youtube, llm),
        "publish": real.Publish(limiter),
    }
