"""Pipeline error taxonomy. The worker's retry logic is driven entirely by these classes.

RateLimited     → HTTP 429 from an external API: re-queued with backoff (separate budget)
TransientError  → may succeed later (5xx, timeouts, video unavailable): backoff, then DLQ
FatalError      → will never succeed (too long, no transcript, bad token): FAILED immediately
FrameError      → one frame failed: never fails the job, becomes a warning
"""

from __future__ import annotations


class PipelineError(Exception):
    default_code = "INTERNAL_ERROR"

    def __init__(self, message: str = "", *, code: str | None = None, **detail: object) -> None:
        super().__init__(message or self.__class__.__name__)
        self.message = message or self.__class__.__name__
        self.code = code or self.default_code
        self.detail = detail


class RateLimited(PipelineError):
    default_code = "RATE_LIMITED"

    def __init__(self, api: str, retry_after: float | None = None, message: str = "", **detail: object):
        super().__init__(message or f"{api} rate limit hit (HTTP 429)", **detail)
        self.api = api
        self.retry_after = retry_after
        # True when OUR shared limiter parked the stage (no provider 429 happened). These waits don't
        # consume the rate-limit retry budget: the job is simply queued behind others.
        self.self_throttled = False


class TransientError(PipelineError):
    default_code = "TRANSIENT_ERROR"


class FatalError(PipelineError):
    default_code = "FATAL_ERROR"


class FrameError(PipelineError):
    default_code = "FRAME_EXTRACTION_FAILED"
