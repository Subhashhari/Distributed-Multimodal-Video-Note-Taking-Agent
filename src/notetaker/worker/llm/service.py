"""LLM service: rate limiting, structured output validation (+1 repair), usage accounting, fallback."""

from __future__ import annotations

import json
import re
from typing import TypeVar

from pydantic import BaseModel, ValidationError

from ...common.config import Settings
from ...common.errors import FatalError, RateLimited, TransientError
from ...common.logging import get_logger
from .. import metrics
from ..pipeline import prompts
from ..pipeline.synthesis import (
    CHUNK_SCHEMA,
    REDUCE_SCHEMA,
    VERDICT_SCHEMA,
    ChunkSynthesis,
    FrameVerdicts,
    ReduceResult,
)
from ..pipeline.transcript import Chunk, estimate_tokens
from ..ratelimit import RateLimiter
from .providers import Provider, build_provider

log = get_logger(__name__)
T = TypeVar("T", bound=BaseModel)

# Provider failures where trying the fallback model makes sense (overload, timeouts, empty answers).
FALLBACK_CODES = {"LLM_UNAVAILABLE", "LLM_TIMEOUT", "LLM_UNREACHABLE", "LLM_EMPTY"}
# The provider is overloaded (HTTP 5xx "high demand") or too slow to answer. Like a 429 this means
# "try again later", so once no fallback is left it is reported as busy, not as a job failure.
OVERLOAD_CODES = {"LLM_UNAVAILABLE", "LLM_TIMEOUT"}


def as_busy(exc: Exception) -> Exception:
    """Turn a provider overload into a rate-limit wait: the job shows as waiting and retries on the
    rate-limit budget instead of using up its failure budget (a Gemini 503 can last minutes)."""
    if isinstance(exc, TransientError) and exc.code in OVERLOAD_CODES:
        return RateLimited("llm", retry_after=None, code="LLM_OVERLOADED",
                           message=f"The AI model is overloaded ({exc.message}); waiting before retrying.")
    return exc


def _extract_json(text: str) -> str:
    text = text.strip()
    fence = re.search(r"```(?:json)?\s*(.*?)```", text, flags=re.DOTALL)
    if fence:
        text = fence.group(1).strip()
    start = min([i for i in (text.find("{"), text.find("[")) if i >= 0], default=0)
    return text[start:]


class LLMService:
    def __init__(self, settings: Settings, limiter: RateLimiter, provider: Provider,
                 fallback: Provider | None = None, faults=None) -> None:
        self.settings = settings
        self.limiter = limiter
        self.provider = provider
        self.fallback = fallback
        self.faults = faults

    async def _call(self, model_cls: type[T], *, system: str, user: str, schema: dict,
                    images: list[bytes] | None = None, vision: bool = False) -> T:
        images = images or []
        max_out = self.settings.llm_max_output_tokens
        est = estimate_tokens(system) + estimate_tokens(user) + 260 * len(images) + max_out
        provider = self.provider
        last_err: str = ""
        for attempt in range(2):  # 1 call + 1 repair re-prompt on invalid output
            prompt = user if attempt == 0 else (
                f"{user}\n\nYour previous answer was invalid ({last_err}). Return ONLY valid JSON for the schema.")
            try:
                text, usage = await self._limited(provider, system, prompt, schema, images, max_out, est, vision)
            except (RateLimited, TransientError) as exc:
                if not self._should_fall_back(exc) or provider is self.fallback:
                    busy = as_busy(exc)
                    if busy is exc:
                        raise
                    raise busy from exc
                log.warning("llm_fallback", reason=exc.code, primary=getattr(provider, "model", provider.name),
                            fallback=getattr(self.fallback, "model", self.fallback.name))
                metrics.LLM_FALLBACKS.labels(exc.code).inc()
                provider = self.fallback
                try:
                    text, usage = await self._limited(provider, system, prompt, schema, images, max_out, est,
                                                      vision, limit=exc.code != "LLM_DAILY_QUOTA")
                except TransientError as exc2:
                    busy = as_busy(exc2)
                    if busy is exc2:
                        raise
                    raise busy from exc2
            try:
                return model_cls.model_validate(json.loads(_extract_json(text)))
            except (ValueError, ValidationError) as exc:
                last_err = str(exc)[:300]
                log.warning("llm_invalid_output", attempt=attempt + 1, error=last_err)
        raise FatalError(f"The AI model returned invalid output twice: {last_err}", code="LLM_INVALID_OUTPUT")

    def _should_fall_back(self, exc: Exception) -> bool:
        """Fall back for a daily-quota stop or an overloaded/unresponsive model, never for a per-minute 429
        (that is ours to pace) or a request/auth error (the fallback would fail the same way)."""
        if self.fallback is None:
            return False
        if isinstance(exc, RateLimited):
            return exc.code == "LLM_DAILY_QUOTA"
        return getattr(exc, "code", "") in FALLBACK_CODES

    async def _limited(self, provider, system, user, schema, images, max_out, est, vision, limit=True):
        # LLM_VISION_MODEL belongs to the primary provider; the fallback always uses its own model.
        model = (self.settings.llm_vision_model or None) if (vision and provider is self.provider) else None
        if self.faults is not None:  # demo: synthetic 429 at the client boundary (same code path as a real one)
            await self.faults.check_rate_limit("llm")
        if not limit or provider.name in ("mock", "ollama"):
            text, usage = await provider.generate_json(system=system, user=user, schema=schema, images=images,
                                                       max_output_tokens=max_out, model=model)
        else:
            permit = await self.limiter.acquire("llm", tokens=est)
            try:
                text, usage = await provider.generate_json(system=system, user=user, schema=schema, images=images,
                                                           max_output_tokens=max_out, model=model)
            except RateLimited as exc:
                if not exc.self_throttled and exc.code != "LLM_DAILY_QUOTA":
                    await self.limiter.penalize("llm", exc.retry_after or 10)
                raise
            finally:
                await self.limiter.release(permit)
            await self.limiter.reconcile(permit, usage.input_tokens + usage.output_tokens)
        metrics.LLM_TOKENS.labels("in").inc(usage.input_tokens)
        metrics.LLM_TOKENS.labels("out").inc(usage.output_tokens)
        return text, usage

    async def synthesize(self, chunk: Chunk, total: int, title: str, chapters: list[dict],
                         prev_heading: str | None) -> ChunkSynthesis:
        return await self._call(ChunkSynthesis, system=prompts.SYNTH_SYSTEM,
                                user=prompts.synth_user(chunk, total, title, chapters, prev_heading),
                                schema=CHUNK_SCHEMA)

    async def reduce(self, title: str, sections) -> ReduceResult:
        return await self._call(ReduceResult, system=prompts.REDUCE_SYSTEM,
                                user=prompts.reduce_user(title, sections), schema=REDUCE_SCHEMA)

    async def verify_frames(self, frames, images: list[bytes]) -> FrameVerdicts:
        return await self._call(FrameVerdicts, system=prompts.VERIFY_SYSTEM, user=prompts.verify_user(frames),
                                schema=VERDICT_SCHEMA, images=images, vision=True)


def build_llm_service(settings: Settings, limiter: RateLimiter, faults=None) -> LLMService:
    provider = build_provider(settings, settings.llm_provider, settings.llm_model)
    fallback = None
    # Either a different provider (e.g. ollama) or just a different model of the same provider
    # (e.g. a lighter Gemini model that is less often overloaded).
    if settings.llm_fallback_provider or settings.llm_fallback_model:
        fallback = build_provider(settings, settings.llm_fallback_provider or settings.llm_provider,
                                  settings.llm_fallback_model or settings.llm_model)
    return LLMService(settings, limiter, provider, fallback, faults)
