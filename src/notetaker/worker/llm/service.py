"""LLM service: rate limiting, structured output validation (+1 repair), usage accounting, fallback."""

from __future__ import annotations

import json
import re
from typing import TypeVar

from pydantic import BaseModel, ValidationError

from ...common.config import Settings
from ...common.errors import FatalError, RateLimited
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
            except RateLimited as exc:
                if exc.code == "LLM_DAILY_QUOTA" and self.fallback and provider is not self.fallback:
                    log.warning("llm_fallback", reason="daily quota", fallback=self.fallback.name)
                    provider = self.fallback
                    text, usage = await self._limited(provider, system, prompt, schema, images, max_out, est,
                                                      vision, limit=False)
                else:
                    raise
            try:
                return model_cls.model_validate(json.loads(_extract_json(text)))
            except (ValueError, ValidationError) as exc:
                last_err = str(exc)[:300]
                log.warning("llm_invalid_output", attempt=attempt + 1, error=last_err)
        raise FatalError(f"The AI model returned invalid output twice: {last_err}", code="LLM_INVALID_OUTPUT")

    async def _limited(self, provider, system, user, schema, images, max_out, est, vision, limit=True):
        model = (self.settings.llm_vision_model or None) if vision else None
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
    if settings.llm_fallback_provider:
        fallback = build_provider(settings, settings.llm_fallback_provider,
                                  settings.llm_fallback_model or settings.llm_model)
    return LLMService(settings, limiter, provider, fallback, faults)
