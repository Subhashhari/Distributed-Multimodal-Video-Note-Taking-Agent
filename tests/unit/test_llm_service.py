import json

import httpx
import pytest

from notetaker.common.errors import FatalError, RateLimited, TransientError
from notetaker.worker.llm.providers import GeminiProvider, MockProvider, Usage
from notetaker.worker.llm.service import LLMService
from notetaker.worker.pipeline.synthesis import REDUCE_SCHEMA, ReduceResult
from notetaker.worker.ratelimit import RateLimiter


class Scripted:
    """Provider that raises/returns a scripted sequence."""

    def __init__(self, name, *outcomes):
        self.name = name
        self.model = name
        self.supports_vision = True
        self.outcomes = list(outcomes)
        self.calls = 0

    async def generate_json(self, **kw):
        self.calls += 1
        out = self.outcomes.pop(0)
        if isinstance(out, Exception):
            raise out
        return out, Usage(10, 10)


GOOD = json.dumps({"tldr": ["ok"]})


def service(settings, redis, primary, fallback=None):
    settings.llm_rpm = settings.llm_tpm = settings.llm_rpd = 0
    return LLMService(settings, RateLimiter(redis, settings), primary, fallback)


async def call(svc):
    return await svc._call(ReduceResult, system="s", user="u", schema=REDUCE_SCHEMA)


@pytest.mark.parametrize("code", ["LLM_UNAVAILABLE", "LLM_TIMEOUT", "LLM_EMPTY"])
async def test_overloaded_primary_falls_back(settings, redis, code):
    primary = Scripted("primary", TransientError("503 high demand", code=code))
    fallback = Scripted("fallback", GOOD)
    res = await call(service(settings, redis, primary, fallback))
    assert res.tldr == ["ok"] and primary.calls == 1 and fallback.calls == 1


async def test_daily_quota_falls_back(settings, redis):
    primary = Scripted("primary", RateLimited("llm", code="LLM_DAILY_QUOTA"))
    fallback = Scripted("fallback", GOOD)
    assert (await call(service(settings, redis, primary, fallback))).tldr == ["ok"]


async def test_per_minute_429_does_not_fall_back(settings, redis):
    """A normal 429 is paced by the shared limiter + queue backoff, not dodged onto another model."""
    primary = Scripted("primary", RateLimited("llm"))
    fallback = Scripted("fallback", GOOD)
    with pytest.raises(RateLimited):
        await call(service(settings, redis, primary, fallback))
    assert fallback.calls == 0


async def test_request_errors_do_not_fall_back(settings, redis):
    primary = Scripted("primary", FatalError("bad key", code="LLM_AUTH_FAILED"))
    fallback = Scripted("fallback", GOOD)
    with pytest.raises(FatalError):
        await call(service(settings, redis, primary, fallback))
    assert fallback.calls == 0


async def test_no_fallback_configured_reraises(settings, redis):
    with pytest.raises(TransientError):
        await call(service(settings, redis, Scripted("primary", TransientError("x", code="LLM_UNREACHABLE"))))


@pytest.mark.parametrize("code", ["LLM_UNAVAILABLE", "LLM_TIMEOUT"])
async def test_overload_without_fallback_is_busy_not_failure(settings, redis, code):
    """A Gemini 503 'high demand' waits on the rate-limit budget instead of burning the job's attempts."""
    with pytest.raises(RateLimited) as exc:
        await call(service(settings, redis, Scripted("primary", TransientError("503", code=code))))
    assert exc.value.code == "LLM_OVERLOADED" and exc.value.api == "llm" and not exc.value.self_throttled


async def test_overload_on_primary_and_fallback_is_busy(settings, redis):
    primary = Scripted("primary", TransientError("503", code="LLM_UNAVAILABLE"))
    fallback = Scripted("fallback", TransientError("503", code="LLM_UNAVAILABLE"))
    with pytest.raises(RateLimited) as exc:
        await call(service(settings, redis, primary, fallback))
    assert exc.value.code == "LLM_OVERLOADED" and primary.calls == 1 and fallback.calls == 1


async def test_invalid_json_gets_one_repair_then_fails(settings, redis):
    primary = Scripted("primary", "not json", "still not json")
    with pytest.raises(FatalError) as exc:
        await call(service(settings, redis, primary))
    assert exc.value.code == "LLM_INVALID_OUTPUT" and primary.calls == 2


async def test_mock_provider_roundtrip(settings, redis):
    assert (await call(service(settings, redis, MockProvider()))).tldr


# ---------------------------------------------------------------- Gemini adapter against a fake API
def gemini(settings, handler, monkeypatch, **overrides):
    settings.llm_api_key = "test-key"
    for k, v in overrides.items():
        setattr(settings, k, v)
    real = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    return GeminiProvider(settings, "gemini-test")


async def test_gemini_empty_answer_is_retryable(settings, monkeypatch):
    def handler(req):
        return httpx.Response(200, json={"candidates": [{"content": {"parts": []}, "finishReason": "MAX_TOKENS"}],
                                         "usageMetadata": {"promptTokenCount": 5, "thoughtsTokenCount": 300}})
    p = gemini(settings, handler, monkeypatch)
    with pytest.raises(TransientError) as exc:
        await p.generate_json(system="s", user="u", schema=REDUCE_SCHEMA, images=[], max_output_tokens=300)
    assert exc.value.code == "LLM_EMPTY" and "LLM_THINKING_LEVEL" in exc.value.message


async def test_gemini_sends_thinking_level_and_skips_thought_parts(settings, monkeypatch):
    seen = {}

    def handler(req):
        seen.update(json.loads(req.content))
        return httpx.Response(200, json={
            "candidates": [{"content": {"parts": [{"text": "reasoning...", "thought": True}, {"text": GOOD}]},
                            "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": 5, "candidatesTokenCount": 7, "thoughtsTokenCount": 3}})
    p = gemini(settings, handler, monkeypatch, llm_thinking_level="low")
    text, usage = await p.generate_json(system="s", user="u", schema=REDUCE_SCHEMA, images=[], max_output_tokens=50)
    assert text == GOOD and usage.output_tokens == 10
    assert seen["generationConfig"]["thinkingConfig"] == {"thinkingLevel": "low"}


async def test_gemini_503_maps_to_unavailable(settings, monkeypatch):
    p = gemini(settings, lambda req: httpx.Response(503, json={"error": {"message": "high demand"}}), monkeypatch)
    with pytest.raises(TransientError) as exc:
        await p.generate_json(system="s", user="u", schema=REDUCE_SCHEMA, images=[], max_output_tokens=50)
    assert exc.value.code == "LLM_UNAVAILABLE"
