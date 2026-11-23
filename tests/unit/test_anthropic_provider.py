"""Claude provider against a fake SDK client: request shape, response parsing and error mapping."""

import json
from types import SimpleNamespace

import anthropic
import httpx2
import pytest

from notetaker.common.errors import FatalError, RateLimited, TransientError
from notetaker.worker.llm.anthropic_provider import FALLBACK_BETA, AnthropicProvider, strict_schema
from notetaker.worker.llm.providers import build_provider
from notetaker.worker.pipeline.synthesis import CHUNK_SCHEMA, REDUCE_SCHEMA

GOOD = json.dumps({"tldr": ["ok"]})


def reply(*blocks, stop_reason="end_turn"):
    return SimpleNamespace(content=list(blocks), stop_reason=stop_reason,
                           usage=SimpleNamespace(input_tokens=11, output_tokens=7))


def text(t):
    return SimpleNamespace(type="text", text=t)


class FakeClient:
    """Records kwargs; returns `result` or raises it."""

    def __init__(self, result):
        self.calls = []

        async def create(**kw):
            self.calls.append(kw)
            if isinstance(result, Exception):
                raise result
            return result

        self.messages = SimpleNamespace(create=create)
        self.beta = SimpleNamespace(messages=SimpleNamespace(create=create))


def api_error(cls, status, headers=None):
    req = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
    return cls("boom", response=httpx2.Response(status, request=req, headers=headers or {}), body=None)


def provider(settings, result, model="claude-opus-5-5"):
    client = FakeClient(result)
    return AnthropicProvider(settings, model, client=client), client


async def gen(p, images=()):
    return await p.generate_json(system="sys", user="u", schema=REDUCE_SCHEMA, images=list(images),
                                 max_output_tokens=4000)


def test_strict_schema_closes_every_object():
    s = strict_schema(CHUNK_SCHEMA)
    assert s["additionalProperties"] is False
    assert s["properties"]["sections"]["items"]["additionalProperties"] is False
    assert s["properties"]["visual_moments"]["items"]["additionalProperties"] is False
    assert "additionalProperties" not in CHUNK_SCHEMA  # original untouched


async def test_request_shape_and_text_extraction(settings):
    thinking = SimpleNamespace(type="thinking", thinking="")
    p, client = provider(settings, reply(thinking, text(GOOD)))
    out, usage = await gen(p, images=[b"\xff\xd8jpeg"])
    assert out == GOOD and usage.input_tokens == 11 and usage.output_tokens == 7
    kw = client.calls[0]
    assert kw["model"] == "claude-opus-5-5" and kw["max_tokens"] >= 16000 and kw["system"] == "sys"
    assert kw["betas"] == [FALLBACK_BETA] and kw["fallbacks"] == "default"
    assert kw["output_config"]["format"]["type"] == "json_schema"
    assert kw["output_config"]["format"]["schema"]["additionalProperties"] is False
    assert kw["output_config"]["effort"] == "medium"
    blocks = kw["messages"][0]["content"]
    assert blocks[0]["type"] == "image" and blocks[0]["source"]["media_type"] == "image/jpeg"
    assert blocks[-1] == {"type": "text", "text": "u"}
    assert "temperature" not in kw


async def test_no_fallback_param_for_models_without_it(settings):
    p, client = provider(settings, reply(text(GOOD)), model="claude-haiku-5-5")
    await gen(p)
    assert "fallbacks" not in client.calls[0] and "betas" not in client.calls[0]


async def test_refusal_fails_clearly(settings):
    p, _ = provider(settings, reply(text("{}"), stop_reason="refusal"))
    with pytest.raises(FatalError) as exc:
        await gen(p)
    assert exc.value.code == "LLM_REFUSED"


async def test_429_is_rate_limited_with_retry_after(settings):
    p, _ = provider(settings, api_error(anthropic.RateLimitError, 429, {"retry-after": "12"}))
    with pytest.raises(RateLimited) as exc:
        await gen(p)
    assert exc.value.api == "llm" and exc.value.retry_after == 12 and not exc.value.self_throttled


@pytest.mark.parametrize("status", [500, 529])
async def test_overload_is_unavailable(settings, status):
    p, _ = provider(settings, api_error(anthropic.APIStatusError, status))
    with pytest.raises(TransientError) as exc:
        await gen(p)
    assert exc.value.code == "LLM_UNAVAILABLE"


async def test_bad_key_is_fatal(settings):
    p, _ = provider(settings, api_error(anthropic.AuthenticationError, 401))
    with pytest.raises(FatalError) as exc:
        await gen(p)
    assert exc.value.code == "LLM_AUTH_FAILED"


def test_missing_key_is_reported(settings):
    settings.anthropic_api_key = ""
    settings.llm_api_key = ""
    with pytest.raises(FatalError) as exc:
        build_provider(settings, "anthropic", "claude-opus-5-5")
    assert exc.value.code == "LLM_NOT_CONFIGURED"
