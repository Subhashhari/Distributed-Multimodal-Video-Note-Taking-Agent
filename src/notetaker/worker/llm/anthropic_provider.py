"""Claude (Anthropic API) provider, through the official `anthropic` Python SDK.

Structured output uses `output_config.format` (JSON schema), so the reply is guaranteed to be valid JSON
for the schema. SDK retries are off: every call goes through our shared rate limiter, and retries are
scheduled by the job queue (429 -> rate-limit wait, 5xx/529/timeout -> overloaded -> "busy").
"""

from __future__ import annotations

import base64
import copy

import anthropic

from ...common.config import Settings
from ...common.errors import FatalError, RateLimited, TransientError
from .. import metrics
from .providers import Usage

# Server-side refusal fallback: if Claude declines on safety grounds, the API re-runs the request on a
# fallback model inside the same call. "default" lets the API pick the fallback by refusal category.
FALLBACK_BETA = "server-side-fallback-2026-07-01"
FALLBACK_MODELS = {"claude-opus-5-5", "claude-opus-5", "claude-sonnet-5-5", "claude-fable-5-1", "claude-fable-5-2"}

# Thinking is always on for current Claude models and counts against max_tokens; a small cap would cut
# the JSON answer off. 16000 keeps non-streaming requests comfortably under the SDK's HTTP timeouts.
MIN_MAX_TOKENS = 16_000


def strict_schema(schema: dict) -> dict:
    """Structured outputs require `additionalProperties: false` on every object in the schema."""
    out = copy.deepcopy(schema)

    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "object":
                node["additionalProperties"] = False
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(out)
    return out


def _retry_after(exc: anthropic.APIStatusError) -> float | None:
    raw = exc.response.headers.get("retry-after") if exc.response is not None else None
    try:
        return max(float(raw), 0.0) if raw else None
    except ValueError:
        return None


class AnthropicProvider:
    name = "anthropic"
    supports_vision = True

    def __init__(self, settings: Settings, model: str, client: anthropic.AsyncAnthropic | None = None) -> None:
        key = settings.anthropic_api_key or (settings.llm_api_key if settings.llm_api_key.startswith("sk-ant") else "")
        if client is None and (not key or key.startswith("__FILL_ME")):
            raise FatalError("ANTHROPIC_API_KEY is not set (create one at console.anthropic.com).",
                             code="LLM_NOT_CONFIGURED")
        if not model or model.startswith("__FILL_ME"):
            raise FatalError("LLM_MODEL is not set (e.g. claude-opus-5-5).", code="LLM_NOT_CONFIGURED")
        self.model = model
        self.effort = settings.llm_effort.strip().lower()
        self.refusal_fallback = settings.anthropic_refusal_fallback
        self._client = client or anthropic.AsyncAnthropic(api_key=key, max_retries=0,
                                                          timeout=settings.llm_timeout_sec)

    async def generate_json(self, *, system, user, schema, images, max_output_tokens, model=None):
        model = model or self.model
        content: list[dict] = [
            {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg",
                                         "data": base64.standard_b64encode(img).decode()}}
            for img in images
        ]
        content.append({"type": "text", "text": user})
        output_config: dict = {"format": {"type": "json_schema", "schema": strict_schema(schema)}}
        if self.effort:
            output_config["effort"] = self.effort
        kwargs = {
            "model": model,
            "max_tokens": max(max_output_tokens, MIN_MAX_TOKENS),
            "system": system,
            "messages": [{"role": "user", "content": content}],
            "output_config": output_config,
        }
        try:
            if self.refusal_fallback and model in FALLBACK_MODELS:
                response = await self._client.beta.messages.create(
                    **kwargs, betas=[FALLBACK_BETA], fallbacks="default")
            else:
                response = await self._client.messages.create(**kwargs)
        except anthropic.RateLimitError as exc:
            metrics.EXTERNAL_REQUESTS.labels("llm", "429").inc()
            metrics.EXTERNAL_429.labels("llm").inc()
            raise RateLimited("llm", retry_after=_retry_after(exc), message="Claude rate limit (429)") from exc
        except (anthropic.AuthenticationError, anthropic.PermissionDeniedError) as exc:
            metrics.EXTERNAL_REQUESTS.labels("llm", str(exc.status_code)).inc()
            raise FatalError(f"Claude rejected the API key: {exc.message}"[:300], code="LLM_AUTH_FAILED") from exc
        except (anthropic.BadRequestError, anthropic.NotFoundError) as exc:
            metrics.EXTERNAL_REQUESTS.labels("llm", str(exc.status_code)).inc()
            raise FatalError(f"Claude request rejected: {exc.message}"[:300], code="LLM_BAD_REQUEST") from exc
        except anthropic.APIStatusError as exc:  # 5xx, 529 overloaded, anything else
            metrics.EXTERNAL_REQUESTS.labels("llm", str(exc.status_code)).inc()
            if exc.status_code >= 500:
                raise TransientError(f"llm returned HTTP {exc.status_code}", code="LLM_UNAVAILABLE") from exc
            raise FatalError(f"Claude request rejected: {exc.message}"[:300], code="LLM_BAD_REQUEST") from exc
        except anthropic.APITimeoutError as exc:
            metrics.EXTERNAL_REQUESTS.labels("llm", "timeout").inc()
            raise TransientError("llm request timed out", code="LLM_TIMEOUT") from exc
        except anthropic.APIConnectionError as exc:
            metrics.EXTERNAL_REQUESTS.labels("llm", "error").inc()
            raise TransientError(f"llm request failed: {exc}", code="LLM_UNREACHABLE") from exc

        metrics.EXTERNAL_REQUESTS.labels("llm", "200").inc()
        if response.stop_reason == "refusal":
            raise FatalError("Claude declined to process this video's content.", code="LLM_REFUSED")
        if response.stop_reason == "max_tokens":
            raise TransientError("Claude's answer was cut off at max_tokens", code="LLM_EMPTY")
        text = "".join(b.text for b in response.content if b.type == "text")
        if not text.strip():
            raise TransientError("Claude returned an empty answer", code="LLM_EMPTY")
        return text, Usage(response.usage.input_tokens, response.usage.output_tokens)
