"""LLM provider adapters. Each returns raw JSON text + token usage; errors map to our taxonomy.

  gemini         Google Gemini API (free tier)   responseSchema JSON mode, inline images
  ollama         local models, offline           format=<JSON schema>, images
  openai_compat  Groq / OpenRouter / GitHub Models / Mistral / OpenAI   response_format json_object
  mock           deterministic, no network (CI / offline demos)
"""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from typing import Protocol

import httpx

from ...common.config import Settings
from ...common.errors import FatalError, RateLimited, TransientError
from ..clients.http import raise_common, retry_after_seconds, send


@dataclass
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0


class Provider(Protocol):
    name: str
    supports_vision: bool

    async def generate_json(self, *, system: str, user: str, schema: dict, images: list[bytes],
                            max_output_tokens: int, model: str | None = None) -> tuple[str, Usage]: ...


def _gemini_schema(schema: dict) -> dict:
    """Convert our JSON schema to Gemini's OpenAPI-subset (uppercase types)."""
    out: dict = {}
    for k, v in schema.items():
        if k == "type":
            out["type"] = str(v).upper()
        elif k == "properties":
            out["properties"] = {pk: _gemini_schema(pv) for pk, pv in v.items()}
        elif k == "items":
            out["items"] = _gemini_schema(v)
        elif k in ("required", "description", "enum"):
            out[k] = v
    return out


def _b64(img: bytes) -> str:
    return base64.b64encode(img).decode()


class GeminiProvider:
    name = "gemini"
    supports_vision = True
    base = "https://generativelanguage.googleapis.com/v1beta"

    def __init__(self, settings: Settings, model: str) -> None:
        if not settings.llm_api_key or settings.llm_api_key.startswith("__FILL_ME"):
            raise FatalError("LLM_API_KEY is not set (get a free key at aistudio.google.com).",
                             code="LLM_NOT_CONFIGURED")
        if not model or model.startswith("__FILL_ME"):
            raise FatalError("LLM_MODEL is not set.", code="LLM_NOT_CONFIGURED")
        self.key = settings.llm_api_key
        self.model = model
        self.base = settings.llm_base_url or self.base

    async def generate_json(self, *, system, user, schema, images, max_output_tokens, model=None):
        parts: list[dict] = [{"text": user}]
        parts += [{"inline_data": {"mime_type": "image/jpeg", "data": _b64(i)}} for i in images]
        body = {
            "system_instruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": _gemini_schema(schema),
                "maxOutputTokens": max_output_tokens,
                "temperature": 0.2,
            },
        }
        url = f"{self.base}/models/{model or self.model}:generateContent"
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await send(client, "llm", "POST", url, json=body, headers={"x-goog-api-key": self.key})
        if resp.status_code == 429:
            text = resp.text
            daily = "PerDay" in text or "per_day" in text.lower() or "daily" in text.lower()
            exc = RateLimited("llm", retry_after=retry_after_seconds(resp) or _gemini_retry_delay(text),
                              code="LLM_DAILY_QUOTA" if daily else "RATE_LIMITED",
                              message="Gemini daily free quota exhausted" if daily else "Gemini rate limit (429)")
            raise exc
        if resp.status_code in (400, 401, 403, 404):
            msg = _err_msg(resp)
            if resp.status_code in (401, 403) or "API key" in msg or "API_KEY" in msg:
                raise FatalError(f"LLM rejected the API key: {msg}", code="LLM_AUTH_FAILED")
            raise FatalError(f"LLM request rejected: {msg}", code="LLM_BAD_REQUEST")
        raise_common("llm", resp)
        data = resp.json()
        cands = data.get("candidates") or []
        if not cands:
            raise TransientError("Gemini returned no candidates (possibly blocked)", code="LLM_EMPTY")
        text = "".join(p.get("text", "") for p in cands[0].get("content", {}).get("parts", []))
        um = data.get("usageMetadata", {})
        return text, Usage(um.get("promptTokenCount", 0), um.get("candidatesTokenCount", 0))


def _gemini_retry_delay(text: str) -> float | None:
    m = re.search(r'"retryDelay"\s*:\s*"(\d+(?:\.\d+)?)s"', text)
    return float(m.group(1)) if m else None


def _err_msg(resp: httpx.Response) -> str:
    try:
        d = resp.json()
        err = d.get("error", d)
        return str(err.get("message", err) if isinstance(err, dict) else err)[:300]
    except Exception:
        return resp.text[:300]


class OllamaProvider:
    name = "ollama"
    supports_vision = True

    def __init__(self, settings: Settings, model: str) -> None:
        self.base = (settings.llm_base_url or "http://ollama:11434").rstrip("/")
        if not model or model.startswith("__FILL_ME"):
            raise FatalError("LLM_MODEL is not set (e.g. an Ollama model you pulled).", code="LLM_NOT_CONFIGURED")
        self.model = model

    async def generate_json(self, *, system, user, schema, images, max_output_tokens, model=None):
        msg: dict = {"role": "user", "content": user}
        if images:
            msg["images"] = [_b64(i) for i in images]
        body = {"model": model or self.model, "stream": False, "format": schema,
                "messages": [{"role": "system", "content": system}, msg],
                "options": {"temperature": 0.2, "num_predict": max_output_tokens}}
        async with httpx.AsyncClient(timeout=600) as client:
            resp = await send(client, "llm", "POST", f"{self.base}/api/chat", json=body)
        if resp.status_code == 404:
            raise FatalError(f"Ollama model not found: {_err_msg(resp)} (run `ollama pull`)", code="LLM_BAD_REQUEST")
        raise_common("llm", resp)
        if resp.status_code >= 400:
            raise FatalError(f"Ollama error: {_err_msg(resp)}", code="LLM_BAD_REQUEST")
        data = resp.json()
        return data.get("message", {}).get("content", ""), Usage(data.get("prompt_eval_count", 0),
                                                                 data.get("eval_count", 0))


class OpenAICompatProvider:
    name = "openai_compat"
    supports_vision = True

    def __init__(self, settings: Settings, model: str, default_base: str = "") -> None:
        self.base = (settings.llm_base_url or default_base).rstrip("/")
        if not self.base:
            raise FatalError("LLM_BASE_URL is required for openai_compat.", code="LLM_NOT_CONFIGURED")
        self.key = settings.llm_api_key
        self.model = model

    async def generate_json(self, *, system, user, schema, images, max_output_tokens, model=None):
        content: list[dict] | str = user
        if images:
            content = [{"type": "text", "text": user}] + [
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{_b64(i)}"}} for i in images]
        sys_prompt = system + "\n\nRespond with a single JSON object matching this JSON schema:\n" + json.dumps(schema)
        body = {"model": model or self.model, "temperature": 0.2, "max_tokens": max_output_tokens,
                "response_format": {"type": "json_object"},
                "messages": [{"role": "system", "content": sys_prompt}, {"role": "user", "content": content}]}
        headers = {"Authorization": f"Bearer {self.key}"} if self.key else {}
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await send(client, "llm", "POST", f"{self.base}/chat/completions", json=body, headers=headers)
        if resp.status_code in (401, 403):
            raise FatalError(f"LLM rejected the API key: {_err_msg(resp)}", code="LLM_AUTH_FAILED")
        raise_common("llm", resp)
        if resp.status_code >= 400:
            raise FatalError(f"LLM request rejected: {_err_msg(resp)}", code="LLM_BAD_REQUEST")
        data = resp.json()
        text = data["choices"][0]["message"].get("content") or ""
        u = data.get("usage", {})
        return text, Usage(u.get("prompt_tokens", 0), u.get("completion_tokens", 0))


class MockProvider:
    """Deterministic fake LLM: builds notes from the transcript text itself. No network."""

    name = "mock"
    supports_vision = True

    def __init__(self, settings: Settings | None = None, model: str = "mock") -> None:
        self.model = model

    async def generate_json(self, *, system, user, schema, images, max_output_tokens, model=None):
        if "frames" in schema.get("properties", {}):
            ts = [float(x) for x in re.findall(r"timestamp (\d+)s", user)]
            return json.dumps({"frames": [{"timestamp_sec": t, "shows_relevant_visual": True,
                                           "caption": f"Frame at {int(t)}s showing the slide being discussed."}
                                          for t in ts]}), Usage(10, 10)
        if "sections" not in schema.get("properties", {}):
            return json.dumps({"tldr": ["Mock summary of the whole video."]}), Usage(10, 10)
        lines = re.findall(r"^\[(\d+):(\d+)\] (.+)$", user, flags=re.MULTILINE)
        rng = re.search(r"time range (\d+)s to (\d+)s", user)
        start, end = (int(rng.group(1)), int(rng.group(2))) if rng else (0, 60)
        pts = [(int(m) * 60 + int(s), t) for m, s, t in lines] or [(start, "content")]
        n = min(3, len(pts))
        size = max(len(pts) // n, 1)
        sections, moments = [], []
        for i in range(n):
            group = pts[i * size:(i + 1) * size] if i < n - 1 else pts[i * size:]
            if not group:
                continue
            s0 = group[0][0]
            s1 = group[-1][0] + 1
            sections.append({"heading": f"Part {i + 1}: {group[0][1][:40]}", "start_sec": s0, "end_sec": s1,
                             "bullets": [t[:120] for _, t in group[:4]]})
            mid = (s0 + s1) // 2
            moments.append({"timestamp_sec": min(max(mid, start), max(end - 1, start)),
                            "caption": f"Slide shown around {mid}s.",
                            "why_flagged": "Mock: speaker refers to the screen.", "importance": 3})
        return json.dumps({"sections": sections, "visual_moments": moments,
                           "tldr": ["Mock TL;DR point."]}), Usage(len(user) // 4, 200)


def build_provider(settings: Settings, name: str, model: str) -> Provider:
    name = (name or "").lower()
    if name == "gemini":
        return GeminiProvider(settings, model)
    if name == "ollama":
        return OllamaProvider(settings, model)
    if name == "openai_compat":
        return OpenAICompatProvider(settings, model)
    if name == "openai":
        return OpenAICompatProvider(settings, model, default_base="https://api.openai.com/v1")
    if name == "mock":
        return MockProvider(settings, model or "mock")
    raise FatalError(f"Unknown LLM_PROVIDER {name!r}", code="LLM_NOT_CONFIGURED")
