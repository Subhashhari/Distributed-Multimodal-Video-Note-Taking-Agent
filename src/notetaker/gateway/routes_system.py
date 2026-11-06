"""Health, public config and the Notion OAuth code exchange."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .. import __version__
from ..common.logging import get_logger
from .api_errors import ApiError

log = get_logger(__name__)
router = APIRouter()

NOTION_TOKEN_URL = "https://api.notion.com/v1/oauth/token"


async def _check(coro, timeout: float = 1.5) -> str:
    try:
        await asyncio.wait_for(coro, timeout)
        return "ok"
    except Exception as exc:
        return f"error: {type(exc).__name__}: {exc}"[:200]


async def _live_workers(redis) -> list[str]:
    ids = []
    async for key in redis.scan_iter(match="worker:*:hb", count=100):
        key = key.decode() if isinstance(key, bytes) else key
        ids.append(key.split(":", 1)[1].rsplit(":", 1)[0])
    return sorted(ids)


@router.get("/livez")
async def livez():
    return {"status": "ok"}


@router.get("/healthz")
async def healthz(request: Request):
    st = request.app.state
    pg, mq, rd = await asyncio.gather(_check(st.db.ping()), _check(st.broker.ping()), _check(st.redis.ping()))
    checks = {"gateway": "ok", "postgres": pg, "rabbitmq": mq, "redis": rd}
    try:
        workers = await _live_workers(st.redis) if rd == "ok" else []
    except Exception:
        workers = []
    healthy = all(v == "ok" for v in checks.values())
    status = "ok" if healthy and workers else ("degraded" if healthy else "unavailable")
    s = st.settings
    quota = None
    if s.llm_rpd and rd == "ok":
        try:
            used = int(await st.redis.get(f"rl:llm:day:{datetime.now(UTC):%Y%m%d}") or 0)
            quota = {"limit_per_day": s.llm_rpd, "remaining_today": max(s.llm_rpd - used, 0)}
        except Exception:
            quota = None
    body = {
        "status": status,
        "checks": checks,
        "workers": {"alive": len(workers), "ids": workers},
        "pipeline_mode": s.pipeline_mode,
        "llm": {"provider": s.llm_provider, "model": s.llm_model if not s.llm_model.startswith("__") else None,
                "rpm": s.llm_rpm, "daily_quota": quota},
        "version": __version__,
    }
    return JSONResponse(body, status_code=200 if healthy else 503)


@router.get("/config/public")
async def public_config(request: Request):
    s = request.app.state.settings
    return {
        "notion_oauth_client_id": s.notion_oauth_client_id,
        "notion_oauth_redirect_uri": s.notion_oauth_redirect_uri,
        "max_video_seconds": s.max_video_seconds,
        "demo_faults_enabled": s.demo_faults_enabled,
        "version": __version__,
    }


class OAuthExchange(BaseModel):
    code: str = Field(min_length=1, max_length=512)
    redirect_uri: str | None = Field(default=None, max_length=512)


@router.post("/oauth/notion/exchange")
async def notion_oauth_exchange(request: Request, body: OAuthExchange):
    """Swap an OAuth code for a token. The client secret stays here; the token is NOT stored."""
    s = request.app.state.settings
    if not s.notion_oauth_client_id or not s.notion_oauth_client_secret or \
            s.notion_oauth_client_id.startswith("__FILL_ME"):
        raise ApiError(503, "NOTION_OAUTH_NOT_CONFIGURED",
                       "Notion OAuth isn't configured on the server (NOTION_OAUTH_CLIENT_ID/SECRET).")
    payload = {
        "grant_type": "authorization_code",
        "code": body.code,
        "redirect_uri": body.redirect_uri or s.notion_oauth_redirect_uri,
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                NOTION_TOKEN_URL,
                auth=(s.notion_oauth_client_id, s.notion_oauth_client_secret),
                json=payload,
                headers={"Notion-Version": s.notion_version},
            )
    except httpx.HTTPError as exc:
        raise ApiError(502, "NOTION_OAUTH_FAILED", "Couldn't reach Notion to finish sign-in.") from exc
    if resp.status_code != 200:
        log.warning("notion_oauth_rejected", status=resp.status_code)
        raise ApiError(400 if resp.status_code < 500 else 502, "NOTION_OAUTH_FAILED",
                       f"Notion rejected the sign-in (HTTP {resp.status_code}). Please try again.")
    data = resp.json()
    keys = ("access_token", "refresh_token", "token_type", "bot_id", "workspace_id", "workspace_name",
            "workspace_icon", "duplicated_template_id")
    return JSONResponse({k: data.get(k) for k in keys}, headers={"Cache-Control": "no-store"})
