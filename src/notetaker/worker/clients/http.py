"""Shared HTTP error mapping: every external call ends up as one of our error classes."""

from __future__ import annotations

from email.utils import parsedate_to_datetime

import httpx

from ...common.errors import RateLimited, TransientError
from .. import metrics


def retry_after_seconds(resp: httpx.Response) -> float | None:
    raw = resp.headers.get("retry-after")
    if not raw:
        return None
    try:
        return max(float(raw), 0.0)
    except ValueError:
        try:
            from datetime import UTC, datetime

            return max((parsedate_to_datetime(raw) - datetime.now(UTC)).total_seconds(), 0.0)
        except (TypeError, ValueError):
            return None


def record(api: str, resp: httpx.Response) -> None:
    metrics.EXTERNAL_REQUESTS.labels(api, str(resp.status_code)).inc()
    if resp.status_code == 429:
        metrics.EXTERNAL_429.labels(api).inc()


def raise_common(api: str, resp: httpx.Response) -> None:
    """429 → RateLimited, 5xx → TransientError. Other statuses are left to the caller."""
    if resp.status_code == 429:
        raise RateLimited(api, retry_after=retry_after_seconds(resp))
    if resp.status_code >= 500:
        raise TransientError(f"{api} returned HTTP {resp.status_code}", code=f"{api.upper()}_UNAVAILABLE")


async def send(client: httpx.AsyncClient, api: str, method: str, url: str, **kw) -> httpx.Response:
    try:
        resp = await client.request(method, url, **kw)
    except httpx.TimeoutException as exc:
        metrics.EXTERNAL_REQUESTS.labels(api, "timeout").inc()
        raise TransientError(f"{api} request timed out", code=f"{api.upper()}_TIMEOUT") from exc
    except httpx.HTTPError as exc:
        metrics.EXTERNAL_REQUESTS.labels(api, "error").inc()
        raise TransientError(f"{api} request failed: {exc}", code=f"{api.upper()}_UNREACHABLE") from exc
    record(api, resp)
    return resp
