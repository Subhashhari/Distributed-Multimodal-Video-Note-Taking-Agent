"""Notion REST client: file uploads, pages, blocks. All calls go through the shared Notion limiter."""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from ...common.config import Settings
from ...common.errors import FatalError, RateLimited, TransientError
from ..ratelimit import RateLimiter
from .http import raise_common, retry_after_seconds, send

API = "https://api.notion.com/v1"


@dataclass
class Parent:
    kind: str  # "page" | "database"
    id: str
    title_prop: str = "title"

    def as_json(self) -> dict:
        return {"page_id": self.id} if self.kind == "page" else {"database_id": self.id}


class NotionClient:
    def __init__(self, token: str, settings: Settings, limiter: RateLimiter, faults=None, transport=None) -> None:
        self.token = token
        self.settings = settings
        self.limiter = limiter
        self.faults = faults
        self._client = httpx.AsyncClient(timeout=30, transport=transport, headers={
            "Authorization": f"Bearer {token}",
            "Notion-Version": settings.notion_version,
        })

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()

    async def _req(self, method: str, path: str, *, ok404: bool = False, **kw) -> dict | None:
        if self.faults is not None:
            await self.faults.check_rate_limit("notion")
        async with self.limiter.limit("notion"):
            resp = await send(self._client, "notion", method, f"{API}{path}", **kw)
        if resp.status_code == 429:
            raise RateLimited("notion", retry_after=retry_after_seconds(resp), message="Notion rate limit (429)")
        if resp.status_code == 404 and ok404:
            return None
        if resp.status_code == 401:
            raise FatalError("Notion access was revoked or the token is invalid. Reconnect Notion.",
                             code="NOTION_UNAUTHORIZED")
        if resp.status_code == 403:
            raise FatalError("The Notion integration lacks permission for this page.", code="NOTION_FORBIDDEN")
        if resp.status_code == 404:
            raise FatalError("Notion page not found. Share the page with the integration.",
                             code="NOTION_PARENT_NOT_FOUND")
        if resp.status_code == 409:
            raise TransientError("Notion conflict, retrying", code="NOTION_CONFLICT")
        raise_common("notion", resp)
        if resp.status_code >= 400:
            try:
                msg = resp.json().get("message", resp.text)
            except ValueError:
                msg = resp.text
            raise NotionValidationError(f"Notion rejected the request: {msg[:300]}")
        return resp.json() if resp.content else {}

    # ------------------------------------------------------------------ destination
    async def resolve_parent(self, parent_id: str | None) -> Parent:
        pid = parent_id or self.settings.notion_default_parent_id or None
        if pid:
            if await self._req("GET", f"/pages/{pid}", ok404=True) is not None:
                return Parent("page", pid)
            db = await self._req("GET", f"/databases/{pid}", ok404=True)
            if db is not None:
                title = next((k for k, v in db.get("properties", {}).items() if v.get("type") == "title"), "Name")
                return Parent("database", pid, title)
            raise FatalError("Notion page not found. Share the page with the integration.",
                             code="NOTION_PARENT_NOT_FOUND")
        # Default: the page the user granted during OAuth (top-level page shared with the integration)
        res = await self._req("POST", "/search", json={
            "filter": {"property": "object", "value": "page"},
            "sort": {"direction": "descending", "timestamp": "last_edited_time"}, "page_size": 50})
        pages = (res or {}).get("results", [])
        top = [p for p in pages if p.get("parent", {}).get("type") == "workspace"] or pages
        if not top:
            raise FatalError("No Notion page is shared with the integration. Share a page (e.g. 'Video Notes').",
                             code="NOTION_NO_DESTINATION")
        return Parent("page", top[0]["id"])

    # ------------------------------------------------------------------ pages
    async def find_child_page(self, parent: Parent, marker: str) -> dict | None:
        """Find a page we created earlier (crash-recovery), by the job marker in its title.
        Uses block children / database query (consistent), not the eventually-consistent search index."""
        if parent.kind == "database":
            res = await self._req("POST", f"/databases/{parent.id}/query", json={
                "filter": {"property": parent.title_prop, "title": {"contains": marker}}, "page_size": 5})
            results = (res or {}).get("results", [])
            return results[0] if results else None
        cursor = None
        while True:
            q = "?page_size=100" + (f"&start_cursor={cursor}" if cursor else "")
            res = await self._req("GET", f"/blocks/{parent.id}/children{q}") or {}
            for b in res.get("results", []):
                if b.get("type") == "child_page" and marker in b.get("child_page", {}).get("title", "") \
                        and not b.get("archived") and not b.get("in_trash"):
                    return await self._req("GET", f"/pages/{b['id']}")
            if not res.get("has_more"):
                return None
            cursor = res.get("next_cursor")

    async def create_page(self, parent: Parent, title: str, emoji: str = "📝") -> dict:
        return await self._req("POST", "/pages", json={
            "parent": parent.as_json(),
            "icon": {"type": "emoji", "emoji": emoji},
            "properties": {parent.title_prop: {"title": [{"type": "text", "text": {"content": title[:2000]}}]}},
        })

    async def list_children(self, block_id: str) -> list[dict]:
        out, cursor = [], None
        while True:
            q = "?page_size=100" + (f"&start_cursor={cursor}" if cursor else "")
            res = await self._req("GET", f"/blocks/{block_id}/children{q}") or {}
            out.extend(res.get("results", []))
            if not res.get("has_more"):
                return out
            cursor = res.get("next_cursor")

    async def delete_block(self, block_id: str) -> None:
        await self._req("DELETE", f"/blocks/{block_id}", ok404=True)

    async def append_children(self, block_id: str, blocks: list[dict]) -> None:
        for i in range(0, len(blocks), 100):  # Notion limit: 100 blocks per request
            await self._req("PATCH", f"/blocks/{block_id}/children", json={"children": blocks[i:i + 100]})

    # ------------------------------------------------------------------ file uploads
    async def upload_image(self, filename: str, data: bytes) -> str:
        created = await self._req("POST", "/file_uploads", json={
            "mode": "single_part", "filename": filename, "content_type": "image/jpeg"})
        upload_id = created["id"]
        await self._req("POST", f"/file_uploads/{upload_id}/send",
                        files={"file": (filename, data, "image/jpeg")})
        return upload_id


class NotionValidationError(FatalError):
    default_code = "NOTION_VALIDATION"
