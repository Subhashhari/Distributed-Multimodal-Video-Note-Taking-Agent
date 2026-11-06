"""Pure, network-free validation of YouTube URLs and Notion ids."""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
YOUTUBE_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "music.youtube.com",
    "youtube-nocookie.com",
    "www.youtube-nocookie.com",
}
SHORT_HOSTS = {"youtu.be", "www.youtu.be"}
PATH_PREFIXES = {"shorts", "embed", "live", "v"}

_NOTION_ID_RE = re.compile(r"[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}")


class InvalidInput(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def parse_youtube_url(raw: str) -> str:
    """Return the 11-character video id, or raise InvalidInput."""
    bad = InvalidInput("INVALID_YOUTUBE_URL", "That doesn't look like a YouTube video link.")
    raw = (raw or "").strip()
    if not raw or any(c.isspace() for c in raw):
        raise bad
    if "://" not in raw:
        raw = "https://" + raw
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https"):
        raise bad
    host = (parsed.hostname or "").lower()

    if host in SHORT_HOSTS:
        candidate = parsed.path.lstrip("/").split("/")[0]
    elif host in YOUTUBE_HOSTS:
        if parsed.path.rstrip("/") == "/watch":
            candidate = parse_qs(parsed.query).get("v", [""])[0]
        else:
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) >= 2 and parts[0] in PATH_PREFIXES:
                candidate = parts[1]
            else:
                raise bad
    else:
        raise InvalidInput("UNSUPPORTED_HOST", "Only YouTube video links are supported.")

    if not VIDEO_ID_RE.match(candidate):
        raise bad
    return candidate


def canonical_watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def normalize_notion_id(raw: str) -> str:
    """Accept a 32-hex id, a dashed UUID, or a pasted Notion page URL; return dashed UUID form."""
    matches = _NOTION_ID_RE.findall((raw or "").strip().lower())
    if not matches:
        raise InvalidInput("INVALID_NOTION_PARENT_ID", "notion_parent_id must be a Notion page or database id.")
    h = matches[-1].replace("-", "")
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"
