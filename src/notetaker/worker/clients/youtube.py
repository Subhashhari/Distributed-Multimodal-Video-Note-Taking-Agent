"""YouTube: metadata (true duration), timestamped transcript, and a direct stream URL for frames.

No full video is ever downloaded. yt-dlp resolves metadata and a signed stream URL; ffmpeg then
seeks into that stream with HTTP range requests (see ffmpeg.py).
"""

from __future__ import annotations

import asyncio
import html
import re
from dataclasses import dataclass

import httpx

from ...common.config import Settings
from ...common.errors import FatalError, RateLimited, TransientError
from ...common.logging import get_logger
from .. import metrics
from ..ratelimit import RateLimiter

log = get_logger(__name__)

STREAM_FORMAT = "bv*[height<=720][ext=mp4]/bv*[height<=720]/b[height<=720]/bv*/b"


@dataclass
class VideoInfo:
    video_id: str
    title: str
    duration: int | None
    is_live: bool
    chapters: list[dict]
    subtitles: dict  # lang → [{ext,url}] (manual)
    automatic_captions: dict  # lang → [{ext,url}] (auto-generated)


@dataclass
class StreamInfo:
    url: str
    headers: dict[str, str]
    format_id: str


def classify_ytdlp_error(msg: str) -> Exception:
    m = msg.lower()
    if "429" in m or "too many requests" in m:
        return RateLimited("youtube", retry_after=None, message="YouTube rate limit (HTTP 429)")
    if "not a bot" in m or "sign in to confirm" in m:
        return TransientError("YouTube asked to confirm we're not a bot (set YTDLP_COOKIES_FILE)",
                              code="YOUTUBE_BOT_CHECK")
    if "private video" in m:
        return TransientError("This video is private.", code="VIDEO_UNAVAILABLE")
    if "members-only" in m or "join this channel" in m:
        return TransientError("This video is members-only.", code="VIDEO_UNAVAILABLE")
    if "age" in m and "confirm" in m:
        return TransientError("This video is age-restricted.", code="VIDEO_UNAVAILABLE")
    if "unavailable" in m or "removed" in m or "does not exist" in m or "terminated" in m:
        return TransientError("This video is unavailable.", code="VIDEO_UNAVAILABLE")
    return TransientError(f"YouTube error: {msg[:300]}", code="YOUTUBE_ERROR")


class YouTubeClient:
    def __init__(self, settings: Settings, limiter: RateLimiter, faults=None) -> None:
        self.settings = settings
        self.limiter = limiter
        self.faults = faults

    async def _fault(self) -> None:
        if self.faults is not None:
            await self.faults.check_rate_limit("youtube")

    def _ydl_opts(self, **extra) -> dict:
        opts = {"quiet": True, "no_warnings": True, "skip_download": True, "noplaylist": True,
                "socket_timeout": 20, "retries": 1, "extractor_retries": 1}
        if self.settings.ytdlp_cookies_file:
            opts["cookiefile"] = self.settings.ytdlp_cookies_file
        opts.update(extra)
        return opts

    async def _extract(self, video_id: str, **opts) -> dict:
        import yt_dlp

        def run() -> dict:
            with yt_dlp.YoutubeDL(self._ydl_opts(**opts)) as ydl:
                return ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)

        await self._fault()
        async with self.limiter.limit("youtube"):
            try:
                info = await asyncio.to_thread(run)
                metrics.EXTERNAL_REQUESTS.labels("youtube", "200").inc()
                return info
            except yt_dlp.utils.DownloadError as exc:
                err = classify_ytdlp_error(str(exc))
                metrics.EXTERNAL_REQUESTS.labels("youtube", "error").inc()
                if isinstance(err, RateLimited):
                    metrics.EXTERNAL_429.labels("youtube").inc()
                raise err from exc

    async def metadata(self, video_id: str) -> VideoInfo:
        info = await self._extract(video_id)
        duration = info.get("duration")
        return VideoInfo(
            video_id=video_id,
            title=info.get("title") or video_id,
            duration=int(duration) if duration is not None else None,
            is_live=bool(info.get("is_live")) or info.get("live_status") in ("is_live", "is_upcoming"),
            chapters=[{"title": c.get("title", ""), "start": c.get("start_time", 0), "end": c.get("end_time", 0)}
                      for c in (info.get("chapters") or [])],
            subtitles=info.get("subtitles") or {},
            automatic_captions=info.get("automatic_captions") or {},
        )

    async def stream(self, video_id: str) -> StreamInfo:
        info = await self._extract(video_id, format=STREAM_FORMAT)
        if info.get("url"):
            fmt = info
        else:
            fmts = info.get("requested_formats") or []
            fmt = next((f for f in fmts if f.get("vcodec") not in (None, "none")), fmts[0] if fmts else None)
        if not fmt or not fmt.get("url"):
            raise TransientError("No playable video stream found", code="STREAM_UNAVAILABLE")
        return StreamInfo(url=fmt["url"], headers=dict(fmt.get("http_headers") or info.get("http_headers") or {}),
                          format_id=str(fmt.get("format_id", "")))

    # ------------------------------------------------------------------ transcript
    async def transcript(self, info: VideoInfo) -> tuple[list[dict], dict]:
        """Returns (segments, meta). Order: transcript API (manual → auto) → yt-dlp captions."""
        langs = [x.strip() for x in self.settings.transcript_langs.split(",") if x.strip()]
        try:
            segs, meta = await self._via_transcript_api(info.video_id, langs)
            if segs:
                return segs, meta
        except RateLimited:
            raise
        except Exception as exc:  # fall through to yt-dlp captions
            log.info("transcript_api_failed", video_id=info.video_id, error=str(exc)[:200])
        segs, meta = await self._via_ytdlp_captions(info, langs)
        if segs:
            return segs, meta
        raise FatalError("This video has no captions we could use.", code="TRANSCRIPT_UNAVAILABLE")

    async def _via_transcript_api(self, video_id: str, langs: list[str]) -> tuple[list[dict], dict]:
        from youtube_transcript_api import YouTubeTranscriptApi
        from youtube_transcript_api import _errors as yte

        def run():
            api = YouTubeTranscriptApi()
            tlist = api.list(video_id)
            generated = False
            try:
                t = tlist.find_manually_created_transcript(langs)
            except yte.NoTranscriptFound:
                try:
                    t = tlist.find_generated_transcript(langs)
                    generated = True
                except yte.NoTranscriptFound:
                    t = next(iter(tlist))  # any language; translate to English if possible
                    generated = t.is_generated
                    if getattr(t, "is_translatable", False) and t.language_code not in langs:
                        t = t.translate(langs[0] if langs else "en")
            fetched = t.fetch()
            return ([{"start": float(s.start), "dur": float(s.duration), "text": s.text} for s in fetched],
                    {"language": t.language_code, "source": "youtube_transcript_api", "auto_generated": generated})

        async with self.limiter.limit("youtube"):
            try:
                result = await asyncio.to_thread(run)
                metrics.EXTERNAL_REQUESTS.labels("youtube", "200").inc()
                return result
            except Exception as exc:
                name = type(exc).__name__
                msg = str(exc).lower()
                if "too many requests" in msg or "429" in msg:
                    metrics.EXTERNAL_429.labels("youtube").inc()
                    raise RateLimited("youtube", message="YouTube transcript rate limit (HTTP 429)") from exc
                if name in ("RequestBlocked", "IpBlocked"):
                    raise TransientError("YouTube is blocking transcript requests from this IP",
                                         code="YOUTUBE_BLOCKED") from exc
                raise

    async def _via_ytdlp_captions(self, info: VideoInfo, langs: list[str]) -> tuple[list[dict], dict]:
        for source, tracks, generated in (("manual", info.subtitles, False),
                                          ("auto", info.automatic_captions, True)):
            for lang in [*langs, *tracks.keys()]:
                fmts = tracks.get(lang) or []
                j3 = next((f for f in fmts if f.get("ext") == "json3"), None)
                if not j3:
                    continue
                async with self.limiter.limit("youtube"), httpx.AsyncClient(timeout=20) as client:
                    resp = await client.get(j3["url"])
                metrics.EXTERNAL_REQUESTS.labels("youtube", str(resp.status_code)).inc()
                if resp.status_code == 429:
                    metrics.EXTERNAL_429.labels("youtube").inc()
                    raise RateLimited("youtube", message="YouTube captions rate limit (HTTP 429)")
                if resp.status_code != 200:
                    continue
                segs = parse_json3(resp.json())
                if segs:
                    return segs, {"language": lang, "source": f"yt-dlp-{source}", "auto_generated": generated}
        return [], {}


def parse_json3(data: dict) -> list[dict]:
    out = []
    for ev in data.get("events", []):
        segs = ev.get("segs")
        if not segs:
            continue
        text = "".join(s.get("utf8", "") for s in segs).replace("\n", " ").strip()
        if text:
            out.append({"start": ev.get("tStartMs", 0) / 1000, "dur": ev.get("dDurationMs", 0) / 1000,
                        "text": text})
    return out


_NOISE = re.compile(r"\[(?:music|applause|laughter|silence|__)\]|♪+", re.IGNORECASE)


def clean_segments(segments: list[dict]) -> list[dict]:
    out = []
    for s in segments:
        text = _NOISE.sub(" ", html.unescape(s.get("text", "")))
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            out.append({"start": round(float(s["start"]), 2), "dur": round(float(s.get("dur", 0)), 2), "text": text})
    out.sort(key=lambda s: s["start"])
    return out
