"""Builds the Notion page body: header, TL;DR, sections with bullets, captioned frames (Constraint B)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from .transcript import mmss

MAX_TEXT = 2000  # Notion rich-text content limit


@dataclass
class PublishedFrame:
    timestamp_sec: int
    caption: str
    why_flagged: str
    file_upload_id: str


def text(content: str, *, link: str | None = None, bold: bool = False, italic: bool = False,
         color: str = "default") -> list[dict]:
    """Rich text objects, split into ≤2000-char pieces."""
    pieces = [content[i:i + MAX_TEXT] for i in range(0, len(content), MAX_TEXT)] or [""]
    out = []
    for p in pieces:
        obj: dict = {"type": "text", "text": {"content": p},
                     "annotations": {"bold": bold, "italic": italic, "color": color}}
        if link:
            obj["text"]["link"] = {"url": link}
        out.append(obj)
    return out


def _block(kind: str, rich: list[dict], **extra) -> dict:
    return {"object": "block", "type": kind, kind: {"rich_text": rich, **extra}}


def yt_link(video_id: str, t: float | None = None) -> str:
    base = f"https://www.youtube.com/watch?v={video_id}"
    return f"{base}&t={int(t)}s" if t is not None else base


def frame_caption(video_id: str, f: PublishedFrame) -> list[dict]:
    """Every image carries the LLM caption: what is shown + why it was flagged, with a timestamp link."""
    return (text(mmss(f.timestamp_sec), link=yt_link(video_id, f.timestamp_sec), bold=True)
            + text(f" | {f.caption.strip()} ")
            + text(f"Why it's here: {f.why_flagged.strip()}", italic=True))


def image_block(video_id: str, f: PublishedFrame) -> dict:
    caption = frame_caption(video_id, f)
    if not f.caption.strip():
        raise ValueError("Constraint B: every frame needs an LLM caption")
    return {"object": "block", "type": "image",
            "image": {"type": "file_upload", "file_upload": {"id": f.file_upload_id}, "caption": caption}}


def build_page_blocks(*, video_id: str, title: str, duration: int | None, job_short: str, tldr: list[str],
                      sections: list[dict], frames: list[PublishedFrame], frames_requested: int,
                      source_note: str = "") -> list[dict]:
    blocks: list[dict] = []
    meta = f"Duration {mmss(duration)} · " if duration else ""
    blocks.append(_block("callout",
                         text("Source: ") + text(title, link=yt_link(video_id))
                         + text(f" · {meta}Generated {datetime.now(UTC):%d %b %Y} · Job {job_short}"
                                + (f" · {source_note}" if source_note else ""), color="gray"),
                         icon={"type": "emoji", "emoji": "🎬"}, color="gray_background"))
    if tldr:
        blocks.append(_block("heading_2", text("TL;DR")))
        blocks += [_block("bulleted_list_item", text(b)) for b in tldr]

    placed: set[int] = set()
    ordered = sorted(sections, key=lambda s: s["start_sec"])
    for idx, sec in enumerate(ordered):
        start, end = sec["start_sec"], sec["end_sec"]
        nxt = ordered[idx + 1]["start_sec"] if idx + 1 < len(ordered) else float("inf")
        blocks.append(_block("heading_2", text(sec["heading"])))
        blocks.append(_block("paragraph", text(f"▶ {mmss(start)} to {mmss(end)}", link=yt_link(video_id, start),
                                               color="gray")))
        blocks += [_block("bulleted_list_item", text(b)) for b in sec["bullets"]]
        for f in frames:  # frames belonging to this section (until the next section starts)
            in_section = (start <= f.timestamp_sec < nxt) or (idx == 0 and f.timestamp_sec < start)
            if in_section and f.timestamp_sec not in placed:
                blocks.append(image_block(video_id, f))
                placed.add(f.timestamp_sec)
    for f in frames:  # anything left (no sections) goes at the end
        if f.timestamp_sec not in placed:
            blocks.append(image_block(video_id, f))

    blocks.append({"object": "block", "type": "divider", "divider": {}})
    blocks.append(_block("paragraph", text(
        f"Images are real frames captured from the video at the timestamps shown "
        f"({len(frames)} of {frames_requested} published). Captions are AI-written.", italic=True, color="gray")))
    return blocks
