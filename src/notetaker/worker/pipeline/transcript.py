"""Transcript formatting and chunking for the LLM."""

from __future__ import annotations

from dataclasses import dataclass


def mmss(seconds: float) -> str:
    s = int(max(seconds, 0))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m:02d}:{sec:02d}"


def estimate_tokens(text: str) -> int:
    return max(len(text) // 4, 1)


def merge_windows(segments: list[dict], window_sec: float = 8.0) -> list[dict]:
    """Join tiny caption segments into ~window_sec lines (fewer tokens, same timestamps)."""
    out: list[dict] = []
    for s in segments:
        if out and s["start"] - out[-1]["start"] < window_sec:
            out[-1]["text"] += " " + s["text"]
            out[-1]["end"] = s["start"] + s.get("dur", 0)
        else:
            out.append({"start": s["start"], "end": s["start"] + s.get("dur", 0), "text": s["text"]})
    return out


@dataclass
class Chunk:
    index: int
    start: float
    end: float
    text: str  # lines "[MM:SS] text"

    @property
    def tokens(self) -> int:
        return estimate_tokens(self.text)


def chunk_transcript(segments: list[dict], max_tokens: int, overlap_sec: float, duration: float | None = None
                     ) -> list[Chunk]:
    lines = merge_windows(segments)
    if not lines:
        return []
    rendered = [f"[{mmss(ln['start'])}] {ln['text']}" for ln in lines]
    chunks: list[Chunk] = []
    i = 0
    while i < len(lines):
        j, used = i, 0
        while j < len(lines) and (used + estimate_tokens(rendered[j]) <= max_tokens or j == i):
            used += estimate_tokens(rendered[j])
            j += 1
        end = lines[j]["start"] if j < len(lines) else (duration or lines[-1]["end"])
        chunks.append(Chunk(len(chunks), lines[i]["start"], end, "\n".join(rendered[i:j])))
        if j >= len(lines):
            break
        # next chunk starts overlap_sec before the boundary (context continuity), but always advances
        k = j
        while k - 1 > i and lines[j]["start"] - lines[k - 1]["start"] <= overlap_sec:
            k -= 1
        i = max(k, i + 1)
    return chunks
