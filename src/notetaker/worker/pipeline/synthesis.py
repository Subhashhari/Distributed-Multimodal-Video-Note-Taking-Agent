"""LLM output schema, validation, and order-preserving merge of chunk results (spec §3.2)."""

from __future__ import annotations

import re

from pydantic import BaseModel, Field, field_validator


class Section(BaseModel):
    heading: str = Field(min_length=1, max_length=200)
    start_sec: float = Field(ge=0)
    end_sec: float = Field(ge=0)
    bullets: list[str] = Field(min_length=1)

    @field_validator("bullets")
    @classmethod
    def _clean(cls, v: list[str]) -> list[str]:
        cleaned = [b.strip().lstrip("•-* ").strip() for b in v if b and b.strip()]
        if not cleaned:
            raise ValueError("bullets must not be empty")
        return cleaned


class VisualMoment(BaseModel):
    timestamp_sec: float = Field(ge=0)
    caption: str = Field(min_length=3, max_length=600)  # what is shown
    why_flagged: str = Field(min_length=3, max_length=600)  # why it was flagged
    importance: int = Field(default=3, ge=1, le=5)


class ChunkSynthesis(BaseModel):
    sections: list[Section] = Field(min_length=1)
    visual_moments: list[VisualMoment] = Field(default_factory=list)
    tldr: list[str] = Field(default_factory=list)


class ReduceResult(BaseModel):
    tldr: list[str] = Field(min_length=1)


class FrameVerdict(BaseModel):
    timestamp_sec: float
    shows_relevant_visual: bool
    caption: str = ""


class FrameVerdicts(BaseModel):
    frames: list[FrameVerdict]


# JSON schemas sent to the model (portable subset: no $ref, no anyOf).
CHUNK_SCHEMA = {
    "type": "object",
    "properties": {
        "sections": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "heading": {"type": "string"},
                    "start_sec": {"type": "number"},
                    "end_sec": {"type": "number"},
                    "bullets": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["heading", "start_sec", "end_sec", "bullets"],
            },
        },
        "visual_moments": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "timestamp_sec": {"type": "number"},
                    "caption": {"type": "string"},
                    "why_flagged": {"type": "string"},
                    "importance": {"type": "integer"},
                },
                "required": ["timestamp_sec", "caption", "why_flagged", "importance"],
            },
        },
        "tldr": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["sections", "visual_moments", "tldr"],
}

REDUCE_SCHEMA = {
    "type": "object",
    "properties": {"tldr": {"type": "array", "items": {"type": "string"}}},
    "required": ["tldr"],
}

VERDICT_SCHEMA = {
    "type": "object",
    "properties": {
        "frames": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "timestamp_sec": {"type": "number"},
                    "shows_relevant_visual": {"type": "boolean"},
                    "caption": {"type": "string"},
                },
                "required": ["timestamp_sec", "shows_relevant_visual", "caption"],
            },
        }
    },
    "required": ["frames"],
}


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9 ]", "", s.lower()).strip()


def _similar(a: str, b: str) -> bool:
    na, nb = _norm(a), _norm(b)
    return bool(na) and (na == nb or na in nb or nb in na)


def merge_chunks(results: list[ChunkSynthesis]) -> tuple[list[Section], list[VisualMoment]]:
    """Concatenate sections IN CHUNK ORDER; merge a section split across a chunk boundary."""
    sections: list[Section] = []
    moments: list[VisualMoment] = []
    for res in results:  # results are already in chunk order
        ordered = sorted(res.sections, key=lambda s: s.start_sec)
        for k, sec in enumerate(ordered):
            if k == 0 and sections and _similar(sections[-1].heading, sec.heading):
                prev = sections[-1]
                seen = {_norm(b) for b in prev.bullets}
                prev.bullets.extend(b for b in sec.bullets if _norm(b) not in seen)
                prev.end_sec = max(prev.end_sec, sec.end_sec)
            else:
                sections.append(sec.model_copy(deep=True))
        moments.extend(res.visual_moments)
    return sections, moments


def select_moments(moments: list[VisualMoment], duration: float, max_frames: int, min_gap: float
                   ) -> list[VisualMoment]:
    """Drop out-of-range, dedupe within min_gap (keep higher importance), cap by importance."""
    valid = [m for m in moments if 0 <= m.timestamp_sec <= max(duration - 1, 0)]
    valid.sort(key=lambda m: m.timestamp_sec)
    kept: list[VisualMoment] = []
    for m in valid:
        if kept and m.timestamp_sec - kept[-1].timestamp_sec < min_gap:
            if m.importance > kept[-1].importance:
                kept[-1] = m
            continue
        kept.append(m)
    kept.sort(key=lambda m: (-m.importance, m.timestamp_sec))
    return sorted(kept[:max_frames], key=lambda m: m.timestamp_sec)
