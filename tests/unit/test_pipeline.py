import json

import pytest

from notetaker.common.errors import FrameError, RateLimited, TransientError
from notetaker.worker.clients.ffmpeg import validate_jpeg
from notetaker.worker.clients.youtube import classify_ytdlp_error, clean_segments, parse_json3
from notetaker.worker.llm.providers import MockProvider, _gemini_schema
from notetaker.worker.pipeline.notion_blocks import PublishedFrame, build_page_blocks, text
from notetaker.worker.pipeline.synthesis import (
    CHUNK_SCHEMA,
    ChunkSynthesis,
    Section,
    VisualMoment,
    merge_chunks,
    select_moments,
)
from notetaker.worker.pipeline.transcript import chunk_transcript, mmss
from tests.helpers import noise_jpeg, transcript


def test_mmss():
    assert mmss(0) == "00:00" and mmss(461) == "07:41" and mmss(3725) == "1:02:05"


def test_single_chunk_for_short_video():
    chunks = chunk_transcript(transcript(1200), max_tokens=12000, overlap_sec=15, duration=1200)
    assert len(chunks) == 1 and chunks[0].start == 0 and chunks[0].end == 1200


def test_chunking_covers_everything_in_order_with_overlap():
    segs = transcript(1200)
    chunks = chunk_transcript(segs, max_tokens=800, overlap_sec=15, duration=1200)
    assert len(chunks) > 3
    assert [c.index for c in chunks] == list(range(len(chunks)))
    for a, b in zip(chunks, chunks[1:], strict=False):
        assert b.start <= a.end  # contiguous or overlapping, never a gap
        assert b.start > a.start  # always advancing
        assert a.end - b.start <= 15 + 8  # bounded overlap
    assert chunks[-1].end == 1200
    covered = "\n".join(c.text for c in chunks)
    assert "point 0." in covered and "point 239." in covered  # first and last caption both present


def test_merge_keeps_chunk_order_and_joins_split_section():
    c1 = ChunkSynthesis(sections=[Section(heading="Intro", start_sec=0, end_sec=50, bullets=["a"]),
                                  Section(heading="Gradient Descent", start_sec=50, end_sec=100, bullets=["b"])],
                        visual_moments=[VisualMoment(timestamp_sec=60, caption="graph", why_flagged="said so")])
    c2 = ChunkSynthesis(sections=[Section(heading="gradient descent", start_sec=95, end_sec=150, bullets=["b", "c"]),
                                  Section(heading="Momentum", start_sec=150, end_sec=200, bullets=["d"])])
    sections, moments = merge_chunks([c1, c2])
    assert [s.heading for s in sections] == ["Intro", "Gradient Descent", "Momentum"]
    assert sections[1].bullets == ["b", "c"] and sections[1].end_sec == 150
    assert len(moments) == 1


def test_select_moments_dedupes_caps_and_drops_out_of_range():
    ms = [VisualMoment(timestamp_sec=t, caption="cap", why_flagged="why", importance=imp)
          for t, imp in [(10, 2), (15, 5), (100, 3), (200, 1), (300, 4), (5000, 5), (400, 2)]]
    chosen = select_moments(ms, duration=600, max_frames=3, min_gap=20)
    assert [m.timestamp_sec for m in chosen] == [15, 100, 300]  # 10 deduped into 15, 5000 out of range


def test_blocks_every_image_has_caption_and_long_text_is_split():
    frames = [PublishedFrame(461, "Diagram of the encoder.", "Speaker says 'this diagram'.", "up-1")]
    blocks = build_page_blocks(video_id="dQw4w9WgXcQ", title="T", duration=1000, job_short="[abc123]",
                               tldr=["x"], sections=[{"heading": "S", "start_sec": 0, "end_sec": 1000,
                                                      "bullets": ["b" * 4500]}],
                               frames=frames, frames_requested=2)
    images = [b for b in blocks if b["type"] == "image"]
    assert len(images) == 1
    cap = "".join(t["text"]["content"] for t in images[0]["image"]["caption"])
    assert "07:41" in cap and "Diagram of the encoder." in cap and "this diagram" in cap
    bullet = next(b for b in blocks
                  if b["type"] == "bulleted_list_item" and len(b["bulleted_list_item"]["rich_text"]) > 1)
    assert all(len(t["text"]["content"]) <= 2000 for t in bullet["bulleted_list_item"]["rich_text"])
    assert len(text("a" * 4001)) == 3


def test_frames_land_in_their_section():
    frames = [PublishedFrame(30, "c1", "w", "u1"), PublishedFrame(130, "c2", "w", "u2")]
    blocks = build_page_blocks(video_id="dQw4w9WgXcQ", title="T", duration=200, job_short="[a]", tldr=[],
                               sections=[{"heading": "A", "start_sec": 0, "end_sec": 100, "bullets": ["x"]},
                                         {"heading": "B", "start_sec": 100, "end_sec": 200, "bullets": ["y"]}],
                               frames=frames, frames_requested=2)
    seq = [b["heading_2"]["rich_text"][0]["text"]["content"] if b["type"] == "heading_2"
           else b["image"]["file_upload"]["id"] for b in blocks if b["type"] in ("heading_2", "image")]
    assert seq == ["A", "u1", "B", "u2"]


def test_parse_json3_and_clean():
    data = {"events": [{"tStartMs": 1000, "dDurationMs": 2000, "segs": [{"utf8": "Hello "}, {"utf8": "world"}]},
                       {"tStartMs": 3000, "segs": [{"utf8": "\n"}]},
                       {"tStartMs": 4000, "dDurationMs": 1000, "segs": [{"utf8": "[Music] it&#39;s fine"}]}]}
    segs = clean_segments(parse_json3(data))
    assert segs == [{"start": 1.0, "dur": 2.0, "text": "Hello world"}, {"start": 4.0, "dur": 1.0, "text": "it's fine"}]


@pytest.mark.parametrize(("msg", "kind", "code"), [
    ("ERROR: [youtube] x: Private video. Sign in if you've been granted access", TransientError, "VIDEO_UNAVAILABLE"),
    ("ERROR: [youtube] x: Video unavailable", TransientError, "VIDEO_UNAVAILABLE"),
    ("ERROR: unable to download webpage: HTTP Error 429: Too Many Requests", RateLimited, "RATE_LIMITED"),
    ("Sign in to confirm you're not a bot", TransientError, "YOUTUBE_BOT_CHECK"),
])
def test_classify_ytdlp_errors(msg, kind, code):
    err = classify_ytdlp_error(msg)
    assert isinstance(err, kind) and err.code == code


def test_validate_jpeg_accepts_real_and_rejects_blank():
    assert validate_jpeg(noise_jpeg()).width == 320
    import io

    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (320, 180), (0, 0, 0)).save(buf, format="JPEG")
    with pytest.raises(FrameError) as exc:
        validate_jpeg(buf.getvalue() + b"\0" * 1024)
    assert exc.value.code == "FRAME_BLANK"


def test_gemini_schema_conversion():
    g = _gemini_schema(CHUNK_SCHEMA)
    assert g["type"] == "OBJECT"
    assert g["properties"]["sections"]["items"]["properties"]["bullets"]["items"]["type"] == "STRING"


async def test_mock_provider_output_validates():
    from notetaker.worker.pipeline.prompts import synth_user
    chunk = chunk_transcript(transcript(300), 12000, 15, 300)[0]
    text_out, _ = await MockProvider().generate_json(system="", user=synth_user(chunk, 1, "T", [], None),
                                                     schema=CHUNK_SCHEMA, images=[], max_output_tokens=100)
    res = ChunkSynthesis.model_validate(json.loads(text_out))
    assert res.sections and all(0 <= m.timestamp_sec <= 300 for m in res.visual_moments)
