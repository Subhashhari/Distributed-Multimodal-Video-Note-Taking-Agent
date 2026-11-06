"""Capture ONE real frame from the video stream at a timestamp (Constraint B).

This is the only code path that produces images. There is no image generation and no placeholder.
If capture or validation fails, the caller omits the frame.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
from dataclasses import dataclass

from PIL import Image, ImageStat

from ...common.errors import FrameError

MAX_WIDTH = 1280


@dataclass
class CapturedFrame:
    jpeg: bytes
    width: int
    height: int
    sha256: str


def _headers_arg(headers: dict[str, str]) -> list[str]:
    if not headers:
        return []
    joined = "".join(f"{k}: {v}\r\n" for k, v in headers.items() if k.lower() not in ("accept-encoding",))
    return ["-headers", joined]


async def capture_frame(url: str, headers: dict[str, str], timestamp_sec: float, timeout: float) -> CapturedFrame:
    args = [
        "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
        *_headers_arg(headers),
        "-ss", f"{max(timestamp_sec, 0):.3f}",  # input seeking: only the needed bytes are fetched
        "-i", url,
        "-frames:v", "1",
        "-vf", f"scale='min({MAX_WIDTH},iw)':-2",
        "-q:v", "3",
        "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1",
    ]
    try:
        proc = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.PIPE)
    except FileNotFoundError as exc:
        raise FrameError("ffmpeg is not installed", code="FFMPEG_MISSING") from exc
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError as exc:
        proc.kill()
        await proc.wait()
        raise FrameError(f"Frame capture timed out at {timestamp_sec}s") from exc
    if proc.returncode != 0 or not out:
        raise FrameError(f"ffmpeg failed at {timestamp_sec}s: {err.decode(errors='ignore')[:200]}")
    return validate_jpeg(out)


def validate_jpeg(data: bytes) -> CapturedFrame:
    if len(data) < 1024:
        raise FrameError("Captured frame is empty", code="FRAME_EXTRACTION_FAILED")
    try:
        img = Image.open(io.BytesIO(data))
        img.load()
    except Exception as exc:
        raise FrameError("Captured frame could not be decoded") from exc
    w, h = img.size
    if w < 16 or h < 16:
        raise FrameError("Captured frame is too small")
    stat = ImageStat.Stat(img.convert("L"))
    mean, std = stat.mean[0], stat.stddev[0]
    if std < 6 and (mean < 12 or mean > 243):
        raise FrameError("Captured frame is blank (solid black/white)", code="FRAME_BLANK")
    return CapturedFrame(jpeg=data, width=w, height=h, sha256=hashlib.sha256(data).hexdigest())


def downscale_for_vision(jpeg: bytes, max_side: int = 768) -> bytes:
    """Smaller copy sent to the vision model only. The published frame stays full size."""
    img = Image.open(io.BytesIO(jpeg))
    img.thumbnail((max_side, max_side))
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=75)
    return buf.getvalue()
