"""Fakes for the real pipeline: YouTube, ffmpeg frames and a small in-memory Notion API."""

from __future__ import annotations

import io
import json
import random
import uuid

import httpx
from PIL import Image

from notetaker.worker.clients.ffmpeg import validate_jpeg
from notetaker.worker.clients.youtube import StreamInfo, VideoInfo

PARENT = "1429989f-e8ac-4eff-bc8f-57f56486db54"


def noise_jpeg(seed: int = 0, size=(320, 180)) -> bytes:
    rnd = random.Random(seed)
    img = Image.new("RGB", size)
    img.putdata([(rnd.randrange(256), rnd.randrange(256), rnd.randrange(256)) for _ in range(size[0] * size[1])])
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def transcript(duration: int, step: int = 5) -> list[dict]:
    return [{"start": float(t), "dur": float(step),
             "text": f"At {t} seconds the speaker says as you can see on this slide point {t // step}."}
            for t in range(0, duration, step)]


class FakeYouTube:
    def __init__(self, duration: int = 600, title: str = "Intro to Testing") -> None:
        self.duration = duration
        self.title = title
        self.calls: list[str] = []

    async def metadata(self, video_id: str) -> VideoInfo:
        self.calls.append("metadata")
        return VideoInfo(video_id, self.title, self.duration, False, [], {}, {})

    async def transcript(self, info: VideoInfo):
        self.calls.append("transcript")
        return transcript(self.duration), {"language": "en", "source": "fake", "auto_generated": False}

    async def stream(self, video_id: str) -> StreamInfo:
        self.calls.append("stream")
        return StreamInfo("https://video.example/stream.mp4", {}, "136")


async def fake_capture(url, headers, ts, timeout):
    return validate_jpeg(noise_jpeg(int(ts)))


class FakeNotion:
    """Just enough of the Notion API for the publish stage."""

    def __init__(self) -> None:
        self.children: dict[str, list[dict]] = {PARENT: []}
        self.pages: dict[str, dict] = {}
        self.uploads: dict[str, str] = {}
        self.fail_next_append = 0
        self.created = 0

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def page_blocks(self, page_id: str) -> list[dict]:
        return self.children.get(page_id, [])

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/v1")
        m = request.method
        body = json.loads(request.content) if request.content and m in ("POST", "PATCH") and \
            request.headers.get("content-type", "").startswith("application/json") else {}
        parts = path.strip("/").split("/")

        if m == "GET" and parts[0] == "pages":
            pid = parts[1]
            if pid == PARENT or pid in self.pages:
                return httpx.Response(200, json={"id": pid, "url": f"https://www.notion.so/{pid.replace('-', '')}"})
            return httpx.Response(404, json={"message": "not found"})
        if m == "GET" and parts[0] == "databases":
            return httpx.Response(404, json={"message": "not found"})
        if m == "POST" and path == "/search":
            return httpx.Response(200, json={"results": [{"id": PARENT, "parent": {"type": "workspace"}}]})
        if m == "POST" and path == "/pages":
            pid = str(uuid.uuid4())
            title = body["properties"]["title"]["title"][0]["text"]["content"]
            self.pages[pid] = {"title": title}
            self.children[pid] = []
            self.children[body["parent"]["page_id"]].append(
                {"id": pid, "type": "child_page", "child_page": {"title": title}})
            self.created += 1
            return httpx.Response(200, json={"id": pid, "url": f"https://www.notion.so/{pid.replace('-', '')}"})
        if m == "GET" and parts[0] == "blocks" and parts[-1] == "children":
            return httpx.Response(200, json={"results": self.children.get(parts[1], []), "has_more": False})
        if m == "PATCH" and parts[0] == "blocks" and parts[-1] == "children":
            if self.fail_next_append:
                self.fail_next_append -= 1
                return httpx.Response(502, json={"message": "bad gateway"})
            for b in body["children"]:
                if b["type"] == "image":
                    assert b["image"]["file_upload"]["id"] in self.uploads, "image references unknown upload"
                    assert b["image"]["caption"], "Constraint B: image without caption"
                self.children[parts[1]].append({"id": str(uuid.uuid4()), **b})
            return httpx.Response(200, json={"results": []})
        if m == "DELETE" and parts[0] == "blocks":
            for kids in self.children.values():
                kids[:] = [k for k in kids if k["id"] != parts[1]]
            return httpx.Response(200, json={})
        if m == "POST" and path == "/file_uploads":
            fid = str(uuid.uuid4())
            self.uploads[fid] = "pending"
            return httpx.Response(200, json={"id": fid, "upload_url": f"https://api.notion.com/v1/file_uploads/{fid}/send"})
        if m == "POST" and parts[0] == "file_uploads" and parts[-1] == "send":
            self.uploads[parts[1]] = "uploaded"
            return httpx.Response(200, json={"id": parts[1], "status": "uploaded"})
        return httpx.Response(400, json={"message": f"unhandled {m} {path}"})
