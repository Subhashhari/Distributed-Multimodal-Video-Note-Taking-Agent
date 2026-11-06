"""Stage handlers. PIPELINE_MODE=stub uses fakes; PIPELINE_MODE=real uses the real integrations (P3)."""

from __future__ import annotations

from ...common.config import Settings
from ..context import StageHandler


def build_handlers(settings: Settings) -> dict[str, StageHandler]:
    if settings.pipeline_mode == "stub":
        from . import stub

        return {
            "transcribe": stub.StubTranscribe(),
            "summarize": stub.StubSummarize(),
            "frames": stub.StubFrames(),
            "publish": stub.StubPublish(),
        }
    raise NotImplementedError(
        "PIPELINE_MODE=real is implemented in phase P3. Use PIPELINE_MODE=stub for now."
    )
