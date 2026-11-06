"""Job state machine: public statuses, allowed transitions, stage ordering and progress mapping."""

from __future__ import annotations

from enum import StrEnum


class Status(StrEnum):
    QUEUED = "QUEUED"
    TRANSCRIBING = "TRANSCRIBING"
    SUMMARIZING = "SUMMARIZING"
    EXTRACTING_FRAMES = "EXTRACTING_FRAMES"
    PUBLISHING = "PUBLISHING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_WARNINGS = "COMPLETED_WITH_WARNINGS"
    FAILED = "FAILED"
    DEAD_LETTERED = "DEAD_LETTERED"


TERMINAL: frozenset[str] = frozenset(
    {Status.COMPLETED, Status.COMPLETED_WITH_WARNINGS, Status.FAILED, Status.DEAD_LETTERED}
)
SUCCESS: frozenset[str] = frozenset({Status.COMPLETED, Status.COMPLETED_WITH_WARNINGS})

STAGES: tuple[str, ...] = ("transcribe", "summarize", "frames", "publish")

STAGE_STATUS: dict[str, Status] = {
    "transcribe": Status.TRANSCRIBING,
    "summarize": Status.SUMMARIZING,
    "frames": Status.EXTRACTING_FRAMES,
    "publish": Status.PUBLISHING,
}

# Non-terminal statuses in pipeline order.
PIPELINE_ORDER: tuple[Status, ...] = (
    Status.QUEUED,
    Status.TRANSCRIBING,
    Status.SUMMARIZING,
    Status.EXTRACTING_FRAMES,
    Status.PUBLISHING,
)

_FAIL = {Status.FAILED, Status.DEAD_LETTERED}
ALLOWED: dict[Status | None, set[Status]] = {
    None: {Status.QUEUED},
    Status.QUEUED: {Status.TRANSCRIBING} | _FAIL,
    Status.TRANSCRIBING: {Status.SUMMARIZING} | _FAIL,
    Status.SUMMARIZING: {Status.EXTRACTING_FRAMES} | _FAIL,
    Status.EXTRACTING_FRAMES: {Status.PUBLISHING} | _FAIL,
    Status.PUBLISHING: {Status.COMPLETED, Status.COMPLETED_WITH_WARNINGS} | _FAIL,
    Status.COMPLETED: set(),
    Status.COMPLETED_WITH_WARNINGS: set(),
    Status.FAILED: set(),
    Status.DEAD_LETTERED: set(),
}

# Progress range (start, end) per stage, in percent.
PROGRESS: dict[str, tuple[int, int]] = {
    "transcribe": (5, 20),
    "summarize": (20, 55),
    "frames": (55, 85),
    "publish": (85, 99),
}


def can_transition(frm: str | None, to: str) -> bool:
    key = Status(frm) if frm is not None else None
    return Status(to) in ALLOWED[key]


def prev_status(stage: str) -> Status:
    """Status a job must be in before `stage` can claim it."""
    idx = STAGES.index(stage)
    return Status.QUEUED if idx == 0 else STAGE_STATUS[STAGES[idx - 1]]


def next_stage(stage: str) -> str | None:
    idx = STAGES.index(stage)
    return STAGES[idx + 1] if idx + 1 < len(STAGES) else None


def pipeline_index(status: str) -> int:
    return PIPELINE_ORDER.index(Status(status))


def stage_progress(stage: str, fraction: float) -> int:
    start, end = PROGRESS[stage]
    fraction = min(max(fraction, 0.0), 1.0)
    return int(round(start + (end - start) * fraction))
