"""Backoff tier selection. Delays always come from the configured schedule so a retry queue exists."""

from __future__ import annotations


def pick_delay(schedule: list[int], attempt: int, retry_after: float | None = None) -> int:
    """attempt is 1-based (1 = first retry). Honors Retry-After by rounding up to the next tier."""
    if not schedule:
        raise ValueError("empty backoff schedule")
    schedule = sorted(schedule)
    delay = schedule[min(max(attempt, 1) - 1, len(schedule) - 1)]
    if retry_after and retry_after > delay:
        delay = next((d for d in schedule if d >= retry_after), schedule[-1])
    return delay
