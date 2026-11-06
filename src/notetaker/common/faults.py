"""Demo-only fault injection (PLAN.md §10.5). Disabled unless DEMO_FAULTS_ENABLED=true.

Faults live in the Redis hash "faults" so they can be toggled at runtime via POST /admin/faults
and are shared by every worker. Counter faults fire N times, then stop.

  llm_429 / notion_429 / youtube_429  int    next N calls to that API raise a synthetic HTTP 429
  transient_error                     int    next N stage runs raise a TransientError
  frame_fail_rate                     float  probability (0..1) that each frame capture fails
  stage_delay_sec                     float  extra sleep inside each stage (time to SIGKILL a worker)
"""

from __future__ import annotations

import random

from .errors import RateLimited, TransientError

FAULTS_KEY = "faults"
COUNTER_FAULTS = {"llm_429", "notion_429", "youtube_429", "transient_error"}
FLOAT_FAULTS = {"frame_fail_rate", "stage_delay_sec"}
ALL_FAULTS = COUNTER_FAULTS | FLOAT_FAULTS


def validate_faults(data: dict) -> dict[str, str]:
    unknown = set(data) - ALL_FAULTS
    if unknown:
        raise ValueError(f"Unknown faults: {sorted(unknown)}. Allowed: {sorted(ALL_FAULTS)}")
    out: dict[str, str] = {}
    for k, v in data.items():
        if k in COUNTER_FAULTS:
            n = int(v)
            if n < 0:
                raise ValueError(f"{k} must be >= 0")
            out[k] = str(n)
        else:
            f = float(v)
            if f < 0 or (k == "frame_fail_rate" and f > 1):
                raise ValueError(f"{k} out of range")
            out[k] = str(f)
    return out


class Faults:
    def __init__(self, redis, enabled: bool) -> None:
        self.redis = redis
        self.enabled = enabled

    async def take(self, name: str) -> bool:
        """Consume one unit of a counter fault. True if the fault fires."""
        if not self.enabled:
            return False
        left = await self.redis.hincrby(FAULTS_KEY, name, -1)
        if left >= 0:
            return True
        await self.redis.hset(FAULTS_KEY, name, 0)
        return False

    async def value(self, name: str) -> float:
        if not self.enabled:
            return 0.0
        raw = await self.redis.hget(FAULTS_KEY, name)
        try:
            return float(raw) if raw is not None else 0.0
        except ValueError:
            return 0.0

    async def check_rate_limit(self, api: str) -> None:
        if await self.take(f"{api}_429"):
            raise RateLimited(api, retry_after=None, message=f"Injected HTTP 429 from {api} (demo fault)")

    async def check_transient(self) -> None:
        if await self.take("transient_error"):
            raise TransientError("Injected transient error (demo fault)", code="INJECTED_TRANSIENT")

    async def frame_should_fail(self) -> bool:
        rate = await self.value("frame_fail_rate")
        return rate > 0 and random.random() < rate
