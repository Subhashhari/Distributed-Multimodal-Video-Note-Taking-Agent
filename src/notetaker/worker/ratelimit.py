"""Global rate limiting shared by ALL workers (PLAN.md §9), backed by Redis.

Three mechanisms, all atomic (Lua) so any number of workers can share them:
  * Sliding-window limiter: at most N units in ANY window of W ms (units = requests or tokens).
    Unlike a token bucket this never allows a 2x burst at a window edge, so a queue burst cannot
    exceed the provider's per-minute limit, which is how we avoid provider 429s.
  * Daily counter: requests/day for free tiers (LLM_RPD).
  * Lease semaphore: max in-flight calls; leases expire so a crashed worker can't leak a slot.

If the wait for capacity would exceed LIMITER_MAX_WAIT_SEC, acquire() raises RateLimited with
self_throttled=True: the stage is parked in a retry queue instead of holding a worker hostage.
"""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime

from ..common.config import Settings
from ..common.errors import RateLimited
from ..common.logging import get_logger
from . import metrics

log = get_logger(__name__)

# KEYS[1]=zset(member→ts)  KEYS[2]=hash(member→cost)
# ARGV: limit, window_ms, cost, now_ms, member      → {granted, wait_ms}
_WINDOW_LUA = """
local limit = tonumber(ARGV[1]); local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3]); local now = tonumber(ARGV[4]); local member = ARGV[5]
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now - window)
for _, m in ipairs(expired) do redis.call('HDEL', KEYS[2], m) end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local used = 0
for _, v in ipairs(redis.call('HVALS', KEYS[2])) do used = used + tonumber(v) end
if used == 0 or used + cost <= limit then
  redis.call('ZADD', KEYS[1], now, member)
  redis.call('HSET', KEYS[2], member, cost)
  redis.call('PEXPIRE', KEYS[1], window * 2); redis.call('PEXPIRE', KEYS[2], window * 2)
  return {1, 0}
end
local entries = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
local freed = 0; local wait = window
for i = 1, #entries, 2 do
  freed = freed + tonumber(redis.call('HGET', KEYS[2], entries[i]) or '0')
  if used - freed + cost <= limit then wait = tonumber(entries[i + 1]) + window - now; break end
end
if wait < 1 then wait = 1 end
return {0, wait}
"""

# KEYS[1]=zset(holder→expiry_ms)  ARGV: limit, now_ms, lease_ms, holder   → 1 granted / 0 busy
_SEMAPHORE_LUA = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', tonumber(ARGV[2]))
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[1]) then
  redis.call('ZADD', KEYS[1], tonumber(ARGV[2]) + tonumber(ARGV[3]), ARGV[4])
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]) * 2)
  return 1
end
return 0
"""


@dataclass
class Window:
    name: str
    limit: int
    window_ms: int


@dataclass
class ApiLimits:
    windows: list[Window] = field(default_factory=list)  # request-count windows
    token_window: Window | None = None  # token-count window (LLM only)
    daily: int = 0  # 0 = unlimited
    concurrency: int = 0  # 0 = unlimited


def limits_from_settings(s: Settings) -> dict[str, ApiLimits]:
    llm = ApiLimits(daily=s.llm_rpd, concurrency=s.llm_max_concurrency)
    if s.llm_rpm > 0:
        llm.windows.append(Window("rpm", s.llm_rpm, 60_000))
    if s.llm_tpm > 0:
        llm.token_window = Window("tpm", s.llm_tpm, 60_000)
    notion = ApiLimits()
    if s.notion_rps > 0:  # 2-second window smooths bursts but keeps the average
        notion.windows.append(Window("rps", max(int(s.notion_rps * 2), 1), 2_000))
    youtube = ApiLimits(concurrency=s.youtube_max_concurrency)
    if s.youtube_rpm > 0:
        youtube.windows.append(Window("rpm", s.youtube_rpm, 60_000))
    return {"llm": llm, "notion": notion, "youtube": youtube}


@dataclass
class Permit:
    api: str
    token_member: str | None = None
    holder: str | None = None


class RateLimiter:
    def __init__(self, redis, settings: Settings, limits: dict[str, ApiLimits] | None = None) -> None:
        self.redis = redis
        self.settings = settings
        self.limits = limits or limits_from_settings(settings)
        self.max_wait = settings.limiter_max_wait_sec
        self._window = redis.register_script(_WINDOW_LUA)
        self._sem = redis.register_script(_SEMAPHORE_LUA)

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    async def _try_window(self, api: str, w: Window, cost: int, member: str) -> int:
        granted, wait = await self._window(
            keys=[f"rl:{api}:{w.name}:z", f"rl:{api}:{w.name}:h"],
            args=[w.limit, w.window_ms, cost, self._now_ms(), member],
        )
        return 0 if int(granted) == 1 else int(wait)

    async def _daily_ok(self, api: str, limit: int) -> bool:
        key = f"rl:{api}:day:{datetime.now(UTC):%Y%m%d}"
        used = await self.redis.incr(key)
        if used == 1:
            await self.redis.expire(key, 48 * 3600)
        if used > limit:
            await self.redis.decr(key)
            return False
        return True

    async def daily_remaining(self, api: str = "llm") -> int | None:
        lim = self.limits[api].daily
        if not lim:
            return None
        used = int(await self.redis.get(f"rl:{api}:day:{datetime.now(UTC):%Y%m%d}") or 0)
        return max(lim - used, 0)

    async def acquire(self, api: str, *, tokens: int = 0) -> Permit:
        lim = self.limits.get(api)
        if lim is None:
            return Permit(api)
        started = time.monotonic()
        permit = Permit(api)

        # 1) request windows + token window: wait (bounded) until all grant
        for w in lim.windows:
            await self._wait_window(api, w, 1, uuid.uuid4().hex, started)
        if lim.token_window and tokens > 0:
            permit.token_member = uuid.uuid4().hex
            await self._wait_window(api, lim.token_window, tokens, permit.token_member, started)

        # 2) daily quota (free tiers)
        if lim.daily and not await self._daily_ok(api, lim.daily):
            raise RateLimited(api, retry_after=None, code=f"{api.upper()}_DAILY_QUOTA",
                              message=f"Daily {api} quota (configured {lim.daily}/day) is used up.")

        # 3) concurrency lease
        if lim.concurrency:
            permit.holder = uuid.uuid4().hex
            lease_ms = self.settings.stage_timeout_sec * 1000
            while not int(await self._sem(keys=[f"sem:{api}"],
                                          args=[lim.concurrency, self._now_ms(), lease_ms, permit.holder])):
                self._check_budget(api, started, 500)
                await asyncio.sleep(0.2 + random.random() * 0.3)

        waited = time.monotonic() - started
        metrics.LIMITER_WAIT.labels(api).observe(waited)
        return permit

    async def _wait_window(self, api: str, w: Window, cost: int, member: str, started: float) -> None:
        while True:
            wait_ms = await self._try_window(api, w, cost, member)
            if wait_ms == 0:
                return
            self._check_budget(api, started, wait_ms)
            await asyncio.sleep(min(wait_ms / 1000, 2.0) + random.random() * 0.2)

    def _check_budget(self, api: str, started: float, wait_ms: int) -> None:
        if time.monotonic() - started + wait_ms / 1000 > self.max_wait:
            metrics.SELF_THROTTLED.labels(api).inc()
            exc = RateLimited(api, retry_after=wait_ms / 1000, code="SELF_THROTTLED",
                              message=f"Waiting for {api} capacity (shared rate limit).")
            exc.self_throttled = True
            raise exc

    async def reconcile(self, permit: Permit, actual_tokens: int) -> None:
        lim = self.limits.get(permit.api)
        if not (lim and lim.token_window and permit.token_member):
            return
        h = f"rl:{permit.api}:{lim.token_window.name}:h"
        if await self.redis.hexists(h, permit.token_member):
            await self.redis.hset(h, permit.token_member, max(int(actual_tokens), 1))

    async def release(self, permit: Permit) -> None:
        if permit.holder:
            await self.redis.zrem(f"sem:{permit.api}", permit.holder)

    async def penalize(self, api: str, seconds: float) -> None:
        """After a real provider 429, fill the request window so every worker backs off."""
        lim = self.limits.get(api)
        if not lim:
            return
        for w in lim.windows:
            await self._window(keys=[f"rl:{api}:{w.name}:z", f"rl:{api}:{w.name}:h"],
                               args=[1_000_000_000, w.window_ms, w.limit,
                                     self._now_ms() - w.window_ms + int(seconds * 1000), uuid.uuid4().hex])

    @asynccontextmanager
    async def limit(self, api: str, *, tokens: int = 0):
        permit = await self.acquire(api, tokens=tokens)
        try:
            yield permit
        finally:
            await self.release(permit)
