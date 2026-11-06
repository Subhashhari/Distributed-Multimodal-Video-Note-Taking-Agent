import asyncio
import time

import pytest

from notetaker.common.errors import RateLimited
from notetaker.worker.ratelimit import ApiLimits, RateLimiter, Window


def limiter(redis, settings, **limits) -> RateLimiter:
    return RateLimiter(redis, settings, {"llm": ApiLimits(**limits)})


async def test_window_grants_up_to_limit_then_self_throttles(redis, settings):
    settings.limiter_max_wait_sec = 0.5
    rl = limiter(redis, settings, windows=[Window("rpm", 3, 60_000)])
    for _ in range(3):
        await rl.acquire("llm")
    with pytest.raises(RateLimited) as exc:
        await rl.acquire("llm")
    assert exc.value.self_throttled and exc.value.code == "SELF_THROTTLED"
    assert 59 <= exc.value.retry_after <= 60


async def test_window_waits_briefly_when_capacity_frees_soon(redis, settings):
    settings.limiter_max_wait_sec = 5
    rl = limiter(redis, settings, windows=[Window("rps", 2, 1_000)])
    started = time.monotonic()
    for _ in range(4):
        await rl.acquire("llm")
    assert 0.8 <= time.monotonic() - started < 4  # waited ~1 window, no error


async def test_many_concurrent_workers_never_exceed_the_window(redis, settings):
    settings.limiter_max_wait_sec = 0.2
    rl = limiter(redis, settings, windows=[Window("rpm", 5, 60_000)])

    async def worker():
        try:
            await rl.acquire("llm")
            return True
        except RateLimited:
            return False

    results = await asyncio.gather(*(worker() for _ in range(20)))
    assert sum(results) == 5  # exactly the limit, across all "workers"


async def test_token_window_and_reconcile(redis, settings):
    settings.limiter_max_wait_sec = 0.2
    rl = limiter(redis, settings, token_window=Window("tpm", 1000, 60_000))
    p = await rl.acquire("llm", tokens=900)  # estimate
    await rl.reconcile(p, 300)  # actual usage was smaller → capacity refunded
    await rl.acquire("llm", tokens=600)
    with pytest.raises(RateLimited):
        await rl.acquire("llm", tokens=200)


async def test_daily_quota(redis, settings):
    rl = limiter(redis, settings, daily=2)
    await rl.acquire("llm")
    await rl.acquire("llm")
    assert await rl.daily_remaining("llm") == 0
    with pytest.raises(RateLimited) as exc:
        await rl.acquire("llm")
    assert exc.value.code == "LLM_DAILY_QUOTA" and not exc.value.self_throttled


async def test_concurrency_semaphore(redis, settings):
    settings.limiter_max_wait_sec = 0.3
    rl = limiter(redis, settings, concurrency=1)
    p = await rl.acquire("llm")
    with pytest.raises(RateLimited):
        await rl.acquire("llm")
    await rl.release(p)
    await rl.acquire("llm")


async def test_unknown_api_is_unlimited(redis, settings):
    rl = limiter(redis, settings)
    for _ in range(10):
        await rl.acquire("other")
