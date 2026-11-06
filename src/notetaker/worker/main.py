"""Worker process. Run: python -m notetaker.worker.main

Each worker PULLS one message at a time (basic.get) from the stage queues, later stages first, so
in-flight jobs finish before new ones start. Exactly one unacked message per concurrency slot means
nothing is hoarded by a busy worker during a burst, and a SIGKILL returns at most that one message
to the queue for another worker.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import time

from prometheus_client import start_http_server
from redis.asyncio import Redis

from .. import __version__
from ..common.config import Settings, get_settings
from ..common.crypto import TokenCipher
from ..common.db import Database
from ..common.faults import Faults
from ..common.logging import configure_logging, get_logger
from ..common.messaging import QUEUE_DLQ_EVENTS, Broker, stage_queue
from ..common.states import STAGES
from .runner import StageRunner, handle_dlq_event
from .stages import build_handlers

log = get_logger(__name__)


def make_worker_id() -> str:
    return f"{socket.gethostname()[:24]}-{os.getpid()}"


async def heartbeat(redis: Redis, worker_id: str, settings: Settings, stop: asyncio.Event) -> None:
    key = f"worker:{worker_id}:hb"
    payload = json.dumps({"stages": settings.stages, "started": time.time(), "version": __version__})
    while not stop.is_set():
        try:
            await redis.set(key, payload, ex=settings.heartbeat_ttl_sec)
        except Exception as exc:
            log.warning("heartbeat_failed", error=str(exc))
        try:
            await asyncio.wait_for(stop.wait(), timeout=max(settings.heartbeat_ttl_sec / 3, 1))
        except TimeoutError:
            pass
    try:
        await redis.delete(key)
    except Exception:
        pass


async def poll_loop(slot: int, broker: Broker, runner: StageRunner, settings: Settings,
                    stop: asyncio.Event) -> None:
    queues = [stage_queue(s) for s in reversed(STAGES) if s in settings.stages]
    while not stop.is_set():
        msg = None
        try:
            for q in queues:
                msg = await broker.get(q)
                if msg is not None:
                    break
        except Exception as exc:
            log.warning("broker_get_failed", error=str(exc))
            await asyncio.sleep(2)
            continue
        if msg is None:
            try:
                await asyncio.wait_for(stop.wait(), timeout=settings.worker_poll_interval_sec)
            except TimeoutError:
                pass
            continue
        try:
            await runner.process(msg.body, redelivered=bool(msg.redelivered))
        except Exception as exc:
            # Infrastructure failure (DB/broker down mid-stage). Give the message back; the quorum
            # queue's delivery-limit eventually dead-letters a message that can never be processed.
            log.exception("runner_crashed", slot=slot, error=str(exc))
            try:
                await msg.nack(requeue=True)
            except Exception:
                pass
            await asyncio.sleep(2)
            continue
        await msg.ack()


async def dlq_watcher(broker: Broker, db: Database, stop: asyncio.Event) -> None:
    queue = broker.queue(QUEUE_DLQ_EVENTS)
    async with queue.iterator() as it:
        async for msg in it:
            try:
                await handle_dlq_event(db, msg.body)
                await msg.ack()
            except Exception as exc:
                log.warning("dlq_event_failed", error=str(exc))
                await msg.nack(requeue=True)
                await asyncio.sleep(1)
            if stop.is_set():
                break


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    worker_id = make_worker_id()
    log.info("worker_starting", worker_id=worker_id, stages=settings.stages,
             pipeline_mode=settings.pipeline_mode, version=__version__)

    db = Database(settings.database_url)
    if settings.auto_create_schema:
        await db.create_all()
    broker = Broker(settings, name=f"worker-{worker_id}")
    await broker.connect()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    faults = Faults(redis, settings.demo_faults_enabled)
    handlers = build_handlers(settings, redis=redis, faults=faults)
    runner = StageRunner(settings, db, broker, faults, TokenCipher(settings.message_enc_key), handlers, worker_id)
    if settings.metrics_port:
        start_http_server(settings.metrics_port)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):  # Windows dev
            pass

    hb = asyncio.create_task(heartbeat(redis, worker_id, settings, stop))
    dlq = asyncio.create_task(dlq_watcher(broker, db, stop))
    loops = [asyncio.create_task(poll_loop(i, broker, runner, settings, stop))
             for i in range(max(settings.worker_concurrency, 1))]
    log.info("worker_ready", worker_id=worker_id)

    await stop.wait()
    log.info("worker_stopping", grace_sec=settings.shutdown_grace_sec)
    # Let in-flight stages finish (they ack); anything still running after the grace period is
    # cancelled, stays unacked, and is redelivered to another worker.
    _, pending = await asyncio.wait(loops, timeout=settings.shutdown_grace_sec)
    for t in [*pending, dlq]:
        t.cancel()
    await asyncio.gather(*pending, dlq, return_exceptions=True)
    await hb
    await broker.close()
    await redis.aclose()
    await db.dispose()
    log.info("worker_stopped", worker_id=worker_id)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
