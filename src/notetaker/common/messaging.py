"""RabbitMQ topology, message envelope and a small broker client (PLAN.md §6).

Topology
  jobs.work   (direct)  ──stage key──▶  stage.<stage>        quorum, x-delivery-limit, DLX=jobs.dlx
  retry.<stage>.<N>s    (classic, TTL=N s, no consumers) ──expires──▶ jobs.work / <stage>
  jobs.dlx    (fanout)  ──▶ jobs.dlq         (inspection only, never auto-consumed)
                        └─▶ jobs.dlq.events  (consumed by workers to mark DB rows DEAD_LETTERED)
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Protocol

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue, AbstractRobustConnection
from pydantic import BaseModel, Field

from .config import Settings
from .logging import get_logger
from .states import STAGES

log = get_logger(__name__)

EXCHANGE_WORK = "jobs.work"
EXCHANGE_DLX = "jobs.dlx"
QUEUE_DLQ = "jobs.dlq"
QUEUE_DLQ_EVENTS = "jobs.dlq.events"


def stage_queue(stage: str) -> str:
    return f"stage.{stage}"


def retry_queue(stage: str, delay_sec: int) -> str:
    # The delay is part of the name so changing the backoff profile never conflicts
    # with an already-declared queue's TTL argument.
    return f"retry.{stage}.{delay_sec}s"


class Envelope(BaseModel):
    v: int = 1
    job_id: uuid.UUID
    stage: str
    attempt: int = 0  # transient failures so far (job-wide)
    rl_attempt: int = 0  # rate-limit retries so far (this stage)
    enc_notion_token: str
    notion_parent_id: str | None = None
    trace: dict[str, str] = Field(default_factory=dict)
    debug: dict[str, Any] = Field(default_factory=dict)

    def for_stage(self, stage: str) -> Envelope:
        return self.model_copy(update={"stage": stage, "rl_attempt": 0})

    @property
    def message_id(self) -> str:
        return f"{self.job_id}:{self.stage}:{self.attempt}:{self.rl_attempt}"


class BrokerProtocol(Protocol):
    async def publish_stage(self, env: Envelope) -> None: ...
    async def publish_retry(self, env: Envelope, delay_sec: int) -> None: ...
    async def publish_dead_letter(self, env: Envelope, error: dict) -> None: ...
    async def ping(self) -> None: ...


def _message(env: Envelope, headers: dict | None = None) -> aio_pika.Message:
    return aio_pika.Message(
        body=env.model_dump_json().encode(),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        message_id=env.message_id,
        headers={"x-job-id": str(env.job_id), "x-stage": env.stage, **(headers or {})},
    )


class Broker:
    """Thin aio-pika wrapper. Publishes use publisher confirms (await = broker has it durably)."""

    def __init__(self, settings: Settings, *, name: str = "notetaker") -> None:
        self.settings = settings
        self.name = name
        self._conn: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._work: AbstractExchange | None = None
        self._dlx: AbstractExchange | None = None
        self._queues: dict[str, AbstractQueue] = {}

    # ------------------------------------------------------------------ lifecycle
    async def connect(self, *, attempts: int = 30, delay: float = 2.0) -> None:
        last: Exception | None = None
        for i in range(attempts):
            try:
                self._conn = await aio_pika.connect_robust(
                    self.settings.broker_url, heartbeat=30, client_properties={"connection_name": self.name}
                )
                break
            except Exception as exc:  # broker still starting
                last = exc
                log.warning("broker_connect_retry", attempt=i + 1, error=str(exc))
                await asyncio.sleep(delay)
        else:
            raise RuntimeError(f"Could not connect to RabbitMQ: {last}")
        self._channel = await self._conn.channel(publisher_confirms=True, on_return_raises=True)
        await self.declare_topology()

    async def close(self) -> None:
        if self._conn and not self._conn.is_closed:
            await self._conn.close()

    @property
    def channel(self) -> AbstractChannel:
        assert self._channel is not None, "Broker not connected"
        return self._channel

    async def declare_topology(self) -> None:
        ch = self.channel
        s = self.settings
        self._work = await ch.declare_exchange(EXCHANGE_WORK, aio_pika.ExchangeType.DIRECT, durable=True)
        self._dlx = await ch.declare_exchange(EXCHANGE_DLX, aio_pika.ExchangeType.FANOUT, durable=True)

        for name in (QUEUE_DLQ, QUEUE_DLQ_EVENTS):
            q = await ch.declare_queue(name, durable=True, arguments={"x-queue-type": "quorum"})
            await q.bind(self._dlx)
            self._queues[name] = q

        for stage in STAGES:
            q = await ch.declare_queue(
                stage_queue(stage),
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-delivery-limit": s.broker_delivery_limit,
                    "x-dead-letter-exchange": EXCHANGE_DLX,
                    "x-dead-letter-strategy": "at-least-once",
                    "x-overflow": "reject-publish",
                },
            )
            await q.bind(self._work, routing_key=stage)
            self._queues[stage_queue(stage)] = q

            for delay in s.backoff_schedule:
                rq = await ch.declare_queue(
                    retry_queue(stage, delay),
                    durable=True,
                    arguments={
                        "x-message-ttl": delay * 1000,
                        "x-dead-letter-exchange": EXCHANGE_WORK,
                        "x-dead-letter-routing-key": stage,
                    },
                )
                self._queues[retry_queue(stage, delay)] = rq

    # ------------------------------------------------------------------ publish
    async def publish_stage(self, env: Envelope) -> None:
        assert self._work is not None
        await self._work.publish(_message(env), routing_key=env.stage, mandatory=True)

    async def publish_retry(self, env: Envelope, delay_sec: int) -> None:
        await self.channel.default_exchange.publish(
            _message(env, {"x-retry-delay": delay_sec}), routing_key=retry_queue(env.stage, delay_sec),
            mandatory=True,
        )

    async def publish_dead_letter(self, env: Envelope, error: dict) -> None:
        assert self._dlx is not None
        await self._dlx.publish(
            _message(env, {"x-last-error": json.dumps(error)[:1000], "x-attempts": env.attempt}),
            routing_key="",
        )

    # ------------------------------------------------------------------ consume
    async def get(self, queue_name: str) -> aio_pika.abc.AbstractIncomingMessage | None:
        return await self._queues[queue_name].get(no_ack=False, fail=False)

    def queue(self, name: str) -> AbstractQueue:
        return self._queues[name]

    async def ping(self) -> None:
        if self._conn is None or self._conn.is_closed:
            raise RuntimeError("not connected")
        # A failed passive declare closes the channel, so use a throwaway one.
        async with self._conn.channel() as ch:
            await ch.declare_queue(stage_queue("transcribe"), passive=True)
