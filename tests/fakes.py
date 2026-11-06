"""In-memory stand-ins for RabbitMQ used by tests."""

from __future__ import annotations

from notetaker.common.messaging import Envelope


class FakeBroker:
    def __init__(self) -> None:
        self.stage: list[Envelope] = []
        self.retries: list[tuple[Envelope, int]] = []
        self.dead: list[tuple[Envelope, dict]] = []
        self.fail_publish = False
        self.healthy = True

    async def publish_stage(self, env: Envelope) -> None:
        if self.fail_publish:
            raise ConnectionError("broker down")
        self.stage.append(env)

    async def publish_retry(self, env: Envelope, delay_sec: int) -> None:
        self.retries.append((env, delay_sec))

    async def publish_dead_letter(self, env: Envelope, error: dict) -> None:
        self.dead.append((env, error))

    async def ping(self) -> None:
        if not self.healthy:
            raise ConnectionError("broker down")

    def pop_stage(self) -> Envelope | None:
        return self.stage.pop(0) if self.stage else None

    def release_retries(self) -> None:
        """Simulate the retry TTL expiring: messages flow back to their stage queue."""
        for env, _ in self.retries:
            self.stage.append(env)
        self.retries.clear()
