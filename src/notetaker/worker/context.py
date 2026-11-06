"""What a stage handler gets to work with."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Protocol

from ..common import repo
from ..common.config import Settings
from ..common.crypto import TokenCipher
from ..common.db import Database, Job
from ..common.faults import Faults
from ..common.messaging import Envelope
from ..common.states import stage_progress


@dataclass
class StageContext:
    job: Job
    env: Envelope
    settings: Settings
    db: Database
    faults: Faults
    cipher: TokenCipher
    worker_id: str
    log: Any
    extras: dict = field(default_factory=dict)

    @property
    def stage(self) -> str:
        return self.env.stage

    @cached_property
    def notion_token(self) -> str:
        return self.cipher.decrypt(self.env.enc_notion_token)

    @property
    def debug(self) -> dict:
        return self.env.debug if self.settings.demo_faults_enabled else {}

    async def progress(self, fraction: float) -> None:
        await repo.set_progress(self.db, self.job.id, stage_progress(self.stage, fraction))

    async def fault_delay(self) -> None:
        """Demo: slow the stage down so a grader has time to kill the worker mid-stage."""
        extra = await self.faults.value("stage_delay_sec")
        if extra > 0:
            await asyncio.sleep(extra)


class StageHandler(Protocol):
    async def run(self, ctx: StageContext) -> dict | None:
        """Do the stage's work idempotently and persist its output.

        Return None for intermediate stages; the final (publish) stage returns the job result dict.
        """
        ...
