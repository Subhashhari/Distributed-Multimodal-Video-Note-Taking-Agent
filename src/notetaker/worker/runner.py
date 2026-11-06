"""Processes one stage message: claim → run → persist → publish next → (caller acks).

Ack protocol (PLAN.md §6.4). The caller acks ONLY after process() returns, which means:
  * the stage's outputs are committed to Postgres, and
  * the next message (next stage / retry / dead-letter) is confirmed by the broker.
If the worker dies anywhere before that, the message is unacked and RabbitMQ redelivers it.
Stages are idempotent, so a redelivery resumes safely.
"""

from __future__ import annotations

import asyncio
from enum import StrEnum

from pydantic import ValidationError

from ..common import repo
from ..common.config import Settings
from ..common.crypto import TokenCipher
from ..common.db import Database
from ..common.errors import FatalError, RateLimited
from ..common.faults import Faults
from ..common.logging import get_logger
from ..common.messaging import BrokerProtocol, Envelope
from ..common.repo import ClaimOutcome
from ..common.states import STAGES, next_stage
from .context import StageContext, StageHandler
from .retry import pick_delay

log = get_logger(__name__)


class Outcome(StrEnum):
    ADVANCED = "ADVANCED"
    COMPLETED = "COMPLETED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    FAILED = "FAILED"
    DEAD_LETTERED = "DEAD_LETTERED"
    DROPPED = "DROPPED"


class StageRunner:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        broker: BrokerProtocol,
        faults: Faults,
        cipher: TokenCipher,
        handlers: dict[str, StageHandler],
        worker_id: str,
    ) -> None:
        self.settings = settings
        self.db = db
        self.broker = broker
        self.faults = faults
        self.cipher = cipher
        self.handlers = handlers
        self.worker_id = worker_id

    async def process(self, body: bytes, *, redelivered: bool = False) -> Outcome:
        try:
            env = Envelope.model_validate_json(body)
        except ValidationError as exc:
            log.error("bad_message_dropped", error=str(exc)[:300])
            return Outcome.DROPPED
        if env.stage not in STAGES:
            log.error("unknown_stage_dropped", stage=env.stage)
            return Outcome.DROPPED

        lg = log.bind(job_id=str(env.job_id), stage=env.stage, worker_id=self.worker_id,
                      attempt=env.attempt, rl_attempt=env.rl_attempt)

        claim, job = await repo.claim_stage(self.db, env.job_id, env.stage, self.worker_id,
                                            redelivered=redelivered)
        if claim not in (ClaimOutcome.CLAIMED, ClaimOutcome.RESUMED):
            lg.info("message_dropped", reason=claim.value)
            return Outcome.DROPPED
        assert job is not None
        lg.info("stage_started", claim=claim.value, redelivered=redelivered)

        ctx = StageContext(job=job, env=env, settings=self.settings, db=self.db, faults=self.faults,
                           cipher=self.cipher, worker_id=self.worker_id, log=lg)
        handler = self.handlers[env.stage]
        try:
            result = await asyncio.wait_for(handler.run(ctx), timeout=self.settings.stage_timeout_sec)
        except RateLimited as exc:
            return await self._on_rate_limited(env, exc, lg)
        except FatalError as exc:
            await repo.mark_failed(self.db, env.job_id, code=exc.code, message=exc.message, stage=env.stage)
            lg.warning("job_failed", code=exc.code, error=exc.message)
            return Outcome.FAILED
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # TransientError, timeouts, and anything unexpected
            return await self._on_transient(env, exc, lg)

        nxt = next_stage(env.stage)
        if nxt is None:
            status = await repo.complete_job(self.db, env.job_id, result or {})
            lg.info("job_completed", status=status)
            return Outcome.COMPLETED
        await self.broker.publish_stage(env.for_stage(nxt))
        lg.info("stage_finished", next_stage=nxt)
        return Outcome.ADVANCED

    # ------------------------------------------------------------------ failure paths
    async def _on_rate_limited(self, env: Envelope, exc: RateLimited, lg) -> Outcome:
        rl = env.rl_attempt + 1
        if rl > self.settings.rl_max_retries:
            return await self._dead_letter(env, "RATE_LIMIT_BUDGET_EXHAUSTED",
                                           f"{exc.api} kept rate-limiting after {rl - 1} retries.", lg)
        delay = pick_delay(self.settings.backoff_schedule, rl, exc.retry_after)
        retry_env = env.model_copy(update={"rl_attempt": rl})
        await self.broker.publish_retry(retry_env, delay)
        await repo.schedule_retry(self.db, env.job_id, stage=env.stage, reason="RATE_LIMITED", api=exc.api,
                                  attempt=env.attempt, rl_attempt=rl, delay_sec=delay, code=exc.code,
                                  message=exc.message)
        lg.info("retry_scheduled", reason="RATE_LIMITED", api=exc.api, delay_sec=delay, rl_attempt=rl)
        return Outcome.RETRY_SCHEDULED

    async def _on_transient(self, env: Envelope, exc: Exception, lg) -> Outcome:
        attempt = env.attempt + 1
        code = getattr(exc, "code", None) or ("STAGE_TIMEOUT" if isinstance(exc, TimeoutError) else
                                              "UNEXPECTED_ERROR")
        message = getattr(exc, "message", None) or str(exc) or type(exc).__name__
        if code == "UNEXPECTED_ERROR":
            lg.exception("stage_error_unexpected", error=message)
        if attempt >= self.settings.max_attempts:
            return await self._dead_letter(env.model_copy(update={"attempt": attempt}), code, message, lg)
        delay = pick_delay(self.settings.backoff_schedule, attempt)
        retry_env = env.model_copy(update={"attempt": attempt})
        await self.broker.publish_retry(retry_env, delay)
        await repo.schedule_retry(self.db, env.job_id, stage=env.stage, reason="TRANSIENT_ERROR", api=None,
                                  attempt=attempt, rl_attempt=env.rl_attempt, delay_sec=delay, code=code,
                                  message=message)
        lg.warning("retry_scheduled", reason="TRANSIENT_ERROR", code=code, delay_sec=delay, attempt=attempt,
                   error=message[:300])
        return Outcome.RETRY_SCHEDULED

    async def _dead_letter(self, env: Envelope, code: str, message: str, lg) -> Outcome:
        error = {"code": code, "message": message, "stage": env.stage, "attempts": env.attempt}
        await repo.mark_dead_lettered(self.db, env.job_id, code=code, message=message, stage=env.stage,
                                      attempts=env.attempt)
        await self.broker.publish_dead_letter(env, error)
        lg.error("job_dead_lettered", code=code, error=message[:300])
        return Outcome.DEAD_LETTERED


async def handle_dlq_event(db: Database, body: bytes) -> bool:
    """A message reached the DLX. If the job isn't terminal yet (broker delivery-limit hit because
    the message kept crashing workers), mark it DEAD_LETTERED. Returns True if we changed the job."""
    try:
        env = Envelope.model_validate_json(body)
    except ValidationError:
        return False
    changed = await repo.mark_dead_lettered(
        db, env.job_id, code="CRASH_LOOP",
        message="This job repeatedly crashed workers and was moved to the dead-letter queue.",
        stage=env.stage, attempts=env.attempt,
    )
    if changed:
        log.error("job_dead_lettered", job_id=str(env.job_id), code="CRASH_LOOP")
    return changed
