"""Database models and engine. PostgreSQL in production; SQLite (aiosqlite) works for tests."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    SmallInteger,
    String,
    Text,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JSONType = JSON().with_variant(JSONB(), "postgresql")


def utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    video_id: Mapped[str] = mapped_column(String(16))
    youtube_url: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), index=True)
    progress: Mapped[int] = mapped_column(SmallInteger, default=0)
    video_title: Mapped[str | None] = mapped_column(Text, default=None)
    duration_sec: Mapped[int | None] = mapped_column(Integer, default=None)
    notion_parent_id: Mapped[str | None] = mapped_column(String(64), default=None)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), unique=True, default=None)

    attempt: Mapped[int] = mapped_column(SmallInteger, default=0)  # transient failures (job-wide)
    rl_attempt: Mapped[int] = mapped_column(SmallInteger, default=0)  # rate-limit retries (current stage)
    retry_reason: Mapped[str | None] = mapped_column(String(32), default=None)
    retry_api: Mapped[str | None] = mapped_column(String(16), default=None)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)

    warnings: Mapped[list] = mapped_column(JSONType, default=list)
    error: Mapped[dict | None] = mapped_column(JSONType, default=None)
    result: Mapped[dict | None] = mapped_column(JSONType, default=None)
    publish_state: Mapped[str | None] = mapped_column(String(32), default=None)
    worker_id: Mapped[str | None] = mapped_column(String(128), default=None)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)


class JobEvent(Base):
    """Append-only audit log: transitions, retries, redeliveries, warnings."""

    __tablename__ = "job_events"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    job_id: Mapped[uuid.UUID] = mapped_column(Uuid, ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    kind: Mapped[str] = mapped_column(String(32))
    from_status: Mapped[str | None] = mapped_column(String(32), default=None)
    to_status: Mapped[str | None] = mapped_column(String(32), default=None)
    detail: Mapped[dict | None] = mapped_column(JSONType, default=None)
    worker_id: Mapped[str | None] = mapped_column(String(128), default=None)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class StageArtifact(Base):
    """Durable output of a stage (or part of one), so redelivery never redoes finished work."""

    __tablename__ = "stage_artifacts"

    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True
    )
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    data: Mapped[dict] = mapped_column(JSONType)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Frame(Base):
    """One LLM-flagged timestamp and the real frame captured for it (if any)."""

    __tablename__ = "frames"

    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True
    )
    timestamp_sec: Mapped[int] = mapped_column(Integer, primary_key=True)
    caption: Mapped[str] = mapped_column(Text)
    why_flagged: Mapped[str] = mapped_column(Text, default="")
    importance: Mapped[int] = mapped_column(SmallInteger, default=3)
    status: Mapped[str] = mapped_column(String(16), default="PENDING")  # PENDING|CAPTURED|FAILED|REJECTED
    failure_code: Mapped[str | None] = mapped_column(String(64), default=None)
    image_jpeg: Mapped[bytes | None] = mapped_column(LargeBinary, default=None)
    width: Mapped[int | None] = mapped_column(Integer, default=None)
    height: Mapped[int | None] = mapped_column(Integer, default=None)
    sha256: Mapped[str | None] = mapped_column(String(64), default=None)
    notion_file_upload_id: Mapped[str | None] = mapped_column(String(64), default=None)
    notion_upload_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)


class Database:
    def __init__(self, url: str) -> None:
        kwargs: dict = {"pool_pre_ping": True}
        if url.startswith("sqlite"):
            kwargs = {}
        self.engine: AsyncEngine = create_async_engine(url, **kwargs)
        self.session = async_sessionmaker(self.engine, expire_on_commit=False)

    async def create_all(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def ping(self) -> None:
        async with self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def dispose(self) -> None:
        await self.engine.dispose()
