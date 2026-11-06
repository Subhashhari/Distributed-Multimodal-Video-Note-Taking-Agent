"""Central configuration. Every value can be set via environment variable (see .env.example)."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

STAGE_NAMES = ("transcribe", "summarize", "frames", "publish")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    # --- pipeline -----------------------------------------------------------
    pipeline_mode: Literal["stub", "real"] = "stub"

    # --- gateway ------------------------------------------------------------
    gateway_base_url: str = "http://localhost:8080"
    cors_allowed_origins: str = "*"

    # --- infrastructure -----------------------------------------------------
    database_url: str = "postgresql+asyncpg://notes:notes@localhost:5432/notes"
    broker_url: str = "amqp://notes:notes@localhost:5672/"
    redis_url: str = "redis://localhost:6379/0"
    auto_create_schema: bool = True

    # --- security -----------------------------------------------------------
    message_enc_key: str = ""
    admin_token: str = ""
    demo_faults_enabled: bool = False

    # --- LLM (used from phase P3) -------------------------------------------
    llm_provider: str = "gemini"
    llm_api_key: str = ""
    llm_model: str = ""
    llm_vision_model: str = ""
    llm_base_url: str = ""
    llm_fallback_provider: str = ""
    llm_fallback_model: str = ""
    llm_rpm: int = 8
    llm_tpm: int = 200_000
    llm_rpd: int = 200
    llm_max_concurrency: int = 2
    llm_chunk_tokens: int = 12_000
    llm_chunk_overlap_sec: int = 15
    llm_max_output_tokens: int = 4000
    vision_verify: bool = True
    vision_batch_size: int = 8

    # --- Notion -------------------------------------------------------------
    notion_oauth_client_id: str = ""
    notion_oauth_client_secret: str = ""
    notion_oauth_redirect_uri: str = ""
    notion_default_parent_id: str = ""
    notion_version: str = "2022-06-28"
    notion_rps: float = 2.5

    # --- YouTube ------------------------------------------------------------
    youtube_rpm: int = 30
    youtube_max_concurrency: int = 3
    ytdlp_cookies_file: str = ""
    transcript_langs: str = "en,en-US,en-GB"
    whisper_fallback: bool = False
    whisper_model: str = "base"

    # --- constraints & frames -----------------------------------------------
    max_video_seconds: int = 1200
    max_frames: int = 8
    min_frame_gap_sec: int = 20
    frame_concurrency: int = 3
    frame_timeout_sec: int = 30

    # --- retries ------------------------------------------------------------
    max_attempts: int = 5
    rl_max_retries: int = 6
    retry_profile: Literal["prod", "demo"] = "prod"
    backoff_schedule_sec: str = "10,30,90"
    demo_backoff_schedule_sec: str = "2,4,8"
    broker_delivery_limit: int = 5
    stage_timeout_sec: int = 600

    # --- worker -------------------------------------------------------------
    worker_stages: str = "all"
    worker_concurrency: int = 1
    worker_poll_interval_sec: float = 0.5
    shutdown_grace_sec: float = 25
    stub_stage_delay_sec: float = 1.0
    heartbeat_ttl_sec: int = 15

    # --- logging ------------------------------------------------------------
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"

    # ------------------------------------------------------------------------
    @property
    def backoff_schedule(self) -> list[int]:
        raw = self.demo_backoff_schedule_sec if self.retry_profile == "demo" else self.backoff_schedule_sec
        delays = sorted({int(x) for x in raw.split(",") if x.strip()})
        if not delays or any(d <= 0 for d in delays):
            raise ValueError(f"Invalid backoff schedule: {raw!r}")
        return delays

    @property
    def stages(self) -> list[str]:
        if self.worker_stages.strip().lower() in ("", "all"):
            return list(STAGE_NAMES)
        chosen = [s.strip() for s in self.worker_stages.split(",") if s.strip()]
        unknown = set(chosen) - set(STAGE_NAMES)
        if unknown:
            raise ValueError(f"Unknown WORKER_STAGES: {sorted(unknown)}")
        return chosen

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.cors_allowed_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
