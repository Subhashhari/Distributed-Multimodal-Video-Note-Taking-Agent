"""API Gateway. Run: uvicorn --factory notetaker.gateway.main:create_app --port 8080"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis

from .. import __version__
from ..common.config import Settings, get_settings
from ..common.crypto import TokenCipher
from ..common.db import Database
from ..common.logging import configure_logging, get_logger
from ..common.messaging import Broker
from . import routes_admin, routes_jobs, routes_system
from .api_errors import install_error_handlers

log = get_logger(__name__)


def create_app(settings: Settings | None = None, *, broker=None, redis=None) -> FastAPI:
    """App factory. Tests inject a fake broker / redis; production connects to the real ones."""
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging(settings.log_level, settings.log_format)
        app.state.settings = settings
        app.state.cipher = TokenCipher(settings.message_enc_key)
        app.state.db = Database(settings.database_url)
        if settings.auto_create_schema:
            await app.state.db.create_all()

        owned_broker = broker is None
        app.state.broker = broker or Broker(settings, name="gateway")
        if owned_broker:
            await app.state.broker.connect()

        owned_redis = redis is None
        app.state.redis = redis or Redis.from_url(settings.redis_url, decode_responses=True)

        log.info("gateway_started", version=__version__, pipeline_mode=settings.pipeline_mode)
        try:
            yield
        finally:
            if owned_broker:
                await app.state.broker.close()
            if owned_redis:
                await app.state.redis.aclose()
            await app.state.db.dispose()

    app = FastAPI(title="Video Notes Gateway", version=__version__, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins or ["*"],
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Idempotency-Key", "Authorization"],
        expose_headers=["Location"],
    )
    install_error_handlers(app)
    app.include_router(routes_system.router)
    app.include_router(routes_jobs.router)
    app.include_router(routes_admin.router)
    return app
