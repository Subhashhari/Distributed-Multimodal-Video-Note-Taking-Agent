"""Structured JSON logging with secret redaction."""

from __future__ import annotations

import logging
import sys

import structlog

REDACT_KEYS = {
    "notion_token",
    "enc_notion_token",
    "access_token",
    "refresh_token",
    "authorization",
    "llm_api_key",
    "api_key",
    "client_secret",
    "message_enc_key",
    "admin_token",
}


def _redact(_logger, _method, event_dict):
    for key in list(event_dict):
        if key.lower() in REDACT_KEYS:
            event_dict[key] = "***"
    return event_dict


def configure_logging(level: str = "INFO", fmt: str = "json") -> None:
    numeric = logging.getLevelName(level.upper())
    if not isinstance(numeric, int):
        numeric = logging.INFO
    logging.basicConfig(level=numeric, stream=sys.stdout, format="%(message)s")
    renderer = structlog.processors.JSONRenderer() if fmt == "json" else structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _redact,
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str | None = None):
    return structlog.get_logger(name)
