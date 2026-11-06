"""Create database tables. Run once before gateway/workers: python -m notetaker.common.migrate"""

from __future__ import annotations

import asyncio

from .config import get_settings
from .db import Database
from .logging import configure_logging, get_logger


async def _run(attempts: int = 30) -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    log = get_logger(__name__)
    db = Database(settings.database_url)
    for i in range(attempts):
        try:
            await db.create_all()
            log.info("schema_ready")
            break
        except Exception as exc:
            log.warning("schema_retry", attempt=i + 1, error=str(exc))
            await asyncio.sleep(2)
    else:
        raise SystemExit("database not reachable")
    await db.dispose()


if __name__ == "__main__":
    asyncio.run(_run())
