# API Gateway image: validation + enqueue only (no ffmpeg, yt-dlp or LLM SDKs here).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install .

RUN useradd --system --uid 10001 --no-create-home app
USER app

EXPOSE 8080
ENTRYPOINT ["tini", "--"]
CMD ["uvicorn", "--factory", "notetaker.gateway.main:create_app", "--host", "0.0.0.0", "--port", "8080", "--workers", "2", "--no-access-log"]
