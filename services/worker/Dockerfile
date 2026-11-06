# Worker image: runs the pipeline stages. ffmpeg is needed for real frame extraction.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install .

RUN useradd --system --uid 10001 --no-create-home app
USER app

# tini forwards SIGTERM so the worker can finish its current stage (graceful shutdown).
# SIGKILL skips that entirely: the unacked message is redelivered to another worker.
ENTRYPOINT ["tini", "--"]
CMD ["python", "-m", "notetaker.worker.main"]
