# Build context must be the project root:
#   docker build -f services/worker/Dockerfile .

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
COPY services/api-gateway/pyproject.toml ./services/api-gateway/pyproject.toml
COPY services/worker/pyproject.toml       ./services/worker/pyproject.toml
COPY services/shared/pyproject.toml      ./services/shared/pyproject.toml

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package worker

# ---------------------------------------------------------------------------
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1

# Apply all available OS security patches from Debian repos.
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY services/shared ./services/shared
COPY services/worker/app ./app

# The venv's editable install for moiraweave_shared has a path-hook that was
# built on the CI/dev host, so it won't resolve inside the image.  Fix by
# adding the source directory to the Python path via a .pth file.
RUN echo '/app/services/shared' > /app/.venv/lib/python3.13/site-packages/moiraweave_shared_local.pth

# Optionally pre-download the embedding model. Keep this disabled by default so
# reproducible image builds do not depend on external model registry uptime.
ARG PRELOAD_FASTEMBED_MODEL=false
ENV FASTEMBED_CACHE_PATH=/app/.models
RUN mkdir -p /app/.models \
    && if [ "$PRELOAD_FASTEMBED_MODEL" = "true" ]; then \
        python -c "from fastembed import TextEmbedding; list(TextEmbedding('BAAI/bge-small-en-v1.5').embed(['warmup']))"; \
    fi

RUN useradd --no-create-home --shell /bin/false appuser \
    && chown -R appuser:appuser /app/.models
USER appuser

CMD ["python", "-m", "app.main"]
