# Build context must be the project root:
#   docker build -f services/api-gateway/Dockerfile .
# (docker-compose handles this automatically via `context: .`)

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Copy workspace manifests — changes here invalidate the dep-install layer,
# but source code changes do not, giving fast rebuilds.
COPY pyproject.toml uv.lock ./
COPY services/api-gateway/pyproject.toml ./services/api-gateway/pyproject.toml
COPY services/worker/pyproject.toml       ./services/worker/pyproject.toml
COPY services/shared/pyproject.toml      ./services/shared/pyproject.toml

# Install only the api-gateway dependencies (no worker deps, no dev tools).
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package api-gateway

# ---------------------------------------------------------------------------
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1

# Apply all available OS security patches from Debian repos.
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the virtualenv produced by the builder stage.
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy application source.
COPY services/shared ./services/shared
COPY services/api-gateway/app ./app

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

# Run as non-root user.
RUN useradd --no-create-home --shell /bin/false appuser \
    && chown -R appuser:appuser /app/.models
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
