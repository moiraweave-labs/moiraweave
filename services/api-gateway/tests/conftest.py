"""Shared fixtures for api-gateway tests.

Environment is configured here at module level so that ``app.main``
imports (which call ``get_settings()``) succeed without a real .env file.
"""

import os
import pathlib
import sys
from collections.abc import AsyncGenerator, Generator
from unittest.mock import AsyncMock, MagicMock

import pytest
from fakeredis.aioredis import FakeRedis
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from moiraweave_shared.control_plane import InMemoryControlPlaneRepository
from qdrant_client import AsyncQdrantClient

# ---------------------------------------------------------------------------
# sys.path: ensure this service's root is first so that `app.*` resolves to
# api-gateway, even when pytest is collecting tests from multiple services.
# ---------------------------------------------------------------------------
_SERVICE_ROOT = str(pathlib.Path(__file__).resolve().parents[1])
for _k in list(sys.modules):
    if _k == "app" or _k.startswith("app."):
        del sys.modules[_k]
if _SERVICE_ROOT not in sys.path:
    sys.path.insert(0, _SERVICE_ROOT)

# ---------------------------------------------------------------------------
# Set test env vars BEFORE any app module is imported.
# ``jwt_secret_key`` has no default, so it must exist when Settings loads.
# ---------------------------------------------------------------------------
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-moiraweave-32chars!!")
os.environ.setdefault("OTEL_ENABLED", "false")

# App imports must come AFTER env vars are set.
from app.config import get_settings  # noqa: E402
from app.dependencies.auth import get_current_user  # noqa: E402
from app.main import app  # noqa: E402
from app.models.auth import TokenData  # noqa: E402

# Snapshot of ALL app.* modules loaded at conftest import time (gateway's).
# Used by _restore_gateway_app to re-populate sys.modules before each test so
# that patch("app.routes.workloads.xxx") resolves to the gateway, not the worker
# or step (which also clear and reload app.* during collection).
_GATEWAY_APP_MODULES: dict[str, object] = {
    k: v for k, v in sys.modules.items() if k == "app" or k.startswith("app.")
}


@pytest.fixture(autouse=True)
def _restore_gateway_app() -> None:
    """Re-populate sys.modules with the gateway's app.* before each test."""
    sys.modules.update(_GATEWAY_APP_MODULES)


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> Generator[None]:
    """Ensure each test gets a fresh Settings instance."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def api_app() -> FastAPI:
    return app


@pytest.fixture
def fake_redis() -> FakeRedis:
    """In-memory Redis compatible with redis.asyncio."""
    return FakeRedis(decode_responses=True)


@pytest.fixture
def mock_qdrant() -> MagicMock:
    """AsyncQdrantClient stub with sensible defaults."""
    client = MagicMock(spec=AsyncQdrantClient)
    client.query = AsyncMock(return_value=[])
    client.get_collections = AsyncMock(return_value=MagicMock(collections=[]))
    client.add = AsyncMock(return_value=["test-id"])
    client.close = AsyncMock()
    client.set_model = MagicMock()
    return client


@pytest.fixture
def control_plane() -> InMemoryControlPlaneRepository:
    """In-memory control-plane repository for API tests."""

    return InMemoryControlPlaneRepository()


@pytest.fixture
def fake_user() -> TokenData:
    return TokenData(subject="testuser")


@pytest.fixture
async def client(
    fake_redis: FakeRedis,
    mock_qdrant: MagicMock,
    control_plane: InMemoryControlPlaneRepository,
) -> AsyncGenerator[AsyncClient]:
    """Unauthenticated test client — auth is NOT bypassed.

    ``ASGITransport`` does not trigger the ASGI lifespan, so we inject
    the mock dependencies directly into ``app.state``.
    """
    app.state.redis = fake_redis
    app.state.qdrant = mock_qdrant
    app.state.control_plane = control_plane
    app.state.search_enabled = True

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            yield ac
    finally:
        app.dependency_overrides.clear()
        for attr in ("redis", "qdrant", "control_plane", "search_enabled"):
            if hasattr(app.state, attr):
                delattr(app.state, attr)


@pytest.fixture
async def auth_client(
    client: AsyncClient,
    fake_user: TokenData,
) -> AsyncGenerator[AsyncClient]:
    """Authenticated test client — composes on ``client``, adds auth override.

    Only the ``get_current_user`` override is added here; all other
    infrastructure (Redis, Qdrant, teardown) is owned by ``client``.
    """

    async def _override_user() -> TokenData:
        return fake_user

    app.dependency_overrides[get_current_user] = _override_user
    try:
        yield client
    finally:
        app.dependency_overrides.pop(get_current_user, None)
