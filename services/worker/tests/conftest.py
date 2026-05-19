"""Shared fixtures for worker tests."""

import os
import pathlib
import sys
from collections.abc import Generator

import pytest
from fakeredis.aioredis import FakeRedis

# ---------------------------------------------------------------------------
# sys.path: ensure this service's root is first so `app.*` resolves to the
# worker service, not to api-gateway when running all tests together.
# ---------------------------------------------------------------------------
_SERVICE_ROOT = str(pathlib.Path(__file__).resolve().parents[1])
for _k in list(sys.modules):
    if _k == "app" or _k.startswith("app."):
        del sys.modules[_k]
if _SERVICE_ROOT not in sys.path:
    sys.path.insert(0, _SERVICE_ROOT)

os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from app.config import Settings, get_settings  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> Generator[None]:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def worker_settings() -> Settings:
    return Settings(
        redis_url="redis://localhost:6379/0",  # type: ignore[arg-type]
    )


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis(decode_responses=True)
