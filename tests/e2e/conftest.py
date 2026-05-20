"""E2E test fixtures.

Requires the full docker-compose stack (docker-compose.yml +
tests/e2e/docker-compose.e2e.yml) to be running before pytest is invoked.
Start with ``make test-e2e`` which handles lifecycle automatically.

Environment variables:
    E2E_BASE_URL      API gateway base URL (default: http://localhost:8000)
    E2E_USERNAME      Login username       (default: admin)
    E2E_PASSWORD      Login password       (default: demo-password)
    E2E_POLL_TIMEOUT  Max seconds to wait for a job to complete (default: 30)
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator

import httpx
import pytest

_BASE_URL = os.getenv("E2E_BASE_URL", "http://localhost:8000")
_USERNAME = os.getenv("E2E_USERNAME", "admin")
_PASSWORD = os.getenv("E2E_PASSWORD", "demo-password")
_POLL_TIMEOUT = int(os.getenv("E2E_POLL_TIMEOUT", "30"))


@pytest.fixture(scope="session")
def event_loop_policy() -> None:
    """Use the default event loop policy for the session."""
    return None


@pytest.fixture
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    """Shared httpx client for the test function."""
    async with httpx.AsyncClient(base_url=_BASE_URL, timeout=10.0) as client:
        yield client


@pytest.fixture
async def auth_token(http_client: httpx.AsyncClient) -> str:
    """Obtain a JWT by logging in to the api-gateway.

    :raises AssertionError: When login fails (stack not ready or wrong creds).
    """
    resp = await http_client.post(
        "/auth/token",
        json={"username": _USERNAME, "password": _PASSWORD},
    )
    assert resp.status_code == 200, (
        f"E2E login failed ({resp.status_code}): {resp.text}\n"
        "Is the docker-compose stack running? Run: make test-e2e"
    )
    return str(resp.json()["access_token"])


@pytest.fixture
async def authed_client(
    http_client: httpx.AsyncClient, auth_token: str
) -> httpx.AsyncClient:
    """Return the shared client pre-configured with the auth header."""
    http_client.headers.update({"Authorization": f"Bearer {auth_token}"})
    return http_client


async def poll_job(
    client: httpx.AsyncClient,
    job_id: str,
    *,
    timeout: int = _POLL_TIMEOUT,
) -> dict[str, object]:
    """Poll ``GET /v1/pipelines/jobs/{job_id}`` until the job leaves *pending*.

    :param client: Authenticated httpx client.
    :param job_id: Job ID returned by the submit endpoint.
    :param timeout: Maximum seconds to wait before raising TimeoutError.
    :returns: Final job status dict.
    :raises TimeoutError: When the job is still pending after *timeout* seconds.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        resp = await client.get(f"/v1/pipelines/jobs/{job_id}")
        resp.raise_for_status()
        data: dict[str, object] = resp.json()
        if data.get("status") not in ("pending", "processing"):
            return data
        if asyncio.get_event_loop().time() >= deadline:
            raise TimeoutError(
                f"Job {job_id} still pending after {timeout}s. Last status: {data}"
            )
        await asyncio.sleep(0.5)
