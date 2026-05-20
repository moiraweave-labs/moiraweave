"""End-to-end tests for the MoiraWeave pipeline flow.

These tests require the full docker-compose stack.  They are excluded from
the default ``pytest`` run and only execute when invoked via ``make test-e2e``
(or explicitly with ``pytest tests/e2e/``).

Flow under test:
    POST /auth/token
        → POST /v1/pipelines/echo/jobs
        → poll GET /v1/pipelines/jobs/{job_id}
        → assert result contains original payload fields
"""

import httpx
import pytest

from tests.e2e.conftest import poll_job

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Pipeline discovery
# ---------------------------------------------------------------------------


async def test_pipeline_list_includes_echo(authed_client: httpx.AsyncClient) -> None:
    """The echo pipeline fixture is visible through the API."""
    resp = await authed_client.get("/v1/pipelines")
    assert resp.status_code == 200
    pipelines = resp.json()
    ids = [p["id"] for p in pipelines]
    assert "echo" in ids, f"echo pipeline not found. Got: {ids}"


# ---------------------------------------------------------------------------
# Happy-path job flow
# ---------------------------------------------------------------------------


async def test_submit_echo_job_returns_202(authed_client: httpx.AsyncClient) -> None:
    """Submitting a valid echo job returns 202 Accepted with a job_id."""
    resp = await authed_client.post(
        "/v1/pipelines/echo/jobs",
        json={"payload": {"text": "hello moiraweave"}},
    )
    assert resp.status_code == 202
    body = resp.json()
    assert "job_id" in body
    assert body["status"] == "pending"
    assert body["pipeline_id"] == "echo"


async def test_echo_job_completes_with_input_in_result(
    authed_client: httpx.AsyncClient,
) -> None:
    """Full round-trip: submit → poll → verify result mirrors the input payload."""
    submit = await authed_client.post(
        "/v1/pipelines/echo/jobs",
        json={"payload": {"text": "round-trip", "value": "42"}},
    )
    assert submit.status_code == 202
    job_id = submit.json()["job_id"]

    job = await poll_job(authed_client, job_id)

    assert job["status"] == "completed", f"Job failed: {job.get('error')}"
    result = job["result"]
    assert result is not None
    # The echo step returns all input tensors as outputs.
    # PipelineRunner extracts {tensor_name: value} from the V2 response.
    assert "text" in result, f"'text' not in result: {result}"
    assert result["text"] == "round-trip"


# ---------------------------------------------------------------------------
# Auth & authorization
# ---------------------------------------------------------------------------


async def test_unauthenticated_submit_returns_401(
    http_client: httpx.AsyncClient,
) -> None:
    """Submitting without a JWT is rejected."""
    # Use the unauthenticated client (no Authorization header)
    client = httpx.AsyncClient(base_url=http_client.base_url, timeout=10.0)
    async with client:
        resp = await client.post(
            "/v1/pipelines/echo/jobs",
            json={"payload": {"text": "no-auth"}},
        )
    assert resp.status_code == 401


async def test_job_not_found_returns_404(authed_client: httpx.AsyncClient) -> None:
    """Polling a non-existent job_id returns 404."""
    resp = await authed_client.get("/v1/pipelines/jobs/nonexistent-job-id-12345")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Unknown pipeline
# ---------------------------------------------------------------------------


async def test_submit_to_unknown_pipeline_returns_404(
    authed_client: httpx.AsyncClient,
) -> None:
    """Submitting to a pipeline that does not exist returns 404."""
    resp = await authed_client.post(
        "/v1/pipelines/does-not-exist/jobs",
        json={"payload": {"text": "nope"}},
    )
    assert resp.status_code == 404
