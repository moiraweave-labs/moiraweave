"""Optional Kubernetes deployment-controller smoke tests.

These tests exercise the public API plus the CLI deployment controller against a
real kube context, normally a local kind cluster. They are destructive within the
configured namespace and are skipped unless ``MOIRAWEAVE_KIND_CONTROLLER_E2E=1``.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import httpx


pytestmark = pytest.mark.e2e

_ENABLED = os.getenv("MOIRAWEAVE_KIND_CONTROLLER_E2E") == "1"
_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKLOAD_NAME = os.getenv("MOIRAWEAVE_KIND_WORKLOAD", "kind-demo-agent")
_ENV = os.getenv("MOIRAWEAVE_KIND_ENV") or f"kind-smoke-{int(time.time())}"
_NAMESPACE = os.getenv("MOIRAWEAVE_KIND_NAMESPACE", "moiraweave")
_RELEASE = os.getenv("MOIRAWEAVE_KIND_RELEASE", "moiraweave")
_CHART_REF = os.getenv("MOIRAWEAVE_KIND_CHART_REF", "infra/helm/moiraweave")
_CONTROLLER_ID = os.getenv(
    "MOIRAWEAVE_KIND_CONTROLLER_ID",
    f"pytest-kind-controller-{int(time.time())}",
)


def _cli_bin() -> str:
    configured = os.getenv("MOIRAWEAVE_CLI_BIN")
    if configured:
        return configured
    sibling = _REPO_ROOT.parent / "moiraweave-cli" / ".venv" / "bin" / "moira"
    if sibling.exists():
        return str(sibling)
    found = shutil.which("moira")
    if found:
        return found
    pytest.fail("moira CLI not found. Set MOIRAWEAVE_CLI_BIN or install moira on PATH.")


async def _create_demo_workload(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/v1/workloads/from-template",
        json={
            "template_id": "demo-agent",
            "parameters": {"name": _WORKLOAD_NAME},
        },
    )
    assert response.status_code in {200, 201}, response.text


async def _create_operation(
    client: httpx.AsyncClient,
    *,
    action: str,
    executor: str = "controller",
) -> dict[str, object]:
    response = await client.post(
        "/v1/deployment-operations",
        json={
            "action": action,
            "workload_name": _WORKLOAD_NAME,
            "target": "kubernetes",
            "env": _ENV,
            "executor": executor,
            "timeout_seconds": 300,
        },
    )
    assert response.status_code == 202, response.text
    return dict(response.json())


async def _get_operation(
    client: httpx.AsyncClient,
    operation_id: str,
) -> dict[str, object]:
    response = await client.get(f"/v1/deployment-operations/{operation_id}")
    assert response.status_code == 200, response.text
    return dict(response.json())


async def _operation_events(
    client: httpx.AsyncClient,
    operation_id: str,
) -> list[dict[str, object]]:
    response = await client.get(f"/v1/deployment-operations/{operation_id}/events")
    assert response.status_code == 200, response.text
    return list(response.json())


async def _claim_with_short_lease(
    client: httpx.AsyncClient,
    operation_id: str,
) -> None:
    response = await client.post(
        f"/v1/deployment-operations/{operation_id}/claim",
        json={
            "controller_id": "pytest-abandoned-controller",
            "lease_seconds": 30,
            "metadata": {"source": "kind-controller-smoke"},
        },
    )
    assert response.status_code == 200, response.text


async def _run_controller(auth_token: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["MOIRA_TOKEN"] = auth_token
    command = [
        _cli_bin(),
        "deploy",
        "controller",
        "run",
        "--api-url",
        str(os.getenv("E2E_BASE_URL", "http://localhost:8000")).rstrip("/"),
        "--env",
        _ENV,
        "--target",
        "kubernetes",
        "--limit",
        "1",
        "--controller-id",
        _CONTROLLER_ID,
        "--namespace",
        _NAMESPACE,
        "--release",
        _RELEASE,
        "--chart-ref",
        _CHART_REF,
        "--repo-root",
        str(_REPO_ROOT),
    ]
    return await asyncio.to_thread(
        subprocess.run,
        command,
        cwd=_REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


async def _run_kubectl(*args: str) -> subprocess.CompletedProcess[str]:
    command = ["kubectl", *args]
    return await asyncio.to_thread(
        subprocess.run,
        command,
        cwd=_REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


async def _assert_controller_succeeds(
    client: httpx.AsyncClient,
    auth_token: str,
    operation_id: str,
) -> dict[str, object]:
    result = await _run_controller(auth_token)
    assert result.returncode == 0, (
        "Deployment controller failed\n"
        f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
    )
    operation = await _get_operation(client, operation_id)
    assert operation["status"] == "succeeded", operation
    events = await _operation_events(client, operation_id)
    event_types = {str(event["type"]) for event in events}
    assert "controller.command" in event_types
    assert "operation.succeeded" in event_types
    return operation


async def _wait_for_workload_available() -> None:
    result = await _run_kubectl(
        "wait",
        "-n",
        _NAMESPACE,
        "--for=condition=available",
        "deployment",
        "-l",
        f"moiraweave.io/workload={_WORKLOAD_NAME}",
        "--timeout=120s",
    )
    assert result.returncode == 0, (
        "Workload deployment did not become available\n"
        f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
    )


@pytest.mark.skipif(
    not _ENABLED,
    reason="Set MOIRAWEAVE_KIND_CONTROLLER_E2E=1 to run kind controller smoke.",
)
async def test_kind_controller_apply_logs_undeploy_and_reclaim(
    authed_client: httpx.AsyncClient,
    auth_token: str,
) -> None:
    await _create_demo_workload(authed_client)

    apply_operation = await _create_operation(authed_client, action="apply")
    await _assert_controller_succeeds(
        authed_client,
        auth_token,
        str(apply_operation["operation_id"]),
    )
    await _wait_for_workload_available()

    logs_operation = await _create_operation(authed_client, action="logs")
    await _assert_controller_succeeds(
        authed_client,
        auth_token,
        str(logs_operation["operation_id"]),
    )

    abandoned_operation = await _create_operation(authed_client, action="undeploy")
    abandoned_id = str(abandoned_operation["operation_id"])
    await _claim_with_short_lease(authed_client, abandoned_id)
    await asyncio.sleep(31)
    await _assert_controller_succeeds(authed_client, auth_token, abandoned_id)

    events = await _operation_events(authed_client, abandoned_id)
    assert any(event["type"] == "operation.reclaimed" for event in events), events
