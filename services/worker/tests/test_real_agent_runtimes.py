"""Optional live-runtime integration tests for agent adapters."""

from __future__ import annotations

import os
from typing import Any

import pytest
from moiraweave_shared.workloads import WorkloadDefinition

from app.agent_adapters import build_agent_adapter

pytestmark = pytest.mark.real_agent


def _live_endpoint(env_name: str) -> str:
    if os.getenv("MOIRAWEAVE_REAL_AGENT_TESTS") != "1":
        pytest.skip("set MOIRAWEAVE_REAL_AGENT_TESTS=1 to run live agent tests")
    endpoint = os.getenv(env_name)
    if not endpoint:
        pytest.skip(f"set {env_name} to run this live agent test")
    return endpoint.rstrip("/")


def _external_agent_workload(
    *,
    name: str,
    endpoint: str,
    adapter: str,
    port_name: str,
    port: int,
    agent: dict[str, Any] | None = None,
) -> WorkloadDefinition:
    agent_spec = {"adapter": adapter, **(agent or {})}
    return WorkloadDefinition.model_validate(
        {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {"name": name},
            "spec": {
                "type": "agent-service",
                "endpoint": endpoint,
                "deployment": {"mode": "external"},
                "execution": {"mode": "session", "timeoutSeconds": 300},
                "ports": [{"name": port_name, "port": port}],
                "agent": agent_spec,
            },
        }
    )


async def test_real_hermes_runtime_health() -> None:
    endpoint = _live_endpoint("MOIRAWEAVE_REAL_HERMES_URL")
    agent: dict[str, Any] = {}
    auth_env = os.getenv("MOIRAWEAVE_REAL_HERMES_AUTH_TOKEN_ENV")
    if auth_env:
        agent["authTokenEnv"] = auth_env

    workload = _external_agent_workload(
        name="hermes-real",
        endpoint=endpoint,
        adapter="hermes",
        port_name="http",
        port=8642,
        agent=agent,
    )

    adapter = build_agent_adapter(workload, timeout_seconds=10)
    status = await adapter.get_status({})

    assert isinstance(status, dict)
    assert status


async def test_real_openclaw_runtime_health() -> None:
    endpoint = _live_endpoint("MOIRAWEAVE_REAL_OPENCLAW_URL")
    agent: dict[str, Any] = {
        "agentId": os.getenv("MOIRAWEAVE_REAL_OPENCLAW_AGENT_ID", "main"),
    }
    auth_env = os.getenv("MOIRAWEAVE_REAL_OPENCLAW_AUTH_TOKEN_ENV")
    if auth_env:
        agent["authTokenEnv"] = auth_env

    workload = _external_agent_workload(
        name="openclaw-real",
        endpoint=endpoint,
        adapter="openclaw",
        port_name="gateway",
        port=18789,
        agent=agent,
    )

    adapter = build_agent_adapter(workload, timeout_seconds=10)
    status = await adapter.get_status({"session_id": "moiraweave-live-health"})

    assert isinstance(status, dict)
    assert status
