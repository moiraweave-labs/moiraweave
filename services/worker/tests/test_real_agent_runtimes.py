"""Optional live-runtime integration tests for agent adapters."""

from __future__ import annotations

import os
import uuid
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


def _turn_enabled(env_name: str) -> None:
    if os.getenv(env_name) != "1":
        pytest.skip(f"set {env_name}=1 to send a live agent turn")


def _cancel_enabled(env_name: str) -> None:
    if os.getenv(env_name) != "1":
        pytest.skip(f"set {env_name}=1 to cancel live agent work")


def _turn_timeout() -> float:
    return float(os.getenv("MOIRAWEAVE_REAL_AGENT_TURN_TIMEOUT_SECONDS", "120"))


async def _never_cancel() -> bool:
    return False


async def _collect_event(
    events: list[tuple[str, str, dict[str, Any] | None]],
    event_type: str,
    message: str,
    data: dict[str, Any] | None,
) -> None:
    events.append((event_type, message, data))


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


async def test_real_hermes_runtime_turn() -> None:
    _turn_enabled("MOIRAWEAVE_REAL_HERMES_TURN_TEST")
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
    payload = {
        "message": os.getenv(
            "MOIRAWEAVE_REAL_HERMES_MESSAGE",
            "Reply with the single word moiraweave-ok.",
        ),
        "session_id": f"moiraweave-live-{uuid.uuid4()}",
        "idempotency_key": str(uuid.uuid4()),
    }
    accepted = await adapter.send_message(payload)
    assert accepted.get("accepted") is True

    events: list[tuple[str, str, dict[str, Any] | None]] = []
    result = await adapter.wait_for_completion(
        payload,
        accepted,
        emit=lambda event_type, message, data: _collect_event(
            events, event_type, message, data
        ),
        is_cancel_requested=_never_cancel,
        timeout_seconds=_turn_timeout(),
    )

    assert isinstance(result, dict)
    assert result
    assert any(event[0] == "agent.external_run_started" for event in events)
    assert isinstance(result.get("artifacts", []), list)
    artifacts = await adapter.list_artifacts({**payload, **accepted})
    assert isinstance(artifacts, list)


async def test_real_hermes_runtime_cancel() -> None:
    _cancel_enabled("MOIRAWEAVE_REAL_HERMES_CANCEL_TEST")
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
    payload = {
        "message": os.getenv(
            "MOIRAWEAVE_REAL_HERMES_CANCEL_MESSAGE",
            "Start a long-running task and wait until cancelled.",
        ),
        "session_id": f"moiraweave-live-cancel-{uuid.uuid4()}",
        "idempotency_key": str(uuid.uuid4()),
    }
    accepted = await adapter.send_message(payload)
    external_run_id = accepted.get("external_run_id")
    if not isinstance(external_run_id, str) or not external_run_id:
        pytest.skip("Hermes runtime did not return an external run id to cancel")

    result = await adapter.cancel({**payload, **accepted})
    assert isinstance(result, dict)
    assert result.get("accepted") is not False


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


async def test_real_openclaw_runtime_turn() -> None:
    _turn_enabled("MOIRAWEAVE_REAL_OPENCLAW_TURN_TEST")
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
    payload = {
        "message": os.getenv(
            "MOIRAWEAVE_REAL_OPENCLAW_MESSAGE",
            "Reply with the single word moiraweave-ok.",
        ),
        "session_id": f"moiraweave-live-{uuid.uuid4()}",
        "idempotency_key": str(uuid.uuid4()),
    }
    accepted = await adapter.send_message(payload)
    assert accepted.get("accepted") is True

    events: list[tuple[str, str, dict[str, Any] | None]] = []
    result = await adapter.wait_for_completion(
        payload,
        accepted,
        emit=lambda event_type, message, data: _collect_event(
            events, event_type, message, data
        ),
        is_cancel_requested=_never_cancel,
        timeout_seconds=_turn_timeout(),
    )

    assert isinstance(result, dict)
    assert result
    if accepted.get("external_run_id"):
        assert any(event[0] == "agent.external_run_started" for event in events)
    assert isinstance(result.get("artifacts", []), list)
    artifacts = await adapter.list_artifacts({**payload, **accepted})
    assert isinstance(artifacts, list)


async def test_real_openclaw_runtime_cancel() -> None:
    _cancel_enabled("MOIRAWEAVE_REAL_OPENCLAW_CANCEL_TEST")
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
    payload = {
        "message": os.getenv(
            "MOIRAWEAVE_REAL_OPENCLAW_CANCEL_MESSAGE",
            "Start a long-running task and wait until cancelled.",
        ),
        "session_id": f"moiraweave-live-cancel-{uuid.uuid4()}",
        "idempotency_key": str(uuid.uuid4()),
    }
    accepted = await adapter.send_message(payload)
    external_run_id = accepted.get("external_run_id")
    if not isinstance(external_run_id, str) or not external_run_id:
        pytest.skip("OpenClaw runtime did not return an external run id to cancel")

    result = await adapter.cancel({**payload, **accepted})
    assert isinstance(result, dict)
    assert result.get("accepted") is not False
