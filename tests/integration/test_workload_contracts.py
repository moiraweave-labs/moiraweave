"""Tests for shared workload contracts."""

from __future__ import annotations

import pytest
from moiraweave_shared.control_plane import (
    InMemoryControlPlaneRepository,
    utc_now_iso,
)
from moiraweave_shared.workloads import (
    DeploymentOperationStateTransitionError,
    RunStateTransitionError,
    WorkloadDefinition,
    ensure_deployment_operation_transition,
    ensure_run_transition,
)


def test_agent_spec_defaults_and_overrides() -> None:
    workload = WorkloadDefinition.model_validate(
        {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {"name": "hermes"},
            "spec": {
                "type": "agent-service",
                "image": "ghcr.io/nousresearch/hermes-agent:latest",
                "agent": {
                    "adapter": "hermes",
                    "capabilities": ["chat", "tools"],
                    "workspaceMount": "/workspace",
                    "exposedChannels": ["ui", "telegram"],
                    "authTokenEnv": "HERMES_API_SERVER_KEY",
                    "model": "hermes-agent",
                    "instructions": "Keep responses operational.",
                    "pollIntervalSeconds": 1.5,
                },
            },
        }
    )

    assert workload.spec.agent.adapter == "hermes"
    assert workload.spec.agent.dispatchTimeoutSeconds == 30.0
    assert workload.spec.agent.pollIntervalSeconds == 1.5
    assert workload.spec.agent.authTokenEnv == "HERMES_API_SERVER_KEY"
    assert workload.spec.agent.model == "hermes-agent"
    assert workload.spec.agent.instructions == "Keep responses operational."
    assert workload.spec.agent.workspaceMount == "/workspace"
    assert "telegram" in workload.spec.agent.exposedChannels
    assert workload.spec.deployment.mode == "managed"
    assert workload.spec.deployment.targets == ["local", "kubernetes"]


def test_agent_channels_are_normalized_and_deduplicated() -> None:
    workload = WorkloadDefinition.model_validate(
        {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {"name": "agent"},
            "spec": {
                "type": "agent-service",
                "image": "ghcr.io/example/agent:latest",
                "agent": {
                    "exposedChannels": ["UI", "telegram", "Telegram"],
                    "externalOwnedChannels": ["Slack", " slack "],
                },
            },
        }
    )

    assert workload.spec.agent.exposedChannels == ["ui", "telegram"]
    assert workload.spec.agent.externalOwnedChannels == ["slack"]


def test_external_agent_requires_endpoint_not_image() -> None:
    workload = WorkloadDefinition.model_validate(
        {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {"name": "external-hermes"},
            "spec": {
                "type": "agent-service",
                "endpoint": "https://agents.example.com/hermes",
                "deployment": {"mode": "external"},
                "agent": {"adapter": "hermes"},
            },
        }
    )

    assert workload.spec.image is None
    assert workload.spec.endpoint == "https://agents.example.com/hermes"
    assert workload.spec.deployment.mode == "external"


def test_external_agent_without_endpoint_is_invalid() -> None:
    with pytest.raises(ValueError, match="spec.endpoint is required"):
        WorkloadDefinition.model_validate(
            {
                "apiVersion": "moiraweave.io/v1alpha1",
                "kind": "Workload",
                "metadata": {"name": "external-hermes"},
                "spec": {
                    "type": "agent-service",
                    "deployment": {"mode": "external"},
                    "agent": {"adapter": "hermes"},
                },
            }
        )


def test_run_state_transition_policy() -> None:
    ensure_run_transition("queued", "starting")
    ensure_run_transition("running", "cancel_requested")
    ensure_run_transition("cancel_requested", "canceled")

    with pytest.raises(RunStateTransitionError):
        ensure_run_transition("succeeded", "running")


def test_deployment_operation_state_transition_policy() -> None:
    ensure_deployment_operation_transition("queued", "running")
    ensure_deployment_operation_transition("running", "running")
    ensure_deployment_operation_transition("running", "succeeded")

    with pytest.raises(DeploymentOperationStateTransitionError):
        ensure_deployment_operation_transition("succeeded", "running")


async def test_control_plane_enforces_run_state_transition_policy() -> None:
    control_plane = InMemoryControlPlaneRepository()
    await control_plane.create_run(
        "run-state-machine",
        "agent",
        {},
        "user",
        created_at=utc_now_iso(),
    )

    with pytest.raises(RunStateTransitionError):
        await control_plane.update_run("run-state-machine", status="succeeded")

    await control_plane.update_run("run-state-machine", status="starting")
    await control_plane.update_run("run-state-machine", status="running")
    await control_plane.update_run("run-state-machine", status="succeeded")

    with pytest.raises(RunStateTransitionError):
        await control_plane.update_run("run-state-machine", status="running")


async def test_control_plane_enforces_deployment_operation_transition_policy() -> None:
    control_plane = InMemoryControlPlaneRepository()
    await control_plane.create_deployment_operation(
        "00000000-0000-0000-0000-000000000001",
        "apply",
        "agent",
        "kubernetes",
        "queued",
        "user",
        now=utc_now_iso(),
    )

    await control_plane.update_deployment_operation(
        "00000000-0000-0000-0000-000000000001",
        status="running",
        metadata={},
    )
    await control_plane.update_deployment_operation(
        "00000000-0000-0000-0000-000000000001",
        status="succeeded",
        metadata={},
    )

    with pytest.raises(DeploymentOperationStateTransitionError):
        await control_plane.update_deployment_operation(
            "00000000-0000-0000-0000-000000000001",
            status="running",
            metadata={},
        )
