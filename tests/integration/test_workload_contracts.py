"""Tests for shared workload contracts."""

from __future__ import annotations

import pytest
from moiraweave_shared.workloads import (
    RunStateTransitionError,
    WorkloadDefinition,
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


def test_run_state_transition_policy() -> None:
    ensure_run_transition("queued", "starting")
    ensure_run_transition("running", "cancel_requested")
    ensure_run_transition("cancel_requested", "canceled")

    with pytest.raises(RunStateTransitionError):
        ensure_run_transition("succeeded", "running")
