"""Contract tests for versioned workload examples."""

from __future__ import annotations

from pathlib import Path

from moiraweave_shared.workloads import WorkloadDefinition

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples" / "workloads"


def _example_workloads() -> list[WorkloadDefinition]:
    return [
        WorkloadDefinition.from_yaml(path)
        for path in sorted(EXAMPLES.glob("*/workload.yaml"))
    ]


def test_agent_examples_are_valid_workload_manifests() -> None:
    workloads = _example_workloads()

    assert {workload.metadata.name for workload in workloads} == {
        "external-hermes",
        "hermes",
        "openclaw",
    }
    for workload in workloads:
        assert workload.apiVersion == "moiraweave.io/v1alpha1"
        assert workload.kind == "Workload"
        assert workload.spec.type == "agent-service"
        assert workload.spec.agent.toolOwnership == "runtime"
        assert "long-running" in workload.spec.agent.capabilities


def test_managed_agent_examples_have_unique_local_services_and_ports() -> None:
    managed = [
        workload
        for workload in _example_workloads()
        if workload.spec.deployment.mode == "managed"
    ]

    service_names = [
        workload.spec.deployment.serviceName or workload.metadata.name
        for workload in managed
    ]
    ports = [port.port for workload in managed for port in workload.spec.ports]

    assert service_names == ["hermes", "openclaw"]
    assert len(service_names) == len(set(service_names))
    assert ports == [8642, 18789]
    assert len(ports) == len(set(ports))


def test_managed_agent_examples_declare_runtime_health_and_workspace() -> None:
    examples = {workload.metadata.name: workload for workload in _example_workloads()}

    hermes = examples["hermes"].to_manifest()["spec"]
    assert hermes["readinessProbe"]["httpGet"] == {
        "path": "/health",
        "port": "http",
    }
    assert hermes["livenessProbe"]["httpGet"] == {
        "path": "/health",
        "port": "http",
    }
    assert hermes["persistence"]["mountPath"] == "/workspace"
    assert hermes["agent"]["workspaceMount"] == "/workspace"

    openclaw = examples["openclaw"].to_manifest()["spec"]
    assert openclaw["readinessProbe"]["tcpSocket"] == {"port": "gateway"}
    assert openclaw["livenessProbe"]["tcpSocket"] == {"port": "gateway"}
    assert openclaw["persistence"]["mountPath"] == "/workspace"
    assert openclaw["agent"]["workspaceMount"] == "/workspace"


def test_agent_example_secret_boundaries_match_adapter_contracts() -> None:
    examples = {workload.metadata.name: workload.to_manifest() for workload in _example_workloads()}

    hermes = examples["hermes"]["spec"]
    assert hermes["secrets"] == ["OPENAI_API_KEY"]
    assert hermes["agent"]["requiredSecrets"] == ["OPENAI_API_KEY"]
    assert hermes["agent"]["authTokenEnv"] == "HERMES_API_SERVER_KEY"

    openclaw = examples["openclaw"]["spec"]
    assert openclaw.get("secrets", []) == []
    assert openclaw["agent"]["authTokenEnv"] == "OPENCLAW_GATEWAY_TOKEN"


def test_external_agent_example_is_supervised_not_deployed() -> None:
    external = next(
        workload
        for workload in _example_workloads()
        if workload.metadata.name == "external-hermes"
    )

    assert external.spec.deployment.mode == "external"
    assert external.spec.image is None
    assert external.spec.endpoint == "https://agents.example.com/hermes"
    assert external.spec.agent.adapter == "hermes"
