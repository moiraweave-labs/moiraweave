"""Integration checks for workload Helm template contracts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]


def _read_text(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def _read_yaml(relative_path: str) -> dict[str, Any]:
    data = yaml.safe_load(_read_text(relative_path))
    assert isinstance(data, dict)
    return data


def _secret_names(workload: dict[str, Any]) -> set[str]:
    names = {str(secret) for secret in workload.get("secrets") or []}
    agent = workload.get("agent") or {}
    names.update(str(secret) for secret in agent.get("requiredSecrets") or [])
    if agent.get("authTokenEnv"):
        names.add(str(agent["authTokenEnv"]))
    return names


def test_sample_workloads_do_not_define_secret_env_literals() -> None:
    values = _read_yaml("tests/helm/values-workloads.yaml")

    for workload in values["workloads"].values():
        env_keys = set((workload.get("env") or {}).keys())
        assert not env_keys.intersection(_secret_names(workload))


def test_workload_template_deduplicates_agent_secret_env_vars() -> None:
    template = _read_text("infra/helm/moiraweave/templates/workloads/deployment.yaml")

    assert "$secretNames = append $secretNames $workload.agent.authTokenEnv" in template
    assert "$secretNames = uniq $secretNames" in template
    assert "if not (has $key $secretNames)" in template
    assert "range $secret := $secretNames" in template


def test_sample_workloads_do_not_duplicate_persistence_and_workspace_mounts() -> None:
    values = _read_yaml("tests/helm/values-workloads.yaml")

    for workload in values["workloads"].values():
        persistence = workload.get("persistence") or {}
        agent = workload.get("agent") or {}
        persistence_path = persistence.get("mountPath")
        workspace_path = agent.get("workspaceMount")
        if persistence.get("enabled") and persistence_path == workspace_path:
            template = _read_text(
                "infra/helm/moiraweave/templates/workloads/deployment.yaml"
            )
            assert "$needsWorkspaceMount" in template
            assert "ne $agentWorkspaceMount $persistenceMountPath" in template


def test_sample_managed_agents_define_runtime_probes() -> None:
    values = _read_yaml("tests/helm/values-workloads.yaml")

    hermes = values["workloads"]["hermes"]
    assert hermes["readinessProbe"]["httpGet"] == {
        "path": "/health",
        "port": "http",
    }
    assert hermes["livenessProbe"]["httpGet"] == {
        "path": "/health",
        "port": "http",
    }

    openclaw = values["workloads"]["openclaw"]
    assert openclaw["readinessProbe"]["tcpSocket"] == {"port": "gateway"}
    assert openclaw["livenessProbe"]["tcpSocket"] == {"port": "gateway"}


def test_workload_template_renders_runtime_probes() -> None:
    template = _read_text("infra/helm/moiraweave/templates/workloads/deployment.yaml")

    assert "if $workload.livenessProbe" in template
    assert "toYaml $workload.livenessProbe" in template
    assert "if $workload.readinessProbe" in template
    assert "toYaml $workload.readinessProbe" in template
