"""Integration checks for Kubernetes observability wiring."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]


def _read_yaml(relative_path: str) -> dict[str, Any]:
    content = (ROOT / relative_path).read_text(encoding="utf-8")
    data = yaml.safe_load(content)
    assert isinstance(data, dict)
    return data


def test_monitoring_install_applies_moiraweave_monitoring_resources() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    target = makefile.split("helm-monitoring-install:", 1)[1].split(
        "helm-monitoring-upgrade:",
        1,
    )[0]

    assert "helm upgrade --install moiraweave-monitoring" in target
    assert "kubectl apply -f infra/k8s/monitoring/" in target


def test_api_gateway_service_monitor_scrapes_metrics_endpoint() -> None:
    service_monitor = _read_yaml("infra/k8s/monitoring/servicemonitor-moiraweave.yaml")

    assert service_monitor["kind"] == "ServiceMonitor"
    assert service_monitor["metadata"]["namespace"] == "monitoring"
    assert service_monitor["spec"]["namespaceSelector"]["matchNames"] == ["moiraweave"]
    selector = service_monitor["spec"]["selector"]["matchLabels"]
    assert selector["app.kubernetes.io/name"] == "moiraweave"
    assert selector["app.kubernetes.io/component"] == "api-gateway"
    endpoint = service_monitor["spec"]["endpoints"][0]
    assert endpoint["port"] == "http"
    assert endpoint["path"] == "/metrics"


def test_worker_pod_monitor_scrapes_metrics_endpoint() -> None:
    pod_monitor = _read_yaml("infra/k8s/monitoring/podmonitor-moiraweave-worker.yaml")

    assert pod_monitor["kind"] == "PodMonitor"
    assert pod_monitor["metadata"]["namespace"] == "monitoring"
    assert pod_monitor["spec"]["namespaceSelector"]["matchNames"] == ["moiraweave"]
    selector = pod_monitor["spec"]["selector"]["matchLabels"]
    assert selector["app.kubernetes.io/name"] == "moiraweave"
    assert selector["app.kubernetes.io/component"] == "worker"
    endpoint = pod_monitor["spec"]["podMetricsEndpoints"][0]
    assert endpoint["port"] == "metrics"
    assert endpoint["path"] == "/metrics"


def test_grafana_dashboards_are_discoverable_by_sidecar() -> None:
    dashboards = list((ROOT / "infra/k8s/monitoring").glob("grafana-dashboard-*.yaml"))
    assert dashboards

    for path in dashboards:
        dashboard = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert dashboard["metadata"]["namespace"] == "monitoring"
        assert dashboard["metadata"]["labels"]["grafana_dashboard"] == "1"
