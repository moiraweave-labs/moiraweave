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


def _read_text(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_monitoring_install_applies_moiraweave_monitoring_resources() -> None:
    makefile = _read_text("Makefile")
    target = makefile.split("helm-monitoring-install:", 1)[1].split(
        "helm-monitoring-upgrade:",
        1,
    )[0]

    assert "helm upgrade --install moiraweave-monitoring" in target
    assert "kubectl apply -f infra/k8s/monitoring/" in target


def test_monitoring_chart_installs_metrics_logs_and_traces() -> None:
    chart = _read_yaml("infra/helm/monitoring/Chart.yaml")
    values = _read_yaml("infra/helm/monitoring/values.yaml")

    dependencies = {
        dependency["name"]: dependency["condition"]
        for dependency in chart["dependencies"]
    }
    assert dependencies["kube-prometheus-stack"] == "kubePrometheusStack.enabled"
    assert dependencies["loki"] == "loki.enabled"
    assert dependencies["promtail"] == "promtail.enabled"
    assert dependencies["jaeger"] == "jaeger.enabled"

    prometheus = values["kube-prometheus-stack"]["prometheus"]["prometheusSpec"]
    assert prometheus["serviceMonitorSelectorNilUsesHelmValues"] is False
    assert prometheus["podMonitorSelectorNilUsesHelmValues"] is False
    assert prometheus["ruleSelectorNilUsesHelmValues"] is False

    grafana = values["kube-prometheus-stack"]["grafana"]
    assert grafana["sidecar"]["dashboards"]["enabled"] is True
    assert grafana["sidecar"]["dashboards"]["label"] == "grafana_dashboard"
    assert grafana["sidecar"]["dashboards"]["searchNamespace"] == "monitoring"
    assert grafana["additionalDataSources"][0]["name"] == "Loki"


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


def test_api_gateway_chart_service_matches_service_monitor() -> None:
    service_template = _read_text("infra/helm/moiraweave/templates/api-gateway/service.yaml")

    assert "app.kubernetes.io/component: api-gateway" in service_template
    assert "name: http" in service_template
    assert "targetPort: {{ .Values.apiGateway.service.targetPort }}" in service_template


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


def test_worker_chart_metrics_port_matches_pod_monitor() -> None:
    worker_template = _read_text("infra/helm/moiraweave/templates/worker/deployment.yaml")

    assert "app.kubernetes.io/component: worker" in worker_template
    assert "name: metrics" in worker_template
    assert "containerPort: 9090" in worker_template


def test_network_policies_allow_prometheus_to_scrape_api_and_worker() -> None:
    network_policy = _read_text("infra/helm/moiraweave/templates/networkpolicy.yaml")
    api_policy = network_policy.split("# 2. API Gateway", 1)[1].split("# 3. Worker", 1)[0]
    worker_policy = network_policy.split("# 3. Worker", 1)[1].split("# 4. Workload", 1)[0]

    assert "kubernetes.io/metadata.name: monitoring" in api_policy
    assert "port: 8000" in api_policy
    assert "kubernetes.io/metadata.name: monitoring" in worker_policy
    assert "port: 9090" in worker_policy


def test_moiraweave_alerts_and_dashboard_use_scraped_metrics() -> None:
    prometheus_rule = _read_yaml("infra/k8s/monitoring/prometheusrule-moiraweave.yaml")
    dashboard = _read_yaml("infra/k8s/monitoring/grafana-dashboard-system-overview.yaml")

    rules = prometheus_rule["spec"]["groups"][0]["rules"]
    alerts = {rule["alert"] for rule in rules}
    assert {
        "APIGatewayHighLatencyP95",
        "APIGatewayHighErrorRate",
        "WorkerNoJobsProcessed",
        "WorkerHighFailureRate",
    }.issubset(alerts)

    rule_text = yaml.safe_dump(prometheus_rule)
    dashboard_text = yaml.safe_dump(dashboard)
    assert "http_request_duration_seconds_bucket" in rule_text
    assert "http_requests_total" in rule_text
    assert "moiraweave_worker_jobs_processed_total" in rule_text
    assert "moiraweave_worker_jobs_processed_total" in dashboard_text
    assert "http_requests_total" in dashboard_text
    assert "moiraweave-mlops" not in rule_text


def test_grafana_dashboards_are_discoverable_by_sidecar() -> None:
    dashboards = list((ROOT / "infra/k8s/monitoring").glob("grafana-dashboard-*.yaml"))
    assert dashboards

    for path in dashboards:
        dashboard = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert dashboard["metadata"]["namespace"] == "monitoring"
        assert dashboard["metadata"]["labels"]["grafana_dashboard"] == "1"
