"""Tests for workload, run, event, artifact, and agent APIs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from moiraweave_shared.streams import CONSUMER_GROUP, RUN_STREAM

from app.models.workloads import DeploymentResponse
from app.routes.workloads import _deployment_probe_url, _probe_deployment_endpoint

if TYPE_CHECKING:
    import pytest
    from fakeredis.aioredis import FakeRedis
    from httpx import AsyncClient
    from moiraweave_shared.control_plane import InMemoryControlPlaneRepository


def _agent_manifest(name: str = "hermes") -> dict[str, Any]:
    return {
        "apiVersion": "moiraweave.io/v1alpha1",
        "kind": "Workload",
        "metadata": {"name": name},
        "spec": {
            "type": "agent-service",
            "image": "ghcr.io/nousresearch/hermes-agent:latest",
            "execution": {"mode": "session", "timeoutSeconds": 172800},
            "ports": [{"name": "http", "port": 8000}],
            "persistence": {"enabled": True, "mountPath": "/data"},
            "secrets": ["OPENAI_API_KEY"],
        },
    }


def _deployment_response(endpoint: str | None = None) -> DeploymentResponse:
    return DeploymentResponse(
        deployment_id="00000000-0000-0000-0000-000000000001",
        workload_name="hermes",
        target="local",
        status="applied",
        user="testuser",
        created_at="2026-01-01T00:00:00+00:00",
        endpoint=endpoint,
        metadata={},
    )


async def _register(
    auth_client: AsyncClient, name: str = "hermes"
) -> dict[str, object]:
    resp = await auth_client.post("/v1/workloads", json=_agent_manifest(name))
    assert resp.status_code == 201
    return resp.json()


async def _advance_run(
    control_plane: InMemoryControlPlaneRepository,
    run_id: str,
    status: str,
    **kwargs: Any,
) -> None:
    paths = {
        "starting": ["starting"],
        "running": ["starting", "running"],
        "cancel_requested": ["cancel_requested"],
        "cancelling": ["cancel_requested", "cancelling"],
        "succeeded": ["starting", "running", "succeeded"],
        "failed": ["starting", "running", "failed"],
        "canceled": ["cancel_requested", "canceled"],
        "lost": ["starting", "running", "lost"],
    }
    for step in paths[status][:-1]:
        await control_plane.update_run(run_id, status=step)
    await control_plane.update_run(run_id, status=paths[status][-1], **kwargs)


def test_deployment_probe_url_defaults_to_health_path() -> None:
    assert _deployment_probe_url("http://hermes:8000") == "http://hermes:8000/health"
    assert (
        _deployment_probe_url("http://hermes:8000/readyz")
        == "http://hermes:8000/readyz"
    )


async def test_probe_deployment_endpoint_skips_missing_endpoint() -> None:
    assert await _probe_deployment_endpoint(_deployment_response()) is None


async def test_probe_deployment_endpoint_rejects_invalid_url() -> None:
    result = await _probe_deployment_endpoint(_deployment_response("hermes:8000"))
    assert result is not None
    ok, reason = result
    assert ok is False
    assert "not a valid HTTP URL" in reason


async def test_register_and_list_workloads(auth_client: AsyncClient) -> None:
    await _register(auth_client)

    resp = await auth_client.get("/v1/workloads")
    assert resp.status_code == 200
    body = resp.json()
    assert body[0]["name"] == "hermes"
    assert body[0]["type"] == "agent-service"
    assert body[0]["execution_mode"] == "session"


async def test_get_workload_returns_manifest(auth_client: AsyncClient) -> None:
    await _register(auth_client)

    resp = await auth_client.get("/v1/workloads/hermes")
    assert resp.status_code == 200
    assert resp.json()["manifest"]["metadata"]["name"] == "hermes"


async def test_list_templates_includes_demo_agent(auth_client: AsyncClient) -> None:
    resp = await auth_client.get("/v1/templates")

    assert resp.status_code == 200
    templates = {item["id"]: item for item in resp.json()}
    assert "demo-agent" in templates
    assert templates["demo-agent"]["manifest"]["spec"]["agent"]["adapter"] == (
        "generic-http"
    )
    assert not templates["demo-agent"]["manifest"]["spec"].get("secrets")


async def test_create_workload_from_template_registers_manifest(
    auth_client: AsyncClient,
) -> None:
    resp = await auth_client.post(
        "/v1/workloads/from-template",
        json={"template_id": "demo-agent", "parameters": {"name": "Demo Agent!"}},
    )

    assert resp.status_code == 201
    body = resp.json()
    assert body["name"] == "demo-agent"
    assert body["type"] == "agent-service"
    assert body["manifest"]["spec"]["command"] == ["python", "-u", "-c"]


async def test_agent_template_accepts_runtime_owned_channels(
    auth_client: AsyncClient,
) -> None:
    resp = await auth_client.post(
        "/v1/workloads/from-template",
        json={
            "template_id": "hermes",
            "parameters": {
                "name": "Hermes Ops",
                "external_channels": "Telegram, slack",
            },
        },
    )

    assert resp.status_code == 201
    manifest = resp.json()["manifest"]
    assert manifest["spec"]["secrets"] == ["OPENAI_API_KEY"]
    assert manifest["spec"]["readinessProbe"]["httpGet"] == {
        "path": "/health",
        "port": "http",
    }
    assert manifest["spec"]["livenessProbe"]["httpGet"] == {
        "path": "/health",
        "port": "http",
    }
    agent = manifest["spec"]["agent"]
    requirements = agent["runtimeRequirements"]
    assert agent["toolOwnership"] == "runtime"
    assert agent["authTokenEnv"] == "HERMES_API_SERVER_KEY"
    assert agent["exposedChannels"] == ["ui", "api"]
    assert agent["externalOwnedChannels"] == ["telegram", "slack"]
    assert requirements["filesystem"]["persistentWorkspace"] is True
    assert requirements["webSearch"]["enabled"] is True
    assert requirements["browser"]["mode"] == "runtime-managed"
    assert requirements["terminal"]["mode"] == "runtime-managed"


async def test_openclaw_template_uses_auth_token_env_as_secret_source(
    auth_client: AsyncClient,
) -> None:
    resp = await auth_client.post(
        "/v1/workloads/from-template",
        json={
            "template_id": "openclaw",
            "parameters": {"name": "OpenClaw Ops"},
        },
    )

    assert resp.status_code == 201
    manifest = resp.json()["manifest"]
    assert manifest["spec"].get("secrets") == []
    assert manifest["spec"].get("env") == {}
    assert manifest["spec"]["readinessProbe"]["tcpSocket"] == {"port": "gateway"}
    assert manifest["spec"]["livenessProbe"]["tcpSocket"] == {"port": "gateway"}
    assert manifest["spec"]["agent"]["authTokenEnv"] == "OPENCLAW_GATEWAY_TOKEN"
    assert "OPENCLAW_GATEWAY_TOKEN" not in manifest["spec"]["secrets"]


async def test_submit_run_queues_dispatch(
    auth_client: AsyncClient,
    fake_redis: FakeRedis,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    await _register(auth_client)

    resp = await auth_client.post(
        "/v1/workloads/hermes/runs",
        json={"payload": {"prompt": "hello"}},
    )
    assert resp.status_code == 202
    body = resp.json()
    run_id = body["run_id"]
    assert body["status"] == "queued"

    run = await control_plane.get_run(run_id)
    assert run is not None
    assert run.status == "queued"
    assert run.workload_name == "hermes"
    assert run.payload == {"prompt": "hello"}

    stream_entries = await fake_redis.xrange(RUN_STREAM)
    assert len(stream_entries) == 1
    assert stream_entries[0][1]["run_id"] == run_id
    assert stream_entries[0][1]["workload_manifest"]


async def test_submit_run_unknown_workload_returns_404(
    auth_client: AsyncClient,
) -> None:
    resp = await auth_client.post("/v1/workloads/missing/runs", json={"payload": {}})
    assert resp.status_code == 404


async def test_submit_run_requires_auth(client: AsyncClient) -> None:
    resp = await client.post("/v1/workloads/hermes/runs", json={"payload": {}})
    assert resp.status_code == 401


async def test_get_run_returns_result(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    await control_plane.create_run(
        "run-1",
        "hermes",
        {},
        "testuser",
        created_at="2026-01-01T00:00:00+00:00",
    )
    await _advance_run(control_plane, "run-1", "succeeded", result={"ok": True})

    resp = await auth_client.get("/v1/runs/run-1")
    assert resp.status_code == 200
    assert resp.json()["result"]["ok"] is True


async def test_get_run_for_other_user_returns_403(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    await control_plane.create_run(
        "run-2",
        "hermes",
        {},
        "another-user",
        created_at="2026-01-01T00:00:00+00:00",
    )

    resp = await auth_client.get("/v1/runs/run-2")
    assert resp.status_code == 403


async def test_list_runs_filters_by_workload(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    for run_id, workload in [("a", "hermes"), ("b", "mock-model")]:
        await control_plane.create_run(
            run_id,
            workload,
            {},
            "testuser",
            created_at=f"2026-01-0{1 if run_id == 'a' else 2}T00:00:00+00:00",
        )

    resp = await auth_client.get("/v1/runs?workload_name=hermes")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["workload_name"] == "hermes"


async def test_list_runs_supports_limit_and_offset(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    for index in range(3):
        await control_plane.create_run(
            f"run-{index}",
            "hermes",
            {},
            "testuser",
            created_at=f"2026-01-0{index + 1}T00:00:00+00:00",
        )

    resp = await auth_client.get("/v1/runs?limit=1&offset=1")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["run_id"] == "run-1"


async def test_cancel_run_sets_cancel_requested(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    await control_plane.create_run(
        "run-cancel",
        "hermes",
        {},
        "testuser",
        created_at="2026-01-01T00:00:00+00:00",
    )
    await _advance_run(control_plane, "run-cancel", "running")

    resp = await auth_client.post("/v1/runs/run-cancel/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancel_requested"
    audit = await auth_client.get("/v1/audit-events?action=run.cancel")
    assert audit.status_code == 200
    assert audit.json()[0]["resource_id"] == "run-cancel"
    assert audit.json()[0]["metadata"]["previous_status"] == "running"


async def test_events_and_artifacts_are_returned(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    await control_plane.create_run(
        "run-events",
        "hermes",
        {},
        "testuser",
        created_at="2026-01-01T00:00:00+00:00",
    )
    await _advance_run(control_plane, "run-events", "running")
    await control_plane.append_run_event(
        "run-events",
        "run.running",
        "Run execution started",
        timestamp="2026-01-01T00:00:00+00:00",
    )
    artifact = {
        "id": "a1",
        "run_id": "run-events",
        "name": "output.json",
        "uri": "file:///artifacts/output.json",
        "created_at": "2026-01-01T00:00:00+00:00",
        "metadata": {},
    }
    await control_plane.record_artifact("run-events", artifact)

    events = await auth_client.get("/v1/runs/run-events/events")
    artifacts = await auth_client.get("/v1/runs/run-events/artifacts")
    assert events.status_code == 200
    assert artifacts.status_code == 200
    assert events.json()[0]["type"] == "run.running"
    assert artifacts.json()[0]["name"] == "output.json"
    assert artifacts.json()[0]["workload_name"] == "hermes"
    assert artifacts.json()[0]["session_id"] is None


async def test_artifact_library_filters_by_workload_session_and_type(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    await control_plane.create_run(
        "run-artifact-library",
        "hermes",
        {},
        "testuser",
        created_at="2026-01-01T00:00:00+00:00",
        session_id="00000000-0000-0000-0000-000000000001",
    )
    await control_plane.record_artifact(
        "run-artifact-library",
        {
            "id": "artifact-library-1",
            "name": "trace.json",
            "uri": "file:///trace.json",
            "content_type": "application/json",
            "created_at": "2026-01-01T00:00:00+00:00",
        },
    )

    resp = await auth_client.get(
        "/v1/artifacts?"
        "workload_name=hermes&"
        "session_id=00000000-0000-0000-0000-000000000001&"
        "content_type=application/json"
    )

    assert resp.status_code == 200
    assert resp.json()[0]["name"] == "trace.json"
    assert resp.json()[0]["workload_name"] == "hermes"
    assert resp.json()[0]["session_id"] == "00000000-0000-0000-0000-000000000001"


async def test_artifact_preview_and_download_from_local_storage(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("ARTIFACTS_DIR", str(tmp_path))
    output = tmp_path / "reports" / "summary.json"
    output.parent.mkdir()
    output.write_text('{"ok": true, "kind": "summary"}', encoding="utf-8")

    await control_plane.create_run(
        "run-artifact-content",
        "hermes",
        {},
        "testuser",
        created_at="2026-01-01T00:00:00+00:00",
    )
    await control_plane.record_artifact(
        "run-artifact-content",
        {
            "id": "summary-json",
            "name": "summary.json",
            "uri": "local://reports/summary.json",
            "content_type": "application/json",
            "created_at": "2026-01-01T00:00:00+00:00",
        },
    )

    preview = await auth_client.get(
        "/v1/runs/run-artifact-content/artifacts/summary-json/preview"
    )
    download = await auth_client.get(
        "/v1/runs/run-artifact-content/artifacts/summary-json/download"
    )

    assert preview.status_code == 200
    assert preview.json()["text"] == '{"ok": true, "kind": "summary"}'
    assert preview.json()["truncated"] is False
    assert download.status_code == 200
    assert download.content == b'{"ok": true, "kind": "summary"}'
    assert "summary.json" in download.headers["content-disposition"]
    audit = await auth_client.get(
        "/v1/audit-events?resource_type=artifact&resource_id=summary-json"
    )
    assert audit.status_code == 200
    actions = {item["action"] for item in audit.json()}
    assert actions == {"artifact.preview", "artifact.download"}


async def test_artifact_preview_rejects_path_traversal(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("ARTIFACTS_DIR", str(tmp_path))
    await control_plane.create_run(
        "run-artifact-escape",
        "hermes",
        {},
        "testuser",
        created_at="2026-01-01T00:00:00+00:00",
    )
    await control_plane.record_artifact(
        "run-artifact-escape",
        {
            "id": "escape",
            "name": "escape.txt",
            "uri": "local://../escape.txt",
            "content_type": "text/plain",
            "created_at": "2026-01-01T00:00:00+00:00",
        },
    )

    preview = await auth_client.get(
        "/v1/runs/run-artifact-escape/artifacts/escape/preview"
    )

    assert preview.status_code == 403


async def test_agent_session_message_creates_run(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    await _register(auth_client)
    session_resp = await auth_client.post("/v1/agents/hermes/sessions", json={})
    assert session_resp.status_code == 201
    session_id = session_resp.json()["session_id"]

    message_resp = await auth_client.post(
        f"/v1/agents/hermes/sessions/{session_id}/messages",
        json={"message": "continue", "context": {"goal": "test"}},
    )
    assert message_resp.status_code == 202
    run_id = message_resp.json()["run_id"]
    run = await control_plane.get_run(run_id)
    assert run is not None
    assert run.session_id == session_id

    history = await auth_client.get(f"/v1/agents/hermes/sessions/{session_id}/messages")
    assert history.status_code == 200
    assert history.json()[0]["message"] == "continue"
    assert history.json()[0]["run_id"] == run_id
    assert history.json()[0]["run_status"] == "queued"
    audit = await auth_client.get("/v1/audit-events?action=agent.message")
    assert audit.status_code == 200
    assert audit.json()[0]["resource_id"] == session_id
    assert audit.json()[0]["metadata"]["run_id"] == run_id


async def test_agent_history_includes_latest_event_and_artifact_count(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    await _register(auth_client)
    session_resp = await auth_client.post("/v1/agents/hermes/sessions", json={})
    session_id = session_resp.json()["session_id"]
    message_resp = await auth_client.post(
        f"/v1/agents/hermes/sessions/{session_id}/messages",
        json={"message": "write report"},
    )
    run_id = message_resp.json()["run_id"]
    await control_plane.append_run_event(
        run_id,
        "executor.agent.call",
        "Dispatching message to agent runtime",
    )
    await control_plane.record_artifact(
        run_id,
        {"id": "agent-artifact", "name": "report.md", "uri": "file:///report.md"},
    )

    history = await auth_client.get(f"/v1/agents/hermes/sessions/{session_id}/messages")

    assert history.status_code == 200
    assert history.json()[0]["latest_event"]["type"] == "executor.agent.call"
    assert history.json()[0]["artifact_count"] == 1


async def test_multiple_agent_workloads_have_independent_sessions(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client, "hermes")
    openclaw = _agent_manifest("openclaw")
    openclaw["spec"]["image"] = "ghcr.io/openclaw/openclaw:latest"
    openclaw["spec"]["ports"] = [{"name": "gateway", "port": 18789}]
    openclaw["spec"]["agent"] = {"adapter": "openclaw", "agentId": "main"}

    register_openclaw = await auth_client.post("/v1/workloads", json=openclaw)
    assert register_openclaw.status_code == 201

    workloads = await auth_client.get("/v1/workloads")
    assert workloads.status_code == 200
    names = {item["name"] for item in workloads.json()}
    assert {"hermes", "openclaw"} <= names

    hermes_session = await auth_client.post("/v1/agents/hermes/sessions", json={})
    openclaw_session = await auth_client.post("/v1/agents/openclaw/sessions", json={})

    assert hermes_session.status_code == 201
    assert openclaw_session.status_code == 201
    assert hermes_session.json()["agent_name"] == "hermes"
    assert openclaw_session.json()["agent_name"] == "openclaw"
    assert hermes_session.json()["session_id"] != openclaw_session.json()["session_id"]


async def test_deployment_record_and_workload_health(auth_client: AsyncClient) -> None:
    await _register(auth_client)

    unknown = await auth_client.get("/v1/workloads/hermes/health")
    assert unknown.status_code == 200
    assert unknown.json()["status"] == "unknown"
    assert unknown.json()["recommendations"]

    deploy = await auth_client.post(
        "/v1/workloads/hermes/deployments",
        json={
            "target": "local",
            "status": "deployed",
            "metadata": {"compose_project": "moiraweave"},
        },
    )
    assert deploy.status_code == 201
    assert deploy.json()["workload_name"] == "hermes"
    assert deploy.json()["env"] == "local"

    deployments = await auth_client.get("/v1/deployments?workload_name=hermes")
    assert deployments.status_code == 200
    assert deployments.json()[0]["target"] == "local"
    assert deployments.json()[0]["env"] == "local"

    health = await auth_client.get("/v1/workloads/hermes/health")
    assert health.status_code == 200
    body = health.json()
    assert body["status"] == "healthy"
    assert body["deployments"][0]["metadata"]["compose_project"] == "moiraweave"


async def test_unreachable_deployment_status_is_degraded(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)

    deploy = await auth_client.post(
        "/v1/workloads/hermes/deployments",
        json={"target": "local", "status": "unreachable"},
    )
    assert deploy.status_code == 201

    health = await auth_client.get("/v1/workloads/hermes/health")
    assert health.status_code == 200
    assert health.json()["status"] == "degraded"


async def test_deployment_records_are_environment_scoped(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)

    dev = await auth_client.post(
        "/v1/workloads/hermes/deployments",
        json={"target": "local", "env": "dev", "status": "generated"},
    )
    prod = await auth_client.post(
        "/v1/workloads/hermes/deployments",
        json={"target": "local", "env": "prod", "status": "reachable"},
    )
    assert dev.status_code == 201
    assert prod.status_code == 201
    assert dev.json()["deployment_id"] != prod.json()["deployment_id"]

    prod_deployments = await auth_client.get(
        "/v1/deployments?workload_name=hermes&env=prod"
    )
    assert prod_deployments.status_code == 200
    assert [item["env"] for item in prod_deployments.json()] == ["prod"]
    assert prod_deployments.json()[0]["status"] == "reachable"

    dev_health = await auth_client.get("/v1/workloads/hermes/health?env=dev")
    prod_health = await auth_client.get("/v1/workloads/hermes/health?env=prod")
    assert dev_health.json()["status"] == "pending"
    assert prod_health.json()["status"] == "healthy"

    environments = await auth_client.get("/v1/environments")
    assert environments.status_code == 200
    by_name = {item["name"]: item for item in environments.json()}
    assert by_name["dev"]["deployment_count"] == 1
    assert by_name["prod"]["deployment_count"] == 1
    assert by_name["prod"]["workload_count"] == 1


async def test_local_deployment_plan_describes_cli_and_compose_apply(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)

    resp = await auth_client.get("/v1/workloads/hermes/deployment-plan?target=local")

    assert resp.status_code == 200
    body = resp.json()
    assert body["target"] == "local"
    assert body["mode"] == "managed"
    assert body["service_name"] == "hermes"
    assert body["endpoint"] == "http://hermes:8000"
    assert ".moiraweave/deploy/docker-compose.workloads.yml" in body["files"]
    assert "moira deploy local" in body["commands"]
    assert any(command.startswith("docker compose") for command in body["commands"])


async def test_kubernetes_deployment_plan_honors_env_and_namespace(
    auth_client: AsyncClient,
) -> None:
    manifest = _agent_manifest()
    manifest["spec"]["deployment"] = {
        "mode": "managed",
        "targets": ["kubernetes"],
        "namespace": "agents",
    }
    register = await auth_client.post("/v1/workloads", json=manifest)
    assert register.status_code == 201

    resp = await auth_client.get(
        "/v1/workloads/hermes/deployment-plan?target=k8s&env=prod"
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["target"] == "kubernetes"
    assert body["files"] == [".moiraweave/deploy/values-workloads-prod.yaml"]
    assert any("--namespace agents" in command for command in body["commands"])


async def test_deployment_plan_rejects_disabled_target(
    auth_client: AsyncClient,
) -> None:
    manifest = _agent_manifest()
    manifest["spec"]["deployment"] = {"mode": "managed", "targets": ["local"]}
    register = await auth_client.post("/v1/workloads", json=manifest)
    assert register.status_code == 201

    resp = await auth_client.get(
        "/v1/workloads/hermes/deployment-plan?target=kubernetes"
    )

    assert resp.status_code == 400
    assert "not enabled" in resp.json()["detail"]


async def test_preflight_reports_secret_warnings(
    auth_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    await _register(auth_client)

    resp = await auth_client.post(
        "/v1/workloads/hermes/preflight",
        json={"target": "local", "env": "local"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "warning"
    secrets = next(check for check in body["checks"] if check["name"] == "secrets")
    assert "OPENAI_API_KEY" in secrets["metadata"]["missing"]
    secret_action = next(
        item for item in body["action_guide"] if item["title"] == "Set Missing Secrets"
    )
    assert secret_action["state"] == "missing"
    assert "OPENAI_API_KEY" in secret_action["detail"]
    assert "Values stay outside the API and UI." in secret_action["detail"]
    assert "OPENAI_API_KEY=..." in secret_action["command"]


async def test_preflight_kubernetes_secrets_require_operator_check(
    auth_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    await _register(auth_client)

    resp = await auth_client.post(
        "/v1/workloads/hermes/preflight",
        json={"target": "kubernetes", "env": "dev"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "warning"
    secrets = next(check for check in body["checks"] if check["name"] == "secrets")
    assert secrets["metadata"]["missing"] == []
    assert secrets["metadata"]["verification"] == "operator-cli"
    assert "OPENAI_API_KEY" in secrets["metadata"]["required"]
    secret_action = next(
        item
        for item in body["action_guide"]
        if item["title"] == "Verify Kubernetes Secret Keys"
    )
    assert secret_action["state"] == "warning"
    assert "OPENAI_API_KEY" in secret_action["detail"]
    assert "Values stay in the cluster" in secret_action["detail"]
    assert (
        secret_action["command"] == "moira secrets list --target kubernetes --env dev "
        "--kubernetes-secret moiraweave-secrets --check"
    )


async def test_preflight_reports_missing_deployment_record(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)

    resp = await auth_client.post(
        "/v1/workloads/hermes/preflight",
        json={"target": "local", "env": "local"},
    )

    assert resp.status_code == 200
    body = resp.json()
    deployment_record = next(
        check for check in body["checks"] if check["name"] == "deployment_record"
    )
    assert deployment_record["status"] == "warning"
    assert "moira deploy local" in deployment_record["remediation"]
    assert deployment_record["remediation"] in body["recommendations"]
    sync_action = next(
        item
        for item in body["action_guide"]
        if item["title"] == "Sync Deployment Record"
    )
    assert sync_action["state"] == "warning"
    assert sync_action["command"] == "moira deploy local --register"


async def test_preflight_probes_registered_runtime_endpoint(
    auth_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _register(auth_client)

    async def _probe(_deployment: object) -> tuple[bool, str]:
        return True, "runtime is reachable"

    monkeypatch.setattr("app.routes.workloads._probe_deployment_endpoint", _probe)
    deploy = await auth_client.post(
        "/v1/workloads/hermes/deployments",
        json={
            "target": "local",
            "status": "running",
            "endpoint": "http://hermes:8000",
        },
    )
    assert deploy.status_code == 201

    resp = await auth_client.post(
        "/v1/workloads/hermes/preflight",
        json={"target": "local", "env": "local"},
    )

    assert resp.status_code == 200
    checks = {check["name"]: check for check in resp.json()["checks"]}
    assert checks["deployment_record"]["status"] == "passed"
    assert checks["runtime_reachability"]["status"] == "passed"
    assert (
        "http://hermes:8000" in checks["runtime_reachability"]["metadata"]["endpoints"]
    )


async def test_preflight_reports_missing_worker_consumer(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)

    resp = await auth_client.post(
        "/v1/workloads/hermes/preflight",
        json={"target": "local", "env": "dev"},
    )

    assert resp.status_code == 200
    checks = {check["name"]: check for check in resp.json()["checks"]}
    assert checks["worker_dispatch"]["status"] == "warning"
    assert "worker" in checks["worker_dispatch"]["remediation"].lower()


async def test_preflight_passes_with_worker_consumer(
    auth_client: AsyncClient,
    fake_redis: FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    await _register(auth_client)
    await fake_redis.xgroup_create(RUN_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    await fake_redis.xadd(RUN_STREAM, {"run_id": "run-worker-check"})
    await fake_redis.xreadgroup(
        CONSUMER_GROUP,
        "worker-test",
        streams={RUN_STREAM: ">"},
        count=1,
    )

    resp = await auth_client.post(
        "/v1/workloads/hermes/preflight",
        json={"target": "local", "env": "dev"},
    )

    assert resp.status_code == 200
    checks = {check["name"]: check for check in resp.json()["checks"]}
    assert checks["worker_dispatch"]["status"] == "passed"
    assert checks["worker_dispatch"]["metadata"]["consumers"] == 1


async def test_preflight_action_guide_reports_ready_when_checks_pass(
    auth_client: AsyncClient,
    fake_redis: FakeRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    await _register(auth_client)
    await fake_redis.xgroup_create(RUN_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    await fake_redis.xadd(RUN_STREAM, {"run_id": "run-ready-check"})
    await fake_redis.xreadgroup(
        CONSUMER_GROUP,
        "worker-test",
        streams={RUN_STREAM: ">"},
        count=1,
    )

    async def _probe(_deployment: object) -> tuple[bool, str]:
        return True, "runtime is reachable"

    monkeypatch.setattr("app.routes.workloads._probe_deployment_endpoint", _probe)
    deploy = await auth_client.post(
        "/v1/workloads/hermes/deployments",
        json={
            "target": "local",
            "env": "local",
            "status": "running",
            "endpoint": "http://hermes:8000",
        },
    )
    assert deploy.status_code == 201

    resp = await auth_client.post(
        "/v1/workloads/hermes/preflight",
        json={"target": "local", "env": "local"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "passed"
    assert body["action_guide"] == [
        {
            "title": "Ready",
            "state": "ready",
            "detail": (
                "No blocking action detected for this workload, target, and "
                "environment from the control-plane perspective."
            ),
            "command": 'moira agent chat hermes "hello" --watch',
        }
    ]


async def test_preflight_reports_runtime_boundaries(
    auth_client: AsyncClient,
) -> None:
    manifest = _agent_manifest()
    manifest["spec"]["persistence"] = {"enabled": False}
    manifest["spec"]["agent"] = {
        "adapter": "hermes",
        "toolOwnership": "runtime",
        "runtimeRequirements": {
            "filesystem": {"persistentWorkspace": True},
            "network": {"egress": "disabled"},
            "webSearch": {"enabled": True},
            "browser": {"mode": "runtime-managed"},
        },
    }
    register = await auth_client.post("/v1/workloads", json=manifest)
    assert register.status_code == 201

    resp = await auth_client.post(
        "/v1/workloads/hermes/preflight",
        json={"target": "local", "env": "dev"},
    )

    assert resp.status_code == 200
    checks = {check["name"]: check for check in resp.json()["checks"]}
    runtime = checks["runtime_boundaries"]
    assert runtime["status"] == "warning"
    assert runtime["metadata"]["toolOwnership"] == "runtime"
    assert runtime["metadata"]["networkEgress"] == "disabled"
    assert "workspace" in runtime["remediation"].lower()
    assert "web search" in runtime["remediation"].lower()


async def test_preflight_external_runtime_uses_external_safe_actions(
    auth_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    external = _agent_manifest("external-hermes")
    spec = external["spec"]
    spec.pop("image", None)
    spec["endpoint"] = "https://agents.example.com/hermes"
    spec["deployment"] = {"mode": "external"}

    register = await auth_client.post("/v1/workloads", json=external)
    assert register.status_code == 201

    async def _probe(_deployment: object) -> tuple[bool, str]:
        return False, "external runtime is not reachable"

    monkeypatch.setattr("app.routes.workloads._probe_deployment_endpoint", _probe)
    resp = await auth_client.post(
        "/v1/workloads/external-hermes/preflight",
        json={"target": "external", "env": "local"},
    )

    assert resp.status_code == 200
    actions = {item["title"]: item for item in resp.json()["action_guide"]}
    assert actions["Sync Deployment Record"]["command"] == (
        "moira deploy local --register"
    )
    assert actions["Restore Worker Dispatch"]["command"] is None
    assert actions["Fix Runtime Reachability"]["command"] is None


async def test_secret_inventory_lists_required_names_without_values(
    auth_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-never-return-this")
    monkeypatch.delenv("HERMES_API_SERVER_KEY", raising=False)
    manifest = _agent_manifest()
    manifest["spec"]["agent"] = {
        "adapter": "hermes",
        "authTokenEnv": "HERMES_API_SERVER_KEY",
    }
    register = await auth_client.post("/v1/workloads", json=manifest)
    assert register.status_code == 201

    resp = await auth_client.get("/v1/secrets?workload_name=hermes")

    assert resp.status_code == 200
    assert "sk-never-return-this" not in resp.text
    body = resp.json()
    assert body["status"] == "warning"
    items = {item["name"]: item for item in body["secrets"]}
    assert items["OPENAI_API_KEY"]["present"] is True
    assert items["HERMES_API_SERVER_KEY"]["present"] is False
    assert items["HERMES_API_SERVER_KEY"]["workloads"] == ["hermes"]
    assert (
        "hermes:spec.agent.authTokenEnv" in items["HERMES_API_SERVER_KEY"]["references"]
    )


async def test_secret_inventory_includes_runtime_requirement_secrets(
    auth_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BROWSER_USE_API_KEY", raising=False)
    manifest = _agent_manifest()
    manifest["spec"]["agent"] = {
        "adapter": "hermes",
        "runtimeRequirements": {
            "browser": {
                "mode": "cloud",
                "requiredSecrets": ["BROWSER_USE_API_KEY"],
            }
        },
    }
    register = await auth_client.post("/v1/workloads", json=manifest)
    assert register.status_code == 201

    resp = await auth_client.get("/v1/secrets?workload_name=hermes")

    assert resp.status_code == 200
    items = {item["name"]: item for item in resp.json()["secrets"]}
    assert items["BROWSER_USE_API_KEY"]["present"] is False
    assert (
        "hermes:spec.agent.runtimeRequirements.browser.requiredSecrets"
        in items["BROWSER_USE_API_KEY"]["references"]
    )


async def test_secret_inventory_unknown_workload_returns_404(
    auth_client: AsyncClient,
) -> None:
    resp = await auth_client.get("/v1/secrets?workload_name=missing")

    assert resp.status_code == 404


async def test_deployment_operation_plan_and_sync(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)

    plan = await auth_client.post(
        "/v1/deployment-operations",
        json={"action": "plan", "workload_name": "hermes", "target": "local"},
    )
    assert plan.status_code == 202
    assert plan.json()["status"] == "succeeded"

    events = await auth_client.get(
        f"/v1/deployment-operations/{plan.json()['operation_id']}/events"
    )
    assert events.status_code == 200
    assert events.json()[0]["type"] == "operation.plan"

    sync = await auth_client.post(
        "/v1/deployment-operations",
        json={
            "action": "sync",
            "workload_name": "hermes",
            "target": "local",
        },
    )
    assert sync.status_code == 202
    assert sync.json()["env"] == "dev"
    deployments = await auth_client.get("/v1/deployments?workload_name=hermes")
    assert deployments.json()[0]["status"] == "deployed"
    assert deployments.json()[0]["env"] == "dev"

    operations = await auth_client.get("/v1/deployment-operations?workload_name=hermes")
    assert operations.status_code == 200
    assert [item["action"] for item in operations.json()] == ["sync", "plan"]

    filtered = await auth_client.get("/v1/deployment-operations?action=sync&env=dev")
    assert filtered.status_code == 200
    assert [item["operation_id"] for item in filtered.json()] == [
        sync.json()["operation_id"]
    ]
    audit = await auth_client.get("/v1/audit-events?action=deployment_operation.sync")
    assert audit.status_code == 200
    assert audit.json()[0]["resource_id"] == sync.json()["operation_id"]
    assert audit.json()[0]["metadata"]["workload_name"] == "hermes"


async def test_deployment_operation_apply_is_blocked_without_controller(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)

    resp = await auth_client.post(
        "/v1/deployment-operations",
        json={"action": "apply", "workload_name": "hermes", "target": "local"},
    )

    assert resp.status_code == 202
    assert resp.json()["status"] == "failed"
    assert (
        resp.json()["metadata"]["blocked_reason"] == "api-gateway-has-no-host-executor"
    )
    assert resp.json()["metadata"]["action_commands"] == [
        "moira deploy local",
        "docker compose -f docker-compose.yml -f .moiraweave/deploy/docker-compose.workloads.yml up -d",
        "moira deploy local --register",
    ]
    assert "next_actions" in resp.json()["metadata"]

    events = await auth_client.get(
        f"/v1/deployment-operations/{resp.json()['operation_id']}/events"
    )
    assert events.status_code == 200
    assert events.json()[-1]["type"] == "operation.blocked"
    assert (
        events.json()[-1]["data"]["commands"]
        == resp.json()["metadata"]["action_commands"]
    )


async def test_deployment_operation_controller_lifecycle(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)

    queued = await auth_client.post(
        "/v1/deployment-operations",
        json={
            "action": "apply",
            "workload_name": "hermes",
            "target": "kubernetes",
            "env": "dev",
            "executor": "controller",
        },
    )

    assert queued.status_code == 202
    body = queued.json()
    assert body["status"] == "queued"
    assert body["completed_at"] is None
    assert body["metadata"]["executor"] == "controller"
    assert body["metadata"]["controller_required"] is True

    listed = await auth_client.get("/v1/deployment-operations?status=queued&scope=all")
    assert listed.status_code == 200
    assert [item["operation_id"] for item in listed.json()] == [body["operation_id"]]

    claim = await auth_client.post(
        f"/v1/deployment-operations/{body['operation_id']}/claim",
        json={
            "controller_id": "moiraweave-k8s-controller/dev",
            "metadata": {"namespace": "moiraweave-dev"},
        },
    )
    assert claim.status_code == 200
    assert claim.json()["status"] == "running"
    assert (
        claim.json()["metadata"]["controller"]["id"] == "moiraweave-k8s-controller/dev"
    )

    event = await auth_client.post(
        f"/v1/deployment-operations/{body['operation_id']}/events",
        json={
            "type": "controller.apply",
            "message": "Applied Helm release.",
            "data": {"revision": "abc123"},
        },
    )
    assert event.status_code == 201
    assert event.json()["type"] == "controller.apply"

    complete = await auth_client.post(
        f"/v1/deployment-operations/{body['operation_id']}/complete",
        json={
            "status": "succeeded",
            "message": "Controller applied workload.",
            "metadata": {"revision": "abc123"},
        },
    )
    assert complete.status_code == 200
    assert complete.json()["status"] == "succeeded"
    assert complete.json()["completed_at"] is not None
    assert complete.json()["metadata"]["controller_result"]["revision"] == "abc123"

    deployments = await auth_client.get("/v1/deployments?workload_name=hermes&env=dev")
    assert deployments.status_code == 200
    assert deployments.json()[0]["target"] == "kubernetes"
    assert deployments.json()[0]["status"] == "deployed"
    assert deployments.json()[0]["metadata"]["source"] == "deployment-controller"

    events = await auth_client.get(
        f"/v1/deployment-operations/{body['operation_id']}/events"
    )
    assert [item["type"] for item in events.json()] == [
        "operation.plan",
        "operation.queued",
        "operation.claimed",
        "controller.apply",
        "operation.succeeded",
    ]


async def test_deployment_operation_undeploy_returns_guidance(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)

    resp = await auth_client.post(
        "/v1/deployment-operations",
        json={"action": "undeploy", "workload_name": "hermes", "target": "local"},
    )

    assert resp.status_code == 202
    assert resp.json()["status"] == "failed"
    assert resp.json()["metadata"]["action_commands"] == [
        "docker compose -f docker-compose.yml -f .moiraweave/deploy/docker-compose.workloads.yml down"
    ]
    assert resp.json()["metadata"]["next_actions"] == [
        "Run the listed commands from an environment with deployment credentials.",
        "Sync the deployment record as stopped, removed, or external-owned.",
    ]


async def test_deployment_operation_logs_returns_guidance(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)

    resp = await auth_client.post(
        "/v1/deployment-operations",
        json={"action": "logs", "workload_name": "hermes", "target": "local"},
    )

    assert resp.status_code == 202
    assert resp.json()["status"] == "succeeded"
    assert resp.json()["metadata"]["log_commands"] == [
        "docker compose logs --tail 200 hermes"
    ]

    events = await auth_client.get(
        f"/v1/deployment-operations/{resp.json()['operation_id']}/events"
    )
    assert events.status_code == 200
    assert events.json()[-1]["type"] == "operation.logs"


async def test_external_deployment_plan_records_runtime_without_apply(
    auth_client: AsyncClient,
) -> None:
    external = _agent_manifest("external-hermes")
    spec = external["spec"]
    spec.pop("image", None)
    spec["endpoint"] = "https://agents.example.com/hermes"
    spec["deployment"] = {"mode": "external"}

    register = await auth_client.post("/v1/workloads", json=external)
    assert register.status_code == 201

    resp = await auth_client.get(
        "/v1/workloads/external-hermes/deployment-plan?target=external"
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["target"] == "external"
    assert body["endpoint"] == "https://agents.example.com/hermes"
    assert body["files"] == []
    assert any("--register" in command for command in body["commands"])


async def test_workload_health_uses_endpoint_probe(
    auth_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _register(auth_client)

    async def _probe(_deployment: object) -> tuple[bool, str]:
        return False, "runtime is not reachable"

    monkeypatch.setattr("app.routes.workloads._probe_deployment_endpoint", _probe)
    await auth_client.post(
        "/v1/workloads/hermes/deployments",
        json={
            "target": "local",
            "status": "applied",
            "endpoint": "http://hermes:8000",
        },
    )

    health = await auth_client.get("/v1/workloads/hermes/health")

    assert health.status_code == 200
    assert health.json()["status"] == "degraded"
    assert health.json()["reason"] == "runtime is not reachable"


async def test_external_agent_deployment_record_is_supported(
    auth_client: AsyncClient,
) -> None:
    external = _agent_manifest("external-hermes")
    spec = external["spec"]
    spec.pop("image", None)
    spec["endpoint"] = "https://agents.example.com/hermes"
    spec["deployment"] = {"mode": "external"}

    register = await auth_client.post("/v1/workloads", json=external)
    assert register.status_code == 201

    deploy = await auth_client.post(
        "/v1/workloads/external-hermes/deployments",
        json={
            "target": "external",
            "status": "running",
            "endpoint": "https://agents.example.com/hermes",
        },
    )

    assert deploy.status_code == 201
    assert deploy.json()["target"] == "external"
    assert deploy.json()["endpoint"] == "https://agents.example.com/hermes"


async def test_channel_message_creates_session_run_and_audit_record(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    manifest = _agent_manifest()
    manifest["spec"]["agent"] = {"exposedChannels": ["ui", "api", "telegram"]}
    assert (await auth_client.post("/v1/workloads", json=manifest)).status_code == 201

    resp = await auth_client.post(
        "/v1/channels/telegram/agents/hermes/messages",
        json={
            "external_user_id": "telegram-user-1",
            "message": "status please",
            "metadata": {"chat_id": "123"},
        },
    )

    assert resp.status_code == 202
    body = resp.json()
    assert body["session_id"]
    run = await control_plane.get_run(body["run_id"])
    assert run is not None
    assert run.session_id == body["session_id"]
    messages = await control_plane.list_agent_messages(body["session_id"])
    assert messages[0].context["run_id"] == body["run_id"]
    assert control_plane.channel_messages[0].channel == "telegram"
    assert control_plane.channel_messages[0].external_user_id == "telegram-user-1"
    audit = await auth_client.get("/v1/audit-events?action=channel.message")
    assert audit.status_code == 200
    assert audit.json()[0]["resource_id"] == body["session_id"]
    assert audit.json()[0]["metadata"]["channel"] == "telegram"
    assert audit.json()[0]["metadata"]["run_id"] == body["run_id"]


async def test_webhook_message_uses_channel_contract(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    manifest = _agent_manifest()
    manifest["spec"]["agent"] = {"exposedChannels": ["ui", "api", "webhook"]}
    assert (await auth_client.post("/v1/workloads", json=manifest)).status_code == 201

    resp = await auth_client.post(
        "/v1/webhooks/webhook/agents/hermes/messages",
        json={
            "external_user_id": "webhook-sender",
            "message": "run diagnostics",
            "metadata": {"source": "incident-webhook"},
        },
    )

    assert resp.status_code == 202
    body = resp.json()
    run = await control_plane.get_run(body["run_id"])
    assert run is not None
    assert run.session_id == body["session_id"]
    assert control_plane.channel_messages[0].channel == "webhook"
    assert control_plane.channel_messages[0].metadata["source"] == "incident-webhook"


async def test_duplicate_agent_messages_keep_distinct_run_links(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)
    session_resp = await auth_client.post("/v1/agents/hermes/sessions", json={})
    session_id = session_resp.json()["session_id"]

    first = await auth_client.post(
        f"/v1/agents/hermes/sessions/{session_id}/messages",
        json={"message": "repeatable prompt"},
    )
    second = await auth_client.post(
        f"/v1/agents/hermes/sessions/{session_id}/messages",
        json={"message": "repeatable prompt"},
    )

    assert first.status_code == 202
    assert second.status_code == 202
    history = await auth_client.get(f"/v1/agents/hermes/sessions/{session_id}/messages")

    assert history.status_code == 200
    run_ids = [item["run_id"] for item in history.json()]
    assert run_ids == [first.json()["run_id"], second.json()["run_id"]]
    assert run_ids[0] != run_ids[1]


async def test_channel_message_requires_declared_agent_channel(
    auth_client: AsyncClient,
) -> None:
    await _register(auth_client)

    resp = await auth_client.post(
        "/v1/channels/telegram/agents/hermes/messages",
        json={"external_user_id": "telegram-user-1", "message": "hello"},
    )

    assert resp.status_code == 400
    assert "spec.agent.exposedChannels" in resp.json()["detail"]


async def test_channel_message_rejects_runtime_owned_channel(
    auth_client: AsyncClient,
) -> None:
    manifest = _agent_manifest()
    manifest["spec"]["agent"] = {
        "exposedChannels": ["ui", "api"],
        "externalOwnedChannels": ["telegram"],
    }
    assert (await auth_client.post("/v1/workloads", json=manifest)).status_code == 201

    resp = await auth_client.post(
        "/v1/channels/telegram/agents/hermes/messages",
        json={"external_user_id": "telegram-user-1", "message": "hello"},
    )

    assert resp.status_code == 409
    assert "owned by the agent runtime" in resp.json()["detail"]


async def test_agent_session_health_reports_latest_run(
    auth_client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
) -> None:
    await _register(auth_client)
    session_resp = await auth_client.post("/v1/agents/hermes/sessions", json={})
    session_id = session_resp.json()["session_id"]
    await control_plane.create_run(
        "run-session-health",
        "hermes",
        {},
        "testuser",
        created_at="2026-01-01T00:00:00+00:00",
        session_id=session_id,
    )
    await _advance_run(control_plane, "run-session-health", "lost")

    health = await auth_client.get(f"/v1/agents/hermes/sessions/{session_id}/health")

    assert health.status_code == 200
    assert health.json()["status"] == "degraded"
    assert health.json()["latest_run_status"] == "lost"
