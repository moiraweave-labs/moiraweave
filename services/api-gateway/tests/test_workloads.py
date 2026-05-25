"""Tests for workload, run, event, artifact, and agent APIs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from moiraweave_shared.streams import RUN_STREAM

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
    await control_plane.update_run("run-1", status="succeeded", result={"ok": True})

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
    await control_plane.update_run("run-cancel", status="running")

    resp = await auth_client.post("/v1/runs/run-cancel/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancel_requested"


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
    await control_plane.update_run("run-events", status="running")
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

    deploy = await auth_client.post(
        "/v1/workloads/hermes/deployments",
        json={
            "target": "local",
            "status": "applied",
            "metadata": {"compose_project": "moiraweave"},
        },
    )
    assert deploy.status_code == 201
    assert deploy.json()["workload_name"] == "hermes"

    deployments = await auth_client.get("/v1/deployments?workload_name=hermes")
    assert deployments.status_code == 200
    assert deployments.json()[0]["target"] == "local"

    health = await auth_client.get("/v1/workloads/hermes/health")
    assert health.status_code == 200
    body = health.json()
    assert body["status"] == "healthy"
    assert body["deployments"][0]["metadata"]["compose_project"] == "moiraweave"


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
) -> None:
    await _register(auth_client)

    resp = await auth_client.post(
        "/v1/workloads/hermes/preflight",
        json={"target": "local", "env": "dev"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "warning"
    secrets = next(check for check in body["checks"] if check["name"] == "secrets")
    assert "OPENAI_API_KEY" in secrets["metadata"]["missing"]


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
            "metadata": {"status": "running"},
        },
    )
    assert sync.status_code == 202
    deployments = await auth_client.get("/v1/deployments?workload_name=hermes")
    assert deployments.json()[0]["status"] == "running"


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
    assert control_plane.channel_messages[0].channel == "telegram"
    assert control_plane.channel_messages[0].external_user_id == "telegram-user-1"


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
    await control_plane.update_run("run-session-health", status="lost")

    health = await auth_client.get(f"/v1/agents/hermes/sessions/{session_id}/health")

    assert health.status_code == 200
    assert health.json()["status"] == "degraded"
    assert health.json()["latest_run_status"] == "lost"
