"""Tests for agent runtime adapters."""

from __future__ import annotations

import json
from typing import Any

import pytest
from moiraweave_shared.workloads import WorkloadDefinition

from app import agent_adapters
from app.agent_adapters import (
    HermesAgentAdapter,
    HttpAgentAdapter,
    OpenClawAgentAdapter,
    build_agent_adapter,
    extract_assistant_message,
)


def _agent_workload(**spec_overrides: Any) -> WorkloadDefinition:
    spec: dict[str, Any] = {
        "type": "agent-service",
        "image": "ghcr.io/example/agent:latest",
        "execution": {"mode": "session", "timeoutSeconds": 60},
        "ports": [{"name": "http", "port": 8000}],
    }
    spec.update(spec_overrides)
    return WorkloadDefinition.model_validate(
        {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {"name": "agent"},
            "spec": spec,
        }
    )


async def test_generic_agent_without_endpoint_accepts_locally() -> None:
    adapter = build_agent_adapter(_agent_workload(ports=[]), timeout_seconds=1.0)

    response = await adapter.send_message({"session_id": "session-1", "message": "hi"})

    assert response["accepted"] is True
    assert response["mode"] == "local-accept"


async def test_generic_agent_endpoint_round_trips_http(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class FakeAsyncClient:
        def __init__(self, *, timeout: float) -> None:
            captured["timeout"] = timeout

        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

        async def post(self, endpoint: str, *, json: dict[str, Any]) -> _FakeResponse:
            captured["post"] = (endpoint, json)
            return _FakeResponse({"response": "ack"})

        async def get(self, endpoint: str) -> _FakeResponse:
            captured["get"] = endpoint
            return _FakeResponse({"status": "ok"})

    monkeypatch.setattr(agent_adapters.httpx, "AsyncClient", FakeAsyncClient)
    adapter = HttpAgentAdapter(
        _agent_workload(endpoint="http://agent:8000"),
        timeout_seconds=3.0,
    )

    assert await adapter.send_message({"message": "hello"}) == {
        "response": "ack",
        "accepted": True,
        "adapter": "generic-http",
    }
    assert await adapter.wait_for_completion(
        {},
        {"accepted": True},
        emit=lambda *_args: None,  # type: ignore[arg-type]
        is_cancel_requested=lambda: None,  # type: ignore[arg-type]
        timeout_seconds=1.0,
    ) == {"accepted": True}
    assert await adapter.get_status({}) == {"status": "ok"}
    assert captured["post"] == ("http://agent:8000", {"message": "hello"})
    assert captured["get"] == "http://agent:8000/health"


async def test_generic_agent_missing_optional_endpoints() -> None:
    adapter = HttpAgentAdapter(_agent_workload(ports=[]), timeout_seconds=1.0)

    assert await adapter.get_status({}) == {
        "status": "unknown",
        "adapter": "generic-http",
    }
    assert await adapter.cancel({}) == {
        "accepted": False,
        "reason": "cancel endpoint not configured",
    }
    assert await adapter.list_artifacts({}) == []


class _FakeResponse:
    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._data


async def test_hermes_adapter_uses_runs_api(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class FakeAsyncClient:
        def __init__(self, *, timeout: float) -> None:
            captured["timeout"] = timeout

        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

        async def post(
            self,
            endpoint: str,
            *,
            json: dict[str, Any],
            headers: dict[str, str],
        ) -> _FakeResponse:
            captured["endpoint"] = endpoint
            captured["payload"] = json
            captured["headers"] = headers
            return _FakeResponse({"run_id": "run-1", "status": "started"})

    monkeypatch.setenv("HERMES_TOKEN", "secret")
    monkeypatch.setattr(agent_adapters.httpx, "AsyncClient", FakeAsyncClient)
    workload = _agent_workload(
        endpoint="http://hermes:8642",
        agent={
            "adapter": "hermes",
            "model": "hermes-agent",
            "instructions": "Stay concise.",
            "authTokenEnv": "HERMES_TOKEN",
        },
    )
    adapter = build_agent_adapter(workload, timeout_seconds=1.0)

    response = await adapter.send_message(
        {"session_id": "s-1", "message": "hello", "previous_response_id": "resp-1"}
    )

    assert captured["endpoint"] == "http://hermes:8642/v1/runs"
    assert captured["payload"] == {
        "input": "hello",
        "session_id": "s-1",
        "model": "hermes-agent",
        "instructions": "Stay concise.",
        "previous_response_id": "resp-1",
    }
    assert captured["headers"] == {"Authorization": "Bearer secret"}
    assert response["accepted"] is True
    assert response["external_run_id"] == "run-1"
    assert response["adapter"] == "hermes"


async def test_hermes_adapter_waits_for_terminal_status(monkeypatch) -> None:
    statuses = [
        {"status": "running"},
        {"status": "completed", "output": "done", "usage": {"total_tokens": 3}},
    ]
    emitted: list[tuple[str, dict[str, Any] | None]] = []
    workload = _agent_workload(
        endpoint="http://hermes:8642",
        agent={"adapter": "hermes", "pollIntervalSeconds": 0.1},
    )
    adapter = HermesAgentAdapter(workload, timeout_seconds=1.0)

    async def fake_status(payload: dict[str, Any]) -> dict[str, Any]:
        assert payload["external_run_id"] == "run-1"
        return statuses.pop(0)

    async def fake_artifacts(payload: dict[str, Any]) -> list[dict[str, Any]]:
        assert payload["external_run_id"] == "run-1"
        return [{"name": "report.md"}]

    async def emit(
        event_type: str, message: str, data: dict[str, Any] | None = None
    ) -> None:
        del message
        emitted.append((event_type, data))

    async def not_canceled() -> bool:
        return False

    monkeypatch.setattr(adapter, "get_status", fake_status)
    monkeypatch.setattr(adapter, "list_artifacts", fake_artifacts)

    result = await adapter.wait_for_completion(
        {"message": "hello"},
        {"external_run_id": "run-1", "accepted": True},
        emit=emit,
        is_cancel_requested=not_canceled,
        timeout_seconds=2.0,
    )

    assert result["response"] == "done"
    assert result["usage"] == {"total_tokens": 3}
    assert result["artifacts"] == [{"name": "report.md"}]
    assert [event_type for event_type, _ in emitted] == [
        "agent.external_run_started",
        "agent.external_status",
        "agent.external_status",
    ]


async def test_hermes_adapter_status_cancel_and_artifacts(monkeypatch) -> None:
    calls: list[tuple[str, str, dict[str, str]]] = []

    class FakeAsyncClient:
        def __init__(self, *, timeout: float) -> None:
            assert timeout == 1.0

        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

        async def get(self, endpoint: str, *, headers: dict[str, str]) -> _FakeResponse:
            calls.append(("GET", endpoint, headers))
            return _FakeResponse(
                {
                    "status": "completed",
                    "artifacts": [{"name": "out.txt"}],
                }
            )

        async def post(
            self,
            endpoint: str,
            *,
            json: dict[str, Any],
            headers: dict[str, str],
        ) -> _FakeResponse:
            del json
            calls.append(("POST", endpoint, headers))
            return _FakeResponse({"status": "stopping"})

    monkeypatch.setenv("API_SERVER_KEY", "server-key")
    monkeypatch.setattr(agent_adapters.httpx, "AsyncClient", FakeAsyncClient)
    adapter = HermesAgentAdapter(
        _agent_workload(endpoint="http://hermes:8642", agent={"adapter": "hermes"}),
        timeout_seconds=1.0,
    )

    assert (await adapter.get_status({}))["status"] == "completed"
    assert (await adapter.get_status({"external_run_id": "run-1"}))[
        "status"
    ] == "completed"
    assert await adapter.list_artifacts({"external_run_id": "run-1"}) == [
        {"name": "out.txt"}
    ]
    assert (await adapter.cancel({"external_run_id": "run-1"}))["accepted"] is True
    assert await adapter.cancel({}) == {
        "accepted": False,
        "reason": "external_run_id missing",
    }
    assert calls == [
        (
            "GET",
            "http://hermes:8642/health/detailed",
            {"Authorization": "Bearer server-key"},
        ),
        (
            "GET",
            "http://hermes:8642/v1/runs/run-1",
            {"Authorization": "Bearer server-key"},
        ),
        (
            "GET",
            "http://hermes:8642/v1/runs/run-1",
            {"Authorization": "Bearer server-key"},
        ),
        (
            "POST",
            "http://hermes:8642/v1/runs/run-1/stop",
            {"Authorization": "Bearer server-key"},
        ),
    ]


async def test_generic_adapter_status_cancel_and_artifacts(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_get(self: HttpAgentAdapter, endpoint: str) -> dict[str, Any]:
        del self
        calls.append(endpoint)
        if endpoint.endswith("/artifacts"):
            return {"artifacts": [{"name": "out.txt"}]}
        return {"status": "running"}

    async def fake_post(
        self: HttpAgentAdapter, endpoint: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        del self, payload
        calls.append(endpoint)
        return {"ok": True}

    monkeypatch.setattr(HttpAgentAdapter, "_get", fake_get)
    monkeypatch.setattr(HttpAgentAdapter, "_post", fake_post)
    adapter = build_agent_adapter(
        _agent_workload(
            endpoint="http://agent:8000",
            agent={
                "adapter": "generic-http",
                "statusPath": "/status",
                "cancelPath": "/stop",
                "artifactsPath": "/artifacts",
            },
        ),
        timeout_seconds=1.0,
    )

    assert (await adapter.get_status({}))["status"] == "running"
    assert (await adapter.cancel({}))["accepted"] is True
    assert (await adapter.list_artifacts({}))[0]["name"] == "out.txt"
    assert calls == [
        "http://agent:8000/status",
        "http://agent:8000/stop",
        "http://agent:8000/artifacts",
    ]


async def test_adapter_uses_service_port_when_endpoint_is_omitted(monkeypatch) -> None:
    captured: dict[str, str] = {}

    async def fake_get(self: HttpAgentAdapter, endpoint: str) -> dict[str, Any]:
        del self
        captured["endpoint"] = endpoint
        return {"status": "ok"}

    monkeypatch.setattr(HttpAgentAdapter, "_get", fake_get)
    adapter = build_agent_adapter(_agent_workload(), timeout_seconds=1.0)

    await adapter.get_status({})

    assert captured["endpoint"] == "http://agent:8000/health"


def test_build_agent_adapter_uses_runtime_specific_adapters() -> None:
    hermes = build_agent_adapter(
        _agent_workload(agent={"adapter": "hermes"}), timeout_seconds=1.0
    )
    openclaw = build_agent_adapter(
        _agent_workload(agent={"adapter": "openclaw"}), timeout_seconds=1.0
    )

    assert isinstance(hermes, HermesAgentAdapter)
    assert isinstance(openclaw, OpenClawAgentAdapter)


async def test_openclaw_adapter_uses_gateway_rpc(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class FakeRpc:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.methods = {
                "sessions.describe",
                "sessions.create",
                "sessions.send",
            }

        async def __aenter__(self) -> FakeRpc:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

        def supports(self, method: str) -> bool:
            return method in self.methods

        async def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
            calls.append((method, params))
            if method == "sessions.describe":
                raise RuntimeError("not found")
            if method == "sessions.send":
                return {"runId": "run-1", "message": "accepted"}
            return {"ok": True}

    monkeypatch.setattr(agent_adapters, "_OpenClawRpcClient", FakeRpc)
    workload = _agent_workload(
        endpoint="http://openclaw:18789",
        agent={"adapter": "openclaw", "agentId": "coder"},
    )
    adapter = OpenClawAgentAdapter(workload, timeout_seconds=1.0)

    result = await adapter.send_message({"session_id": "s-1", "message": "hello"})

    assert result["external_run_id"] == "run-1"
    assert result["session_key"] == "agent:coder:s-1"
    assert calls == [
        ("sessions.describe", {"key": "agent:coder:s-1"}),
        ("sessions.create", {"key": "agent:coder:s-1", "agentId": "coder"}),
        (
            "sessions.send",
            {
                "key": "agent:coder:s-1",
                "sessionKey": "agent:coder:s-1",
                "message": "hello",
                "text": "hello",
                "agentId": "coder",
                "idempotencyKey": result["raw"].get(
                    "idempotencyKey", calls[-1][1]["idempotencyKey"]
                ),
            },
        ),
    ]


async def test_openclaw_adapter_waits_and_lists_artifacts(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class FakeRpc:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.methods = {"agent.wait", "artifacts.list"}

        async def __aenter__(self) -> FakeRpc:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

        def supports(self, method: str) -> bool:
            return method in self.methods

        async def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
            calls.append((method, params))
            if method == "agent.wait":
                return {"status": "completed", "output": "done"}
            if method == "artifacts.list":
                return {"artifacts": [{"name": "artifact.txt"}]}
            return {}

    async def emit(
        event_type: str, message: str, data: dict[str, Any] | None = None
    ) -> None:
        del event_type, message, data

    async def not_canceled() -> bool:
        return False

    monkeypatch.setattr(agent_adapters, "_OpenClawRpcClient", FakeRpc)
    workload = _agent_workload(agent={"adapter": "openclaw"})
    adapter = OpenClawAgentAdapter(workload, timeout_seconds=1.0)

    result = await adapter.wait_for_completion(
        {"session_id": "s-1"},
        {"external_run_id": "run-1", "session_key": "agent:main:s-1"},
        emit=emit,
        is_cancel_requested=not_canceled,
        timeout_seconds=1.0,
    )

    assert result["response"] == "done"
    assert result["artifacts"] == [{"name": "artifact.txt"}]
    assert calls == [
        ("agent.wait", {"runId": "run-1", "timeoutMs": 1}),
        (
            "artifacts.list",
            {"sessionKey": "agent:main:s-1", "runId": "run-1"},
        ),
    ]


async def test_openclaw_adapter_falls_back_to_chat_rpc_and_cancel(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class FakeRpc:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.methods = {"chat.send", "chat.abort", "health"}

        async def __aenter__(self) -> FakeRpc:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

        def supports(self, method: str) -> bool:
            return method in self.methods

        async def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
            calls.append((method, params))
            if method == "chat.send":
                return {"id": "chat-run-1", "response": "accepted"}
            if method == "health":
                return {"status": "ok"}
            return {"ok": True}

    monkeypatch.setattr(agent_adapters, "_OpenClawRpcClient", FakeRpc)
    adapter = OpenClawAgentAdapter(
        _agent_workload(agent={"adapter": "openclaw"}),
        timeout_seconds=1.0,
    )

    sent = await adapter.send_message({"session_id": "s-1", "prompt": "hello"})
    health = await adapter.get_status({})
    canceled = await adapter.cancel({"session_id": "s-1", "external_run_id": "run-1"})
    artifacts = await adapter.list_artifacts({"session_id": "s-1"})

    assert sent["external_run_id"] == "chat-run-1"
    assert health == {"status": "ok"}
    assert canceled["accepted"] is True
    assert artifacts == []
    assert calls == [
        (
            "chat.send",
            {
                "key": "agent:main:s-1",
                "sessionKey": "agent:main:s-1",
                "message": "hello",
                "text": "hello",
                "agentId": "main",
                "idempotencyKey": calls[0][1]["idempotencyKey"],
            },
        ),
        ("health", {}),
        ("chat.abort", {"key": "agent:main:s-1", "runId": "run-1"}),
    ]


async def test_openclaw_wait_handles_missing_run_and_cancel(monkeypatch) -> None:
    canceled_calls: list[dict[str, Any]] = []
    adapter = OpenClawAgentAdapter(
        _agent_workload(agent={"adapter": "openclaw"}),
        timeout_seconds=1.0,
    )

    async def emit(
        event_type: str, message: str, data: dict[str, Any] | None = None
    ) -> None:
        del event_type, message, data

    async def canceled() -> bool:
        return True

    async def fake_cancel(payload: dict[str, Any]) -> dict[str, Any]:
        canceled_calls.append(payload)
        return {"accepted": True}

    monkeypatch.setattr(adapter, "cancel", fake_cancel)

    assert await adapter.wait_for_completion(
        {},
        {"accepted": True},
        emit=emit,
        is_cancel_requested=canceled,
        timeout_seconds=1.0,
    ) == {"accepted": True}
    assert await adapter.wait_for_completion(
        {"session_id": "s-1"},
        {"external_run_id": "run-1", "session_key": "agent:main:s-1"},
        emit=emit,
        is_cancel_requested=canceled,
        timeout_seconds=1.0,
    ) == {
        "external_run_id": "run-1",
        "session_key": "agent:main:s-1",
        "status": "cancelled",
    }
    assert canceled_calls == [
        {
            "session_id": "s-1",
            "external_run_id": "run-1",
            "session_key": "agent:main:s-1",
        }
    ]


async def test_openclaw_rpc_client_handshake_and_call(monkeypatch) -> None:
    workload = _agent_workload(
        endpoint="https://openclaw.example:18789",
        agent={"adapter": "openclaw", "authTokenEnv": "OPENCLAW_TOKEN"},
    )
    sent: list[dict[str, Any]] = []

    class FakeWebSocket:
        def __init__(self) -> None:
            self.closed = False
            self.challenge_sent = False

        async def send(self, raw: str) -> None:
            sent.append(json.loads(raw))

        async def recv(self) -> str:
            if not sent and not self.challenge_sent:
                self.challenge_sent = True
                return json.dumps({"type": "event", "event": "connect.challenge"})
            request_id = sent[-1]["id"]
            return json.dumps(
                {
                    "type": "res",
                    "id": request_id,
                    "ok": True,
                    "payload": {
                        "features": {"methods": ["health"]},
                        "status": "ok",
                    },
                }
            )

        async def close(self) -> None:
            self.closed = True

    fake_socket = FakeWebSocket()

    async def fake_connect(url: str) -> FakeWebSocket:
        assert url == "wss://openclaw.example:18789/"
        return fake_socket

    monkeypatch.setenv("OPENCLAW_TOKEN", "token")
    monkeypatch.setattr(agent_adapters.websockets, "connect", fake_connect)

    async with agent_adapters._OpenClawRpcClient(
        workload,
        timeout_seconds=1.0,
    ) as rpc:
        assert rpc.supports("health") is True
        assert rpc.supports("sessions.send") is False
        result = await rpc.call("health", {})

    assert result["status"] == "ok"
    assert fake_socket.closed is True
    assert sent[0]["method"] == "connect"
    assert sent[0]["params"]["auth"] == {"token": "token"}
    assert sent[1]["method"] == "health"


async def test_openclaw_rpc_client_raises_runtime_errors() -> None:
    workload = _agent_workload(agent={"adapter": "openclaw"})

    class FakeWebSocket:
        async def send(self, raw: str) -> None:
            self.request_id = json.loads(raw)["id"]

        async def recv(self) -> str:
            return json.dumps(
                {
                    "type": "res",
                    "id": self.request_id,
                    "ok": False,
                    "error": {"message": "boom"},
                }
            )

    rpc = agent_adapters._OpenClawRpcClient(workload, timeout_seconds=1.0)
    rpc.websocket = FakeWebSocket()

    with pytest.raises(RuntimeError, match="boom"):
        await rpc.call("health", {})


def test_extract_assistant_message_common_shapes() -> None:
    assert extract_assistant_message({"response": "done"}) == "done"
    assert extract_assistant_message({"assistant": {"content": "nested"}}) == "nested"
    assert extract_assistant_message({"accepted": True}) is None
