"""Agent runtime adapters.

MoiraWeave owns the control plane around agents. The actual reasoning loop,
tools, memory, and runtime-specific behavior stay inside the deployed agent.
Adapters provide the operational contract MoiraWeave needs: dispatch a message,
watch runtime progress, request cancellation, and discover artifacts.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import urlparse, urlunparse

import httpx
import websockets

if TYPE_CHECKING:
    from moiraweave_shared.workloads import WorkloadDefinition

EventEmitter = Callable[[str, str, dict[str, Any] | None], Awaitable[None]]
CancelChecker = Callable[[], Awaitable[bool]]

TERMINAL_AGENT_STATUSES = {
    "completed",
    "complete",
    "succeeded",
    "success",
    "failed",
    "error",
    "cancelled",
    "canceled",
}


class AgentAdapter(Protocol):
    """Operational contract for agent runtimes."""

    name: str

    async def send_message(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    async def wait_for_completion(
        self,
        payload: dict[str, Any],
        accepted: dict[str, Any],
        *,
        emit: EventEmitter,
        is_cancel_requested: CancelChecker,
        timeout_seconds: float,
    ) -> dict[str, Any]: ...

    async def get_status(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    async def cancel(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    async def list_artifacts(self, payload: dict[str, Any]) -> list[dict[str, Any]]: ...


class HttpAgentAdapter:
    """HTTP adapter for simple custom agents."""

    def __init__(self, workload: WorkloadDefinition, *, timeout_seconds: float) -> None:
        self.workload = workload
        self.timeout_seconds = timeout_seconds
        self.name = _adapter_name(workload)

    async def send_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        endpoint = self._url(payload, operation="message")
        if endpoint is None:
            return {
                "workload": self.workload.metadata.name,
                "session_id": payload.get("session_id"),
                "accepted": True,
                "adapter": self.name,
                "mode": "local-accept",
            }
        response = await self._post(endpoint, payload)
        response.setdefault("accepted", True)
        response.setdefault("adapter", self.name)
        return response

    async def wait_for_completion(
        self,
        payload: dict[str, Any],
        accepted: dict[str, Any],
        *,
        emit: EventEmitter,
        is_cancel_requested: CancelChecker,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        del payload, emit, is_cancel_requested, timeout_seconds
        return accepted

    async def get_status(self, payload: dict[str, Any]) -> dict[str, Any]:
        endpoint = self._url(payload, operation="status")
        if endpoint is None:
            return {"status": "unknown", "adapter": self.name}
        return await self._get(endpoint)

    async def cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
        endpoint = self._url(payload, operation="cancel")
        if endpoint is None:
            return {"accepted": False, "reason": "cancel endpoint not configured"}
        response = await self._post(endpoint, payload)
        response.setdefault("accepted", True)
        return response

    async def list_artifacts(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        endpoint = self._url(payload, operation="artifacts")
        if endpoint is None:
            return []
        response = await self._get(endpoint)
        artifacts = response.get("artifacts")
        return artifacts if isinstance(artifacts, list) else []

    async def _post(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(endpoint, json=payload)
            response.raise_for_status()
            return _json_dict(response)

    async def _get(self, endpoint: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.get(endpoint)
            response.raise_for_status()
            return _json_dict(response)

    def _url(self, payload: dict[str, Any], *, operation: str) -> str | None:
        base = _base_endpoint(self.workload)
        if base is None:
            return None
        path = self._path(payload, operation=operation)
        return _join_url(base, path)

    def _path(self, _payload: dict[str, Any], *, operation: str) -> str:
        agent_spec = self.workload.spec.agent
        configured = {
            "message": agent_spec.messagePath,
            "status": agent_spec.statusPath,
            "cancel": agent_spec.cancelPath,
            "artifacts": agent_spec.artifactsPath,
        }[operation]
        if configured:
            return configured

        defaults = {
            "message": "",
            "status": "/health",
            "cancel": "/cancel",
            "artifacts": "/artifacts",
        }
        return defaults[operation]


class HermesAgentAdapter(HttpAgentAdapter):
    """Adapter for Hermes Agent's OpenAI-compatible API server."""

    def __init__(self, workload: WorkloadDefinition, *, timeout_seconds: float) -> None:
        super().__init__(workload, timeout_seconds=timeout_seconds)
        self.name = "hermes"

    async def send_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        endpoint = self._hermes_url("/v1/runs")
        if endpoint is None:
            return await super().send_message(payload)

        body: dict[str, Any] = {
            "input": _message_text(payload),
            "session_id": payload.get("session_id"),
        }
        if self.workload.spec.agent.model:
            body["model"] = self.workload.spec.agent.model
        if self.workload.spec.agent.instructions:
            body["instructions"] = self.workload.spec.agent.instructions
        for key in ("conversation_history", "previous_response_id"):
            if key in payload:
                body[key] = payload[key]

        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(
                endpoint,
                json={key: value for key, value in body.items() if value is not None},
                headers=self._headers(),
            )
            response.raise_for_status()
            data = _json_dict(response)
        external_run_id = data.get("run_id") or data.get("id")
        data.update(
            {
                "accepted": True,
                "adapter": self.name,
                "external_run_id": external_run_id,
            }
        )
        return data

    async def wait_for_completion(
        self,
        payload: dict[str, Any],
        accepted: dict[str, Any],
        *,
        emit: EventEmitter,
        is_cancel_requested: CancelChecker,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        external_run_id = accepted.get("external_run_id")
        if not isinstance(external_run_id, str) or not external_run_id:
            return accepted

        await emit(
            "agent.external_run_started",
            "Hermes accepted run",
            {"external_run_id": external_run_id},
        )
        status_payload = {**payload, "external_run_id": external_run_id}
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        last_status: str | None = None

        while loop.time() < deadline:
            if await is_cancel_requested():
                await self.cancel(status_payload)
                return {
                    **accepted,
                    "status": "cancelled",
                    "external_run_id": external_run_id,
                }

            status = await self.get_status(status_payload)
            status_value = _status_value(status)
            if status_value != last_status:
                last_status = status_value
                await emit(
                    "agent.external_status",
                    "Hermes run status changed",
                    {"external_run_id": external_run_id, "status": status_value},
                )
            if status_value in TERMINAL_AGENT_STATUSES:
                artifacts = await self.list_artifacts(status_payload)
                return _normalize_hermes_result(accepted, status, artifacts)
            await asyncio.sleep(self.workload.spec.agent.pollIntervalSeconds)

        raise TimeoutError(f"Hermes run {external_run_id} did not finish before timeout")

    async def get_status(self, payload: dict[str, Any]) -> dict[str, Any]:
        external_run_id = payload.get("external_run_id")
        if not isinstance(external_run_id, str) or not external_run_id:
            return await self._hermes_get("/health/detailed")
        return await self._hermes_get(f"/v1/runs/{external_run_id}")

    async def cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
        external_run_id = payload.get("external_run_id")
        if not isinstance(external_run_id, str) or not external_run_id:
            return {"accepted": False, "reason": "external_run_id missing"}
        endpoint = self._hermes_url(f"/v1/runs/{external_run_id}/stop")
        if endpoint is None:
            return {"accepted": False, "reason": "endpoint missing"}
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(endpoint, json={}, headers=self._headers())
            response.raise_for_status()
            data = _json_dict(response)
        data.setdefault("accepted", True)
        return data

    async def list_artifacts(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        status = await self.get_status(payload)
        artifacts = status.get("artifacts")
        return artifacts if isinstance(artifacts, list) else []

    async def _hermes_get(self, path: str) -> dict[str, Any]:
        endpoint = self._hermes_url(path)
        if endpoint is None:
            return {"status": "unknown", "adapter": self.name}
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.get(endpoint, headers=self._headers())
            response.raise_for_status()
            return _json_dict(response)

    def _hermes_url(self, path: str) -> str | None:
        base = _base_endpoint(self.workload)
        return _join_url(base, path) if base else None

    def _headers(self) -> dict[str, str]:
        token = _auth_token(self.workload, "HERMES_API_SERVER_KEY", "API_SERVER_KEY")
        return {"Authorization": f"Bearer {token}"} if token else {}


class OpenClawAgentAdapter(HttpAgentAdapter):
    """Adapter for OpenClaw Gateway's WebSocket JSON-RPC protocol."""

    def __init__(self, workload: WorkloadDefinition, *, timeout_seconds: float) -> None:
        super().__init__(workload, timeout_seconds=timeout_seconds)
        self.name = "openclaw"

    async def send_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        session_key = _openclaw_session_key(self.workload, payload)
        agent_id = self.workload.spec.agent.agentId or "main"
        async with _OpenClawRpcClient(self.workload, timeout_seconds=self.timeout_seconds) as rpc:
            await _openclaw_ensure_session(rpc, session_key, agent_id)
            method = "sessions.send" if rpc.supports("sessions.send") else "chat.send"
            result = await rpc.call(
                method,
                {
                    "key": session_key,
                    "sessionKey": session_key,
                    "message": _message_text(payload),
                    "text": _message_text(payload),
                    "agentId": agent_id,
                    "idempotencyKey": str(payload.get("idempotency_key") or uuid.uuid4()),
                },
            )
        run_id = _first_string(result, "runId", "run_id", "taskId", "id")
        return {
            "accepted": True,
            "adapter": self.name,
            "session_key": session_key,
            "external_run_id": run_id,
            "response": _first_string(result, "message", "output", "text", "response"),
            "raw": result,
        }

    async def wait_for_completion(
        self,
        payload: dict[str, Any],
        accepted: dict[str, Any],
        *,
        emit: EventEmitter,
        is_cancel_requested: CancelChecker,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        run_id = accepted.get("external_run_id")
        session_key = str(accepted.get("session_key") or _openclaw_session_key(self.workload, payload))
        if not isinstance(run_id, str) or not run_id:
            return accepted

        await emit(
            "agent.external_run_started",
            "OpenClaw accepted run",
            {"external_run_id": run_id, "session_key": session_key},
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        last_status: str | None = None

        while loop.time() < deadline:
            if await is_cancel_requested():
                await self.cancel({**payload, "external_run_id": run_id, "session_key": session_key})
                return {**accepted, "status": "cancelled"}
            async with _OpenClawRpcClient(self.workload, timeout_seconds=self.timeout_seconds) as rpc:
                status = await _openclaw_status(rpc, run_id, session_key)
            status_value = _status_value(status)
            if status_value != last_status:
                last_status = status_value
                await emit(
                    "agent.external_status",
                    "OpenClaw run status changed",
                    {"external_run_id": run_id, "session_key": session_key, "status": status_value},
                )
            if status_value in TERMINAL_AGENT_STATUSES:
                artifacts = await self.list_artifacts(
                    {**payload, "external_run_id": run_id, "session_key": session_key}
                )
                return _normalize_openclaw_result(accepted, status, artifacts)
            await asyncio.sleep(self.workload.spec.agent.pollIntervalSeconds)

        raise TimeoutError(f"OpenClaw run {run_id} did not finish before timeout")

    async def get_status(self, payload: dict[str, Any]) -> dict[str, Any]:
        run_id = payload.get("external_run_id")
        session_key = str(payload.get("session_key") or _openclaw_session_key(self.workload, payload))
        async with _OpenClawRpcClient(self.workload, timeout_seconds=self.timeout_seconds) as rpc:
            if isinstance(run_id, str) and run_id:
                return await _openclaw_status(rpc, run_id, session_key)
            return await rpc.call("health", {})

    async def cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
        session_key = str(payload.get("session_key") or _openclaw_session_key(self.workload, payload))
        params: dict[str, Any] = {"key": session_key}
        if payload.get("external_run_id"):
            params["runId"] = payload["external_run_id"]
        async with _OpenClawRpcClient(self.workload, timeout_seconds=self.timeout_seconds) as rpc:
            method = "sessions.abort" if rpc.supports("sessions.abort") else "chat.abort"
            result = await rpc.call(method, params)
        return {"accepted": True, "adapter": self.name, "raw": result}

    async def list_artifacts(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        params = {
            "sessionKey": str(payload.get("session_key") or _openclaw_session_key(self.workload, payload)),
        }
        if payload.get("external_run_id"):
            params["runId"] = payload["external_run_id"]
        async with _OpenClawRpcClient(self.workload, timeout_seconds=self.timeout_seconds) as rpc:
            if not rpc.supports("artifacts.list"):
                return []
            result = await rpc.call("artifacts.list", params)
        artifacts = result.get("artifacts") if isinstance(result, dict) else None
        return artifacts if isinstance(artifacts, list) else []


class _OpenClawRpcClient:
    def __init__(self, workload: WorkloadDefinition, *, timeout_seconds: float) -> None:
        self.workload = workload
        self.timeout_seconds = timeout_seconds
        self.websocket: Any = None
        self.methods: set[str] = set()

    async def __aenter__(self) -> _OpenClawRpcClient:
        self.websocket = await websockets.connect(_openclaw_ws_url(self.workload))
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.websocket.recv(), timeout=0.5)
        hello = await self.call(
            "connect",
            {
                "minProtocol": 4,
                "maxProtocol": 4,
                "client": {
                    "id": "moiraweave-worker",
                    "version": "0.1.0",
                    "platform": "linux",
                    "mode": "operator",
                },
                "role": "operator",
                "scopes": ["operator.read", "operator.write"],
                "caps": [],
                "commands": [],
                "permissions": {},
                "auth": _openclaw_auth(self.workload),
                "locale": "en-US",
                "userAgent": "moiraweave-worker/0.1.0",
            },
        )
        features = hello.get("features")
        if isinstance(features, Mapping):
            methods = features.get("methods")
            if isinstance(methods, list):
                self.methods = {str(method) for method in methods}
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self.websocket is not None:
            await self.websocket.close()

    async def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        request_id = str(uuid.uuid4())
        await self.websocket.send(
            json.dumps(
                {
                    "type": "req",
                    "id": request_id,
                    "method": method,
                    "params": params,
                }
            )
        )
        while True:
            raw = await asyncio.wait_for(
                self.websocket.recv(),
                timeout=self.timeout_seconds,
            )
            message = json.loads(raw)
            if message.get("type") != "res" or message.get("id") != request_id:
                continue
            if message.get("ok") is False:
                error = message.get("error") or {}
                raise RuntimeError(error.get("message") or f"OpenClaw RPC {method} failed")
            payload = message.get("payload")
            return payload if isinstance(payload, dict) else {"response": payload}

    def supports(self, method: str) -> bool:
        return not self.methods or method in self.methods


def build_agent_adapter(
    workload: WorkloadDefinition, *, timeout_seconds: float
) -> AgentAdapter:
    """Build the adapter declared by a workload manifest."""

    name = _adapter_name(workload)
    if name == "hermes":
        return HermesAgentAdapter(workload, timeout_seconds=timeout_seconds)
    if name == "openclaw":
        return OpenClawAgentAdapter(workload, timeout_seconds=timeout_seconds)
    return HttpAgentAdapter(workload, timeout_seconds=timeout_seconds)


def extract_assistant_message(result: Mapping[str, Any]) -> str | None:
    """Extract a readable agent response from common runtime response shapes."""

    for key in ("message", "response", "content", "text", "output"):
        value = result.get(key)
        if isinstance(value, str) and value.strip():
            return value

    nested = result.get("assistant")
    if isinstance(nested, Mapping):
        content = nested.get("message") or nested.get("content")
        if isinstance(content, str) and content.strip():
            return content
    return None


def _adapter_name(workload: WorkloadDefinition) -> str:
    if workload.spec.adapter:
        return "generic-http" if workload.spec.adapter == "generic" else workload.spec.adapter
    if workload.spec.agent.adapter == "generic":
        return "generic-http"
    return workload.spec.agent.adapter


def _base_endpoint(workload: WorkloadDefinition) -> str | None:
    if workload.spec.endpoint:
        return workload.spec.endpoint.rstrip("/")
    if not workload.spec.ports:
        return None
    port = workload.spec.ports[0].port
    return f"http://{workload.metadata.name}:{port}"


def _join_url(base: str | None, path: str) -> str:
    if base is None:
        raise ValueError("base endpoint is required")
    if not path:
        return base
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


def _json_dict(response: httpx.Response) -> dict[str, Any]:
    data = response.json()
    return data if isinstance(data, dict) else {"response": data}


def _message_text(payload: Mapping[str, Any]) -> str:
    value = payload.get("message") or payload.get("input") or payload.get("prompt")
    if isinstance(value, str) and value.strip():
        return value
    return json.dumps(payload)


def _first_string(data: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _status_value(data: Mapping[str, Any]) -> str:
    value = data.get("status") or data.get("state")
    if isinstance(value, str):
        return value.lower()
    if data.get("ok") is True:
        return "completed"
    return "unknown"


def _normalize_hermes_result(
    accepted: Mapping[str, Any],
    status: Mapping[str, Any],
    artifacts: list[dict[str, Any]],
) -> dict[str, Any]:
    output = status.get("output") or status.get("response") or status.get("message")
    result = {**accepted, **status, "artifacts": artifacts}
    if isinstance(output, str):
        result["response"] = output
    return result


def _normalize_openclaw_result(
    accepted: Mapping[str, Any],
    status: Mapping[str, Any],
    artifacts: list[dict[str, Any]],
) -> dict[str, Any]:
    output = status.get("output") or status.get("message") or status.get("summary")
    result = {**accepted, **status, "artifacts": artifacts}
    if isinstance(output, str):
        result["response"] = output
    return result


def _auth_token(workload: WorkloadDefinition, *fallback_envs: str) -> str | None:
    env_name = workload.spec.agent.authTokenEnv
    if env_name and os.getenv(env_name):
        return os.getenv(env_name)
    for fallback in fallback_envs:
        if os.getenv(fallback):
            return os.getenv(fallback)
    return None


def _openclaw_auth(workload: WorkloadDefinition) -> dict[str, str]:
    token = _auth_token(workload, "OPENCLAW_GATEWAY_TOKEN")
    return {"token": token} if token else {}


def _openclaw_ws_url(workload: WorkloadDefinition) -> str:
    base = _base_endpoint(workload) or "http://openclaw:18789"
    parsed = urlparse(base)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunparse((scheme, parsed.netloc, parsed.path or "/", "", "", ""))


def _openclaw_session_key(workload: WorkloadDefinition, payload: Mapping[str, Any]) -> str:
    agent_id = workload.spec.agent.agentId or "main"
    value = payload.get("session_key") or payload.get("session_id") or "default"
    session = str(value)
    if session.startswith("agent:"):
        return session
    return f"agent:{agent_id}:{session}"


async def _openclaw_ensure_session(
    rpc: _OpenClawRpcClient, session_key: str, agent_id: str
) -> None:
    if rpc.supports("sessions.describe"):
        with contextlib.suppress(Exception):
            await rpc.call("sessions.describe", {"key": session_key})
            return
    if rpc.supports("sessions.create"):
        await rpc.call("sessions.create", {"key": session_key, "agentId": agent_id})


async def _openclaw_status(
    rpc: _OpenClawRpcClient, run_id: str, session_key: str
) -> dict[str, Any]:
    if rpc.supports("agent.wait"):
        with contextlib.suppress(Exception):
            return await rpc.call("agent.wait", {"runId": run_id, "timeoutMs": 1})
    if rpc.supports("tasks.get"):
        with contextlib.suppress(Exception):
            return await rpc.call("tasks.get", {"taskId": run_id})
    return await rpc.call("sessions.describe", {"key": session_key})
