"""Workload control-plane API routes."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import AsyncGenerator  # noqa: TC003
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import httpx
from fastapi import APIRouter, HTTPException, Query, Request, status
from moiraweave_shared.control_plane import (
    ControlPlaneRepository,
    StoredArtifact,
    StoredRun,
    StoredRunEvent,
    utc_now_iso,
    workloads_by_name,
)
from moiraweave_shared.schemas import RunMessage
from moiraweave_shared.streams import RUN_STREAM
from moiraweave_shared.workloads import (
    TERMINAL_RUN_STATUSES,
    RunStateTransitionError,
    WorkloadDefinition,
    ensure_run_transition,
    load_workloads,
)
from starlette.responses import StreamingResponse

from app.config import Settings, get_settings
from app.dependencies.auth import CurrentUser  # noqa: TC001
from app.dependencies.control_plane import ControlPlane  # noqa: TC001
from app.dependencies.redis import RedisClient  # noqa: TC001
from app.middleware.rate_limit import limiter
from app.models.workloads import (
    AgentMessageHistoryItem,
    AgentMessageRequest,
    AgentMessageResponse,
    AgentSessionHealthResponse,
    AgentSessionRequest,
    AgentSessionResponse,
    ChannelMessageRequest,
    DeploymentOperationEvent,
    DeploymentOperationRequest,
    DeploymentOperationResponse,
    DeploymentPlanResponse,
    DeploymentRequest,
    DeploymentResponse,
    PreflightCheck,
    PreflightRequest,
    PreflightResponse,
    RunArtifact,
    RunEvent,
    RunRequest,
    RunResponse,
    RunStatusResponse,
    WorkloadFromTemplateRequest,
    WorkloadHealthResponse,
    WorkloadInfo,
    WorkloadTemplateInfo,
    WorkloadTemplateParameter,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1", tags=["workloads"])


_DEMO_AGENT_SCRIPT = r"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def _send(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/health"):
            self._send({"status": "healthy", "ok": True})
            return
        if self.path.startswith("/artifacts"):
            self._send({"artifacts": []})
            return
        self._send({"error": "not found"}, status=404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        payload = json.loads(raw.decode("utf-8") or "{}")
        text = payload.get("message") or payload.get("prompt") or "hello"
        self._send(
            {
                "accepted": True,
                "status": "succeeded",
                "response": f"Demo agent received: {text}",
                "artifacts": [
                    {
                        "id": f"{payload.get('session_id', 'demo')}-reply",
                        "name": "demo-reply.json",
                        "uri": "memory://demo-reply.json",
                        "content_type": "application/json",
                        "metadata": {"source": "demo-agent"},
                    }
                ],
            }
        )


HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
""".strip()


_TEMPLATE_PARAMETERS: dict[str, list[WorkloadTemplateParameter]] = {
    "demo-agent": [
        WorkloadTemplateParameter(
            name="name",
            label="Name",
            default="demo-agent",
            description="Workload name for the local demo agent.",
        )
    ],
    "hermes": [
        WorkloadTemplateParameter(name="name", label="Name", default="hermes"),
        WorkloadTemplateParameter(
            name="image",
            label="Image",
            default="ghcr.io/nousresearch/hermes-agent:latest",
        ),
        WorkloadTemplateParameter(
            name="port", label="Port", type="number", default=8642
        ),
        WorkloadTemplateParameter(
            name="model",
            label="Model",
            default="hermes-agent",
            required=False,
        ),
    ],
    "openclaw": [
        WorkloadTemplateParameter(name="name", label="Name", default="openclaw"),
        WorkloadTemplateParameter(
            name="image",
            label="Image",
            default="ghcr.io/openclaw/openclaw:latest",
        ),
        WorkloadTemplateParameter(
            name="port", label="Gateway port", type="number", default=18789
        ),
        WorkloadTemplateParameter(
            name="agent_id", label="Agent ID", default="main", required=False
        ),
    ],
    "generic-http-agent": [
        WorkloadTemplateParameter(name="name", label="Name", default="generic-agent"),
        WorkloadTemplateParameter(
            name="image", label="Image", default="ghcr.io/example/agent:latest"
        ),
        WorkloadTemplateParameter(
            name="port", label="Port", type="number", default=8000
        ),
        WorkloadTemplateParameter(
            name="message_path", label="Message path", default="/message"
        ),
    ],
    "external-agent": [
        WorkloadTemplateParameter(name="name", label="Name", default="external-agent"),
        WorkloadTemplateParameter(
            name="endpoint", label="Endpoint", default="https://agent.example.com"
        ),
        WorkloadTemplateParameter(
            name="adapter",
            label="Adapter",
            default="generic-http",
            options=["generic-http", "hermes", "openclaw"],
        ),
    ],
    "model-service": [
        WorkloadTemplateParameter(name="name", label="Name", default="model-service"),
        WorkloadTemplateParameter(
            name="image", label="Image", default="ghcr.io/example/model:latest"
        ),
        WorkloadTemplateParameter(
            name="port", label="Port", type="number", default=8080
        ),
    ],
    "pipeline": [
        WorkloadTemplateParameter(name="name", label="Name", default="sample-pipeline")
    ],
}


def _clean_workload_name(value: Any, default: str) -> str:
    raw = str(value or default).strip().lower()
    cleaned = re.sub(r"[^a-z0-9-]+", "-", raw).strip("-")
    return cleaned or default


def _template_param(
    params: dict[str, Any],
    template_id: str,
    name: str,
) -> Any:
    if name in params and params[name] not in {None, ""}:
        return params[name]
    for parameter in _TEMPLATE_PARAMETERS[template_id]:
        if parameter.name == name:
            return parameter.default
    return None


def _template_manifest(template_id: str, params: dict[str, Any]) -> dict[str, Any]:
    if template_id not in _TEMPLATE_PARAMETERS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Template {template_id!r} not found",
        )

    name = _clean_workload_name(
        _template_param(params, template_id, "name"),
        str(_template_param(params, template_id, "name") or template_id),
    )

    if template_id == "demo-agent":
        return {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {
                "name": name,
                "labels": {"moiraweave.io/template": "demo-agent"},
            },
            "spec": {
                "type": "agent-service",
                "image": "python:3.13-slim",
                "deployment": {
                    "mode": "managed",
                    "targets": ["local", "kubernetes"],
                    "serviceName": name,
                    "localNetwork": "moiraweave-net",
                },
                "execution": {"mode": "session", "timeoutSeconds": 3600},
                "ports": [{"name": "http", "port": 8000}],
                "agent": {
                    "adapter": "generic-http",
                    "messagePath": "/message",
                    "statusPath": "/health",
                    "artifactsPath": "/artifacts",
                    "exposedChannels": ["ui", "api", "webhook"],
                    "capabilities": ["demo", "chat"],
                    "dispatchTimeoutSeconds": 5,
                    "pollIntervalSeconds": 1,
                },
                "command": ["python", "-u", "-c"],
                "args": [_DEMO_AGENT_SCRIPT],
            },
        }

    if template_id == "hermes":
        port = int(_template_param(params, template_id, "port") or 8642)
        return {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {
                "name": name,
                "labels": {"moiraweave.io/template": "hermes"},
            },
            "spec": {
                "type": "agent-service",
                "image": _template_param(params, template_id, "image"),
                "deployment": {
                    "mode": "managed",
                    "targets": ["local", "kubernetes"],
                    "serviceName": name,
                    "localNetwork": "moiraweave-net",
                },
                "execution": {"mode": "session", "timeoutSeconds": 172800},
                "ports": [{"name": "http", "port": port}],
                "persistence": {"enabled": True, "mountPath": "/workspace"},
                "env": {
                    "API_SERVER_ENABLED": "true",
                    "API_SERVER_HOST": "0.0.0.0",
                    "API_SERVER_PORT": str(port),
                },
                "secrets": ["OPENAI_API_KEY", "HERMES_API_SERVER_KEY"],
                "agent": {
                    "adapter": "hermes",
                    "requiredSecrets": ["OPENAI_API_KEY"],
                    "workspaceMount": "/workspace",
                    "authTokenEnv": "HERMES_API_SERVER_KEY",
                    "model": _template_param(params, template_id, "model"),
                    "exposedChannels": ["ui", "api"],
                    "capabilities": ["chat", "tools", "long-running"],
                    "pollIntervalSeconds": 2,
                },
            },
        }

    if template_id == "openclaw":
        port = int(_template_param(params, template_id, "port") or 18789)
        return {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {
                "name": name,
                "labels": {"moiraweave.io/template": "openclaw"},
            },
            "spec": {
                "type": "agent-service",
                "image": _template_param(params, template_id, "image"),
                "deployment": {
                    "mode": "managed",
                    "targets": ["local", "kubernetes"],
                    "serviceName": name,
                    "localNetwork": "moiraweave-net",
                },
                "execution": {"mode": "session", "timeoutSeconds": 172800},
                "ports": [{"name": "gateway", "port": port}],
                "persistence": {"enabled": True, "mountPath": "/workspace"},
                "secrets": ["OPENCLAW_GATEWAY_TOKEN"],
                "agent": {
                    "adapter": "openclaw",
                    "agentId": _template_param(params, template_id, "agent_id"),
                    "authTokenEnv": "OPENCLAW_GATEWAY_TOKEN",
                    "workspaceMount": "/workspace",
                    "exposedChannels": ["ui", "api"],
                    "capabilities": ["browser", "tools", "long-running"],
                    "pollIntervalSeconds": 2,
                },
            },
        }

    if template_id == "generic-http-agent":
        port = int(_template_param(params, template_id, "port") or 8000)
        return {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {
                "name": name,
                "labels": {"moiraweave.io/template": "generic-http-agent"},
            },
            "spec": {
                "type": "agent-service",
                "image": _template_param(params, template_id, "image"),
                "deployment": {
                    "mode": "managed",
                    "targets": ["local", "kubernetes"],
                    "serviceName": name,
                    "localNetwork": "moiraweave-net",
                },
                "execution": {"mode": "session", "timeoutSeconds": 86400},
                "ports": [{"name": "http", "port": port}],
                "agent": {
                    "adapter": "generic-http",
                    "messagePath": _template_param(params, template_id, "message_path"),
                    "statusPath": "/health",
                    "cancelPath": "/cancel",
                    "artifactsPath": "/artifacts",
                    "exposedChannels": ["ui", "api"],
                },
            },
        }

    if template_id == "external-agent":
        return {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {
                "name": name,
                "labels": {"moiraweave.io/template": "external-agent"},
            },
            "spec": {
                "type": "agent-service",
                "deployment": {"mode": "external"},
                "endpoint": _template_param(params, template_id, "endpoint"),
                "execution": {"mode": "session", "timeoutSeconds": 86400},
                "agent": {
                    "adapter": _template_param(params, template_id, "adapter"),
                    "exposedChannels": ["ui", "api"],
                },
            },
        }

    if template_id == "model-service":
        port = int(_template_param(params, template_id, "port") or 8080)
        return {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {
                "name": name,
                "labels": {"moiraweave.io/template": "model-service"},
            },
            "spec": {
                "type": "model-service",
                "image": _template_param(params, template_id, "image"),
                "deployment": {
                    "mode": "managed",
                    "targets": ["local", "kubernetes"],
                    "serviceName": name,
                    "localNetwork": "moiraweave-net",
                },
                "execution": {"mode": "sync", "timeoutSeconds": 300},
                "ports": [{"name": "http", "port": port}],
            },
        }

    return {
        "apiVersion": "moiraweave.io/v1alpha1",
        "kind": "Workload",
        "metadata": {
            "name": name,
            "labels": {"moiraweave.io/template": "pipeline"},
        },
        "spec": {
            "type": "pipeline",
            "execution": {"mode": "async", "timeoutSeconds": 3600},
            "steps": [],
        },
    }


def _template_info(template_id: str) -> WorkloadTemplateInfo:
    catalog = {
        "demo-agent": (
            "Demo Agent",
            "agent",
            "Local mock agent with chat, events, and artifacts; no secrets needed.",
            "agent-service",
            ["demo", "local", "no-secrets"],
        ),
        "hermes": (
            "Hermes Agent",
            "agent",
            "Managed Hermes runtime with persistence, secrets, and UI/API sessions.",
            "agent-service",
            ["hermes", "managed", "long-running"],
        ),
        "openclaw": (
            "OpenClaw",
            "agent",
            "Managed OpenClaw gateway runtime with session-oriented dispatch.",
            "agent-service",
            ["openclaw", "managed", "browser"],
        ),
        "generic-http-agent": (
            "Generic HTTP Agent",
            "agent",
            "Any HTTP runtime exposing message, health, cancel, and artifact hooks.",
            "agent-service",
            ["generic-http", "adapter"],
        ),
        "external-agent": (
            "External Agent",
            "agent",
            "Agent already deployed outside MoiraWeave, supervised by endpoint.",
            "agent-service",
            ["external", "supervised"],
        ),
        "model-service": (
            "Model Service",
            "model",
            "Managed HTTP/KServe-compatible inference service.",
            "model-service",
            ["model", "inference"],
        ),
        "pipeline": (
            "Pipeline",
            "pipeline",
            "DAG workload whose nodes call other MoiraWeave workloads.",
            "pipeline",
            ["dag", "composition"],
        ),
    }
    name, category, description, workload_type, tags = catalog[template_id]
    return WorkloadTemplateInfo(
        id=template_id,
        name=name,
        category=category,
        description=description,
        workload_type=workload_type,
        tags=tags,
        parameters=_TEMPLATE_PARAMETERS[template_id],
        manifest=_template_manifest(template_id, {}),
    )


def _workload_info(workload: WorkloadDefinition) -> WorkloadInfo:
    return WorkloadInfo(
        name=workload.metadata.name,
        type=workload.spec.type,
        execution_mode=workload.spec.execution.mode,
        image=workload.spec.image,
        manifest=workload.to_manifest(),
    )


def _run_response(run: StoredRun) -> RunStatusResponse:
    return RunStatusResponse(**run.model_dump())


def _event_response(event: StoredRunEvent) -> RunEvent:
    return RunEvent(**event.model_dump())


def _artifact_response(artifact: StoredArtifact) -> RunArtifact:
    return RunArtifact(**artifact.model_dump())


def _deployment_response(deployment: Any) -> DeploymentResponse:
    return DeploymentResponse(**deployment.model_dump())


def _deployment_operation_response(operation: Any) -> DeploymentOperationResponse:
    return DeploymentOperationResponse(**operation.model_dump())


def _deployment_operation_event_response(event: Any) -> DeploymentOperationEvent:
    return DeploymentOperationEvent(**event.model_dump())


def _deployment_service_name(workload: WorkloadDefinition) -> str:
    return workload.spec.deployment.serviceName or workload.metadata.name


def _deployment_endpoint(workload: WorkloadDefinition) -> str | None:
    if workload.spec.endpoint:
        return workload.spec.endpoint.rstrip("/")
    if not workload.spec.ports:
        return None
    port = workload.spec.ports[0].port
    return f"http://{_deployment_service_name(workload)}:{port}"


def _deployment_plan_response(
    workload: WorkloadDefinition,
    *,
    target: str,
    env: str,
) -> DeploymentPlanResponse:
    requested_target = "kubernetes" if target == "k8s" else target
    mode = workload.spec.deployment.mode
    service_name = _deployment_service_name(workload)
    endpoint = _deployment_endpoint(workload)

    if mode == "external":
        return DeploymentPlanResponse(
            workload_name=workload.metadata.name,
            target="external",
            mode=mode,
            service_name=service_name,
            endpoint=endpoint,
            commands=[
                "moira deploy local --register",
                f"moira deploy k8s --env {env} --register",
            ],
            notes=[
                "Runtime deployment is owned outside MoiraWeave.",
                "MoiraWeave records the external endpoint for sessions, runs, and health.",
            ],
        )

    if requested_target == "external":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="external target is only valid for deployment.mode external",
        )
    if requested_target not in workload.spec.deployment.targets:
        allowed = ", ".join(workload.spec.deployment.targets)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Target {requested_target!r} is not enabled for this workload. "
            f"Allowed targets: {allowed}",
        )

    if requested_target == "local":
        compose_file = ".moiraweave/deploy/docker-compose.workloads.yml"
        return DeploymentPlanResponse(
            workload_name=workload.metadata.name,
            target="local",
            mode=mode,
            service_name=service_name,
            endpoint=endpoint,
            files=[compose_file],
            commands=[
                "moira deploy local",
                f"docker compose -f docker-compose.yml -f {compose_file} up -d",
                "moira deploy local --register",
            ],
            notes=[
                "The UI can register the deployment record, but local Docker apply "
                "still runs through CLI or automation with host Docker access.",
            ],
        )

    values_file = f".moiraweave/deploy/values-workloads-{env}.yaml"
    namespace = workload.spec.deployment.namespace or "moiraweave"
    return DeploymentPlanResponse(
        workload_name=workload.metadata.name,
        target="kubernetes",
        mode=mode,
        service_name=service_name,
        endpoint=endpoint,
        files=[values_file],
        commands=[
            f"moira deploy k8s --env {env}",
            "helm upgrade --install moiraweave infra/helm/moiraweave "
            f"--namespace {namespace} --create-namespace -f {values_file}",
            f"moira deploy k8s --env {env} --register",
        ],
        notes=[
            "Kubernetes apply requires cluster credentials and should run from "
            "CLI, CI, or a future MoiraWeave deployment operator.",
        ],
    )


async def _all_workloads(
    control_plane: ControlPlaneRepository, settings: Settings
) -> dict[str, WorkloadDefinition]:
    workloads = workloads_by_name(load_workloads(settings.workloads_dir))
    workloads.update(workloads_by_name(await control_plane.list_workloads()))
    return workloads


async def _get_workload(
    name: str,
    control_plane: ControlPlaneRepository,
    settings: Settings,
) -> WorkloadDefinition:
    workloads = await _all_workloads(control_plane, settings)
    if name not in workloads:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workload {name!r} not found",
        )
    return workloads[name]


async def _authorize_run(
    run_id: str,
    control_plane: ControlPlaneRepository,
    current_user: Any,
) -> StoredRun:
    run = await control_plane.get_run(run_id)
    if run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Run not found"
        )
    if run.user != current_user.subject:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    return run


async def _create_run(
    redis: Any,
    control_plane: ControlPlaneRepository,
    workload: WorkloadDefinition,
    payload: dict[str, Any],
    user: str,
    *,
    session_id: str | None = None,
) -> RunResponse:
    run_id = str(uuid4())
    created_at = utc_now_iso()
    workload_name = workload.metadata.name
    run = await control_plane.create_run(
        run_id,
        workload_name,
        payload,
        user,
        created_at=created_at,
        session_id=session_id,
    )
    await control_plane.append_run_event(
        run_id,
        "run.queued",
        "Run queued for dispatch",
        data={"workload_name": workload_name, "session_id": session_id},
    )
    msg = RunMessage(
        run_id=run_id,
        workload_name=workload_name,
        payload=json.dumps(payload),
        user=user,
        workload_manifest=json.dumps(workload.to_manifest()),
    )
    await redis.xadd(
        RUN_STREAM,
        {
            "run_id": msg.run_id,
            "workload_name": msg.workload_name,
            "payload": msg.payload,
            "user": msg.user,
            "workload_manifest": msg.workload_manifest,
        },
    )
    return RunResponse(
        run_id=run.run_id,
        workload_name=run.workload_name,
        status=run.status,
        created_at=run.created_at,
    )


def _session_payload(session: Any) -> dict[str, Any]:
    return {
        "session_id": session.session_id,
        "agent_name": session.agent_name,
        "status": session.status,
        "created_at": session.created_at,
        "metadata": session.metadata,
    }


def _message_payload(
    message: Any,
    *,
    run: StoredRun | None = None,
    latest_event: StoredRunEvent | None = None,
    artifact_count: int = 0,
) -> dict[str, Any]:
    payload = {
        "message_id": message.message_id,
        "session_id": message.session_id,
        "role": message.role,
        "message": message.message,
        "context": message.context,
        "created_at": message.created_at,
    }
    if run is not None:
        payload.update(
            {
                "run_id": run.run_id,
                "run_status": run.status,
                "latest_event": _event_response(latest_event).model_dump()
                if latest_event
                else None,
                "artifact_count": artifact_count,
            }
        )
    return payload


async def _authorize_agent_session(
    name: str,
    session_id: str,
    control_plane: ControlPlaneRepository,
    current_user: Any,
) -> Any:
    session = await control_plane.get_agent_session(session_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found"
        )
    if session.user != current_user.subject or session.agent_name != name:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    return session


def _validate_agent_channel(workload: WorkloadDefinition, channel: str) -> str:
    normalized = channel.strip().lower()
    if not normalized:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Channel cannot be empty",
        )

    agent = workload.spec.agent
    if normalized in set(agent.externalOwnedChannels):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Channel {normalized!r} is owned by the agent runtime. "
                "Use the runtime connector directly and monitor it from MoiraWeave."
            ),
        )
    if normalized not in set(agent.exposedChannels):
        allowed = ", ".join(agent.exposedChannels) or "none"
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Channel {normalized!r} is not exposed by workload "
                f"{workload.metadata.name!r}. Add it to spec.agent.exposedChannels. "
                f"Allowed channels: {allowed}."
            ),
        )
    return normalized


def _deployment_probe_url(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    if parsed.path and parsed.path != "/":
        return endpoint
    return endpoint.rstrip("/") + "/health"


async def _probe_deployment_endpoint(
    deployment: DeploymentResponse,
) -> tuple[bool, str] | None:
    if not deployment.endpoint:
        return None
    parsed = urlparse(deployment.endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return (
            False,
            f"Deployment endpoint is not a valid HTTP URL: {deployment.endpoint}",
        )
    url = _deployment_probe_url(deployment.endpoint)
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(url)
    except httpx.HTTPError as exc:
        return False, f"Health probe failed for {url}: {exc.__class__.__name__}"
    if 200 <= response.status_code < 400:
        return True, f"Health probe succeeded for {url}"
    return False, f"Health probe for {url} returned HTTP {response.status_code}"


async def _deployment_health_status(
    deployments: list[DeploymentResponse],
) -> tuple[str, str]:
    if not deployments:
        return "unknown", "No deployment record has been registered for this workload"
    probes = [
        probe
        for probe in [
            await _probe_deployment_endpoint(deployment) for deployment in deployments
        ]
        if probe is not None
    ]
    if probes:
        if any(ok for ok, _reason in probes):
            return "healthy", next(reason for ok, reason in probes if ok)
        return "degraded", "; ".join(reason for _ok, reason in probes)
    statuses = {deployment.status for deployment in deployments}
    if statuses & {"failed", "lost", "unhealthy"}:
        return "degraded", "At least one deployment is reporting a failed state"
    if statuses & {"applied", "running", "healthy"}:
        return "healthy", "A deployment record is active"
    return "pending", "Deployment exists but is not active yet"


def _preflight_status(checks: list[PreflightCheck]) -> str:
    if any(check.status == "failed" for check in checks):
        return "failed"
    if any(check.status == "warning" for check in checks):
        return "warning"
    return "passed"


async def _run_preflight(
    workload: WorkloadDefinition,
    *,
    target: str,
    env: str,
    control_plane: ControlPlaneRepository,
    redis: Any,
) -> PreflightResponse:
    checks: list[PreflightCheck] = []
    normalized_target = "kubernetes" if target == "k8s" else target

    checks.append(
        PreflightCheck(
            name="manifest",
            status="passed",
            message="Workload manifest is valid.",
            metadata={
                "type": workload.spec.type,
                "execution": workload.spec.execution.mode,
            },
        )
    )

    try:
        plan = _deployment_plan_response(workload, target=normalized_target, env=env)
        checks.append(
            PreflightCheck(
                name="deployment_target",
                status="passed",
                message=f"{normalized_target} deployment plan can be generated.",
                metadata=plan.model_dump(),
            )
        )
    except HTTPException as exc:
        checks.append(
            PreflightCheck(
                name="deployment_target",
                status="failed",
                message=str(exc.detail),
                remediation="Adjust spec.deployment.targets or choose another target.",
            )
        )

    if workload.spec.deployment.mode == "external":
        endpoint = workload.spec.endpoint
        parsed = urlparse(endpoint or "")
        endpoint_ok = parsed.scheme in {"http", "https"} and bool(parsed.netloc)
        checks.append(
            PreflightCheck(
                name="runtime_location",
                status="passed" if endpoint_ok else "failed",
                message=(
                    f"External runtime endpoint is {endpoint}."
                    if endpoint_ok
                    else "External workloads need a valid HTTP endpoint."
                ),
                remediation="Set spec.endpoint to an http(s) runtime base URL."
                if not endpoint_ok
                else None,
            )
        )
    elif workload.spec.type != "pipeline":
        missing = []
        if not workload.spec.image:
            missing.append("image")
        if not workload.spec.ports:
            missing.append("ports")
        checks.append(
            PreflightCheck(
                name="runtime_location",
                status="failed" if missing else "passed",
                message=(
                    "Managed runtime declares image and network port."
                    if not missing
                    else f"Managed runtime is missing: {', '.join(missing)}."
                ),
                remediation="Set spec.image and at least one spec.ports entry."
                if missing
                else None,
            )
        )

    secret_names = set(workload.spec.secrets)
    secret_names.update(workload.spec.agent.requiredSecrets)
    if workload.spec.agent.authTokenEnv:
        secret_names.add(workload.spec.agent.authTokenEnv)
    missing_secrets = sorted(name for name in secret_names if not os.getenv(name))
    checks.append(
        PreflightCheck(
            name="secrets",
            status="warning" if missing_secrets else "passed",
            message=(
                "All required secret environment variables are present."
                if not missing_secrets
                else f"Missing secret references: {', '.join(missing_secrets)}."
            ),
            remediation="Add missing names to local .env or Kubernetes secrets."
            if missing_secrets
            else None,
            metadata={"required": sorted(secret_names), "missing": missing_secrets},
        )
    )

    if workload.spec.type == "agent-service":
        adapter = workload.spec.agent.adapter
        checks.append(
            PreflightCheck(
                name="agent_adapter",
                status="passed",
                message=f"Agent adapter {adapter!r} is supported.",
                metadata={
                    "adapter": adapter,
                    "channels": workload.spec.agent.exposedChannels,
                },
            )
        )

    try:
        await control_plane.ping()
        checks.append(
            PreflightCheck(
                name="postgres",
                status="passed",
                message="Control-plane storage is reachable.",
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(
            PreflightCheck(
                name="postgres",
                status="failed",
                message=f"Control-plane storage is not reachable: {exc}",
                remediation="Start Postgres and restart the API gateway.",
            )
        )

    try:
        await redis.ping()
        checks.append(
            PreflightCheck(
                name="redis",
                status="passed",
                message="Dispatch queue is reachable.",
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(
            PreflightCheck(
                name="redis",
                status="failed",
                message=f"Dispatch queue is not reachable: {exc}",
                remediation="Start Redis and restart the API gateway.",
            )
        )

    endpoint = (
        workload.spec.endpoint if workload.spec.deployment.mode == "external" else None
    )
    if endpoint:
        probe = await _probe_deployment_endpoint(
            DeploymentResponse(
                deployment_id=str(uuid4()),
                workload_name=workload.metadata.name,
                target=normalized_target,
                status="preflight",
                user="preflight",
                created_at=utc_now_iso(),
                endpoint=endpoint,
                metadata={},
            )
        )
        if probe is not None:
            ok, reason = probe
            checks.append(
                PreflightCheck(
                    name="runtime_reachability",
                    status="passed" if ok else "warning",
                    message=reason,
                    remediation="Deploy the runtime or inspect workload logs."
                    if not ok
                    else None,
                    metadata={"endpoint": endpoint},
                )
            )

    return PreflightResponse(
        workload_name=workload.metadata.name,
        target=normalized_target,
        status=_preflight_status(checks),
        checks=checks,
    )


async def _artifacts_for_runs(
    runs: list[StoredRun],
    control_plane: ControlPlaneRepository,
    *,
    content_type: str | None,
    created_from: str | None,
    created_to: str | None,
) -> list[RunArtifact]:
    artifacts: list[RunArtifact] = []
    for run in runs:
        for artifact in await control_plane.list_artifacts(run.run_id):
            if content_type and artifact.content_type != content_type:
                continue
            if created_from and artifact.created_at < created_from:
                continue
            if created_to and artifact.created_at > created_to:
                continue
            artifacts.append(_artifact_response(artifact))
    return sorted(artifacts, key=lambda item: item.created_at, reverse=True)


@router.get("/workloads", response_model=list[WorkloadInfo])
async def list_workloads(control_plane: ControlPlane) -> list[WorkloadInfo]:
    settings = get_settings()
    workloads = await _all_workloads(control_plane, settings)
    return [_workload_info(workload) for workload in workloads.values()]


@router.get("/templates", response_model=list[WorkloadTemplateInfo])
async def list_workload_templates() -> list[WorkloadTemplateInfo]:
    return [_template_info(template_id) for template_id in _TEMPLATE_PARAMETERS]


@router.post(
    "/workloads/from-template",
    response_model=WorkloadInfo,
    status_code=status.HTTP_201_CREATED,
)
async def create_workload_from_template(
    body: WorkloadFromTemplateRequest,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> WorkloadInfo:
    manifest = _template_manifest(body.template_id, body.parameters)
    workload = WorkloadDefinition.model_validate(manifest)
    await control_plane.upsert_workload(
        workload,
        current_user.subject,
        now=utc_now_iso(),
    )
    return _workload_info(workload)


@router.post(
    "/workloads",
    response_model=WorkloadInfo,
    status_code=status.HTTP_201_CREATED,
)
async def register_workload(
    body: WorkloadDefinition,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> WorkloadInfo:
    await control_plane.upsert_workload(body, current_user.subject, now=utc_now_iso())
    return _workload_info(body)


@router.get("/workloads/{name}", response_model=WorkloadInfo)
async def get_workload(
    name: str,
    control_plane: ControlPlane,
) -> WorkloadInfo:
    settings = get_settings()
    workload = await _get_workload(name, control_plane, settings)
    return _workload_info(workload)


@router.post(
    "/workloads/{name}/deployments",
    response_model=DeploymentResponse,
    status_code=status.HTTP_201_CREATED,
)
async def record_workload_deployment(
    name: str,
    body: DeploymentRequest,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> DeploymentResponse:
    settings = get_settings()
    await _get_workload(name, control_plane, settings)
    target = "kubernetes" if body.target == "k8s" else body.target
    deployment = await control_plane.upsert_deployment(
        str(uuid4()),
        name,
        target,
        body.status,
        current_user.subject,
        endpoint=body.endpoint,
        metadata=body.metadata,
        now=utc_now_iso(),
    )
    return _deployment_response(deployment)


@router.get("/workloads/{name}/deployment-plan", response_model=DeploymentPlanResponse)
async def workload_deployment_plan(
    name: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
    target: str = Query(default="local", pattern="^(local|kubernetes|k8s|external)$"),
    env: str = Query(default="dev", min_length=1, max_length=64),
) -> DeploymentPlanResponse:
    del current_user
    settings = get_settings()
    workload = await _get_workload(name, control_plane, settings)
    return _deployment_plan_response(workload, target=target, env=env)


@router.post("/workloads/{name}/preflight", response_model=PreflightResponse)
async def workload_preflight(
    name: str,
    body: PreflightRequest,
    redis: RedisClient,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> PreflightResponse:
    del current_user
    settings = get_settings()
    workload = await _get_workload(name, control_plane, settings)
    return await _run_preflight(
        workload,
        target=body.target,
        env=body.env,
        control_plane=control_plane,
        redis=redis,
    )


@router.get("/deployments", response_model=list[DeploymentResponse])
async def list_deployments(
    control_plane: ControlPlane,
    current_user: CurrentUser,
    workload_name: str | None = None,
) -> list[DeploymentResponse]:
    deployments = await control_plane.list_deployments(
        current_user.subject,
        workload_name=workload_name,
    )
    return [_deployment_response(deployment) for deployment in deployments]


@router.post(
    "/deployment-operations",
    response_model=DeploymentOperationResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_deployment_operation(
    body: DeploymentOperationRequest,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> DeploymentOperationResponse:
    settings = get_settings()
    normalized_target = "kubernetes" if body.target == "k8s" else body.target
    operation_id = str(uuid4())
    now = utc_now_iso()
    metadata = dict(body.metadata)
    events: list[tuple[str, str, dict[str, Any]]] = []
    operation_status = "succeeded"

    try:
        workload = await _get_workload(body.workload_name, control_plane, settings)
        plan = _deployment_plan_response(
            workload,
            target=normalized_target,
            env=body.env,
        )
        metadata["plan"] = plan.model_dump()
        events.append(
            (
                "operation.plan",
                "Deployment plan generated.",
                {"plan": plan.model_dump()},
            )
        )

        if body.action == "sync":
            deployment = await control_plane.upsert_deployment(
                str(uuid4()),
                workload.metadata.name,
                plan.target,
                str(body.metadata.get("status") or "running"),
                current_user.subject,
                endpoint=plan.endpoint,
                metadata={
                    "source": "deployment-operation",
                    "operation_id": operation_id,
                    "service_name": plan.service_name,
                    "environment": body.env,
                    **body.metadata,
                },
                now=now,
            )
            metadata["deployment_id"] = deployment.deployment_id
            events.append(
                (
                    "operation.sync",
                    "Deployment record synchronized.",
                    {"deployment_id": deployment.deployment_id},
                )
            )
        elif body.action in {"apply", "logs", "undeploy"}:
            operation_status = "failed"
            events.append(
                (
                    "operation.blocked",
                    (
                        "This action needs a CLI or deployment controller with "
                        "Docker/Kubernetes credentials."
                    ),
                    {
                        "commands": plan.commands,
                        "reason": "api-gateway-has-no-host-executor",
                    },
                )
            )
    except HTTPException as exc:
        operation_status = "failed"
        metadata["error"] = str(exc.detail)
        events.append(
            (
                "operation.error",
                str(exc.detail),
                {"status_code": exc.status_code},
            )
        )

    operation = await control_plane.create_deployment_operation(
        operation_id,
        body.action,
        body.workload_name,
        normalized_target,
        operation_status,
        current_user.subject,
        metadata=metadata,
        now=now,
        completed_at=utc_now_iso(),
    )
    for event_type, message, data in events:
        await control_plane.append_deployment_operation_event(
            operation_id,
            event_type,
            message,
            data=data,
        )
    return _deployment_operation_response(operation)


@router.get(
    "/deployment-operations/{operation_id}",
    response_model=DeploymentOperationResponse,
)
async def get_deployment_operation(
    operation_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> DeploymentOperationResponse:
    operation = await control_plane.get_deployment_operation(operation_id)
    if operation is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Deployment operation not found",
        )
    if operation.user != current_user.subject:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    return _deployment_operation_response(operation)


@router.get(
    "/deployment-operations/{operation_id}/events",
    response_model=list[DeploymentOperationEvent],
)
async def list_deployment_operation_events(
    operation_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> list[DeploymentOperationEvent]:
    operation = await control_plane.get_deployment_operation(operation_id)
    if operation is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Deployment operation not found",
        )
    if operation.user != current_user.subject:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    events = await control_plane.list_deployment_operation_events(operation_id)
    return [_deployment_operation_event_response(event) for event in events]


@router.get("/workloads/{name}/deployments", response_model=list[DeploymentResponse])
async def list_workload_deployments(
    name: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> list[DeploymentResponse]:
    settings = get_settings()
    await _get_workload(name, control_plane, settings)
    deployments = await control_plane.list_deployments(
        current_user.subject,
        workload_name=name,
    )
    return [_deployment_response(deployment) for deployment in deployments]


@router.get("/workloads/{name}/health", response_model=WorkloadHealthResponse)
async def workload_health(
    name: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> WorkloadHealthResponse:
    settings = get_settings()
    await _get_workload(name, control_plane, settings)
    deployments = [
        _deployment_response(deployment)
        for deployment in await control_plane.list_deployments(
            current_user.subject,
            workload_name=name,
        )
    ]
    health_status, reason = await _deployment_health_status(deployments)
    return WorkloadHealthResponse(
        workload_name=name,
        status=health_status,
        reason=reason,
        deployments=deployments,
    )


@router.post(
    "/workloads/{name}/runs",
    response_model=RunResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@limiter.limit("20/minute")
async def submit_run(
    request: Request,
    name: str,
    body: RunRequest,
    redis: RedisClient,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> RunResponse:
    del request
    settings = get_settings()
    workload = await _get_workload(name, control_plane, settings)
    response = await _create_run(
        redis,
        control_plane,
        workload,
        body.payload,
        current_user.subject,
    )
    logger.info("run_submitted run_id=%s workload=%s", response.run_id, name)
    return response


@router.get("/runs", response_model=list[RunStatusResponse])
async def list_runs(
    control_plane: ControlPlane,
    current_user: CurrentUser,
    workload_name: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[RunStatusResponse]:
    runs = await control_plane.list_runs(
        current_user.subject,
        workload_name=workload_name,
        limit=limit,
        offset=offset,
    )
    return [_run_response(run) for run in runs]


@router.get("/runs/{run_id}", response_model=RunStatusResponse)
async def get_run(
    run_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> RunStatusResponse:
    run = await _authorize_run(run_id, control_plane, current_user)
    return _run_response(run)


@router.post("/runs/{run_id}/cancel", response_model=RunStatusResponse)
async def cancel_run(
    run_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> RunStatusResponse:
    run = await _authorize_run(run_id, control_plane, current_user)
    if run.status not in TERMINAL_RUN_STATUSES:
        previous_status = run.status
        now = utc_now_iso()
        try:
            ensure_run_transition(previous_status, "cancel_requested")
        except RunStateTransitionError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=str(exc)
            ) from exc
        updated_run = await control_plane.update_run(
            run_id,
            status="cancel_requested",
            updated_at=now,
        )
        if updated_run is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Run not found"
            )
        run = updated_run
        await control_plane.append_run_event(
            run_id,
            "run.cancel_requested",
            "Cancellation requested",
            data={"previous_status": previous_status},
        )
    return _run_response(run)


@router.get("/runs/{run_id}/events", response_model=list[RunEvent])
async def list_run_events(
    run_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> list[RunEvent]:
    await _authorize_run(run_id, control_plane, current_user)
    events = await control_plane.list_run_events(run_id)
    return [_event_response(event) for event in events]


@router.get("/runs/{run_id}/events/stream")
async def stream_run_events(
    run_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> StreamingResponse:
    await _authorize_run(run_id, control_plane, current_user)

    async def _events() -> AsyncGenerator[str]:
        seen: set[str] = set()
        while True:
            entries = await control_plane.list_run_events(run_id)
            for event in entries:
                if event.id in seen:
                    continue
                seen.add(event.id)
                response = _event_response(event)
                yield (
                    f"id: {response.id}\n"
                    f"event: {response.type}\n"
                    f"data: {response.model_dump_json()}\n\n"
                )
            await asyncio.sleep(1.0)

    return StreamingResponse(_events(), media_type="text/event-stream")


@router.get("/runs/{run_id}/artifacts", response_model=list[RunArtifact])
async def list_run_artifacts(
    run_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> list[RunArtifact]:
    await _authorize_run(run_id, control_plane, current_user)
    artifacts = await control_plane.list_artifacts(run_id)
    return [_artifact_response(artifact) for artifact in artifacts]


@router.get("/artifacts", response_model=list[RunArtifact])
async def list_artifacts(
    control_plane: ControlPlane,
    current_user: CurrentUser,
    workload_name: str | None = None,
    session_id: str | None = None,
    run_id: str | None = None,
    content_type: str | None = None,
    created_from: str | None = None,
    created_to: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[RunArtifact]:
    if run_id:
        run = await _authorize_run(run_id, control_plane, current_user)
        runs = [run]
    else:
        candidate_runs = await control_plane.list_runs(
            current_user.subject,
            workload_name=workload_name,
            limit=200,
            offset=0,
        )
        runs = [
            run
            for run in candidate_runs
            if session_id is None or run.session_id == session_id
        ]
    artifacts = await _artifacts_for_runs(
        runs,
        control_plane,
        content_type=content_type,
        created_from=created_from,
        created_to=created_to,
    )
    return artifacts[offset : offset + limit]


@router.post(
    "/agents/{name}/sessions",
    response_model=AgentSessionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_agent_session(
    name: str,
    body: AgentSessionRequest,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> AgentSessionResponse:
    settings = get_settings()
    workload = await _get_workload(name, control_plane, settings)
    if workload.spec.type != "agent-service":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Workload {name!r} is not an agent-service",
        )

    created_at = utc_now_iso()
    session = await control_plane.create_agent_session(
        str(uuid4()),
        name,
        current_user.subject,
        metadata=body.metadata,
        created_at=created_at,
    )
    return AgentSessionResponse(
        session_id=session.session_id,
        agent_name=session.agent_name,
        status=session.status,
        created_at=session.created_at,
    )


@router.post(
    "/agents/{name}/sessions/{session_id}/messages",
    response_model=AgentMessageResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def post_agent_message(
    name: str,
    session_id: str,
    body: AgentMessageRequest,
    redis: RedisClient,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> AgentMessageResponse:
    settings = get_settings()
    workload = await _get_workload(name, control_plane, settings)
    if workload.spec.type != "agent-service":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Workload {name!r} is not an agent-service",
        )
    await _authorize_agent_session(name, session_id, control_plane, current_user)

    created_at = utc_now_iso()
    message = await control_plane.append_agent_message(
        session_id,
        "user",
        body.message,
        context=body.context,
        created_at=created_at,
    )
    run = await _create_run(
        redis,
        control_plane,
        workload,
        {
            "session_id": session_id,
            "message": body.message,
            "context": body.context,
        },
        current_user.subject,
        session_id=session_id,
    )
    return AgentMessageResponse(
        message_id=message.message_id,
        run_id=run.run_id,
        session_id=session_id,
        status=run.status,
        created_at=message.created_at,
    )


@router.post(
    "/channels/{channel}/agents/{name}/messages",
    response_model=AgentMessageResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def post_channel_agent_message(
    channel: str,
    name: str,
    body: ChannelMessageRequest,
    redis: RedisClient,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> AgentMessageResponse:
    settings = get_settings()
    workload = await _get_workload(name, control_plane, settings)
    if workload.spec.type != "agent-service":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Workload {name!r} is not an agent-service",
        )
    channel = _validate_agent_channel(workload, channel)

    session_id = body.session_id
    if session_id is None:
        session = await control_plane.create_agent_session(
            str(uuid4()),
            name,
            current_user.subject,
            metadata={
                "channel": channel,
                "external_user_id": body.external_user_id,
                **body.metadata,
            },
            created_at=utc_now_iso(),
        )
        session_id = session.session_id
    else:
        await _authorize_agent_session(name, session_id, control_plane, current_user)

    created_at = utc_now_iso()
    message = await control_plane.append_agent_message(
        session_id,
        "user",
        body.message,
        context={
            "channel": channel,
            "external_user_id": body.external_user_id,
            **body.metadata,
        },
        created_at=created_at,
    )
    run = await _create_run(
        redis,
        control_plane,
        workload,
        {
            "session_id": session_id,
            "message": body.message,
            "context": {
                "channel": channel,
                "external_user_id": body.external_user_id,
                **body.metadata,
            },
        },
        current_user.subject,
        session_id=session_id,
    )
    await control_plane.record_channel_message(
        channel,
        name,
        body.external_user_id,
        session_id,
        "inbound",
        body.message,
        current_user.subject,
        run_id=run.run_id,
        metadata=body.metadata,
        created_at=created_at,
    )
    return AgentMessageResponse(
        message_id=message.message_id,
        run_id=run.run_id,
        session_id=session_id,
        status=run.status,
        created_at=message.created_at,
    )


@router.get("/agents/{name}/sessions")
async def list_agent_sessions(
    name: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> list[dict[str, Any]]:
    settings = get_settings()
    workload = await _get_workload(name, control_plane, settings)
    if workload.spec.type != "agent-service":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Workload {name!r} is not an agent-service",
        )
    sessions = await control_plane.list_agent_sessions(name, current_user.subject)
    return [_session_payload(session) for session in sessions]


@router.get(
    "/agents/{name}/sessions/{session_id}/messages",
    response_model=list[AgentMessageHistoryItem],
)
async def list_agent_session_messages(
    name: str,
    session_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> list[AgentMessageHistoryItem]:
    await _authorize_agent_session(name, session_id, control_plane, current_user)

    messages = await control_plane.list_agent_messages(session_id)
    runs = [
        run
        for run in await control_plane.list_runs(
            current_user.subject,
            workload_name=name,
            limit=200,
        )
        if run.session_id == session_id
    ]
    runs_by_id = {run.run_id: run for run in runs}
    user_runs = {
        str(run.payload.get("message")): run
        for run in runs
        if isinstance(run.payload, dict) and run.payload.get("message")
    }

    enriched: list[AgentMessageHistoryItem] = []
    for message in messages:
        run = None
        context_run_id = message.context.get("run_id")
        if isinstance(context_run_id, str):
            run = runs_by_id.get(context_run_id)
        if run is None and message.role == "user":
            run = user_runs.get(message.message)

        latest_event = None
        artifact_count = 0
        if run is not None:
            events = await control_plane.list_run_events(run.run_id)
            latest_event = events[-1] if events else None
            artifact_count = len(await control_plane.list_artifacts(run.run_id))
        enriched.append(
            AgentMessageHistoryItem(
                **_message_payload(
                    message,
                    run=run,
                    latest_event=latest_event,
                    artifact_count=artifact_count,
                )
            )
        )
    return enriched


@router.get(
    "/agents/{name}/sessions/{session_id}/health",
    response_model=AgentSessionHealthResponse,
)
async def agent_session_health(
    name: str,
    session_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> AgentSessionHealthResponse:
    session = await _authorize_agent_session(
        name,
        session_id,
        control_plane,
        current_user,
    )
    messages = await control_plane.list_agent_messages(session_id)
    runs = await control_plane.list_runs(
        current_user.subject,
        workload_name=name,
        limit=50,
    )
    latest_run = next((run for run in runs if run.session_id == session_id), None)
    status_value = session.status
    if latest_run is not None and latest_run.status in {"failed", "lost"}:
        status_value = "degraded"
    return AgentSessionHealthResponse(
        session_id=session.session_id,
        agent_name=session.agent_name,
        status=status_value,
        latest_run_status=latest_run.status if latest_run else None,
        message_count=len(messages),
    )
