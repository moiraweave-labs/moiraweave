"""Workload control-plane API routes."""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import mimetypes
import os
import re
from collections.abc import AsyncGenerator, Mapping  # noqa: TC003
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse
from uuid import uuid4

import httpx
from fastapi import APIRouter, Header, HTTPException, Query, Request, status
from moiraweave_shared.control_plane import (
    ControlPlaneRepository,
    StoredArtifact,
    StoredAuditEvent,
    StoredRun,
    StoredRunEvent,
    StoredWorkload,
    utc_now_iso,
)
from moiraweave_shared.schemas import RunMessage
from moiraweave_shared.streams import CONSUMER_GROUP, DEAD_LETTER_STREAM, RUN_STREAM
from moiraweave_shared.workloads import (
    TERMINAL_RUN_STATUSES,
    RunStateTransitionError,
    WorkloadDefinition,
    ensure_run_transition,
    load_workloads,
)
from pydantic import ValidationError
from redis.exceptions import ResponseError
from starlette.responses import FileResponse, StreamingResponse

if TYPE_CHECKING:
    from redis.typing import EncodableT

from app.config import Settings, get_settings
from app.dependencies.auth import AdminUser, CurrentUser, OperatorUser  # noqa: TC001
from app.dependencies.control_plane import ControlPlane  # noqa: TC001
from app.dependencies.redis import RedisClient  # noqa: TC001
from app.middleware.rate_limit import limiter
from app.models.auth import TokenData
from app.models.workloads import (
    AgentMessageHistoryItem,
    AgentMessageRequest,
    AgentMessageResponse,
    AgentSessionHealthResponse,
    AgentSessionRequest,
    AgentSessionResponse,
    ArtifactPreviewResponse,
    AuditEventResponse,
    ChannelMessageRequest,
    DeadLetterEntry,
    DeadLetterReplayResponse,
    DeploymentOperationClaimRequest,
    DeploymentOperationCompleteRequest,
    DeploymentOperationEvent,
    DeploymentOperationEventRequest,
    DeploymentOperationHeartbeatRequest,
    DeploymentOperationRequest,
    DeploymentOperationResponse,
    DeploymentPlanResponse,
    DeploymentRequest,
    DeploymentResponse,
    EnvironmentInfo,
    OperationsAlert,
    PreflightAction,
    PreflightCheck,
    PreflightRequest,
    PreflightResponse,
    RunArtifact,
    RunEvent,
    RunRequest,
    RunResponse,
    RunStatusResponse,
    SecretInventoryItem,
    SecretInventoryResponse,
    WorkloadFromTemplateRequest,
    WorkloadHealthResponse,
    WorkloadInfo,
    WorkloadTemplateInfo,
    WorkloadTemplateParameter,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1", tags=["workloads"])

_RATE_LIMIT_CONTROLLER_MUTATION = "60/minute"
_RATE_LIMIT_DEAD_LETTER_MUTATION = "20/minute"
_RATE_LIMIT_DEPLOYMENT_OPERATION = "30/minute"
_RATE_LIMIT_WEBHOOK_INGRESS = "60/minute"
_WORKLOAD_TEAM_ANNOTATION = "moiraweave.io/team-id"

_PREVIEWABLE_CONTENT_TYPES = {
    "application/json",
    "application/ld+json",
    "application/x-ndjson",
    "application/yaml",
    "application/x-yaml",
    "application/xml",
    "application/javascript",
}


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
        WorkloadTemplateParameter(
            name="external_channels",
            label="Runtime-owned channels",
            default="",
            required=False,
            description="Comma-separated channels handled by the runtime, for example telegram.",
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
        WorkloadTemplateParameter(
            name="external_channels",
            label="Runtime-owned channels",
            default="",
            required=False,
            description="Comma-separated channels handled by the runtime, for example telegram.",
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
        WorkloadTemplateParameter(
            name="external_channels",
            label="Runtime-owned channels",
            default="",
            required=False,
            description="Comma-separated channels handled by the runtime, for example telegram.",
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
        WorkloadTemplateParameter(
            name="external_channels",
            label="Runtime-owned channels",
            default="",
            required=False,
            description="Comma-separated channels handled by the runtime, for example telegram.",
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


def _template_channel_list(
    params: dict[str, Any],
    template_id: str,
    name: str,
) -> list[str]:
    value = _template_param(params, template_id, name)
    items = value if isinstance(value, list) else str(value or "").split(",")
    channels: list[str] = []
    seen: set[str] = set()
    for item in items:
        channel = str(item).strip().lower()
        if channel and channel not in seen:
            seen.add(channel)
            channels.append(channel)
    return channels


def _http_probe(port: str, path: str = "/health") -> dict[str, Any]:
    return {
        "httpGet": {"path": path, "port": port},
        "initialDelaySeconds": 5,
        "periodSeconds": 10,
        "timeoutSeconds": 5,
        "failureThreshold": 6,
    }


def _tcp_probe(port: str) -> dict[str, Any]:
    return {
        "tcpSocket": {"port": port},
        "initialDelaySeconds": 5,
        "periodSeconds": 10,
        "timeoutSeconds": 5,
        "failureThreshold": 6,
    }


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
                    "toolOwnership": "runtime",
                    "messagePath": "/message",
                    "statusPath": "/health",
                    "artifactsPath": "/artifacts",
                    "exposedChannels": ["ui", "api", "webhook"],
                    "capabilities": ["demo", "chat"],
                    "runtimeRequirements": {
                        "filesystem": {"persistentWorkspace": False},
                        "network": {"egress": "restricted"},
                        "webSearch": {"enabled": False},
                        "browser": {"mode": "none"},
                        "terminal": {"mode": "none"},
                        "messaging": {"enabled": False},
                    },
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
                "livenessProbe": {
                    **_http_probe("http"),
                    "initialDelaySeconds": 15,
                    "periodSeconds": 30,
                    "failureThreshold": 3,
                },
                "readinessProbe": _http_probe("http"),
                "env": {
                    "API_SERVER_ENABLED": "true",
                    "API_SERVER_HOST": "0.0.0.0",
                    "API_SERVER_PORT": str(port),
                },
                "secrets": ["OPENAI_API_KEY"],
                "agent": {
                    "adapter": "hermes",
                    "toolOwnership": "runtime",
                    "requiredSecrets": ["OPENAI_API_KEY"],
                    "workspaceMount": "/workspace",
                    "authTokenEnv": "HERMES_API_SERVER_KEY",
                    "model": _template_param(params, template_id, "model"),
                    "exposedChannels": ["ui", "api"],
                    "externalOwnedChannels": _template_channel_list(
                        params,
                        template_id,
                        "external_channels",
                    ),
                    "capabilities": ["chat", "tools", "long-running"],
                    "runtimeRequirements": {
                        "filesystem": {
                            "persistentWorkspace": True,
                            "workspaceMount": "/workspace",
                        },
                        "network": {"egress": "enabled"},
                        "webSearch": {"enabled": True},
                        "browser": {"mode": "runtime-managed"},
                        "terminal": {
                            "mode": "runtime-managed",
                            "approval": "runtime",
                        },
                        "mcp": {"enabled": True},
                        "messaging": {"enabled": True},
                    },
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
                "livenessProbe": {
                    **_tcp_probe("gateway"),
                    "initialDelaySeconds": 15,
                    "periodSeconds": 30,
                    "failureThreshold": 3,
                },
                "readinessProbe": _tcp_probe("gateway"),
                "agent": {
                    "adapter": "openclaw",
                    "toolOwnership": "runtime",
                    "agentId": _template_param(params, template_id, "agent_id"),
                    "authTokenEnv": "OPENCLAW_GATEWAY_TOKEN",
                    "workspaceMount": "/workspace",
                    "exposedChannels": ["ui", "api"],
                    "externalOwnedChannels": _template_channel_list(
                        params,
                        template_id,
                        "external_channels",
                    ),
                    "capabilities": ["browser", "tools", "long-running"],
                    "runtimeRequirements": {
                        "filesystem": {
                            "persistentWorkspace": True,
                            "workspaceMount": "/workspace",
                        },
                        "network": {"egress": "enabled"},
                        "webSearch": {"enabled": True},
                        "browser": {"mode": "runtime-managed"},
                        "terminal": {
                            "mode": "runtime-managed",
                            "approval": "runtime",
                        },
                        "mcp": {"enabled": True},
                        "messaging": {"enabled": True},
                    },
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
                    "toolOwnership": "runtime",
                    "messagePath": _template_param(params, template_id, "message_path"),
                    "statusPath": "/health",
                    "cancelPath": "/cancel",
                    "artifactsPath": "/artifacts",
                    "exposedChannels": ["ui", "api"],
                    "externalOwnedChannels": _template_channel_list(
                        params,
                        template_id,
                        "external_channels",
                    ),
                    "runtimeRequirements": {
                        "filesystem": {"persistentWorkspace": False},
                        "network": {"egress": "restricted"},
                        "webSearch": {"enabled": False},
                        "browser": {"mode": "none"},
                        "terminal": {"mode": "runtime-managed"},
                    },
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
                    "toolOwnership": "runtime",
                    "exposedChannels": ["ui", "api"],
                    "externalOwnedChannels": _template_channel_list(
                        params,
                        template_id,
                        "external_channels",
                    ),
                    "runtimeRequirements": {
                        "filesystem": {"persistentWorkspace": False},
                        "network": {"egress": "restricted"},
                        "webSearch": {"enabled": False},
                        "browser": {"mode": "none"},
                        "terminal": {"mode": "runtime-managed"},
                    },
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


def _workload_info(
    workload: WorkloadDefinition,
    *,
    owner_subject: str | None = None,
    team_id: str | None = None,
) -> WorkloadInfo:
    return WorkloadInfo(
        name=workload.metadata.name,
        type=workload.spec.type,
        execution_mode=workload.spec.execution.mode,
        image=workload.spec.image,
        owner_subject=owner_subject,
        team_id=team_id,
        manifest=workload.to_manifest(),
    )


def _run_response(run: StoredRun) -> RunStatusResponse:
    return RunStatusResponse(**run.model_dump())


def _event_response(event: StoredRunEvent) -> RunEvent:
    return RunEvent(**event.model_dump())


def _redis_stream_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _redis_stream_fields(fields: Mapping[Any, Any] | None) -> dict[str, Any]:
    if fields is None:
        return {}
    return {
        str(_redis_stream_value(key)): _redis_stream_value(value)
        for key, value in fields.items()
    }


def _dead_letter_entry(message_id: str, fields: dict[str, Any]) -> DeadLetterEntry:
    payload_raw = fields.get("payload", "{}")
    payload: dict[str, Any]
    if isinstance(payload_raw, str):
        try:
            parsed = json.loads(payload_raw)
            payload = parsed if isinstance(parsed, dict) else {"raw": payload_raw}
        except json.JSONDecodeError:
            payload = {"raw": payload_raw}
    elif isinstance(payload_raw, dict):
        payload = payload_raw
    else:
        payload = {"raw": str(payload_raw)}
    return DeadLetterEntry(
        message_id=message_id,
        source_stream=str(fields.get("source_stream", "")),
        source_id=str(fields.get("source_id", "")),
        reason=str(fields.get("reason", "unknown")),
        payload=payload,
        created_at=(
            str(fields["created_at"]) if fields.get("created_at") is not None else None
        ),
    )


def _audit_event_response(event: StoredAuditEvent) -> AuditEventResponse:
    return AuditEventResponse(**event.model_dump())


async def _audit(
    control_plane: ControlPlaneRepository,
    current_user: CurrentUser,
    action: str,
    resource_type: str,
    resource_id: str,
    *,
    metadata: dict[str, Any] | None = None,
) -> None:
    await control_plane.record_audit_event(
        current_user.subject,
        action,
        resource_type,
        resource_id,
        metadata=metadata,
        timestamp=utc_now_iso(),
    )


def _artifact_response(
    artifact: StoredArtifact,
    *,
    run: StoredRun | None = None,
) -> RunArtifact:
    return RunArtifact(
        **artifact.model_dump(),
        workload_name=run.workload_name if run else None,
        session_id=run.session_id if run else None,
    )


def _artifact_media_type(artifact: StoredArtifact, path: Path) -> str:
    return (
        artifact.content_type
        or mimetypes.guess_type(path.name)[0]
        or "application/octet-stream"
    )


def _is_previewable_content_type(content_type: str) -> bool:
    normalized = content_type.split(";", 1)[0].strip().lower()
    return (
        normalized.startswith("text/")
        or normalized in _PREVIEWABLE_CONTENT_TYPES
        or normalized.endswith("+json")
        or normalized.endswith("+xml")
    )


def _artifact_filesystem_path(
    artifact: StoredArtifact,
    settings: Settings,
) -> Path:
    root = Path(settings.artifacts_dir).expanduser().resolve()
    parsed = urlparse(artifact.uri)

    if parsed.scheme == "file":
        candidate = Path(parsed.path).expanduser().resolve()
    elif parsed.scheme in {"", "local", "artifact", "artifacts"}:
        relative = (
            artifact.uri
            if not parsed.scheme
            else f"{parsed.netloc}{parsed.path}".lstrip("/")
        )
        candidate = (root / relative).expanduser().resolve()
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Artifact URI scheme {parsed.scheme!r} is not served by the API",
        )

    if candidate != root and root not in candidate.parents:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Artifact path is outside the configured artifact storage root",
        )
    if not candidate.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Artifact file was not found in configured artifact storage",
        )
    return candidate


async def _authorize_artifact(
    run_id: str,
    artifact_id: str,
    control_plane: ControlPlaneRepository,
    current_user: CurrentUser,
) -> StoredArtifact:
    await _authorize_run(run_id, control_plane, current_user)
    for artifact in await control_plane.list_artifacts(run_id):
        if artifact.id == artifact_id:
            return artifact
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Artifact not found",
    )


def _deployment_response(deployment: Any) -> DeploymentResponse:
    return DeploymentResponse(**deployment.model_dump())


def _deployment_operation_response(operation: Any) -> DeploymentOperationResponse:
    return DeploymentOperationResponse(**operation.model_dump())


def _deployment_operation_event_response(event: Any) -> DeploymentOperationEvent:
    return DeploymentOperationEvent(**event.model_dump())


async def _can_access_deployment_operation(
    operation: Any,
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
) -> bool:
    return await _subject_visible(operation.user, control_plane, current_user)


def _parse_utc_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _operation_lease_expiry(now: str, lease_seconds: int) -> str:
    return (_parse_utc_timestamp(now) + timedelta(seconds=lease_seconds)).isoformat()


def _pending_stream_count(info: Any) -> int:
    if isinstance(info, dict):
        return int(info.get("pending") or 0)
    if isinstance(info, (list, tuple)) and info:
        return int(info[0] or 0)
    return 0


def _deployment_operation_lease_expired(operation: Any, now: str) -> bool:
    if not operation.lease_expires_at:
        return False
    return _parse_utc_timestamp(operation.lease_expires_at) <= _parse_utc_timestamp(now)


async def _sync_deployment_record_for_completed_operation(
    operation: Any,
    control_plane: ControlPlaneRepository,
    current_user: Any,
    *,
    now: str,
) -> None:
    if operation.status != "succeeded" or operation.action not in {"apply", "undeploy"}:
        return

    settings = get_settings()
    workload = await _get_workload(operation.workload_name, control_plane, settings)
    plan = _deployment_plan_response(
        workload, target=operation.target, env=operation.env
    )
    deployment_status = "deployed" if operation.action == "apply" else "stopped"
    await control_plane.upsert_deployment(
        str(uuid4()),
        workload.metadata.name,
        plan.target,
        deployment_status,
        operation.user,
        env=operation.env,
        endpoint=plan.endpoint,
        metadata={
            "source": "deployment-controller",
            "operation_id": operation.operation_id,
            "controller_subject": current_user.subject,
            "service_name": plan.service_name,
            "environment": operation.env,
        },
        now=now,
    )


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


def _deployment_log_commands(
    workload: WorkloadDefinition,
    plan: DeploymentPlanResponse,
) -> list[str]:
    service_name = plan.service_name or workload.metadata.name
    if plan.target == "local":
        return [f"docker compose logs --tail 200 {service_name}"]
    if plan.target == "kubernetes":
        namespace = workload.spec.deployment.namespace or "moiraweave"
        return [
            f"kubectl logs -n {namespace} deployment/{service_name} --tail=200",
            f"kubectl describe deployment -n {namespace} {service_name}",
        ]
    return [
        f"Inspect logs in the external runtime that serves {plan.endpoint or service_name}."
    ]


def _deployment_action_commands(
    action: str,
    workload: WorkloadDefinition,
    plan: DeploymentPlanResponse,
) -> list[str]:
    if action == "apply":
        return plan.commands
    if action == "undeploy":
        if plan.target == "local":
            return [
                "docker compose -f docker-compose.yml -f "
                ".moiraweave/deploy/docker-compose.workloads.yml down"
            ]
        if plan.target == "kubernetes":
            namespace = workload.spec.deployment.namespace or "moiraweave"
            return [f"helm uninstall moiraweave --namespace {namespace}"]
        return [
            "Remove or stop the external runtime outside MoiraWeave, then sync "
            "the deployment record."
        ]
    return []


def _deployment_operation_next_actions(
    action: str,
    plan: DeploymentPlanResponse,
) -> list[str]:
    if action == "apply":
        if plan.target == "local":
            return [
                "Run the listed commands from the workspace with Docker access.",
                "After containers start, run Sync or `moira deploy local --register`.",
            ]
        if plan.target == "kubernetes":
            return [
                "Run the listed commands from CLI, CI, or a deployment controller with kubeconfig.",
                "After resources apply, run Sync or `moira deploy k8s --register`.",
            ]
        return [
            "Deploy the external runtime in its owner system.",
            "Register or sync the external deployment record in MoiraWeave.",
        ]
    if action == "undeploy":
        return [
            "Run the listed commands from an environment with deployment credentials.",
            "Sync the deployment record as stopped, removed, or external-owned.",
        ]
    return []


async def _all_workloads(
    control_plane: ControlPlaneRepository, settings: Settings
) -> dict[str, WorkloadDefinition]:
    records = await _all_workload_records(control_plane, settings)
    return {name: record.workload for name, record in records.items()}


async def _all_workload_records(
    control_plane: ControlPlaneRepository, settings: Settings
) -> dict[str, StoredWorkload]:
    """Return local shared workloads plus API-registered owned workloads."""

    records = {
        workload.metadata.name: StoredWorkload(workload=workload)
        for workload in load_workloads(settings.workloads_dir)
    }
    records.update(
        {
            record.workload.metadata.name: record
            for record in await control_plane.list_workload_records()
        }
    )
    return records


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


def _workload_team_id(workload: WorkloadDefinition) -> str | None:
    value = workload.metadata.annotations.get(_WORKLOAD_TEAM_ANNOTATION)
    if not isinstance(value, str):
        return None
    return value.strip() or None


async def _get_workload_record(
    name: str,
    control_plane: ControlPlaneRepository,
    settings: Settings,
) -> StoredWorkload:
    records = await _all_workload_records(control_plane, settings)
    record = records.get(name)
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workload {name!r} not found",
        )
    return record


async def _visible_team_ids(
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
) -> set[str]:
    team_ids: set[str] = set()
    if current_user.team_id:
        team_ids.add(current_user.team_id)
    for membership in await control_plane.list_team_members(
        subject=current_user.subject
    ):
        team_ids.add(membership.team_id)
    return team_ids


async def _workload_visible(
    record: StoredWorkload,
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
) -> bool:
    if current_user.role == "admin" or record.user is None:
        return True
    team_id = _workload_team_id(record.workload)
    if team_id:
        return team_id in await _visible_team_ids(control_plane, current_user)
    # Platform workloads are shared unless an explicit team annotation scopes them.
    return True


async def _list_visible_workloads(
    control_plane: ControlPlaneRepository,
    settings: Settings,
    current_user: TokenData,
) -> list[StoredWorkload]:
    records = await _all_workload_records(control_plane, settings)
    return [
        record
        for _, record in sorted(records.items())
        if await _workload_visible(record, control_plane, current_user)
    ]


async def _get_visible_workload(
    name: str,
    control_plane: ControlPlaneRepository,
    settings: Settings,
    current_user: TokenData,
) -> WorkloadDefinition:
    record = await _get_workload_record(name, control_plane, settings)
    if not await _workload_visible(record, control_plane, current_user):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workload {name!r} not found",
        )
    return record.workload


async def _store_owned_workload(
    workload: WorkloadDefinition,
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
) -> tuple[WorkloadDefinition, str | None]:
    """Persist a workload without accidentally dropping its team ownership."""

    existing = await control_plane.get_workload_record(workload.metadata.name)
    existing_team_id = (
        _workload_team_id(existing.workload) if existing is not None else None
    )
    team_id = _workload_team_id(workload)
    if team_id is None and existing_team_id is not None:
        annotations = dict(workload.metadata.annotations)
        annotations[_WORKLOAD_TEAM_ANNOTATION] = existing_team_id
        workload = workload.model_copy(
            update={
                "metadata": workload.metadata.model_copy(
                    update={"annotations": annotations}
                )
            }
        )
        team_id = existing_team_id

    if team_id and not any(
        team.team_id == team_id for team in await control_plane.list_teams()
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Team {team_id!r} does not exist",
        )

    owner_subject = (
        existing.user if existing and existing.user else current_user.subject
    )
    await control_plane.upsert_workload(
        workload,
        owner_subject,
        now=utc_now_iso(),
    )
    return workload, owner_subject


async def _visible_subjects(
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
) -> set[str] | None:
    """Return subjects visible to the user, or None for unrestricted admin scope."""

    if current_user.role == "admin":
        return None
    team_ids = await _visible_team_ids(control_plane, current_user)
    subjects = {current_user.subject}
    for team_id in team_ids:
        for member in await control_plane.list_team_members(team_id=team_id):
            subjects.add(member.subject)
    return subjects


async def _subject_visible(
    subject: str,
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
) -> bool:
    subjects = await _visible_subjects(control_plane, current_user)
    return subjects is None or subject in subjects


async def _authorize_channel_message_team_scope(
    body: ChannelMessageRequest,
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
) -> None:
    team_id = body.team_id
    if team_id is None:
        return
    if not any(team.team_id == team_id for team in await control_plane.list_teams()):
        raise HTTPException(status_code=404, detail="Team not found")
    if current_user.role == "admin":
        return
    if team_id not in await _visible_team_ids(control_plane, current_user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Current credential cannot use the requested team scope",
        )


async def _list_visible_runs(
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
    *,
    workload_name: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[StoredRun]:
    subjects = await _visible_subjects(control_plane, current_user)
    if subjects is None:
        return await control_plane.list_runs(
            None,
            workload_name=workload_name,
            limit=limit,
            offset=offset,
        )
    runs: list[StoredRun] = []
    for subject in sorted(subjects):
        runs.extend(
            await control_plane.list_runs(
                subject,
                workload_name=workload_name,
                limit=limit,
                offset=0,
            )
        )
    runs.sort(key=lambda run: run.created_at, reverse=True)
    return runs[offset : offset + limit]


async def _list_visible_deployments(
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
    *,
    workload_name: str | None = None,
    env: str | None = None,
) -> list[Any]:
    subjects = await _visible_subjects(control_plane, current_user)
    if subjects is None:
        return await control_plane.list_deployments(
            None,
            workload_name=workload_name,
            env=env,
        )
    deployments: list[Any] = []
    for subject in sorted(subjects):
        deployments.extend(
            await control_plane.list_deployments(
                subject,
                workload_name=workload_name,
                env=env,
            )
        )
    return sorted(
        deployments,
        key=lambda deployment: deployment.updated_at or deployment.created_at,
        reverse=True,
    )


async def _visible_workload_names_for_env(
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
    env: str | None,
) -> set[str] | None:
    if env is None:
        return None
    deployments = await _list_visible_deployments(
        control_plane,
        current_user,
        env=env,
    )
    return {deployment.workload_name for deployment in deployments}


async def _list_visible_runs_for_filters(
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
    *,
    workload_name: str | None = None,
    env: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[StoredRun]:
    workload_names = await _visible_workload_names_for_env(
        control_plane,
        current_user,
        env,
    )
    if workload_names is None:
        return await _list_visible_runs(
            control_plane,
            current_user,
            workload_name=workload_name,
            limit=limit,
            offset=offset,
        )
    if workload_name is not None:
        if workload_name not in workload_names:
            return []
        workload_names = {workload_name}
    runs: list[StoredRun] = []
    per_workload_limit = min(max(limit + offset, 1), 500)
    for name in sorted(workload_names):
        runs.extend(
            await _list_visible_runs(
                control_plane,
                current_user,
                workload_name=name,
                limit=per_workload_limit,
                offset=0,
            )
        )
    runs.sort(key=lambda run: run.created_at, reverse=True)
    return runs[offset : offset + limit]


async def _list_visible_operations(
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
    *,
    workload_name: str | None = None,
    target: str | None = None,
    env: str | None = None,
    status_filter: str | None = None,
    action: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[Any]:
    subjects = await _visible_subjects(control_plane, current_user)
    if subjects is None:
        return await control_plane.list_deployment_operations(
            None,
            workload_name=workload_name,
            target=target,
            env=env,
            status=status_filter,
            action=action,
            limit=limit,
            offset=offset,
        )
    operations: list[Any] = []
    for subject in sorted(subjects):
        operations.extend(
            await control_plane.list_deployment_operations(
                subject,
                workload_name=workload_name,
                target=target,
                env=env,
                status=status_filter,
                action=action,
                limit=limit,
                offset=0,
            )
        )
    operations.sort(key=lambda operation: operation.created_at, reverse=True)
    return operations[offset : offset + limit]


async def _list_visible_audit_events(
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
    *,
    action: str | None = None,
    resource_type: str | None = None,
    resource_id: str | None = None,
    env: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[StoredAuditEvent]:
    subjects = await _visible_subjects(control_plane, current_user)
    if subjects is None:
        return await control_plane.list_audit_events(
            None,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            env=env,
            limit=limit,
            offset=offset,
        )
    events: list[StoredAuditEvent] = []
    for subject in sorted(subjects):
        events.extend(
            await control_plane.list_audit_events(
                subject,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                env=env,
                limit=limit,
                offset=0,
            )
        )
    events.sort(key=lambda event: event.timestamp, reverse=True)
    return events[offset : offset + limit]


async def _authorize_run(
    run_id: str,
    control_plane: ControlPlaneRepository,
    current_user: TokenData,
) -> StoredRun:
    run = await control_plane.get_run(run_id)
    if run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Run not found"
        )
    if not await _subject_visible(run.user, control_plane, current_user):
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
    current_user: TokenData,
) -> Any:
    session = await control_plane.get_agent_session(session_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Agent session not found"
        )
    if session.agent_name != name or not await _subject_visible(
        session.user,
        control_plane,
        current_user,
    ):
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


def _verify_webhook_signature(
    settings: Settings,
    signature: str | None,
    body: bytes,
) -> None:
    secret = settings.webhook_signing_secret
    if secret is None or not secret.get_secret_value():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook signing secret is not configured",
        )
    if not signature:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing webhook signature",
        )
    expected = hmac.new(
        secret.get_secret_value().encode(),
        body,
        sha256,
    ).hexdigest()
    expected_header = f"sha256={expected}"
    if not hmac.compare_digest(signature, expected_header):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook signature",
        )


async def _webhook_token_data(
    channel: str,
    body: ChannelMessageRequest,
    control_plane: ControlPlaneRepository,
) -> TokenData:
    normalized_channel = channel.strip().lower()
    team_id = body.team_id
    if team_id and not any(
        team.team_id == team_id for team in await control_plane.list_teams()
    ):
        raise HTTPException(status_code=404, detail="Team not found")
    return TokenData(
        subject=f"webhook:{normalized_channel}",
        role="operator",
        team_id=team_id,
    )


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
    if statuses & {"failed", "lost", "unhealthy", "unreachable"}:
        return "degraded", "At least one deployment is reporting a failed state"
    if statuses & {"applied", "running", "deployed", "reachable", "healthy"}:
        return "healthy", "A deployment record is active"
    return "pending", "Deployment exists but is not active yet"


def _workload_health_recommendations(
    status_value: str,
    deployments: list[DeploymentResponse],
) -> list[str]:
    if status_value == "unknown":
        return [
            "Generate and apply the workload deployment, then register a deployment record."
        ]
    if status_value == "pending":
        return [
            "Apply the generated deployment assets, then sync the deployment record status."
        ]
    if status_value == "degraded":
        if any(deployment.endpoint for deployment in deployments):
            return ["Inspect the runtime endpoint, health path, and workload logs."]
        return ["Inspect workload logs and sync the deployment record status."]
    return []


def _preflight_status(checks: list[PreflightCheck]) -> str:
    if any(check.status == "failed" for check in checks):
        return "failed"
    if any(check.status == "warning" for check in checks):
        return "warning"
    return "passed"


def _preflight_recommendations(checks: list[PreflightCheck]) -> list[str]:
    recommendations: list[str] = []
    for check in checks:
        if check.status == "passed" or not check.remediation:
            continue
        if check.remediation not in recommendations:
            recommendations.append(check.remediation)
    return recommendations


def _preflight_action_guide(
    checks: list[PreflightCheck],
    *,
    workload: WorkloadDefinition,
    target: str,
    env: str,
) -> list[PreflightAction]:
    actions: list[PreflightAction] = []
    missing_secrets = _preflight_missing_secrets(checks)
    required_secrets = _preflight_required_secrets(checks)
    if target == "kubernetes" and required_secrets:
        actions.append(
            PreflightAction(
                title="Verify Kubernetes Secret Keys",
                state="warning",
                detail=(
                    "Required secret names must exist as Kubernetes Secret keys: "
                    f"{', '.join(required_secrets)}. Values stay in the cluster "
                    "or external secret manager."
                ),
                command=(
                    "moira secrets list --target kubernetes "
                    f"--env {env} --kubernetes-secret moiraweave-secrets --check"
                ),
            )
        )
    elif missing_secrets:
        local_secret_lines = "\\n".join(f"{name}=..." for name in missing_secrets)
        kubernetes_secret_args = " ".join(
            f"--from-literal={name}=..." for name in missing_secrets
        )
        actions.append(
            PreflightAction(
                title="Set Missing Secrets",
                state="missing",
                detail=(
                    "Required secret names are missing: "
                    f"{', '.join(missing_secrets)}. Values stay outside the API "
                    "and UI."
                ),
                command=(
                    f"kubectl create secret generic moiraweave-secrets {kubernetes_secret_args}"
                    if target == "kubernetes"
                    else f"printf '{local_secret_lines}\\n' >> .env"
                ),
            )
        )

    for check in checks:
        if check.status == "passed":
            continue
        if check.name == "secrets" and (missing_secrets or target == "kubernetes"):
            continue
        if check.name == "deployment_record":
            actions.append(
                PreflightAction(
                    title="Sync Deployment Record",
                    state=check.status,
                    detail=(
                        check.remediation
                        or f"Register or sync the {target}/{env} deployment record."
                    ),
                    command=_preflight_deployment_register_command(target, env),
                )
            )
            continue
        if check.name == "worker_dispatch":
            actions.append(
                PreflightAction(
                    title="Restore Worker Dispatch",
                    state=check.status,
                    detail=(
                        check.remediation
                        or "The API cannot see an attached worker consumer."
                    ),
                    command=_preflight_platform_log_command("worker", target),
                )
            )
            continue
        if check.name == "runtime_reachability":
            actions.append(
                PreflightAction(
                    title="Fix Runtime Reachability",
                    state=check.status,
                    detail=(
                        check.remediation
                        or "The registered endpoint did not respond from the control plane."
                    ),
                    command=_preflight_workload_log_command(workload, target),
                )
            )
            continue
        if check.name in {"postgres", "redis"}:
            actions.append(
                PreflightAction(
                    title=f"Restore {_preflight_check_title(check.name)}",
                    state=check.status,
                    detail=check.remediation or check.message,
                    command=_preflight_platform_log_command(check.name, target),
                )
            )
            continue
        if check.name == "deployment_target":
            actions.append(
                PreflightAction(
                    title="Choose Supported Target",
                    state=check.status,
                    detail=check.remediation or check.message,
                )
            )
            continue
        if check.name == "runtime_boundaries":
            actions.append(
                PreflightAction(
                    title="Adjust Runtime Boundaries",
                    state=check.status,
                    detail=check.remediation or check.message,
                )
            )
            continue
        if check.name == "runtime_location":
            actions.append(
                PreflightAction(
                    title="Complete Runtime Location",
                    state=check.status,
                    detail=check.remediation or check.message,
                )
            )
            continue
        actions.append(
            PreflightAction(
                title=_preflight_check_title(check.name),
                state=check.status,
                detail=check.remediation or check.message,
            )
        )

    if actions:
        return actions
    return [
        PreflightAction(
            title="Ready",
            state="ready",
            detail=(
                "No blocking action detected for this workload, target, and "
                "environment from the control-plane perspective."
            ),
            command=(
                f'moira agent chat {workload.metadata.name} "hello" --watch'
                if workload.spec.type == "agent-service"
                else f"moira run submit {workload.metadata.name} --watch"
            ),
        )
    ]


def _preflight_missing_secrets(checks: list[PreflightCheck]) -> list[str]:
    secret_check = next((check for check in checks if check.name == "secrets"), None)
    missing = secret_check.metadata.get("missing") if secret_check else None
    if not isinstance(missing, list):
        return []
    return sorted({str(name) for name in missing if str(name)})


def _preflight_required_secrets(checks: list[PreflightCheck]) -> list[str]:
    secret_check = next((check for check in checks if check.name == "secrets"), None)
    required = secret_check.metadata.get("required") if secret_check else None
    if not isinstance(required, list):
        return []
    return sorted({str(name) for name in required if str(name)})


def _preflight_check_title(name: str) -> str:
    return " ".join(part.capitalize() for part in name.split("_"))


def _preflight_deployment_register_command(target: str, env: str) -> str:
    if target == "kubernetes":
        return f"moira deploy k8s --env {env} --register"
    return "moira deploy local --register"


def _preflight_platform_log_command(component: str, target: str) -> str | None:
    if target == "local":
        return f"docker compose logs {component}"
    if target == "kubernetes":
        return f"kubectl logs deploy/moiraweave-{component}"
    return None


def _preflight_workload_log_command(
    workload: WorkloadDefinition, target: str
) -> str | None:
    if target == "local":
        return f"docker compose logs {workload.metadata.name}"
    if target == "kubernetes":
        return f"kubectl logs deploy/{workload.metadata.name}"
    return None


def _is_secret_present(name: str) -> bool:
    value = os.getenv(name)
    return value is not None and value != ""


def _workload_secret_references(workload: WorkloadDefinition) -> list[tuple[str, str]]:
    references = [(str(secret), "spec.secrets") for secret in workload.spec.secrets]
    references.extend(
        (str(secret), "spec.agent.requiredSecrets")
        for secret in workload.spec.agent.requiredSecrets
    )
    if workload.spec.agent.authTokenEnv:
        references.append((workload.spec.agent.authTokenEnv, "spec.agent.authTokenEnv"))
    runtime_requirements = workload.spec.agent.runtimeRequirements.model_dump()
    for path, value in _runtime_requirement_secret_refs(runtime_requirements):
        references.append((value, f"spec.agent.runtimeRequirements.{path}"))
    return references


def _runtime_requirement_secret_refs(
    value: Any,
    *,
    path: str = "",
) -> list[tuple[str, str]]:
    if isinstance(value, dict):
        refs: list[tuple[str, str]] = []
        for key, item in value.items():
            next_path = f"{path}.{key}" if path else str(key)
            if key == "requiredSecrets" and isinstance(item, list):
                refs.extend((next_path, str(secret)) for secret in item)
            else:
                refs.extend(_runtime_requirement_secret_refs(item, path=next_path))
        return refs
    if isinstance(value, list):
        refs = []
        for index, item in enumerate(value):
            refs.extend(_runtime_requirement_secret_refs(item, path=f"{path}.{index}"))
        return refs
    return []


def _secret_inventory_response(
    workloads: list[WorkloadDefinition],
) -> SecretInventoryResponse:
    inventory: dict[str, dict[str, Any]] = {}
    for workload in workloads:
        workload_name = workload.metadata.name
        for secret_name, reference in _workload_secret_references(workload):
            item = inventory.setdefault(
                secret_name,
                {"workloads": set(), "references": set()},
            )
            item["workloads"].add(workload_name)
            item["references"].add(f"{workload_name}:{reference}")

    secrets: list[SecretInventoryItem] = []
    for name, data in sorted(inventory.items()):
        present = _is_secret_present(name)
        secrets.append(
            SecretInventoryItem(
                name=name,
                present=present,
                source="api-env" if present else "missing",
                workloads=sorted(data["workloads"]),
                references=sorted(data["references"]),
                remediation=(
                    None
                    if present
                    else (
                        "Define this name in the API/worker environment, local .env, "
                        "Kubernetes Secret, or external secret manager before deploying."
                    )
                ),
            )
        )
    missing = sum(1 for secret in secrets if not secret.present)
    return SecretInventoryResponse(
        status="warning" if missing else "passed",
        total=len(secrets),
        missing=missing,
        secrets=secrets,
    )


def _deployment_target_recommendation(
    workload: WorkloadDefinition,
    target: str,
    env: str,
) -> str:
    if workload.spec.deployment.mode == "external" or target == "external":
        return (
            "Register the external endpoint with a deployment record after verifying "
            "the runtime URL and credentials."
        )
    if target == "kubernetes":
        return (
            f"Run `moira deploy k8s --env {env} --apply --register`, or let CI/a "
            "deployment controller apply and sync this target."
        )
    return (
        "Run `moira up`, or `moira deploy local --up --register`, then refresh "
        "Operations."
    )


def _deployment_record_check(
    workload: WorkloadDefinition,
    *,
    target: str,
    env: str,
    deployments: list[DeploymentResponse],
) -> PreflightCheck:
    target_records = [
        deployment
        for deployment in deployments
        if deployment.target == target and deployment.env == env
    ]
    if not target_records:
        known_targets = sorted(
            {f"{deployment.target}:{deployment.env}" for deployment in deployments}
        )
        return PreflightCheck(
            name="deployment_record",
            status="warning",
            message=(
                f"No {target} deployment record is registered for environment {env!r}."
            ),
            remediation=_deployment_target_recommendation(workload, target, env),
            metadata={
                "target": target,
                "env": env,
                "known_targets": known_targets,
                "deployment_count": len(deployments),
            },
        )

    latest = target_records[0]
    bad_statuses = {"failed", "lost", "unhealthy", "unreachable"}
    active_statuses = {"applied", "running", "deployed", "reachable", "healthy"}
    status_value = (
        "failed"
        if latest.status in bad_statuses
        else "passed"
        if latest.status in active_statuses
        else "warning"
    )
    remediation = None
    if status_value == "failed":
        remediation = "Inspect workload logs, runtime health, and deployment events."
    elif status_value == "warning":
        remediation = _deployment_target_recommendation(workload, target, env)
    return PreflightCheck(
        name="deployment_record",
        status=status_value,
        message=(
            f"Latest {target}/{env} deployment record is {latest.status!r}."
            if status_value != "passed"
            else f"Latest {target}/{env} deployment record is active."
        ),
        remediation=remediation,
        metadata={
            "target": target,
            "env": env,
            "deployment_id": latest.deployment_id,
            "status": latest.status,
            "endpoint": latest.endpoint,
            "record_count": len(target_records),
        },
    )


async def _runtime_reachability_check(
    workload: WorkloadDefinition,
    *,
    target: str,
    env: str,
    deployments: list[DeploymentResponse],
) -> PreflightCheck | None:
    candidates = [
        deployment
        for deployment in deployments
        if deployment.target == target and deployment.env == env and deployment.endpoint
    ]
    if not candidates and workload.spec.deployment.mode == "external":
        endpoint = workload.spec.endpoint
        if endpoint:
            candidates = [
                DeploymentResponse(
                    deployment_id=str(uuid4()),
                    workload_name=workload.metadata.name,
                    target=target,
                    env=env,
                    status="preflight",
                    user="preflight",
                    created_at=utc_now_iso(),
                    endpoint=endpoint,
                    metadata={},
                )
            ]
    if not candidates:
        return None

    probes = [
        probe
        for probe in [
            await _probe_deployment_endpoint(deployment) for deployment in candidates
        ]
        if probe is not None
    ]
    if not probes:
        return None
    if any(ok for ok, _reason in probes):
        return PreflightCheck(
            name="runtime_reachability",
            status="passed",
            message=next(reason for ok, reason in probes if ok),
            metadata={
                "target": target,
                "endpoints": [deployment.endpoint for deployment in candidates],
            },
        )
    return PreflightCheck(
        name="runtime_reachability",
        status="warning",
        message="; ".join(reason for _ok, reason in probes),
        remediation="Inspect the runtime endpoint, health path, network, and workload logs.",
        metadata={
            "target": target,
            "endpoints": [deployment.endpoint for deployment in candidates],
        },
    )


def _redis_mapping_value(item: dict[Any, Any], key: str, default: Any = None) -> Any:
    return item.get(key, item.get(key.encode(), default))


def _redis_group_name(item: dict[Any, Any]) -> str:
    value = _redis_mapping_value(item, "name", "")
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


async def _worker_dispatch_check(redis: Any) -> PreflightCheck:
    try:
        groups = await redis.xinfo_groups(RUN_STREAM)
    except ResponseError as exc:
        return PreflightCheck(
            name="worker_dispatch",
            status="warning",
            message=(
                f"Worker consumer group {CONSUMER_GROUP!r} is not registered on "
                f"{RUN_STREAM!r}: {exc}"
            ),
            remediation="Start the worker service and wait for it to create the run consumer group.",
            metadata={"stream": RUN_STREAM, "consumer_group": CONSUMER_GROUP},
        )
    except Exception as exc:  # noqa: BLE001
        return PreflightCheck(
            name="worker_dispatch",
            status="failed",
            message=f"Worker dispatch state could not be inspected: {exc}",
            remediation="Check Redis connectivity and API gateway logs.",
            metadata={"stream": RUN_STREAM, "consumer_group": CONSUMER_GROUP},
        )

    group = next(
        (item for item in groups if _redis_group_name(item) == CONSUMER_GROUP),
        None,
    )
    if group is None:
        return PreflightCheck(
            name="worker_dispatch",
            status="warning",
            message=f"Worker consumer group {CONSUMER_GROUP!r} is not registered.",
            remediation="Start the worker service and wait for it to attach to the run queue.",
            metadata={
                "stream": RUN_STREAM,
                "consumer_group": CONSUMER_GROUP,
                "known_groups": [_redis_group_name(item) for item in groups],
            },
        )

    consumers = int(_redis_mapping_value(group, "consumers", 0) or 0)
    pending = int(_redis_mapping_value(group, "pending", 0) or 0)
    lag = _redis_mapping_value(group, "lag")
    metadata = {
        "stream": RUN_STREAM,
        "consumer_group": CONSUMER_GROUP,
        "consumers": consumers,
        "pending": pending,
        "lag": lag,
    }
    if consumers <= 0:
        return PreflightCheck(
            name="worker_dispatch",
            status="warning",
            message="Run queue exists, but no worker consumer is attached.",
            remediation="Start or restart the worker service before submitting long-running work.",
            metadata=metadata,
        )
    return PreflightCheck(
        name="worker_dispatch",
        status="passed",
        message=f"{consumers} worker consumer(s) are attached to the run queue.",
        metadata=metadata,
    )


def _runtime_boundaries_check(workload: WorkloadDefinition) -> PreflightCheck:
    agent = workload.spec.agent
    requirements = agent.runtimeRequirements
    filesystem = requirements.filesystem
    network = requirements.network
    browser = requirements.browser
    terminal = requirements.terminal
    warnings: list[str] = []

    workspace_mount = filesystem.workspaceMount or agent.workspaceMount
    if filesystem.persistentWorkspace and not workspace_mount:
        warnings.append(
            "Declare spec.agent.workspaceMount or "
            "spec.agent.runtimeRequirements.filesystem.workspaceMount."
        )
    if filesystem.persistentWorkspace and not workload.spec.persistence.enabled:
        warnings.append(
            "Enable spec.persistence so runtime-owned filesystem tools survive restarts."
        )
    if requirements.webSearch.enabled and network.egress == "disabled":
        warnings.append("Enable network egress for runtime-owned web search.")
    if browser.mode != "none" and network.egress == "disabled":
        warnings.append("Enable network egress for runtime-owned browser automation.")
    if terminal.mode in {"ssh", "daytona", "modal"} and network.egress == "disabled":
        warnings.append(
            f"Enable network egress for the runtime terminal backend {terminal.mode!r}."
        )
    if filesystem.hostMounts:
        warnings.append(
            "Host mounts are declared but MoiraWeave does not mount arbitrary host "
            "paths automatically; use workspace/PVC or an external-owned runtime."
        )

    metadata = {
        "toolOwnership": agent.toolOwnership,
        "networkEgress": network.egress,
        "persistentWorkspace": filesystem.persistentWorkspace,
        "workspaceMount": workspace_mount,
        "webSearch": requirements.webSearch.enabled,
        "browserMode": browser.mode,
        "terminalMode": terminal.mode,
        "terminalApproval": terminal.approval,
        "mcp": requirements.mcp.enabled,
        "messaging": requirements.messaging.enabled,
        "exposedChannels": agent.exposedChannels,
        "externalOwnedChannels": agent.externalOwnedChannels,
    }
    return PreflightCheck(
        name="runtime_boundaries",
        status="warning" if warnings else "passed",
        message=(
            "Runtime owns agent tools; MoiraWeave only prepares environment boundaries."
            if not warnings
            else "Runtime-owned tools need environment boundary adjustments."
        ),
        remediation=" ".join(warnings) if warnings else None,
        metadata=metadata,
    )


async def _run_preflight(
    workload: WorkloadDefinition,
    *,
    target: str,
    env: str,
    user: str,
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

    deployments: list[DeploymentResponse] = []
    try:
        deployments = [
            _deployment_response(deployment)
            for deployment in await control_plane.list_deployments(
                user,
                workload_name=workload.metadata.name,
                env=env,
            )
        ]
        checks.append(
            _deployment_record_check(
                workload,
                target=normalized_target,
                env=env,
                deployments=deployments,
            )
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(
            PreflightCheck(
                name="deployment_record",
                status="failed",
                message=f"Deployment records could not be read: {exc}",
                remediation="Check Postgres connectivity and API gateway logs.",
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

    secret_names = sorted(
        {secret_name for secret_name, _ in _workload_secret_references(workload)}
    )
    if normalized_target == "kubernetes":
        checks.append(
            PreflightCheck(
                name="secrets",
                status="warning" if secret_names else "passed",
                message=(
                    "Kubernetes Secret keys must be verified from an operator "
                    f"shell: {', '.join(secret_names)}."
                    if secret_names
                    else "No required secrets declared."
                ),
                remediation=(
                    "Run moira secrets list --target kubernetes "
                    f"--env {env} --kubernetes-secret moiraweave-secrets --check."
                )
                if secret_names
                else None,
                metadata={
                    "required": secret_names,
                    "missing": [],
                    "verification": "operator-cli",
                    "kubernetes_secret": "moiraweave-secrets",
                },
            )
        )
    else:
        missing_secrets = sorted(
            name for name in secret_names if not _is_secret_present(name)
        )
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
                metadata={"required": secret_names, "missing": missing_secrets},
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
        checks.append(_runtime_boundaries_check(workload))

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

    checks.append(await _worker_dispatch_check(redis))

    runtime_check = await _runtime_reachability_check(
        workload,
        target=normalized_target,
        env=env,
        deployments=deployments,
    )
    if runtime_check:
        checks.append(runtime_check)

    return PreflightResponse(
        workload_name=workload.metadata.name,
        target=normalized_target,
        status=_preflight_status(checks),
        checks=checks,
        recommendations=_preflight_recommendations(checks),
        action_guide=_preflight_action_guide(
            checks,
            workload=workload,
            target=normalized_target,
            env=env,
        ),
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
            artifacts.append(_artifact_response(artifact, run=run))
    return sorted(artifacts, key=lambda item: item.created_at, reverse=True)


@router.get("/workloads", response_model=list[WorkloadInfo])
async def list_workloads(
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> list[WorkloadInfo]:
    settings = get_settings()
    return [
        _workload_info(
            record.workload,
            owner_subject=record.user,
            team_id=_workload_team_id(record.workload),
        )
        for record in await _list_visible_workloads(
            control_plane,
            settings,
            current_user,
        )
    ]


@router.get("/templates", response_model=list[WorkloadTemplateInfo])
async def list_workload_templates() -> list[WorkloadTemplateInfo]:
    return [_template_info(template_id) for template_id in _TEMPLATE_PARAMETERS]


@router.get("/secrets", response_model=SecretInventoryResponse)
async def list_secret_inventory(
    control_plane: ControlPlane,
    current_user: AdminUser,
    workload_name: str | None = None,
) -> SecretInventoryResponse:
    settings = get_settings()
    workloads = await _all_workloads(control_plane, settings)
    if workload_name:
        workload = workloads.get(workload_name)
        if workload is None:
            raise HTTPException(status_code=404, detail="Workload not found")
        inventory = _secret_inventory_response([workload])
    else:
        inventory = _secret_inventory_response(list(workloads.values()))
    await _audit(
        control_plane,
        current_user,
        "secret_inventory.read",
        "secret_inventory",
        workload_name or "all",
        metadata={
            "workload_name": workload_name,
            "secret_names": [item.name for item in inventory.secrets],
            "missing": inventory.missing,
            "total": inventory.total,
        },
    )
    return inventory


@router.post(
    "/workloads/from-template",
    response_model=WorkloadInfo,
    status_code=status.HTTP_201_CREATED,
)
async def create_workload_from_template(
    body: WorkloadFromTemplateRequest,
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> WorkloadInfo:
    manifest = _template_manifest(body.template_id, body.parameters)
    if body.team_id:
        metadata = manifest.setdefault("metadata", {})
        annotations = metadata.setdefault("annotations", {})
        annotations[_WORKLOAD_TEAM_ANNOTATION] = body.team_id
    workload = WorkloadDefinition.model_validate(manifest)
    workload, owner_subject = await _store_owned_workload(
        workload,
        control_plane,
        current_user,
    )
    return _workload_info(
        workload,
        owner_subject=owner_subject,
        team_id=_workload_team_id(workload),
    )


@router.post(
    "/workloads",
    response_model=WorkloadInfo,
    status_code=status.HTTP_201_CREATED,
)
async def register_workload(
    body: WorkloadDefinition,
    control_plane: ControlPlane,
    current_user: AdminUser,
) -> WorkloadInfo:
    workload, owner_subject = await _store_owned_workload(
        body,
        control_plane,
        current_user,
    )
    return _workload_info(
        workload,
        owner_subject=owner_subject,
        team_id=_workload_team_id(workload),
    )


@router.get("/workloads/{name}", response_model=WorkloadInfo)
async def get_workload(
    name: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> WorkloadInfo:
    settings = get_settings()
    record = await _get_workload_record(name, control_plane, settings)
    if not await _workload_visible(record, control_plane, current_user):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workload {name!r} not found",
        )
    return _workload_info(
        record.workload,
        owner_subject=record.user,
        team_id=_workload_team_id(record.workload),
    )


@router.post(
    "/workloads/{name}/deployments",
    response_model=DeploymentResponse,
    status_code=status.HTTP_201_CREATED,
)
async def record_workload_deployment(
    name: str,
    body: DeploymentRequest,
    control_plane: ControlPlane,
    current_user: OperatorUser,
) -> DeploymentResponse:
    settings = get_settings()
    await _get_visible_workload(name, control_plane, settings, current_user)
    target = "kubernetes" if body.target == "k8s" else body.target
    deployment = await control_plane.upsert_deployment(
        str(uuid4()),
        name,
        target,
        body.status,
        current_user.subject,
        env=body.env,
        endpoint=body.endpoint,
        metadata=body.metadata,
        now=utc_now_iso(),
    )
    await _audit(
        control_plane,
        current_user,
        "deployment.record",
        "deployment",
        deployment.deployment_id,
        metadata={
            "workload_name": name,
            "target": target,
            "env": body.env,
            "status": body.status,
            "endpoint": body.endpoint,
        },
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
    settings = get_settings()
    workload = await _get_visible_workload(name, control_plane, settings, current_user)
    return _deployment_plan_response(workload, target=target, env=env)


@router.post("/workloads/{name}/preflight", response_model=PreflightResponse)
async def workload_preflight(
    name: str,
    body: PreflightRequest,
    redis: RedisClient,
    control_plane: ControlPlane,
    current_user: OperatorUser,
) -> PreflightResponse:
    settings = get_settings()
    workload = await _get_visible_workload(name, control_plane, settings, current_user)
    return await _run_preflight(
        workload,
        target=body.target,
        env=body.env,
        user=current_user.subject,
        control_plane=control_plane,
        redis=redis,
    )


@router.get("/deployments", response_model=list[DeploymentResponse])
async def list_deployments(
    control_plane: ControlPlane,
    current_user: CurrentUser,
    workload_name: str | None = None,
    env: str | None = Query(default=None, min_length=1, max_length=64),
) -> list[DeploymentResponse]:
    deployments = await _list_visible_deployments(
        control_plane,
        current_user,
        workload_name=workload_name,
        env=env,
    )
    return [_deployment_response(deployment) for deployment in deployments]


@router.get("/environments", response_model=list[EnvironmentInfo])
async def list_environments(
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> list[EnvironmentInfo]:
    deployments = await _list_visible_deployments(control_plane, current_user)
    operations = await _list_visible_operations(
        control_plane,
        current_user,
        limit=500,
        offset=0,
    )
    envs: dict[str, EnvironmentInfo] = {
        name: EnvironmentInfo(name=name) for name in ["local", "dev", "staging", "prod"]
    }
    workloads_by_env: dict[str, set[str]] = {}

    for deployment in deployments:
        info = envs.setdefault(deployment.env, EnvironmentInfo(name=deployment.env))
        info.deployment_count += 1
        workloads_by_env.setdefault(deployment.env, set()).add(deployment.workload_name)

    for operation in operations:
        info = envs.setdefault(operation.env, EnvironmentInfo(name=operation.env))
        info.operation_count += 1
        workloads_by_env.setdefault(operation.env, set()).add(operation.workload_name)

    for env, workload_names in workloads_by_env.items():
        envs[env].workload_count = len(workload_names)

    return sorted(envs.values(), key=lambda item: item.name)


@router.get("/audit-events", response_model=list[AuditEventResponse])
async def list_audit_events(
    control_plane: ControlPlane,
    current_user: CurrentUser,
    action: str | None = None,
    resource_type: str | None = None,
    resource_id: str | None = None,
    env: str | None = Query(default=None, min_length=1, max_length=64),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[AuditEventResponse]:
    events = await _list_visible_audit_events(
        control_plane,
        current_user,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        env=env,
        limit=limit,
        offset=offset,
    )
    return [_audit_event_response(event) for event in events]


@router.post(
    "/deployment-operations",
    response_model=DeploymentOperationResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@limiter.limit(_RATE_LIMIT_DEPLOYMENT_OPERATION)
async def create_deployment_operation(
    request: Request,
    body: DeploymentOperationRequest,
    control_plane: ControlPlane,
    current_user: OperatorUser,
) -> DeploymentOperationResponse:
    del request
    settings = get_settings()
    normalized_target = "kubernetes" if body.target == "k8s" else body.target
    operation_id = str(uuid4())
    now = utc_now_iso()
    metadata = dict(body.metadata)
    metadata["executor"] = body.executor
    if body.timeout_seconds is not None:
        metadata["timeout_seconds"] = body.timeout_seconds
    events: list[tuple[str, str, dict[str, Any]]] = []
    operation_status = "succeeded"
    completed_at: str | None = now

    try:
        workload = await _get_visible_workload(
            body.workload_name,
            control_plane,
            settings,
            current_user,
        )
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
                str(body.metadata.get("status") or "deployed"),
                current_user.subject,
                env=body.env,
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
        elif body.action == "logs":
            commands = _deployment_log_commands(workload, plan)
            metadata["log_commands"] = commands
            events.append(
                (
                    "operation.logs",
                    "Log command guidance generated.",
                    {"commands": commands},
                )
            )
        elif body.action in {"apply", "undeploy"}:
            commands = _deployment_action_commands(body.action, workload, plan)
            next_actions = _deployment_operation_next_actions(body.action, plan)
            metadata["action_commands"] = commands
            metadata["next_actions"] = next_actions
            if body.executor == "controller":
                operation_status = "queued"
                completed_at = None
                metadata["controller_required"] = True
                events.append(
                    (
                        "operation.queued",
                        "Deployment operation queued for a deployment controller.",
                        {
                            "commands": commands,
                            "next_actions": next_actions,
                            "executor": body.executor,
                        },
                    )
                )
            else:
                operation_status = "failed"
                metadata["blocked_reason"] = "api-gateway-has-no-host-executor"
                events.append(
                    (
                        "operation.blocked",
                        (
                            "This action needs a CLI or deployment controller with "
                            "Docker/Kubernetes credentials."
                        ),
                        {
                            "commands": commands,
                            "next_actions": next_actions,
                            "reason": metadata["blocked_reason"],
                        },
                    )
                )
    except HTTPException as exc:
        operation_status = "failed"
        completed_at = utc_now_iso()
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
        env=body.env,
        metadata=metadata,
        now=now,
        completed_at=completed_at,
        timeout_seconds=body.timeout_seconds,
    )
    for event_type, message, data in events:
        await control_plane.append_deployment_operation_event(
            operation_id,
            event_type,
            message,
            data=data,
        )
    await _audit(
        control_plane,
        current_user,
        f"deployment_operation.{body.action}",
        "deployment_operation",
        operation.operation_id,
        metadata={
            "workload_name": body.workload_name,
            "target": normalized_target,
            "env": body.env,
            "status": operation.status,
        },
    )
    return _deployment_operation_response(operation)


@router.get("/operations/alerts", response_model=list[OperationsAlert])
async def list_operations_alerts(
    redis: RedisClient,
    control_plane: ControlPlane,
    current_user: CurrentUser,
    env: str | None = Query(default=None, min_length=1, max_length=64),
    scope: str = Query(default="mine", pattern="^(mine|all)$"),
) -> list[OperationsAlert]:
    if scope == "all" and current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Requires admin role",
        )

    alerts: list[OperationsAlert] = []
    try:
        dead_letter_count = await redis.xlen(DEAD_LETTER_STREAM)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        alerts.append(
            OperationsAlert(
                id="redis-dead-letter-unavailable",
                severity="critical",
                title="Dead-letter queue cannot be inspected",
                detail=f"Redis returned an error while reading {DEAD_LETTER_STREAM!r}.",
                action="Check Redis connectivity from the API gateway.",
                resource_type="redis",
                resource_id=DEAD_LETTER_STREAM,
                command="moira doctor",
                metadata={"error": str(exc)},
            )
        )
    else:
        if dead_letter_count:
            alerts.append(
                OperationsAlert(
                    id="dead-letter-messages",
                    severity="warning",
                    title="Dead-letter messages need review",
                    detail=(
                        f"{dead_letter_count} worker dispatch message(s) are in "
                        "dead-letter storage."
                    ),
                    action=(
                        "Inspect the failed messages, fix the cause, then replay "
                        "or purge them."
                    ),
                    resource_type="dead_letter",
                    resource_id=DEAD_LETTER_STREAM,
                    count=int(dead_letter_count),
                    command="moira run dead-letter list",
                )
            )

    try:
        pending_info = await redis.xpending(RUN_STREAM, CONSUMER_GROUP)
    except (IndexError, ResponseError) as exc:
        if isinstance(exc, IndexError) or "NOGROUP" in str(exc):
            pending_count = 0
        else:  # pragma: no cover - defensive runtime guard
            alerts.append(
                OperationsAlert(
                    id="redis-pending-unavailable",
                    severity="critical",
                    title="Run dispatch pending state cannot be inspected",
                    detail=f"Redis returned an error while reading {RUN_STREAM!r}.",
                    action="Check Redis connectivity and worker consumer group state.",
                    resource_type="redis",
                    resource_id=RUN_STREAM,
                    command="moira doctor",
                    metadata={"error": str(exc)},
                )
            )
            pending_count = 0
    else:
        pending_count = _pending_stream_count(pending_info)
    if pending_count:
        alerts.append(
            OperationsAlert(
                id="run-dispatch-pending-reclaim",
                severity="warning",
                title="Run dispatch messages are pending",
                detail=(
                    f"{pending_count} Redis Stream message(s) are pending worker "
                    "acknowledgement."
                ),
                action=(
                    "Check worker health and reclaim settings; healthy workers "
                    "will reclaim abandoned queued/cancel-pending runs."
                ),
                resource_type="redis_stream",
                resource_id=RUN_STREAM,
                count=pending_count,
                command="moira doctor",
                metadata={"stream": RUN_STREAM, "consumer_group": CONSUMER_GROUP},
            )
        )

    operations = (
        await control_plane.list_deployment_operations(None, env=env, limit=200)
        if scope == "all"
        else await _list_visible_operations(
            control_plane,
            current_user,
            env=env,
            limit=200,
            offset=0,
        )
    )
    queued_ops = [operation for operation in operations if operation.status == "queued"]
    running_ops = [
        operation for operation in operations if operation.status == "running"
    ]
    now = utc_now_iso()
    expired_lease_ops = [
        operation
        for operation in running_ops
        if _deployment_operation_lease_expired(operation, now)
    ]
    if queued_ops:
        alerts.append(
            OperationsAlert(
                id="deployment-operations-queued",
                severity="warning",
                title="Deployment operations are waiting",
                detail=(
                    f"{len(queued_ops)} deployment operation(s) are queued for "
                    "a CLI/controller executor."
                ),
                action=(
                    "Start a deployment controller or run the generated CLI "
                    "commands from a trusted operator workstation."
                ),
                resource_type="deployment_operation",
                env=env,
                count=len(queued_ops),
                command="moira deploy operations list --status queued",
                metadata={
                    "operation_ids": [
                        operation.operation_id for operation in queued_ops[:10]
                    ]
                },
            )
        )
    if running_ops:
        alerts.append(
            OperationsAlert(
                id="deployment-operations-running",
                severity="info",
                title="Deployment operations are running",
                detail=f"{len(running_ops)} deployment operation(s) are in progress.",
                action="Watch operation events and confirm the controller heartbeat.",
                resource_type="deployment_operation",
                env=env,
                count=len(running_ops),
                command="moira deploy operations list --status running",
                metadata={
                    "operation_ids": [
                        operation.operation_id for operation in running_ops[:10]
                    ]
                },
            )
        )
    if expired_lease_ops:
        alerts.append(
            OperationsAlert(
                id="deployment-controller-lease-expired",
                severity="critical",
                title="Deployment controller lease expired",
                detail=(
                    f"{len(expired_lease_ops)} running deployment operation(s) "
                    "missed their controller heartbeat lease."
                ),
                action=(
                    "Restart the deployment controller or reclaim the operation "
                    "from a healthy controller."
                ),
                resource_type="deployment_operation",
                env=env,
                count=len(expired_lease_ops),
                command="moira deploy operations list --status running",
                metadata={
                    "operation_ids": [
                        operation.operation_id for operation in expired_lease_ops[:10]
                    ],
                    "controller_ids": [
                        operation.controller_id
                        for operation in expired_lease_ops[:10]
                        if operation.controller_id is not None
                    ],
                },
            )
        )

    runs = (
        await control_plane.list_runs(None, limit=200)
        if scope == "all"
        else await _list_visible_runs(
            control_plane,
            current_user,
            limit=200,
            offset=0,
        )
    )
    if env is not None:
        runs = [
            run
            for run in runs
            if str(
                (run.payload or {}).get("env") or (run.result or {}).get("env") or ""
            )
            == env
        ]
    lost_runs = [run for run in runs if run.status == "lost"]
    failed_runs = [run for run in runs if run.status == "failed"]
    cancel_pending_runs = [
        run for run in runs if run.status in {"cancel_requested", "cancelling"}
    ]
    duplicate_ack_events: list[tuple[str, str]] = []
    for run in runs:
        for event in await control_plane.list_run_events(run.run_id):
            if event.type == "run.duplicate_ignored":
                duplicate_ack_events.append((run.run_id, event.id))
    if lost_runs:
        alerts.append(
            OperationsAlert(
                id="runs-lost",
                severity="critical",
                title="Runs marked lost",
                detail=f"{len(lost_runs)} run(s) lost heartbeat or worker ownership.",
                action="Check worker logs, runtime health, and stale-run recovery.",
                resource_type="run",
                env=env,
                count=len(lost_runs),
                command="moira run list --status lost",
                metadata={"run_ids": [run.run_id for run in lost_runs[:10]]},
            )
        )
    if failed_runs:
        alerts.append(
            OperationsAlert(
                id="runs-failed",
                severity="warning",
                title="Recent runs failed",
                detail=f"{len(failed_runs)} recent run(s) ended in failed state.",
                action="Open run details, inspect events and artifacts, then retry.",
                resource_type="run",
                env=env,
                count=len(failed_runs),
                command="moira run list --status failed",
                metadata={"run_ids": [run.run_id for run in failed_runs[:10]]},
            )
        )
    if cancel_pending_runs:
        alerts.append(
            OperationsAlert(
                id="runs-cancel-pending",
                severity="warning",
                title="Runs are waiting on cancellation",
                detail=(
                    f"{len(cancel_pending_runs)} run(s) are waiting for "
                    "cooperative cancellation."
                ),
                action="Check agent/runtime cancellation support and worker logs.",
                resource_type="run",
                env=env,
                count=len(cancel_pending_runs),
                command="moira run list --status cancel_requested",
                metadata={"run_ids": [run.run_id for run in cancel_pending_runs[:10]]},
            )
        )
    if duplicate_ack_events:
        run_ids = list(dict.fromkeys(run_id for run_id, _ in duplicate_ack_events))
        alerts.append(
            OperationsAlert(
                id="run-dispatch-duplicate-acks",
                severity="info",
                title="Duplicate run dispatches acknowledged",
                detail=(
                    f"{len(duplicate_ack_events)} duplicate dispatch message(s) "
                    "were acknowledged without re-executing active runs."
                ),
                action=(
                    "Inspect run events and worker logs if this grows; active "
                    "runs are protected from duplicate execution."
                ),
                resource_type="run_event",
                env=env,
                count=len(duplicate_ack_events),
                command=f"moira run events {run_ids[0]}",
                metadata={
                    "run_ids": run_ids[:10],
                    "event_ids": [
                        event_id for _, event_id in duplicate_ack_events[:10]
                    ],
                },
            )
        )

    return alerts


@router.get("/deployment-operations", response_model=list[DeploymentOperationResponse])
async def list_deployment_operations(
    control_plane: ControlPlane,
    current_user: CurrentUser,
    workload_name: str | None = None,
    target: str | None = None,
    env: str | None = Query(default=None, min_length=1, max_length=64),
    operation_status: str | None = Query(default=None, alias="status"),
    action: str | None = None,
    scope: str = Query(default="mine", pattern="^(mine|all)$"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[DeploymentOperationResponse]:
    normalized_target = "kubernetes" if target == "k8s" else target
    if scope == "all" and current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Requires admin role",
        )
    operations = (
        await control_plane.list_deployment_operations(
            None,
            workload_name=workload_name,
            target=normalized_target,
            env=env,
            status=operation_status,
            action=action,
            limit=limit,
            offset=offset,
        )
        if scope == "all"
        else await _list_visible_operations(
            control_plane,
            current_user,
            workload_name=workload_name,
            target=normalized_target,
            env=env,
            status_filter=operation_status,
            action=action,
            limit=limit,
            offset=offset,
        )
    )
    return [_deployment_operation_response(operation) for operation in operations]


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
    if not await _can_access_deployment_operation(
        operation,
        control_plane,
        current_user,
    ):
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
    if not await _can_access_deployment_operation(
        operation,
        control_plane,
        current_user,
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    events = await control_plane.list_deployment_operation_events(operation_id)
    return [_deployment_operation_event_response(event) for event in events]


@router.post(
    "/deployment-operations/{operation_id}/claim",
    response_model=DeploymentOperationResponse,
)
@limiter.limit(_RATE_LIMIT_CONTROLLER_MUTATION)
async def claim_deployment_operation(
    request: Request,
    operation_id: str,
    body: DeploymentOperationClaimRequest,
    control_plane: ControlPlane,
    current_user: OperatorUser,
) -> DeploymentOperationResponse:
    del request
    operation = await control_plane.get_deployment_operation(operation_id)
    if operation is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Deployment operation not found",
        )
    if not await _can_access_deployment_operation(
        operation,
        control_plane,
        current_user,
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    now = utc_now_iso()
    lease_expired = (
        operation.status == "running"
        and _deployment_operation_lease_expired(operation, now)
    )
    if operation.status != "queued" and not lease_expired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Deployment operation is {operation.status}, not queued or expired"
            ),
        )

    lease_expires_at = _operation_lease_expiry(now, body.lease_seconds)
    metadata = {
        **operation.metadata,
        "controller": {
            "id": body.controller_id,
            "claimed_by": current_user.subject,
            "claimed_at": now,
            "lease_seconds": body.lease_seconds,
            "lease_expires_at": lease_expires_at,
            "reclaimed": lease_expired,
            **body.metadata,
        },
    }
    updated = await control_plane.update_deployment_operation(
        operation_id,
        status="running",
        metadata=metadata,
        updated_at=now,
        lease_expires_at=lease_expires_at,
        controller_id=body.controller_id,
        heartbeat_at=now,
    )
    event_type = "operation.reclaimed" if lease_expired else "operation.claimed"
    await control_plane.append_deployment_operation_event(
        operation_id,
        event_type,
        (
            "Deployment operation reclaimed by controller."
            if lease_expired
            else "Deployment operation claimed by controller."
        ),
        data={
            "controller_id": body.controller_id,
            "claimed_by": current_user.subject,
            "lease_expires_at": lease_expires_at,
            "reclaimed": lease_expired,
        },
        timestamp=now,
    )
    await _audit(
        control_plane,
        current_user,
        "deployment_operation.claim",
        "deployment_operation",
        operation_id,
        metadata={
            "workload_name": operation.workload_name,
            "target": operation.target,
            "env": operation.env,
            "controller_id": body.controller_id,
        },
    )
    return _deployment_operation_response(updated)


@router.post(
    "/deployment-operations/{operation_id}/heartbeat",
    response_model=DeploymentOperationResponse,
)
@limiter.limit(_RATE_LIMIT_CONTROLLER_MUTATION)
async def heartbeat_deployment_operation(
    request: Request,
    operation_id: str,
    body: DeploymentOperationHeartbeatRequest,
    control_plane: ControlPlane,
    current_user: OperatorUser,
) -> DeploymentOperationResponse:
    del request
    operation = await control_plane.get_deployment_operation(operation_id)
    if operation is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Deployment operation not found",
        )
    if not await _can_access_deployment_operation(
        operation,
        control_plane,
        current_user,
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    if operation.status != "running":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Deployment operation is {operation.status}, not running",
        )
    if operation.controller_id and operation.controller_id != body.controller_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Deployment operation is claimed by a different controller",
        )

    now = utc_now_iso()
    lease_expires_at = _operation_lease_expiry(now, body.lease_seconds)
    metadata = {
        **operation.metadata,
        "controller": {
            **dict(operation.metadata.get("controller") or {}),
            "id": body.controller_id,
            "last_heartbeat_at": now,
            "lease_seconds": body.lease_seconds,
            "lease_expires_at": lease_expires_at,
            **body.metadata,
        },
    }
    updated = await control_plane.update_deployment_operation(
        operation_id,
        status="running",
        metadata=metadata,
        updated_at=now,
        lease_expires_at=lease_expires_at,
        controller_id=body.controller_id,
        heartbeat_at=now,
    )
    await control_plane.append_deployment_operation_event(
        operation_id,
        "operation.heartbeat",
        "Deployment controller heartbeat received.",
        data={
            "controller_id": body.controller_id,
            "lease_expires_at": lease_expires_at,
        },
        timestamp=now,
    )
    await _audit(
        control_plane,
        current_user,
        "deployment_operation.heartbeat",
        "deployment_operation",
        operation_id,
        metadata={
            "workload_name": operation.workload_name,
            "target": operation.target,
            "env": operation.env,
            "controller_id": body.controller_id,
            "lease_expires_at": lease_expires_at,
        },
    )
    return _deployment_operation_response(updated)


@router.post(
    "/deployment-operations/{operation_id}/events",
    response_model=DeploymentOperationEvent,
    status_code=status.HTTP_201_CREATED,
)
@limiter.limit(_RATE_LIMIT_CONTROLLER_MUTATION)
async def append_deployment_operation_event(
    request: Request,
    operation_id: str,
    body: DeploymentOperationEventRequest,
    control_plane: ControlPlane,
    current_user: OperatorUser,
) -> DeploymentOperationEvent:
    del request
    operation = await control_plane.get_deployment_operation(operation_id)
    if operation is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Deployment operation not found",
        )
    if not await _can_access_deployment_operation(
        operation,
        control_plane,
        current_user,
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    event = await control_plane.append_deployment_operation_event(
        operation_id,
        body.type,
        body.message,
        data=body.data,
    )
    return _deployment_operation_event_response(event)


@router.post(
    "/deployment-operations/{operation_id}/complete",
    response_model=DeploymentOperationResponse,
)
@limiter.limit(_RATE_LIMIT_CONTROLLER_MUTATION)
async def complete_deployment_operation(
    request: Request,
    operation_id: str,
    body: DeploymentOperationCompleteRequest,
    control_plane: ControlPlane,
    current_user: OperatorUser,
) -> DeploymentOperationResponse:
    del request
    operation = await control_plane.get_deployment_operation(operation_id)
    if operation is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Deployment operation not found",
        )
    if not await _can_access_deployment_operation(
        operation,
        control_plane,
        current_user,
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
    if operation.status in {"succeeded", "failed", "canceled"}:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Deployment operation is already {operation.status}",
        )

    now = utc_now_iso()
    metadata = {
        **operation.metadata,
        "controller_result": {
            "completed_by": current_user.subject,
            "completed_at": now,
            **body.metadata,
        },
    }
    if body.message:
        metadata["controller_result"]["message"] = body.message
    if body.stdout_summary is not None:
        metadata["controller_result"]["stdout_summary"] = body.stdout_summary
    if body.stderr_summary is not None:
        metadata["controller_result"]["stderr_summary"] = body.stderr_summary
    updated = await control_plane.update_deployment_operation(
        operation_id,
        status=body.status,
        metadata=metadata,
        updated_at=now,
        completed_at=now,
        stdout_summary=body.stdout_summary,
        stderr_summary=body.stderr_summary,
    )
    await control_plane.append_deployment_operation_event(
        operation_id,
        f"operation.{body.status}",
        body.message or f"Deployment operation {body.status}.",
        data=body.metadata,
        timestamp=now,
    )
    await _sync_deployment_record_for_completed_operation(
        updated,
        control_plane,
        current_user,
        now=now,
    )
    await _audit(
        control_plane,
        current_user,
        "deployment_operation.complete",
        "deployment_operation",
        operation_id,
        metadata={
            "workload_name": operation.workload_name,
            "target": operation.target,
            "env": operation.env,
            "status": body.status,
        },
    )
    return _deployment_operation_response(updated)


@router.get("/workloads/{name}/deployments", response_model=list[DeploymentResponse])
async def list_workload_deployments(
    name: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
    env: str | None = Query(default=None, min_length=1, max_length=64),
) -> list[DeploymentResponse]:
    settings = get_settings()
    await _get_visible_workload(name, control_plane, settings, current_user)
    deployments = await _list_visible_deployments(
        control_plane,
        current_user,
        workload_name=name,
        env=env,
    )
    return [_deployment_response(deployment) for deployment in deployments]


@router.get("/workloads/{name}/health", response_model=WorkloadHealthResponse)
async def workload_health(
    name: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
    env: str | None = Query(default=None, min_length=1, max_length=64),
) -> WorkloadHealthResponse:
    settings = get_settings()
    await _get_visible_workload(name, control_plane, settings, current_user)
    deployments = [
        _deployment_response(deployment)
        for deployment in await _list_visible_deployments(
            control_plane,
            current_user,
            workload_name=name,
            env=env,
        )
    ]
    health_status, reason = await _deployment_health_status(deployments)
    return WorkloadHealthResponse(
        workload_name=name,
        status=health_status,
        reason=reason,
        deployments=deployments,
        recommendations=_workload_health_recommendations(health_status, deployments),
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
    current_user: OperatorUser,
) -> RunResponse:
    del request
    settings = get_settings()
    workload = await _get_visible_workload(name, control_plane, settings, current_user)
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
    env: str | None = Query(default=None, min_length=1, max_length=64),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[RunStatusResponse]:
    runs = await _list_visible_runs_for_filters(
        control_plane,
        current_user,
        workload_name=workload_name,
        env=env,
        limit=limit,
        offset=offset,
    )
    return [_run_response(run) for run in runs]


@router.get("/runs/dead-letter", response_model=list[DeadLetterEntry])
async def list_dead_letter_entries(
    redis: RedisClient,
    current_user: OperatorUser,
    limit: int = Query(default=50, ge=1, le=200),
) -> list[DeadLetterEntry]:
    del current_user
    entries: Any = await redis.xrevrange(DEAD_LETTER_STREAM, count=limit)
    return [
        _dead_letter_entry(
            str(_redis_stream_value(message_id)), _redis_stream_fields(fields)
        )
        for message_id, fields in entries
    ]


@router.delete("/runs/dead-letter/{message_id}", response_model=DeadLetterEntry)
@limiter.limit(_RATE_LIMIT_DEAD_LETTER_MUTATION)
async def purge_dead_letter_entry(
    request: Request,
    message_id: str,
    redis: RedisClient,
    control_plane: ControlPlane,
    current_user: OperatorUser,
) -> DeadLetterEntry:
    del request
    entries: Any = await redis.xrange(
        DEAD_LETTER_STREAM,
        min=message_id,
        max=message_id,
        count=1,
    )
    if not entries:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dead-letter entry not found",
        )
    _message_id, fields = entries[0]
    entry = _dead_letter_entry(
        str(_redis_stream_value(_message_id)),
        _redis_stream_fields(fields),
    )
    deleted = await redis.xdel(DEAD_LETTER_STREAM, message_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dead-letter entry not found",
        )
    await _audit(
        control_plane,
        current_user,
        "queue.dead_letter.purge",
        "dead_letter",
        message_id,
        metadata={
            "reason": entry.reason,
            "source_stream": entry.source_stream,
            "source_id": entry.source_id,
        },
    )
    return entry


@router.post(
    "/runs/dead-letter/{message_id}/replay",
    response_model=DeadLetterReplayResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@limiter.limit(_RATE_LIMIT_DEAD_LETTER_MUTATION)
async def replay_dead_letter_entry(
    request: Request,
    message_id: str,
    redis: RedisClient,
    control_plane: ControlPlane,
    current_user: OperatorUser,
) -> DeadLetterReplayResponse:
    del request
    entries: Any = await redis.xrange(
        DEAD_LETTER_STREAM,
        min=message_id,
        max=message_id,
        count=1,
    )
    if not entries:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dead-letter entry not found",
        )
    _message_id, fields = entries[0]
    entry = _dead_letter_entry(
        str(_redis_stream_value(_message_id)),
        _redis_stream_fields(fields),
    )
    try:
        msg = RunMessage.model_validate(entry.payload)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Dead-letter payload is not a replayable run message",
        ) from exc

    run = await _authorize_run(msg.run_id, control_plane, current_user)
    if run.status not in {"queued", "cancel_requested"}:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Run status {run.status!r} cannot be replayed",
        )
    await _get_visible_workload(
        msg.workload_name,
        control_plane,
        get_settings(),
        current_user,
    )

    replay_payload: dict[EncodableT, EncodableT] = {
        "run_id": msg.run_id,
        "workload_name": msg.workload_name,
        "payload": msg.payload,
        "user": msg.user,
    }
    if msg.workload_manifest is not None:
        replay_payload["workload_manifest"] = msg.workload_manifest
    replayed_message_id = await redis.xadd(RUN_STREAM, replay_payload)
    deleted = await redis.xdel(DEAD_LETTER_STREAM, message_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dead-letter entry not found",
        )
    await control_plane.append_run_event(
        msg.run_id,
        "queue.dead_letter.replayed",
        "Dead-letter message replayed to worker queue.",
        data={
            "dead_letter_message_id": message_id,
            "replayed_message_id": str(replayed_message_id),
            "reason": entry.reason,
        },
        timestamp=utc_now_iso(),
    )
    await _audit(
        control_plane,
        current_user,
        "queue.dead_letter.replay",
        "dead_letter",
        message_id,
        metadata={
            "run_id": msg.run_id,
            "workload_name": msg.workload_name,
            "reason": entry.reason,
            "replayed_message_id": str(replayed_message_id),
        },
    )
    return DeadLetterReplayResponse(
        message_id=message_id,
        replayed_message_id=str(replayed_message_id),
        run_id=msg.run_id,
        workload_name=msg.workload_name,
        reason=entry.reason,
    )


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
    current_user: OperatorUser,
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
        await _audit(
            control_plane,
            current_user,
            "run.cancel",
            "run",
            run_id,
            metadata={
                "workload_name": run.workload_name,
                "previous_status": previous_status,
                "status": run.status,
            },
        )
    return _run_response(run)


@router.get("/runs/{run_id}/events", response_model=list[RunEvent])
async def list_run_events(
    run_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
    after_id: int | None = Query(default=None, ge=0),
    limit: int = Query(default=200, ge=1, le=1000),
    tail: bool = Query(default=False),
) -> list[RunEvent]:
    await _authorize_run(run_id, control_plane, current_user)
    if tail and after_id is not None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="tail and after_id cannot be combined",
        )
    events = await control_plane.list_run_events(
        run_id, after_id=after_id, limit=limit, tail=tail
    )
    return [_event_response(event) for event in events]


@router.get("/runs/{run_id}/events/stream")
async def stream_run_events(
    run_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
    after_id: int | None = Query(default=None, ge=0),
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    await _authorize_run(run_id, control_plane, current_user)
    cursor = after_id
    if cursor is None and last_event_id is not None:
        try:
            cursor = int(last_event_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="Last-Event-ID must be a numeric run event id",
            ) from exc

    async def _events() -> AsyncGenerator[str]:
        nonlocal cursor
        while True:
            entries = await control_plane.list_run_events(
                run_id, after_id=cursor, limit=200
            )
            for event in entries:
                cursor = int(event.id)
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
    run = await _authorize_run(run_id, control_plane, current_user)
    artifacts = await control_plane.list_artifacts(run_id)
    return [_artifact_response(artifact, run=run) for artifact in artifacts]


@router.get(
    "/runs/{run_id}/artifacts/{artifact_id}/preview",
    response_model=ArtifactPreviewResponse,
)
async def preview_run_artifact(
    run_id: str,
    artifact_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
    max_bytes: int = Query(default=65536, ge=1, le=262144),
) -> ArtifactPreviewResponse:
    artifact = await _authorize_artifact(
        run_id,
        artifact_id,
        control_plane,
        current_user,
    )
    path = _artifact_filesystem_path(artifact, get_settings())
    content_type = _artifact_media_type(artifact, path)
    if not _is_previewable_content_type(content_type):
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Artifact content type {content_type!r} is not previewable",
        )

    size_bytes = path.stat().st_size
    with path.open("rb") as handle:
        preview_bytes = handle.read(max_bytes + 1)
    truncated = size_bytes > max_bytes or len(preview_bytes) > max_bytes
    preview_bytes = preview_bytes[:max_bytes]
    await _audit(
        control_plane,
        current_user,
        "artifact.preview",
        "artifact",
        artifact.id,
        metadata={"run_id": run_id, "name": artifact.name, "uri": artifact.uri},
    )
    return ArtifactPreviewResponse(
        artifact_id=artifact.id,
        run_id=run_id,
        name=artifact.name,
        content_type=content_type,
        text=preview_bytes.decode("utf-8", errors="replace"),
        truncated=truncated,
        size_bytes=size_bytes,
    )


@router.get("/runs/{run_id}/artifacts/{artifact_id}/download")
async def download_run_artifact(
    run_id: str,
    artifact_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> FileResponse:
    artifact = await _authorize_artifact(
        run_id,
        artifact_id,
        control_plane,
        current_user,
    )
    path = _artifact_filesystem_path(artifact, get_settings())
    await _audit(
        control_plane,
        current_user,
        "artifact.download",
        "artifact",
        artifact.id,
        metadata={"run_id": run_id, "name": artifact.name, "uri": artifact.uri},
    )
    return FileResponse(
        path,
        media_type=_artifact_media_type(artifact, path),
        filename=artifact.name,
    )


@router.get("/artifacts", response_model=list[RunArtifact])
async def list_artifacts(
    control_plane: ControlPlane,
    current_user: CurrentUser,
    workload_name: str | None = None,
    env: str | None = Query(default=None, min_length=1, max_length=64),
    session_id: str | None = None,
    run_id: str | None = None,
    content_type: str | None = None,
    created_from: str | None = None,
    created_to: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[RunArtifact]:
    workload_names = await _visible_workload_names_for_env(
        control_plane,
        current_user,
        env,
    )
    if run_id:
        run = await _authorize_run(run_id, control_plane, current_user)
        if workload_names is not None and run.workload_name not in workload_names:
            return []
        runs = [run]
    else:
        if workload_names is not None:
            target_workload_names = workload_names
            if workload_name is not None:
                if workload_name not in workload_names:
                    target_workload_names = set()
                else:
                    target_workload_names = {workload_name}
            candidate_runs = []
            for name in sorted(target_workload_names):
                candidate_runs.extend(
                    await _list_visible_runs(
                        control_plane,
                        current_user,
                        workload_name=name,
                        limit=200,
                        offset=0,
                    )
                )
            candidate_runs.sort(key=lambda run: run.created_at, reverse=True)
        else:
            candidate_runs = await _list_visible_runs(
                control_plane,
                current_user,
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
    current_user: OperatorUser,
) -> AgentSessionResponse:
    settings = get_settings()
    workload = await _get_visible_workload(name, control_plane, settings, current_user)
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
    current_user: OperatorUser,
) -> AgentMessageResponse:
    settings = get_settings()
    workload = await _get_visible_workload(name, control_plane, settings, current_user)
    if workload.spec.type != "agent-service":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Workload {name!r} is not an agent-service",
        )
    await _authorize_agent_session(name, session_id, control_plane, current_user)

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
    created_at = utc_now_iso()
    message = await control_plane.append_agent_message(
        session_id,
        "user",
        body.message,
        context={**body.context, "run_id": run.run_id},
        created_at=created_at,
    )
    await _audit(
        control_plane,
        current_user,
        "agent.message",
        "agent_session",
        session_id,
        metadata={
            "agent_name": name,
            "run_id": run.run_id,
            "message_id": message.message_id,
            "channel": "api",
        },
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
    current_user: OperatorUser,
) -> AgentMessageResponse:
    settings = get_settings()
    workload = await _get_visible_workload(name, control_plane, settings, current_user)
    if workload.spec.type != "agent-service":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Workload {name!r} is not an agent-service",
        )
    channel = _validate_agent_channel(workload, channel)
    await _authorize_channel_message_team_scope(body, control_plane, current_user)

    session_id = body.session_id
    if session_id is None:
        session = await control_plane.create_agent_session(
            str(uuid4()),
            name,
            current_user.subject,
            metadata={
                "channel": channel,
                "external_user_id": body.external_user_id,
                **({"team_id": body.team_id} if body.team_id else {}),
                **body.metadata,
            },
            created_at=utc_now_iso(),
        )
        session_id = session.session_id
    else:
        await _authorize_agent_session(name, session_id, control_plane, current_user)

    message_context = {
        "channel": channel,
        "external_user_id": body.external_user_id,
        **({"team_id": body.team_id} if body.team_id else {}),
        **body.metadata,
    }
    run = await _create_run(
        redis,
        control_plane,
        workload,
        {
            "session_id": session_id,
            "message": body.message,
            "context": message_context,
        },
        current_user.subject,
        session_id=session_id,
    )
    created_at = utc_now_iso()
    message = await control_plane.append_agent_message(
        session_id,
        "user",
        body.message,
        context={**message_context, "run_id": run.run_id},
        created_at=created_at,
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
        metadata={
            **({"team_id": body.team_id} if body.team_id else {}),
            **body.metadata,
        },
        created_at=created_at,
    )
    await _audit(
        control_plane,
        current_user,
        "channel.message",
        "agent_session",
        session_id,
        metadata={
            "agent_name": name,
            "run_id": run.run_id,
            "message_id": message.message_id,
            "channel": channel,
            "external_user_id": body.external_user_id,
            "team_id": body.team_id,
        },
    )
    return AgentMessageResponse(
        message_id=message.message_id,
        run_id=run.run_id,
        session_id=session_id,
        status=run.status,
        created_at=message.created_at,
    )


@router.post(
    "/webhooks/{channel}/agents/{name}/messages",
    response_model=AgentMessageResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@limiter.limit(_RATE_LIMIT_WEBHOOK_INGRESS)
async def post_signed_webhook_agent_message(
    channel: str,
    name: str,
    body: ChannelMessageRequest,
    request: Request,
    redis: RedisClient,
    control_plane: ControlPlane,
) -> AgentMessageResponse:
    settings = get_settings()
    raw_body = await request.body()
    _verify_webhook_signature(
        settings,
        request.headers.get("x-moiraweave-signature"),
        raw_body,
    )
    webhook_user = await _webhook_token_data(channel, body, control_plane)
    return await post_channel_agent_message(
        channel,
        name,
        body,
        redis,
        control_plane,
        webhook_user,
    )


@router.get("/agents/{name}/sessions")
async def list_agent_sessions(
    name: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]]:
    settings = get_settings()
    workload = await _get_visible_workload(name, control_plane, settings, current_user)
    if workload.spec.type != "agent-service":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Workload {name!r} is not an agent-service",
        )
    subjects = await _visible_subjects(control_plane, current_user)
    if subjects is None:
        sessions = await control_plane.list_agent_sessions(
            name, None, limit=limit, offset=offset
        )
    else:
        sessions = []
        for subject in sorted(subjects):
            sessions.extend(await control_plane.list_agent_sessions(name, subject))
        sessions.sort(key=lambda session: session.created_at, reverse=True)
        sessions = sessions[offset : offset + limit]
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
    before_id: int | None = Query(default=None, ge=1),
    limit: int = Query(default=100, ge=1, le=200),
) -> list[AgentMessageHistoryItem]:
    await _authorize_agent_session(name, session_id, control_plane, current_user)

    messages = await control_plane.list_agent_messages(
        session_id, before_id=before_id, limit=limit
    )
    runs = [
        run
        for run in await _list_visible_runs(
            control_plane,
            current_user,
            workload_name=name,
            limit=200,
            offset=0,
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
            events = await control_plane.list_run_events(run.run_id, limit=1, tail=True)
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
    runs = await _list_visible_runs(
        control_plane,
        current_user,
        workload_name=name,
        limit=50,
        offset=0,
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
