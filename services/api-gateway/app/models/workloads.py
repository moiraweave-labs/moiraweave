"""Pydantic models for workload, run, session, event, and artifact APIs."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class WorkloadInfo(BaseModel):
    name: str
    type: str
    execution_mode: str
    image: str | None = None
    manifest: dict[str, Any]


class RunRequest(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)


class RunResponse(BaseModel):
    run_id: str
    workload_name: str
    status: str
    created_at: str


class RunStatusResponse(BaseModel):
    run_id: str
    workload_name: str
    status: str
    user: str
    created_at: str
    updated_at: str | None = None
    heartbeat_at: str | None = None
    completed_at: str | None = None
    session_id: str | None = None
    payload: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    error: str | None = None


class RunEvent(BaseModel):
    id: str
    run_id: str
    timestamp: str
    type: str
    message: str
    data: dict[str, Any] = Field(default_factory=dict)


class RunArtifact(BaseModel):
    id: str
    run_id: str
    name: str
    uri: str
    content_type: str | None = None
    size_bytes: int | None = None
    created_at: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactPreviewResponse(BaseModel):
    artifact_id: str
    run_id: str
    name: str
    content_type: str | None = None
    text: str
    truncated: bool = False
    size_bytes: int


class WorkloadTemplateParameter(BaseModel):
    name: str
    label: str
    type: str = "string"
    required: bool = True
    default: Any | None = None
    description: str | None = None
    options: list[str] = Field(default_factory=list)


class WorkloadTemplateInfo(BaseModel):
    id: str
    name: str
    category: str
    description: str
    workload_type: str
    tags: list[str] = Field(default_factory=list)
    parameters: list[WorkloadTemplateParameter] = Field(default_factory=list)
    manifest: dict[str, Any] | None = None


class WorkloadFromTemplateRequest(BaseModel):
    template_id: str
    parameters: dict[str, Any] = Field(default_factory=dict)


class PreflightRequest(BaseModel):
    target: str = Field(default="local", pattern="^(local|kubernetes|k8s|external)$")
    env: str = Field(default="dev", min_length=1, max_length=64)


class PreflightCheck(BaseModel):
    name: str
    status: str = Field(pattern="^(passed|warning|failed)$")
    message: str
    remediation: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PreflightResponse(BaseModel):
    workload_name: str
    target: str
    status: str = Field(pattern="^(passed|warning|failed)$")
    checks: list[PreflightCheck]
    recommendations: list[str] = Field(default_factory=list)


class SecretInventoryItem(BaseModel):
    name: str
    present: bool
    source: str = "api-env"
    workloads: list[str] = Field(default_factory=list)
    references: list[str] = Field(default_factory=list)
    remediation: str | None = None


class SecretInventoryResponse(BaseModel):
    status: str = Field(pattern="^(passed|warning)$")
    total: int
    missing: int
    secrets: list[SecretInventoryItem]


class AgentSessionRequest(BaseModel):
    metadata: dict[str, Any] = Field(default_factory=dict)


class AgentSessionResponse(BaseModel):
    session_id: str
    agent_name: str
    status: str
    created_at: str


class AgentMessageRequest(BaseModel):
    message: str
    context: dict[str, Any] = Field(default_factory=dict)


class AgentMessageResponse(BaseModel):
    message_id: str
    run_id: str
    session_id: str
    status: str
    created_at: str


class AgentMessageHistoryItem(BaseModel):
    message_id: str
    session_id: str
    role: str
    message: str
    context: dict[str, Any] = Field(default_factory=dict)
    created_at: str
    run_id: str | None = None
    run_status: str | None = None
    latest_event: dict[str, Any] | None = None
    artifact_count: int = 0


class DeploymentRequest(BaseModel):
    target: str = Field(pattern="^(local|kubernetes|k8s|external)$")
    env: str = Field(default="local", min_length=1, max_length=64)
    status: str = "planned"
    endpoint: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DeploymentResponse(BaseModel):
    deployment_id: str
    workload_name: str
    target: str
    env: str = "local"
    status: str
    user: str
    created_at: str
    updated_at: str | None = None
    endpoint: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DeploymentPlanResponse(BaseModel):
    workload_name: str
    target: str
    mode: str
    service_name: str | None = None
    endpoint: str | None = None
    files: list[str] = Field(default_factory=list)
    commands: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class DeploymentOperationRequest(BaseModel):
    action: str = Field(pattern="^(plan|sync|apply|logs|undeploy)$")
    workload_name: str
    target: str = Field(default="local", pattern="^(local|kubernetes|k8s|external)$")
    env: str = Field(default="dev", min_length=1, max_length=64)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DeploymentOperationResponse(BaseModel):
    operation_id: str
    action: str
    workload_name: str
    target: str
    env: str = "dev"
    status: str
    user: str
    created_at: str
    updated_at: str | None = None
    completed_at: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DeploymentOperationEvent(BaseModel):
    id: str
    operation_id: str
    timestamp: str
    type: str
    message: str
    data: dict[str, Any] = Field(default_factory=dict)


class AuditEventResponse(BaseModel):
    event_id: str
    timestamp: str
    actor: str
    action: str
    resource_type: str
    resource_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkloadHealthResponse(BaseModel):
    workload_name: str
    status: str
    reason: str
    deployments: list[DeploymentResponse] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)


class AgentSessionHealthResponse(BaseModel):
    session_id: str
    agent_name: str
    status: str
    latest_run_status: str | None = None
    message_count: int


class ChannelMessageRequest(BaseModel):
    external_user_id: str
    message: str
    session_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
