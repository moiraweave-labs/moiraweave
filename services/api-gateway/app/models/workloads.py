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


class DeploymentRequest(BaseModel):
    target: str = Field(pattern="^(local|kubernetes|k8s|external)$")
    status: str = "planned"
    endpoint: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DeploymentResponse(BaseModel):
    deployment_id: str
    workload_name: str
    target: str
    status: str
    user: str
    created_at: str
    updated_at: str | None = None
    endpoint: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkloadHealthResponse(BaseModel):
    workload_name: str
    status: str
    reason: str
    deployments: list[DeploymentResponse] = Field(default_factory=list)


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
