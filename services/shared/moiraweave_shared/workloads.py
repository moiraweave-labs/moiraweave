"""Workload manifest models and loaders.

``workload.yaml`` is the single MoiraWeave deployment/runtime manifest.  The
same document drives local Compose, Kubernetes rendering, API validation, and
worker execution dispatch.
"""

# ruff: noqa: N815

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

WorkloadType = Literal["model-service", "pipeline", "agent-service"]
ExecutionMode = Literal["sync", "async", "session"]
DeploymentMode = Literal["managed", "external"]
DeploymentTarget = Literal["local", "kubernetes"]
ToolOwnership = Literal["runtime"]
NetworkEgressMode = Literal["disabled", "restricted", "enabled"]
BrowserRuntimeMode = Literal["none", "runtime-managed", "cloud", "sidecar"]
TerminalRuntimeMode = Literal[
    "none",
    "runtime-managed",
    "container",
    "ssh",
    "daytona",
    "modal",
]
RuntimeApprovalMode = Literal["runtime", "required", "none"]
RunStatus = Literal[
    "queued",
    "starting",
    "running",
    "cancel_requested",
    "cancelling",
    "succeeded",
    "failed",
    "canceled",
    "lost",
]
DeploymentOperationStatus = Literal[
    "queued",
    "running",
    "succeeded",
    "failed",
    "canceled",
]

TERMINAL_RUN_STATUSES: set[str] = {"succeeded", "failed", "canceled", "lost"}
ACTIVE_RUN_STATUSES: set[str] = {
    "queued",
    "starting",
    "running",
    "cancel_requested",
    "cancelling",
}


def default_deployment_targets() -> list[DeploymentTarget]:
    """Return the default places where MoiraWeave can deploy workloads."""

    return ["local", "kubernetes"]


VALID_RUN_TRANSITIONS: dict[str, set[str]] = {
    "queued": {"starting", "cancel_requested", "failed", "lost"},
    "starting": {
        "running",
        "cancel_requested",
        "cancelling",
        "succeeded",
        "failed",
        "canceled",
        "lost",
    },
    "running": {
        "cancel_requested",
        "cancelling",
        "succeeded",
        "failed",
        "canceled",
        "lost",
    },
    "cancel_requested": {"cancelling", "canceled", "failed", "lost"},
    "cancelling": {"canceled", "failed", "lost"},
    "succeeded": set(),
    "failed": set(),
    "canceled": set(),
    "lost": set(),
}

VALID_DEPLOYMENT_OPERATION_TRANSITIONS: dict[str, set[str]] = {
    "queued": {"running", "succeeded", "failed", "canceled"},
    "running": {"running", "succeeded", "failed", "canceled"},
    "succeeded": set(),
    "failed": set(),
    "canceled": set(),
}


class RunStateTransitionError(ValueError):
    """Raised when a run attempts an invalid lifecycle transition."""


class DeploymentOperationStateTransitionError(ValueError):
    """Raised when a deployment operation attempts an invalid transition."""


def is_valid_run_transition(current: str, target: str) -> bool:
    """Return whether a run can move from *current* to *target*."""

    return current == target or target in VALID_RUN_TRANSITIONS.get(current, set())


def ensure_run_transition(current: str, target: str) -> None:
    """Validate a run lifecycle transition."""

    if not is_valid_run_transition(current, target):
        raise RunStateTransitionError(
            f"Invalid run state transition: {current!r} -> {target!r}"
        )


def is_valid_deployment_operation_transition(current: str, target: str) -> bool:
    """Return whether a deployment operation can move between states."""

    return current == target or target in VALID_DEPLOYMENT_OPERATION_TRANSITIONS.get(
        current, set()
    )


def ensure_deployment_operation_transition(current: str, target: str) -> None:
    """Validate a deployment operation lifecycle transition."""

    if not is_valid_deployment_operation_transition(current, target):
        raise DeploymentOperationStateTransitionError(
            f"Invalid deployment operation state transition: {current!r} -> {target!r}"
        )


class WorkloadMetadata(BaseModel):
    """Identity and labels for a workload."""

    name: str
    labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("metadata.name cannot be empty")
        return cleaned


class WorkloadExecution(BaseModel):
    """Execution contract for one workload."""

    mode: ExecutionMode = "async"
    timeoutSeconds: int = Field(default=3600, ge=1)


class WorkloadPort(BaseModel):
    """Network port exposed by a workload runtime."""

    name: str
    port: int = Field(ge=1, le=65535)
    targetPort: int | None = Field(default=None, ge=1, le=65535)
    protocol: Literal["TCP", "UDP"] = "TCP"


class WorkloadPersistence(BaseModel):
    """Filesystem persistence requested by the workload."""

    enabled: bool = False
    mountPath: str | None = None
    size: str = "10Gi"
    storageClass: str | None = None

    @field_validator("mountPath")
    @classmethod
    def _validate_mount_path(cls, value: str | None) -> str | None:
        if value is not None and not value.startswith("/"):
            raise ValueError("persistence.mountPath must be absolute")
        return value


class WorkloadDeploymentSpec(BaseModel):
    """Where and how MoiraWeave should place the workload runtime."""

    mode: DeploymentMode = "managed"
    targets: list[DeploymentTarget] = Field(default_factory=default_deployment_targets)
    serviceName: str | None = None
    replicas: int = Field(default=1, ge=0)
    localNetwork: str = "moiraweave-net"
    namespace: str | None = None

    @field_validator("serviceName", "localNetwork", "namespace")
    @classmethod
    def _validate_optional_dns_label(cls, value: str | None) -> str | None:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("deployment names cannot be empty")
        return cleaned


class PipelineNode(BaseModel):
    """One node in a pipeline workload DAG."""

    id: str
    uses: str
    inputFrom: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class RuntimeCapabilityRequirement(BaseModel):
    """Generic runtime-owned capability requirement."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = False
    requiredSecrets: list[str] = Field(default_factory=list)


class RuntimeFilesystemRequirement(BaseModel):
    """Filesystem boundary required by the runtime-owned tools."""

    model_config = ConfigDict(extra="allow")

    persistentWorkspace: bool = False
    workspaceMount: str | None = None
    hostMounts: list[str] = Field(default_factory=list)

    @field_validator("workspaceMount")
    @classmethod
    def _validate_workspace_mount(cls, value: str | None) -> str | None:
        if value is not None and not value.startswith("/"):
            raise ValueError(
                "runtimeRequirements.filesystem.workspaceMount must be absolute"
            )
        return value


class RuntimeNetworkRequirement(BaseModel):
    """Network boundary required by the runtime-owned tools."""

    model_config = ConfigDict(extra="allow")

    egress: NetworkEgressMode = "restricted"


class RuntimeBrowserRequirement(BaseModel):
    """Browser automation boundary required by the runtime-owned tools."""

    model_config = ConfigDict(extra="allow")

    mode: BrowserRuntimeMode = "none"
    requiredSecrets: list[str] = Field(default_factory=list)


class RuntimeTerminalRequirement(BaseModel):
    """Terminal execution boundary required by the runtime-owned tools."""

    model_config = ConfigDict(extra="allow")

    mode: TerminalRuntimeMode = "none"
    approval: RuntimeApprovalMode = "runtime"
    requiredSecrets: list[str] = Field(default_factory=list)


class AgentRuntimeRequirements(BaseModel):
    """Environment MoiraWeave must provide while tools stay runtime-owned."""

    model_config = ConfigDict(extra="allow")

    filesystem: RuntimeFilesystemRequirement = Field(
        default_factory=RuntimeFilesystemRequirement
    )
    network: RuntimeNetworkRequirement = Field(
        default_factory=RuntimeNetworkRequirement
    )
    webSearch: RuntimeCapabilityRequirement = Field(
        default_factory=RuntimeCapabilityRequirement
    )
    browser: RuntimeBrowserRequirement = Field(
        default_factory=RuntimeBrowserRequirement
    )
    terminal: RuntimeTerminalRequirement = Field(
        default_factory=RuntimeTerminalRequirement
    )
    mcp: RuntimeCapabilityRequirement = Field(
        default_factory=RuntimeCapabilityRequirement
    )
    messaging: RuntimeCapabilityRequirement = Field(
        default_factory=RuntimeCapabilityRequirement
    )


class WorkloadAgentSpec(BaseModel):
    """Agent-runtime contract exposed through MoiraWeave."""

    model_config = ConfigDict(extra="allow")

    adapter: Literal["generic-http", "generic", "hermes", "openclaw"] = "generic-http"
    toolOwnership: ToolOwnership = "runtime"
    runtimeRequirements: AgentRuntimeRequirements = Field(
        default_factory=AgentRuntimeRequirements
    )
    capabilities: list[str] = Field(default_factory=list)
    configSchema: dict[str, Any] = Field(default_factory=dict)
    workspaceMount: str | None = None
    requiredSecrets: list[str] = Field(default_factory=list)
    exposedChannels: list[str] = Field(default_factory=lambda: ["ui", "api"])
    externalOwnedChannels: list[str] = Field(default_factory=list)
    authTokenEnv: str | None = None
    agentId: str | None = None
    model: str | None = None
    instructions: str | None = None
    messagePath: str | None = None
    statusPath: str | None = None
    cancelPath: str | None = None
    artifactsPath: str | None = None
    dispatchTimeoutSeconds: float = Field(default=30.0, ge=0.1)
    pollIntervalSeconds: float = Field(default=2.0, ge=0.1)

    @field_validator("workspaceMount")
    @classmethod
    def _validate_workspace_mount(cls, value: str | None) -> str | None:
        if value is not None and not value.startswith("/"):
            raise ValueError("agent.workspaceMount must be absolute")
        return value

    @field_validator("exposedChannels", "externalOwnedChannels")
    @classmethod
    def _normalize_channels(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for channel in value:
            cleaned = str(channel).strip().lower()
            if not cleaned:
                raise ValueError("agent channels cannot be empty")
            if cleaned not in seen:
                seen.add(cleaned)
                normalized.append(cleaned)
        return normalized


class WorkloadSpec(BaseModel):
    """Runtime and deployment intent for a workload."""

    model_config = ConfigDict(extra="allow")

    type: WorkloadType
    image: str | None = None
    deployment: WorkloadDeploymentSpec = Field(default_factory=WorkloadDeploymentSpec)
    execution: WorkloadExecution = Field(default_factory=WorkloadExecution)
    ports: list[WorkloadPort] = Field(default_factory=list)
    persistence: WorkloadPersistence = Field(default_factory=WorkloadPersistence)
    secrets: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    resources: dict[str, Any] = Field(default_factory=dict)
    steps: list[PipelineNode] = Field(default_factory=list)
    endpoint: str | None = None
    adapter: str | None = None
    agent: WorkloadAgentSpec = Field(default_factory=WorkloadAgentSpec)
    command: list[str] | None = None
    args: list[str] | None = None

    @model_validator(mode="after")
    def _validate_runtime_location(self) -> WorkloadSpec:
        if self.type == "pipeline":
            return self
        if self.deployment.mode == "external":
            if not self.endpoint:
                raise ValueError(
                    "spec.endpoint is required when deployment.mode is external"
                )
            return self
        if not self.image:
            raise ValueError(
                "spec.image is required for managed model-service and agent-service"
            )
        return self


class WorkloadDefinition(BaseModel):
    """Full ``workload.yaml`` document."""

    apiVersion: Literal["moiraweave.io/v1alpha1"] = "moiraweave.io/v1alpha1"
    kind: Literal["Workload"] = "Workload"
    metadata: WorkloadMetadata
    spec: WorkloadSpec

    @classmethod
    def from_yaml(cls, path: Path) -> WorkloadDefinition:
        """Load and validate a workload definition from a YAML file."""

        data: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return cls.model_validate(data)

    def to_manifest(self) -> dict[str, Any]:
        """Return a YAML/JSON friendly manifest dictionary."""

        return self.model_dump(mode="json", exclude_none=True)


def load_workloads(workloads_dir: str | Path) -> list[WorkloadDefinition]:
    """Load all ``workload.yaml`` files under *workloads_dir*."""

    base = Path(workloads_dir)
    if not base.exists() or not base.is_dir():
        return []

    workloads: list[WorkloadDefinition] = []
    for yaml_path in sorted(base.glob("*/workload.yaml")):
        workloads.append(WorkloadDefinition.from_yaml(yaml_path))
    return sorted(workloads, key=lambda workload: workload.metadata.name)
