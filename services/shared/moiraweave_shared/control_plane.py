"""Control-plane storage abstractions for workloads, runs, sessions, and events."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel, Field

from moiraweave_shared.alembic.control_plane_schema import (
    CONTROL_PLANE_ALEMBIC_BASELINE,
)
from moiraweave_shared.workloads import (
    WorkloadDefinition,
    ensure_deployment_operation_transition,
    ensure_run_transition,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


HEARTBEAT_RUN_STATUSES = {"starting", "running", "cancelling"}


def utc_now_iso() -> str:
    """Return an ISO-8601 UTC timestamp."""

    return datetime.now(UTC).isoformat()


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _pg_timestamp(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def _json_dict(value: Any) -> dict[str, Any] | None:
    parsed = _json_value(value)
    return parsed if isinstance(parsed, dict) else None


def _row_get(row: Any, key: str) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return None


class StoredRun(BaseModel):
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


class StoredRunEvent(BaseModel):
    id: str
    run_id: str
    timestamp: str
    type: str
    message: str
    data: dict[str, Any] = Field(default_factory=dict)


class StoredArtifact(BaseModel):
    id: str
    run_id: str
    name: str
    uri: str
    content_type: str | None = None
    size_bytes: int | None = None
    created_at: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class StoredAgentSession(BaseModel):
    session_id: str
    agent_name: str
    status: str
    user: str
    created_at: str
    updated_at: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class StoredAgentMessage(BaseModel):
    message_id: str
    session_id: str
    role: str
    message: str
    context: dict[str, Any] = Field(default_factory=dict)
    created_at: str


class StoredDeployment(BaseModel):
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


class StoredDeploymentOperation(BaseModel):
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
    lease_expires_at: str | None = None
    controller_id: str | None = None
    heartbeat_at: str | None = None
    timeout_seconds: int | None = None
    stdout_summary: str | None = None
    stderr_summary: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class StoredDeploymentOperationEvent(BaseModel):
    id: str
    operation_id: str
    timestamp: str
    type: str
    message: str
    data: dict[str, Any] = Field(default_factory=dict)


class StoredChannelMessage(BaseModel):
    message_id: str
    channel: str
    agent_name: str
    external_user_id: str
    session_id: str
    run_id: str | None
    direction: str
    message: str
    user: str
    created_at: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class StoredAuditEvent(BaseModel):
    event_id: str
    timestamp: str
    actor: str
    action: str
    resource_type: str
    resource_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class StoredApiKey(BaseModel):
    key_id: str
    name: str
    secret_hash: str
    secret_prefix: str
    subject: str
    role: str
    created_by: str
    created_at: str
    team_id: str | None = None
    last_used_at: str | None = None
    revoked_at: str | None = None


class StoredUser(BaseModel):
    subject: str
    display_name: str | None = None
    password_hash: str
    role: str
    created_by: str
    created_at: str
    updated_at: str
    disabled_at: str | None = None


class StoredTeam(BaseModel):
    team_id: str
    name: str
    description: str | None = None
    created_by: str
    created_at: str
    updated_at: str


class StoredTeamMember(BaseModel):
    team_id: str
    subject: str
    role: str
    created_by: str
    created_at: str


class ControlPlaneRepository(Protocol):
    async def init(self) -> None: ...

    async def ping(self) -> None: ...

    async def close(self) -> None: ...

    async def upsert_workload(
        self, workload: WorkloadDefinition, user: str, *, now: str | None = None
    ) -> None: ...

    async def list_workloads(self) -> list[WorkloadDefinition]: ...

    async def get_workload(self, name: str) -> WorkloadDefinition | None: ...

    async def create_run(
        self,
        run_id: str,
        workload_name: str,
        payload: dict[str, Any],
        user: str,
        *,
        created_at: str,
        session_id: str | None = None,
    ) -> StoredRun: ...

    async def list_runs(
        self,
        user: str | None,
        *,
        workload_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[StoredRun]: ...

    async def get_run(self, run_id: str) -> StoredRun | None: ...

    async def update_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        result: dict[str, Any] | None = None,
        error: str | None = None,
        heartbeat_at: str | None = None,
        completed_at: str | None = None,
        updated_at: str | None = None,
    ) -> StoredRun | None: ...

    async def append_run_event(
        self,
        run_id: str,
        event_type: str,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        timestamp: str | None = None,
    ) -> StoredRunEvent: ...

    async def list_run_events(self, run_id: str) -> list[StoredRunEvent]: ...

    async def record_artifact(
        self,
        run_id: str,
        artifact: dict[str, Any],
        *,
        fallback_index: int = 0,
    ) -> StoredArtifact: ...

    async def list_artifacts(self, run_id: str) -> list[StoredArtifact]: ...

    async def create_agent_session(
        self,
        session_id: str,
        agent_name: str,
        user: str,
        *,
        metadata: dict[str, Any] | None = None,
        created_at: str,
    ) -> StoredAgentSession: ...

    async def get_agent_session(self, session_id: str) -> StoredAgentSession | None: ...

    async def list_agent_sessions(
        self, agent_name: str, user: str | None
    ) -> list[StoredAgentSession]: ...

    async def append_agent_message(
        self,
        session_id: str,
        role: str,
        message: str,
        *,
        context: dict[str, Any] | None = None,
        created_at: str,
    ) -> StoredAgentMessage: ...

    async def list_agent_messages(
        self, session_id: str
    ) -> list[StoredAgentMessage]: ...

    async def upsert_deployment(
        self,
        deployment_id: str,
        workload_name: str,
        target: str,
        status: str,
        user: str,
        *,
        env: str = "local",
        endpoint: str | None = None,
        metadata: dict[str, Any] | None = None,
        now: str | None = None,
    ) -> StoredDeployment: ...

    async def list_deployments(
        self,
        user: str | None,
        *,
        workload_name: str | None = None,
        env: str | None = None,
    ) -> list[StoredDeployment]: ...

    async def get_deployment(self, deployment_id: str) -> StoredDeployment | None: ...

    async def create_deployment_operation(
        self,
        operation_id: str,
        action: str,
        workload_name: str,
        target: str,
        status: str,
        user: str,
        *,
        env: str = "dev",
        metadata: dict[str, Any] | None = None,
        now: str | None = None,
        completed_at: str | None = None,
        lease_expires_at: str | None = None,
        controller_id: str | None = None,
        heartbeat_at: str | None = None,
        timeout_seconds: int | None = None,
        stdout_summary: str | None = None,
        stderr_summary: str | None = None,
    ) -> StoredDeploymentOperation: ...

    async def get_deployment_operation(
        self, operation_id: str
    ) -> StoredDeploymentOperation | None: ...

    async def update_deployment_operation(
        self,
        operation_id: str,
        *,
        status: str,
        metadata: dict[str, Any],
        updated_at: str | None = None,
        completed_at: str | None = None,
        lease_expires_at: str | None = None,
        controller_id: str | None = None,
        heartbeat_at: str | None = None,
        timeout_seconds: int | None = None,
        stdout_summary: str | None = None,
        stderr_summary: str | None = None,
    ) -> StoredDeploymentOperation: ...

    async def list_deployment_operations(
        self,
        user: str | None,
        *,
        workload_name: str | None = None,
        target: str | None = None,
        env: str | None = None,
        status: str | None = None,
        action: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[StoredDeploymentOperation]: ...

    async def append_deployment_operation_event(
        self,
        operation_id: str,
        event_type: str,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        timestamp: str | None = None,
    ) -> StoredDeploymentOperationEvent: ...

    async def list_deployment_operation_events(
        self, operation_id: str
    ) -> list[StoredDeploymentOperationEvent]: ...

    async def record_channel_message(
        self,
        channel: str,
        agent_name: str,
        external_user_id: str,
        session_id: str,
        direction: str,
        message: str,
        user: str,
        *,
        run_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        created_at: str,
    ) -> StoredChannelMessage: ...

    async def record_audit_event(
        self,
        actor: str,
        action: str,
        resource_type: str,
        resource_id: str,
        *,
        metadata: dict[str, Any] | None = None,
        timestamp: str | None = None,
    ) -> StoredAuditEvent: ...

    async def list_audit_events(
        self,
        actor: str | None,
        *,
        action: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        env: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[StoredAuditEvent]: ...

    async def create_api_key(
        self,
        key_id: str,
        name: str,
        secret_hash: str,
        secret_prefix: str,
        subject: str,
        role: str,
        created_by: str,
        *,
        team_id: str | None = None,
        now: str | None = None,
    ) -> StoredApiKey: ...

    async def list_api_keys(self) -> list[StoredApiKey]: ...

    async def get_api_key_by_secret_hash(
        self, secret_hash: str
    ) -> StoredApiKey | None: ...

    async def touch_api_key(
        self, key_id: str, *, last_used_at: str | None = None
    ) -> StoredApiKey | None: ...

    async def revoke_api_key(
        self, key_id: str, *, revoked_at: str | None = None
    ) -> StoredApiKey | None: ...

    async def upsert_user(
        self,
        subject: str,
        password_hash: str,
        role: str,
        created_by: str,
        *,
        display_name: str | None = None,
        now: str | None = None,
    ) -> StoredUser: ...

    async def get_user(self, subject: str) -> StoredUser | None: ...

    async def list_users(self) -> list[StoredUser]: ...

    async def update_user(
        self,
        subject: str,
        *,
        display_name: str | None = None,
        role: str | None = None,
        password_hash: str | None = None,
        updated_at: str | None = None,
    ) -> StoredUser | None: ...

    async def disable_user(
        self, subject: str, *, disabled_at: str | None = None
    ) -> StoredUser | None: ...

    async def enable_user(
        self, subject: str, *, updated_at: str | None = None
    ) -> StoredUser | None: ...

    async def upsert_team(
        self,
        team_id: str,
        name: str,
        created_by: str,
        *,
        description: str | None = None,
        now: str | None = None,
    ) -> StoredTeam: ...

    async def list_teams(self) -> list[StoredTeam]: ...

    async def update_team(
        self,
        team_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        updated_at: str | None = None,
    ) -> StoredTeam | None: ...

    async def add_team_member(
        self,
        team_id: str,
        subject: str,
        role: str,
        created_by: str,
        *,
        now: str | None = None,
    ) -> StoredTeamMember: ...

    async def list_team_members(
        self, *, team_id: str | None = None, subject: str | None = None
    ) -> list[StoredTeamMember]: ...

    async def remove_team_member(
        self, team_id: str, subject: str
    ) -> StoredTeamMember | None: ...

    async def find_stale_runs(
        self, *, before: str, statuses: Iterable[str] = HEARTBEAT_RUN_STATUSES
    ) -> list[StoredRun]: ...


class InMemoryControlPlaneRepository:
    """Small async repository for tests and local unit-level usage."""

    def __init__(self) -> None:
        self.workloads: dict[str, WorkloadDefinition] = {}
        self.runs: dict[str, StoredRun] = {}
        self.events: dict[str, list[StoredRunEvent]] = {}
        self.artifacts: dict[str, list[StoredArtifact]] = {}
        self.sessions: dict[str, StoredAgentSession] = {}
        self.messages: dict[str, list[StoredAgentMessage]] = {}
        self.deployments: dict[str, StoredDeployment] = {}
        self.deployment_operations: dict[str, StoredDeploymentOperation] = {}
        self.deployment_operation_events: dict[
            str, list[StoredDeploymentOperationEvent]
        ] = {}
        self.channel_messages: list[StoredChannelMessage] = []
        self.audit_events: list[StoredAuditEvent] = []
        self.api_keys: dict[str, StoredApiKey] = {}
        self.users: dict[str, StoredUser] = {}
        self.teams: dict[str, StoredTeam] = {}
        self.team_members: dict[tuple[str, str], StoredTeamMember] = {}
        self._event_id = 0
        self._message_id = 0
        self._deployment_operation_event_id = 0
        self._channel_message_id = 0
        self._audit_event_id = 0

    async def init(self) -> None:
        return None

    async def ping(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def upsert_workload(
        self, workload: WorkloadDefinition, user: str, *, now: str | None = None
    ) -> None:
        del user, now
        self.workloads[workload.metadata.name] = workload

    async def list_workloads(self) -> list[WorkloadDefinition]:
        return list(self.workloads.values())

    async def get_workload(self, name: str) -> WorkloadDefinition | None:
        return self.workloads.get(name)

    async def create_run(
        self,
        run_id: str,
        workload_name: str,
        payload: dict[str, Any],
        user: str,
        *,
        created_at: str,
        session_id: str | None = None,
    ) -> StoredRun:
        run = StoredRun(
            run_id=run_id,
            workload_name=workload_name,
            status="queued",
            user=user,
            created_at=created_at,
            updated_at=created_at,
            payload=payload,
            session_id=session_id,
        )
        self.runs[run_id] = run
        return run

    async def list_runs(
        self,
        user: str | None,
        *,
        workload_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[StoredRun]:
        runs = [
            run
            for run in self.runs.values()
            if (user is None or run.user == user)
            and (workload_name is None or run.workload_name == workload_name)
        ]
        sorted_runs = sorted(runs, key=lambda run: run.created_at, reverse=True)
        return sorted_runs[offset : offset + limit]

    async def get_run(self, run_id: str) -> StoredRun | None:
        return self.runs.get(run_id)

    async def update_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        result: dict[str, Any] | None = None,
        error: str | None = None,
        heartbeat_at: str | None = None,
        completed_at: str | None = None,
        updated_at: str | None = None,
    ) -> StoredRun | None:
        run = self.runs.get(run_id)
        if run is None:
            return None
        data = run.model_dump()
        if status is not None:
            ensure_run_transition(run.status, status)
            data["status"] = status
        if result is not None:
            data["result"] = result
        if error is not None:
            data["error"] = error
        if heartbeat_at is not None:
            data["heartbeat_at"] = heartbeat_at
        if completed_at is not None:
            data["completed_at"] = completed_at
        data["updated_at"] = updated_at or utc_now_iso()
        updated = StoredRun.model_validate(data)
        self.runs[run_id] = updated
        return updated

    async def append_run_event(
        self,
        run_id: str,
        event_type: str,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        timestamp: str | None = None,
    ) -> StoredRunEvent:
        self._event_id += 1
        event = StoredRunEvent(
            id=str(self._event_id),
            run_id=run_id,
            timestamp=timestamp or utc_now_iso(),
            type=event_type,
            message=message,
            data=data or {},
        )
        self.events.setdefault(run_id, []).append(event)
        return event

    async def list_run_events(self, run_id: str) -> list[StoredRunEvent]:
        return list(self.events.get(run_id, []))

    async def record_artifact(
        self,
        run_id: str,
        artifact: dict[str, Any],
        *,
        fallback_index: int = 0,
    ) -> StoredArtifact:
        item = _artifact_from_dict(run_id, artifact, fallback_index)
        bucket = self.artifacts.setdefault(run_id, [])
        bucket[:] = [existing for existing in bucket if existing.id != item.id]
        bucket.append(item)
        return item

    async def list_artifacts(self, run_id: str) -> list[StoredArtifact]:
        return list(self.artifacts.get(run_id, []))

    async def create_agent_session(
        self,
        session_id: str,
        agent_name: str,
        user: str,
        *,
        metadata: dict[str, Any] | None = None,
        created_at: str,
    ) -> StoredAgentSession:
        session = StoredAgentSession(
            session_id=session_id,
            agent_name=agent_name,
            status="active",
            user=user,
            created_at=created_at,
            updated_at=created_at,
            metadata=metadata or {},
        )
        self.sessions[session_id] = session
        return session

    async def get_agent_session(self, session_id: str) -> StoredAgentSession | None:
        return self.sessions.get(session_id)

    async def list_agent_sessions(
        self, agent_name: str, user: str | None
    ) -> list[StoredAgentSession]:
        sessions = [
            session
            for session in self.sessions.values()
            if session.agent_name == agent_name
            and (user is None or session.user == user)
        ]
        return sorted(sessions, key=lambda session: session.created_at, reverse=True)

    async def append_agent_message(
        self,
        session_id: str,
        role: str,
        message: str,
        *,
        context: dict[str, Any] | None = None,
        created_at: str,
    ) -> StoredAgentMessage:
        self._message_id += 1
        stored = StoredAgentMessage(
            message_id=str(self._message_id),
            session_id=session_id,
            role=role,
            message=message,
            context=context or {},
            created_at=created_at,
        )
        self.messages.setdefault(session_id, []).append(stored)
        return stored

    async def list_agent_messages(self, session_id: str) -> list[StoredAgentMessage]:
        return list(self.messages.get(session_id, []))

    async def upsert_deployment(
        self,
        deployment_id: str,
        workload_name: str,
        target: str,
        status: str,
        user: str,
        *,
        env: str = "local",
        endpoint: str | None = None,
        metadata: dict[str, Any] | None = None,
        now: str | None = None,
    ) -> StoredDeployment:
        timestamp = now or utc_now_iso()
        existing = next(
            (
                item
                for item in self.deployments.values()
                if item.workload_name == workload_name
                and item.target == target
                and item.env == env
                and item.user == user
            ),
            None,
        )
        deployment = StoredDeployment(
            deployment_id=existing.deployment_id if existing else deployment_id,
            workload_name=workload_name,
            target=target,
            env=env,
            status=status,
            user=user,
            endpoint=endpoint,
            metadata=metadata or {},
            created_at=existing.created_at if existing else timestamp,
            updated_at=timestamp,
        )
        self.deployments[deployment.deployment_id] = deployment
        return deployment

    async def list_deployments(
        self,
        user: str | None,
        *,
        workload_name: str | None = None,
        env: str | None = None,
    ) -> list[StoredDeployment]:
        deployments = [
            deployment
            for deployment in self.deployments.values()
            if (user is None or deployment.user == user)
            and (workload_name is None or deployment.workload_name == workload_name)
            and (env is None or deployment.env == env)
        ]
        return sorted(deployments, key=lambda item: item.updated_at or "", reverse=True)

    async def get_deployment(self, deployment_id: str) -> StoredDeployment | None:
        return self.deployments.get(deployment_id)

    async def create_deployment_operation(
        self,
        operation_id: str,
        action: str,
        workload_name: str,
        target: str,
        status: str,
        user: str,
        *,
        env: str = "dev",
        metadata: dict[str, Any] | None = None,
        now: str | None = None,
        completed_at: str | None = None,
        lease_expires_at: str | None = None,
        controller_id: str | None = None,
        heartbeat_at: str | None = None,
        timeout_seconds: int | None = None,
        stdout_summary: str | None = None,
        stderr_summary: str | None = None,
    ) -> StoredDeploymentOperation:
        timestamp = now or utc_now_iso()
        operation = StoredDeploymentOperation(
            operation_id=operation_id,
            action=action,
            workload_name=workload_name,
            target=target,
            env=env,
            status=status,
            user=user,
            metadata=metadata or {},
            created_at=timestamp,
            updated_at=timestamp,
            completed_at=completed_at,
            lease_expires_at=lease_expires_at,
            controller_id=controller_id,
            heartbeat_at=heartbeat_at,
            timeout_seconds=timeout_seconds,
            stdout_summary=stdout_summary,
            stderr_summary=stderr_summary,
        )
        self.deployment_operations[operation_id] = operation
        return operation

    async def get_deployment_operation(
        self, operation_id: str
    ) -> StoredDeploymentOperation | None:
        return self.deployment_operations.get(operation_id)

    async def list_deployment_operations(
        self,
        user: str | None,
        *,
        workload_name: str | None = None,
        target: str | None = None,
        env: str | None = None,
        status: str | None = None,
        action: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[StoredDeploymentOperation]:
        operations = [
            operation
            for operation in self.deployment_operations.values()
            if (user is None or operation.user == user)
            and (workload_name is None or operation.workload_name == workload_name)
            and (target is None or operation.target == target)
            and (env is None or operation.env == env)
            and (status is None or operation.status == status)
            and (action is None or operation.action == action)
        ]
        operations.sort(key=lambda item: item.created_at, reverse=True)
        return operations[offset : offset + limit]

    async def update_deployment_operation(
        self,
        operation_id: str,
        *,
        status: str,
        metadata: dict[str, Any],
        updated_at: str | None = None,
        completed_at: str | None = None,
        lease_expires_at: str | None = None,
        controller_id: str | None = None,
        heartbeat_at: str | None = None,
        timeout_seconds: int | None = None,
        stdout_summary: str | None = None,
        stderr_summary: str | None = None,
    ) -> StoredDeploymentOperation:
        operation = self.deployment_operations[operation_id]
        ensure_deployment_operation_transition(operation.status, status)
        updated = operation.model_copy(
            update={
                "status": status,
                "metadata": metadata,
                "updated_at": updated_at or utc_now_iso(),
                "completed_at": completed_at,
                "lease_expires_at": lease_expires_at
                if lease_expires_at is not None
                else operation.lease_expires_at,
                "controller_id": controller_id
                if controller_id is not None
                else operation.controller_id,
                "heartbeat_at": heartbeat_at
                if heartbeat_at is not None
                else operation.heartbeat_at,
                "timeout_seconds": timeout_seconds
                if timeout_seconds is not None
                else operation.timeout_seconds,
                "stdout_summary": stdout_summary
                if stdout_summary is not None
                else operation.stdout_summary,
                "stderr_summary": stderr_summary
                if stderr_summary is not None
                else operation.stderr_summary,
            }
        )
        self.deployment_operations[operation_id] = updated
        return updated

    async def append_deployment_operation_event(
        self,
        operation_id: str,
        event_type: str,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        timestamp: str | None = None,
    ) -> StoredDeploymentOperationEvent:
        self._deployment_operation_event_id += 1
        event = StoredDeploymentOperationEvent(
            id=str(self._deployment_operation_event_id),
            operation_id=operation_id,
            timestamp=timestamp or utc_now_iso(),
            type=event_type,
            message=message,
            data=data or {},
        )
        self.deployment_operation_events.setdefault(operation_id, []).append(event)
        return event

    async def list_deployment_operation_events(
        self, operation_id: str
    ) -> list[StoredDeploymentOperationEvent]:
        return list(self.deployment_operation_events.get(operation_id, []))

    async def record_channel_message(
        self,
        channel: str,
        agent_name: str,
        external_user_id: str,
        session_id: str,
        direction: str,
        message: str,
        user: str,
        *,
        run_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        created_at: str,
    ) -> StoredChannelMessage:
        self._channel_message_id += 1
        stored = StoredChannelMessage(
            message_id=str(self._channel_message_id),
            channel=channel,
            agent_name=agent_name,
            external_user_id=external_user_id,
            session_id=session_id,
            run_id=run_id,
            direction=direction,
            message=message,
            user=user,
            metadata=metadata or {},
            created_at=created_at,
        )
        self.channel_messages.append(stored)
        return stored

    async def record_audit_event(
        self,
        actor: str,
        action: str,
        resource_type: str,
        resource_id: str,
        *,
        metadata: dict[str, Any] | None = None,
        timestamp: str | None = None,
    ) -> StoredAuditEvent:
        self._audit_event_id += 1
        event = StoredAuditEvent(
            event_id=str(self._audit_event_id),
            timestamp=timestamp or utc_now_iso(),
            actor=actor,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            metadata=metadata or {},
        )
        self.audit_events.append(event)
        return event

    async def list_audit_events(
        self,
        actor: str | None,
        *,
        action: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        env: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[StoredAuditEvent]:
        events = [
            event
            for event in self.audit_events
            if (actor is None or event.actor == actor)
            and (action is None or event.action == action)
            and (resource_type is None or event.resource_type == resource_type)
            and (resource_id is None or event.resource_id == resource_id)
            and (
                env is None
                or event.metadata.get("env") == env
                or event.metadata.get("environment") == env
            )
        ]
        events.sort(key=lambda event: event.timestamp, reverse=True)
        return events[offset : offset + limit]

    async def create_api_key(
        self,
        key_id: str,
        name: str,
        secret_hash: str,
        secret_prefix: str,
        subject: str,
        role: str,
        created_by: str,
        *,
        team_id: str | None = None,
        now: str | None = None,
    ) -> StoredApiKey:
        timestamp = now or utc_now_iso()
        api_key = StoredApiKey(
            key_id=key_id,
            name=name,
            secret_hash=secret_hash,
            secret_prefix=secret_prefix,
            subject=subject,
            role=role,
            created_by=created_by,
            created_at=timestamp,
            team_id=team_id,
        )
        self.api_keys[key_id] = api_key
        return api_key

    async def list_api_keys(self) -> list[StoredApiKey]:
        keys = list(self.api_keys.values())
        return sorted(keys, key=lambda item: item.created_at, reverse=True)

    async def get_api_key_by_secret_hash(self, secret_hash: str) -> StoredApiKey | None:
        return next(
            (
                item
                for item in self.api_keys.values()
                if item.secret_hash == secret_hash and item.revoked_at is None
            ),
            None,
        )

    async def touch_api_key(
        self, key_id: str, *, last_used_at: str | None = None
    ) -> StoredApiKey | None:
        api_key = self.api_keys.get(key_id)
        if api_key is None:
            return None
        updated = api_key.model_copy(
            update={"last_used_at": last_used_at or utc_now_iso()}
        )
        self.api_keys[key_id] = updated
        return updated

    async def revoke_api_key(
        self, key_id: str, *, revoked_at: str | None = None
    ) -> StoredApiKey | None:
        api_key = self.api_keys.get(key_id)
        if api_key is None:
            return None
        updated = api_key.model_copy(
            update={"revoked_at": api_key.revoked_at or revoked_at or utc_now_iso()}
        )
        self.api_keys[key_id] = updated
        return updated

    async def upsert_user(
        self,
        subject: str,
        password_hash: str,
        role: str,
        created_by: str,
        *,
        display_name: str | None = None,
        now: str | None = None,
    ) -> StoredUser:
        timestamp = now or utc_now_iso()
        existing = self.users.get(subject)
        user = StoredUser(
            subject=subject,
            display_name=display_name,
            password_hash=password_hash,
            role=role,
            created_by=existing.created_by if existing else created_by,
            created_at=existing.created_at if existing else timestamp,
            updated_at=timestamp,
            disabled_at=existing.disabled_at if existing else None,
        )
        self.users[subject] = user
        return user

    async def get_user(self, subject: str) -> StoredUser | None:
        return self.users.get(subject)

    async def list_users(self) -> list[StoredUser]:
        users = list(self.users.values())
        return sorted(users, key=lambda item: item.created_at, reverse=True)

    async def update_user(
        self,
        subject: str,
        *,
        display_name: str | None = None,
        role: str | None = None,
        password_hash: str | None = None,
        updated_at: str | None = None,
    ) -> StoredUser | None:
        user = self.users.get(subject)
        if user is None:
            return None
        update: dict[str, Any] = {"updated_at": updated_at or utc_now_iso()}
        if display_name is not None:
            update["display_name"] = display_name
        if role is not None:
            update["role"] = role
        if password_hash is not None:
            update["password_hash"] = password_hash
        updated = user.model_copy(update=update)
        self.users[subject] = updated
        return updated

    async def disable_user(
        self, subject: str, *, disabled_at: str | None = None
    ) -> StoredUser | None:
        user = self.users.get(subject)
        if user is None:
            return None
        updated = user.model_copy(
            update={
                "disabled_at": user.disabled_at or disabled_at or utc_now_iso(),
                "updated_at": utc_now_iso(),
            }
        )
        self.users[subject] = updated
        return updated

    async def enable_user(
        self, subject: str, *, updated_at: str | None = None
    ) -> StoredUser | None:
        user = self.users.get(subject)
        if user is None:
            return None
        updated = user.model_copy(
            update={"disabled_at": None, "updated_at": updated_at or utc_now_iso()}
        )
        self.users[subject] = updated
        return updated

    async def upsert_team(
        self,
        team_id: str,
        name: str,
        created_by: str,
        *,
        description: str | None = None,
        now: str | None = None,
    ) -> StoredTeam:
        timestamp = now or utc_now_iso()
        existing = self.teams.get(team_id)
        team = StoredTeam(
            team_id=team_id,
            name=name,
            description=description,
            created_by=existing.created_by if existing else created_by,
            created_at=existing.created_at if existing else timestamp,
            updated_at=timestamp,
        )
        self.teams[team_id] = team
        return team

    async def list_teams(self) -> list[StoredTeam]:
        teams = list(self.teams.values())
        return sorted(teams, key=lambda item: item.created_at, reverse=True)

    async def update_team(
        self,
        team_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        updated_at: str | None = None,
    ) -> StoredTeam | None:
        team = self.teams.get(team_id)
        if team is None:
            return None
        update: dict[str, Any] = {"updated_at": updated_at or utc_now_iso()}
        if name is not None:
            update["name"] = name
        if description is not None:
            update["description"] = description
        updated = team.model_copy(update=update)
        self.teams[team_id] = updated
        return updated

    async def add_team_member(
        self,
        team_id: str,
        subject: str,
        role: str,
        created_by: str,
        *,
        now: str | None = None,
    ) -> StoredTeamMember:
        member = StoredTeamMember(
            team_id=team_id,
            subject=subject,
            role=role,
            created_by=created_by,
            created_at=now or utc_now_iso(),
        )
        self.team_members[(team_id, subject)] = member
        return member

    async def list_team_members(
        self, *, team_id: str | None = None, subject: str | None = None
    ) -> list[StoredTeamMember]:
        members = [
            item
            for item in self.team_members.values()
            if (team_id is None or item.team_id == team_id)
            and (subject is None or item.subject == subject)
        ]
        return sorted(members, key=lambda item: item.created_at, reverse=True)

    async def remove_team_member(
        self, team_id: str, subject: str
    ) -> StoredTeamMember | None:
        return self.team_members.pop((team_id, subject), None)

    async def find_stale_runs(
        self, *, before: str, statuses: Iterable[str] = HEARTBEAT_RUN_STATUSES
    ) -> list[StoredRun]:
        status_set = set(statuses)
        threshold = datetime.fromisoformat(before)
        stale: list[StoredRun] = []
        for run in self.runs.values():
            if run.status not in status_set:
                continue
            heartbeat_raw = run.heartbeat_at or run.updated_at or run.created_at
            heartbeat = datetime.fromisoformat(heartbeat_raw)
            if heartbeat < threshold:
                stale.append(run)
        return stale


class PostgresControlPlaneRepository:
    """Postgres-backed control-plane repository.

    The class accepts an asyncpg pool but keeps the import lazy so tests can use
    the in-memory repository without requiring a live Postgres dependency.
    """

    def __init__(self, pool: Any) -> None:
        self.pool = pool

    async def init(self) -> None:
        async with self.pool.acquire() as conn:
            revision = await conn.fetchval(
                "SELECT version_num FROM alembic_version LIMIT 1"
            )
            if revision != CONTROL_PLANE_ALEMBIC_BASELINE:
                raise RuntimeError(
                    "Control-plane schema is not migrated. "
                    f"Expected Alembic revision {CONTROL_PLANE_ALEMBIC_BASELINE}, "
                    f"found {revision or 'none'}."
                )

    async def close(self) -> None:
        await self.pool.close()

    async def ping(self) -> None:
        await self.pool.execute("SELECT 1")

    async def upsert_workload(
        self, workload: WorkloadDefinition, user: str, *, now: str | None = None
    ) -> None:
        timestamp = _pg_timestamp(now or utc_now_iso())
        await self.pool.execute(
            """
            INSERT INTO workloads (name, manifest, user_subject, created_at, updated_at)
            VALUES ($1, $2::jsonb, $3, $4::timestamptz, $4::timestamptz)
            ON CONFLICT (name) DO UPDATE SET
                manifest = EXCLUDED.manifest,
                user_subject = EXCLUDED.user_subject,
                updated_at = EXCLUDED.updated_at
            """,
            workload.metadata.name,
            json.dumps(workload.to_manifest()),
            user,
            timestamp,
        )

    async def list_workloads(self) -> list[WorkloadDefinition]:
        rows = await self.pool.fetch("SELECT manifest FROM workloads ORDER BY name ASC")
        return [
            WorkloadDefinition.model_validate(_json_dict(row["manifest"]))
            for row in rows
            if _json_dict(row["manifest"]) is not None
        ]

    async def get_workload(self, name: str) -> WorkloadDefinition | None:
        row = await self.pool.fetchrow(
            "SELECT manifest FROM workloads WHERE name = $1", name
        )
        if row is None:
            return None
        manifest = _json_dict(row["manifest"])
        return WorkloadDefinition.model_validate(manifest) if manifest else None

    async def create_run(
        self,
        run_id: str,
        workload_name: str,
        payload: dict[str, Any],
        user: str,
        *,
        created_at: str,
        session_id: str | None = None,
    ) -> StoredRun:
        row = await self.pool.fetchrow(
            """
            INSERT INTO runs (
                run_id, workload_name, user_subject, status, payload, session_id,
                created_at, updated_at
            )
            VALUES (
                $1::uuid, $2, $3, 'queued', $4::jsonb, $5::uuid,
                $6::timestamptz, $6::timestamptz
            )
            RETURNING run_id::text, workload_name, user_subject, status, payload,
                result, error, session_id::text, created_at, updated_at,
                heartbeat_at, completed_at
            """,
            run_id,
            workload_name,
            user,
            json.dumps(payload),
            session_id,
            _pg_timestamp(created_at),
        )
        return _run_from_row(row)

    async def list_runs(
        self,
        user: str | None,
        *,
        workload_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[StoredRun]:
        rows = await self.pool.fetch(
            """
            SELECT run_id::text, workload_name, user_subject, status, payload,
                result, error, session_id::text, created_at, updated_at,
                heartbeat_at, completed_at
            FROM runs
            WHERE ($1::text IS NULL OR user_subject = $1)
                AND ($2::text IS NULL OR workload_name = $2)
            ORDER BY created_at DESC
            LIMIT $3
            OFFSET $4
            """,
            user,
            workload_name,
            limit,
            offset,
        )
        return [_run_from_row(row) for row in rows]

    async def get_run(self, run_id: str) -> StoredRun | None:
        row = await self.pool.fetchrow(
            """
            SELECT run_id::text, workload_name, user_subject, status, payload,
                result, error, session_id::text, created_at, updated_at,
                heartbeat_at, completed_at
            FROM runs
            WHERE run_id = $1::uuid
            """,
            run_id,
        )
        return _run_from_row(row) if row else None

    async def update_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        result: dict[str, Any] | None = None,
        error: str | None = None,
        heartbeat_at: str | None = None,
        completed_at: str | None = None,
        updated_at: str | None = None,
    ) -> StoredRun | None:
        if status is not None:
            run = await self.get_run(run_id)
            if run is None:
                return None
            ensure_run_transition(run.status, status)

        fields: list[str] = []
        args: list[Any] = [run_id]

        def add(column: str, value: Any, cast: str = "") -> None:
            args.append(value)
            fields.append(f"{column} = ${len(args)}{cast}")

        if status is not None:
            add("status", status)
        if result is not None:
            add("result", json.dumps(result), "::jsonb")
        if error is not None:
            add("error", error)
        if heartbeat_at is not None:
            add("heartbeat_at", _pg_timestamp(heartbeat_at), "::timestamptz")
        if completed_at is not None:
            add("completed_at", _pg_timestamp(completed_at), "::timestamptz")
        add("updated_at", _pg_timestamp(updated_at or utc_now_iso()), "::timestamptz")

        row = await self.pool.fetchrow(
            f"""
            UPDATE runs
            SET {", ".join(fields)}
            WHERE run_id = $1::uuid
            RETURNING run_id::text, workload_name, user_subject, status, payload,
                result, error, session_id::text, created_at, updated_at,
                heartbeat_at, completed_at
            """,
            *args,
        )
        return _run_from_row(row) if row else None

    async def append_run_event(
        self,
        run_id: str,
        event_type: str,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        timestamp: str | None = None,
    ) -> StoredRunEvent:
        row = await self.pool.fetchrow(
            """
            INSERT INTO run_events (run_id, timestamp, type, message, data)
            VALUES ($1::uuid, $2::timestamptz, $3, $4, $5::jsonb)
            RETURNING id::text, run_id::text, timestamp, type, message, data
            """,
            run_id,
            _pg_timestamp(timestamp or utc_now_iso()),
            event_type,
            message,
            json.dumps(data or {}),
        )
        return _event_from_row(row)

    async def list_run_events(self, run_id: str) -> list[StoredRunEvent]:
        rows = await self.pool.fetch(
            """
            SELECT id::text, run_id::text, timestamp, type, message, data
            FROM run_events
            WHERE run_id = $1::uuid
            ORDER BY id ASC
            """,
            run_id,
        )
        return [_event_from_row(row) for row in rows]

    async def record_artifact(
        self,
        run_id: str,
        artifact: dict[str, Any],
        *,
        fallback_index: int = 0,
    ) -> StoredArtifact:
        item = _artifact_from_dict(run_id, artifact, fallback_index)
        row = await self.pool.fetchrow(
            """
            INSERT INTO artifacts (
                id, run_id, name, uri, content_type, size_bytes, created_at, metadata
            )
            VALUES (
                $1, $2::uuid, $3, $4, $5, $6, $7::timestamptz, $8::jsonb
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                uri = EXCLUDED.uri,
                content_type = EXCLUDED.content_type,
                size_bytes = EXCLUDED.size_bytes,
                metadata = EXCLUDED.metadata
            RETURNING id, run_id::text, name, uri, content_type, size_bytes,
                created_at, metadata
            """,
            item.id,
            item.run_id,
            item.name,
            item.uri,
            item.content_type,
            item.size_bytes,
            _pg_timestamp(item.created_at),
            json.dumps(item.metadata),
        )
        return _artifact_from_row(row)

    async def list_artifacts(self, run_id: str) -> list[StoredArtifact]:
        rows = await self.pool.fetch(
            """
            SELECT id, run_id::text, name, uri, content_type, size_bytes,
                created_at, metadata
            FROM artifacts
            WHERE run_id = $1::uuid
            ORDER BY created_at ASC, id ASC
            """,
            run_id,
        )
        return [_artifact_from_row(row) for row in rows]

    async def create_agent_session(
        self,
        session_id: str,
        agent_name: str,
        user: str,
        *,
        metadata: dict[str, Any] | None = None,
        created_at: str,
    ) -> StoredAgentSession:
        row = await self.pool.fetchrow(
            """
            INSERT INTO agent_sessions (
                session_id, agent_name, user_subject, status, metadata,
                created_at, updated_at
            )
            VALUES (
                $1::uuid, $2, $3, 'active', $4::jsonb,
                $5::timestamptz, $5::timestamptz
            )
            RETURNING session_id::text, agent_name, user_subject, status,
                metadata, created_at, updated_at
            """,
            session_id,
            agent_name,
            user,
            json.dumps(metadata or {}),
            _pg_timestamp(created_at),
        )
        return _session_from_row(row)

    async def get_agent_session(self, session_id: str) -> StoredAgentSession | None:
        row = await self.pool.fetchrow(
            """
            SELECT session_id::text, agent_name, user_subject, status,
                metadata, created_at, updated_at
            FROM agent_sessions
            WHERE session_id = $1::uuid
            """,
            session_id,
        )
        return _session_from_row(row) if row else None

    async def list_agent_sessions(
        self, agent_name: str, user: str | None
    ) -> list[StoredAgentSession]:
        rows = await self.pool.fetch(
            """
            SELECT session_id::text, agent_name, user_subject, status,
                metadata, created_at, updated_at
            FROM agent_sessions
            WHERE agent_name = $1 AND ($2::text IS NULL OR user_subject = $2)
            ORDER BY created_at DESC
            """,
            agent_name,
            user,
        )
        return [_session_from_row(row) for row in rows]

    async def append_agent_message(
        self,
        session_id: str,
        role: str,
        message: str,
        *,
        context: dict[str, Any] | None = None,
        created_at: str,
    ) -> StoredAgentMessage:
        row = await self.pool.fetchrow(
            """
            INSERT INTO agent_messages (session_id, role, message, context, created_at)
            VALUES ($1::uuid, $2, $3, $4::jsonb, $5::timestamptz)
            RETURNING id::text, session_id::text, role, message, context, created_at
            """,
            session_id,
            role,
            message,
            json.dumps(context or {}),
            _pg_timestamp(created_at),
        )
        return _message_from_row(row)

    async def list_agent_messages(self, session_id: str) -> list[StoredAgentMessage]:
        rows = await self.pool.fetch(
            """
            SELECT id::text, session_id::text, role, message, context, created_at
            FROM agent_messages
            WHERE session_id = $1::uuid
            ORDER BY id ASC
            """,
            session_id,
        )
        return [_message_from_row(row) for row in rows]

    async def upsert_deployment(
        self,
        deployment_id: str,
        workload_name: str,
        target: str,
        status: str,
        user: str,
        *,
        env: str = "local",
        endpoint: str | None = None,
        metadata: dict[str, Any] | None = None,
        now: str | None = None,
    ) -> StoredDeployment:
        timestamp = _pg_timestamp(now or utc_now_iso())
        row = await self.pool.fetchrow(
            """
            INSERT INTO deployments (
                deployment_id, workload_name, target, environment, status, endpoint,
                user_subject, metadata, created_at, updated_at
            )
            VALUES (
                $1::uuid, $2, $3, $4, $5, $6, $7, $8::jsonb,
                $9::timestamptz, $9::timestamptz
            )
            ON CONFLICT (workload_name, target, environment, user_subject) DO UPDATE SET
                status = EXCLUDED.status,
                endpoint = EXCLUDED.endpoint,
                metadata = EXCLUDED.metadata,
                updated_at = EXCLUDED.updated_at
            RETURNING deployment_id::text, workload_name, target, environment,
                status, endpoint,
                user_subject, metadata, created_at, updated_at
            """,
            deployment_id,
            workload_name,
            target,
            env,
            status,
            endpoint,
            user,
            json.dumps(metadata or {}),
            timestamp,
        )
        return _deployment_from_row(row)

    async def list_deployments(
        self,
        user: str | None,
        *,
        workload_name: str | None = None,
        env: str | None = None,
    ) -> list[StoredDeployment]:
        rows = await self.pool.fetch(
            """
            SELECT deployment_id::text, workload_name, target, environment,
                status, endpoint,
                user_subject, metadata, created_at, updated_at
            FROM deployments
            WHERE ($1::text IS NULL OR user_subject = $1)
                AND ($2::text IS NULL OR workload_name = $2)
                AND ($3::text IS NULL OR environment = $3)
            ORDER BY updated_at DESC
            """,
            user,
            workload_name,
            env,
        )
        return [_deployment_from_row(row) for row in rows]

    async def get_deployment(self, deployment_id: str) -> StoredDeployment | None:
        row = await self.pool.fetchrow(
            """
            SELECT deployment_id::text, workload_name, target, environment,
                status, endpoint,
                user_subject, metadata, created_at, updated_at
            FROM deployments
            WHERE deployment_id = $1::uuid
            """,
            deployment_id,
        )
        return _deployment_from_row(row) if row else None

    async def create_deployment_operation(
        self,
        operation_id: str,
        action: str,
        workload_name: str,
        target: str,
        status: str,
        user: str,
        *,
        env: str = "dev",
        metadata: dict[str, Any] | None = None,
        now: str | None = None,
        completed_at: str | None = None,
        lease_expires_at: str | None = None,
        controller_id: str | None = None,
        heartbeat_at: str | None = None,
        timeout_seconds: int | None = None,
        stdout_summary: str | None = None,
        stderr_summary: str | None = None,
    ) -> StoredDeploymentOperation:
        timestamp = _pg_timestamp(now or utc_now_iso())
        row = await self.pool.fetchrow(
            """
            INSERT INTO deployment_operations (
                operation_id, action, workload_name, target, environment, status,
                user_subject, metadata, created_at, updated_at, completed_at,
                lease_expires_at, controller_id, heartbeat_at, timeout_seconds,
                stdout_summary, stderr_summary
            )
            VALUES (
                $1::uuid, $2, $3, $4, $5, $6, $7, $8::jsonb,
                $9::timestamptz, $9::timestamptz, $10::timestamptz,
                $11::timestamptz, $12, $13::timestamptz, $14, $15, $16
            )
            RETURNING operation_id::text, action, workload_name, target,
                environment, status, user_subject, metadata, created_at, updated_at,
                completed_at, lease_expires_at, controller_id, heartbeat_at,
                timeout_seconds, stdout_summary, stderr_summary
            """,
            operation_id,
            action,
            workload_name,
            target,
            env,
            status,
            user,
            json.dumps(metadata or {}),
            timestamp,
            _pg_timestamp(completed_at),
            _pg_timestamp(lease_expires_at),
            controller_id,
            _pg_timestamp(heartbeat_at),
            timeout_seconds,
            stdout_summary,
            stderr_summary,
        )
        return _deployment_operation_from_row(row)

    async def get_deployment_operation(
        self, operation_id: str
    ) -> StoredDeploymentOperation | None:
        row = await self.pool.fetchrow(
            """
            SELECT operation_id::text, action, workload_name, target, environment,
                status, user_subject, metadata, created_at, updated_at, completed_at,
                lease_expires_at, controller_id, heartbeat_at, timeout_seconds,
                stdout_summary, stderr_summary
            FROM deployment_operations
            WHERE operation_id = $1::uuid
            """,
            operation_id,
        )
        return _deployment_operation_from_row(row) if row else None

    async def list_deployment_operations(
        self,
        user: str | None,
        *,
        workload_name: str | None = None,
        target: str | None = None,
        env: str | None = None,
        status: str | None = None,
        action: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[StoredDeploymentOperation]:
        rows = await self.pool.fetch(
            """
            SELECT operation_id::text, action, workload_name, target, environment,
                status, user_subject, metadata, created_at, updated_at,
                completed_at, lease_expires_at, controller_id, heartbeat_at,
                timeout_seconds, stdout_summary, stderr_summary
            FROM deployment_operations
            WHERE ($1::text IS NULL OR user_subject = $1)
              AND ($2::text IS NULL OR workload_name = $2)
              AND ($3::text IS NULL OR target = $3)
              AND ($4::text IS NULL OR environment = $4)
              AND ($5::text IS NULL OR status = $5)
              AND ($6::text IS NULL OR action = $6)
            ORDER BY created_at DESC
            LIMIT $7 OFFSET $8
            """,
            user,
            workload_name,
            target,
            env,
            status,
            action,
            limit,
            offset,
        )
        return [_deployment_operation_from_row(row) for row in rows]

    async def update_deployment_operation(
        self,
        operation_id: str,
        *,
        status: str,
        metadata: dict[str, Any],
        updated_at: str | None = None,
        completed_at: str | None = None,
        lease_expires_at: str | None = None,
        controller_id: str | None = None,
        heartbeat_at: str | None = None,
        timeout_seconds: int | None = None,
        stdout_summary: str | None = None,
        stderr_summary: str | None = None,
    ) -> StoredDeploymentOperation:
        existing = await self.get_deployment_operation(operation_id)
        if existing is None:
            raise KeyError(operation_id)
        ensure_deployment_operation_transition(existing.status, status)
        row = await self.pool.fetchrow(
            """
            UPDATE deployment_operations
            SET status = $2,
                metadata = $3::jsonb,
                updated_at = $4::timestamptz,
                completed_at = $5::timestamptz,
                lease_expires_at = COALESCE($6::timestamptz, lease_expires_at),
                controller_id = COALESCE($7, controller_id),
                heartbeat_at = COALESCE($8::timestamptz, heartbeat_at),
                timeout_seconds = COALESCE($9, timeout_seconds),
                stdout_summary = COALESCE($10, stdout_summary),
                stderr_summary = COALESCE($11, stderr_summary)
            WHERE operation_id = $1::uuid
            RETURNING operation_id::text, action, workload_name, target,
                environment, status, user_subject, metadata, created_at, updated_at,
                completed_at, lease_expires_at, controller_id, heartbeat_at,
                timeout_seconds, stdout_summary, stderr_summary
            """,
            operation_id,
            status,
            json.dumps(metadata),
            _pg_timestamp(updated_at or utc_now_iso()),
            _pg_timestamp(completed_at),
            _pg_timestamp(lease_expires_at),
            controller_id,
            _pg_timestamp(heartbeat_at),
            timeout_seconds,
            stdout_summary,
            stderr_summary,
        )
        if row is None:
            raise KeyError(operation_id)
        return _deployment_operation_from_row(row)

    async def append_deployment_operation_event(
        self,
        operation_id: str,
        event_type: str,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        timestamp: str | None = None,
    ) -> StoredDeploymentOperationEvent:
        row = await self.pool.fetchrow(
            """
            INSERT INTO deployment_operation_events (
                operation_id, timestamp, type, message, data
            )
            VALUES ($1::uuid, $2::timestamptz, $3, $4, $5::jsonb)
            RETURNING id::text, operation_id::text, timestamp, type, message, data
            """,
            operation_id,
            _pg_timestamp(timestamp or utc_now_iso()),
            event_type,
            message,
            json.dumps(data or {}),
        )
        return _deployment_operation_event_from_row(row)

    async def list_deployment_operation_events(
        self, operation_id: str
    ) -> list[StoredDeploymentOperationEvent]:
        rows = await self.pool.fetch(
            """
            SELECT id::text, operation_id::text, timestamp, type, message, data
            FROM deployment_operation_events
            WHERE operation_id = $1::uuid
            ORDER BY id ASC
            """,
            operation_id,
        )
        return [_deployment_operation_event_from_row(row) for row in rows]

    async def record_channel_message(
        self,
        channel: str,
        agent_name: str,
        external_user_id: str,
        session_id: str,
        direction: str,
        message: str,
        user: str,
        *,
        run_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        created_at: str,
    ) -> StoredChannelMessage:
        row = await self.pool.fetchrow(
            """
            INSERT INTO channel_messages (
                channel, agent_name, external_user_id, session_id, run_id,
                direction, message, user_subject, metadata, created_at
            )
            VALUES (
                $1, $2, $3, $4::uuid, $5::uuid, $6, $7, $8, $9::jsonb,
                $10::timestamptz
            )
            RETURNING id::text, channel, agent_name, external_user_id,
                session_id::text, run_id::text, direction, message, user_subject,
                metadata, created_at
            """,
            channel,
            agent_name,
            external_user_id,
            session_id,
            run_id,
            direction,
            message,
            user,
            json.dumps(metadata or {}),
            _pg_timestamp(created_at),
        )
        return _channel_message_from_row(row)

    async def record_audit_event(
        self,
        actor: str,
        action: str,
        resource_type: str,
        resource_id: str,
        *,
        metadata: dict[str, Any] | None = None,
        timestamp: str | None = None,
    ) -> StoredAuditEvent:
        row = await self.pool.fetchrow(
            """
            INSERT INTO audit_events (
                timestamp, actor_subject, action, resource_type, resource_id, metadata
            )
            VALUES ($1::timestamptz, $2, $3, $4, $5, $6::jsonb)
            RETURNING id::text, timestamp, actor_subject, action, resource_type,
                resource_id, metadata
            """,
            _pg_timestamp(timestamp or utc_now_iso()),
            actor,
            action,
            resource_type,
            resource_id,
            json.dumps(metadata or {}),
        )
        return _audit_event_from_row(row)

    async def list_audit_events(
        self,
        actor: str | None,
        *,
        action: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        env: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[StoredAuditEvent]:
        rows = await self.pool.fetch(
            """
            SELECT id::text, timestamp, actor_subject, action, resource_type,
                resource_id, metadata
            FROM audit_events
            WHERE ($1::text IS NULL OR actor_subject = $1)
              AND ($2::text IS NULL OR action = $2)
              AND ($3::text IS NULL OR resource_type = $3)
              AND ($4::text IS NULL OR resource_id = $4)
              AND (
                $5::text IS NULL
                OR COALESCE(metadata ->> 'env', metadata ->> 'environment') = $5
              )
            ORDER BY timestamp DESC, id DESC
            LIMIT $6 OFFSET $7
            """,
            actor,
            action,
            resource_type,
            resource_id,
            env,
            limit,
            offset,
        )
        return [_audit_event_from_row(row) for row in rows]

    async def create_api_key(
        self,
        key_id: str,
        name: str,
        secret_hash: str,
        secret_prefix: str,
        subject: str,
        role: str,
        created_by: str,
        *,
        team_id: str | None = None,
        now: str | None = None,
    ) -> StoredApiKey:
        row = await self.pool.fetchrow(
            """
            INSERT INTO api_keys (
                key_id, name, secret_hash, secret_prefix, subject, role,
                created_by, team_id, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::timestamptz)
            RETURNING key_id, name, secret_hash, secret_prefix, subject, role,
                created_by, team_id, created_at, last_used_at, revoked_at
            """,
            key_id,
            name,
            secret_hash,
            secret_prefix,
            subject,
            role,
            created_by,
            team_id,
            _pg_timestamp(now or utc_now_iso()),
        )
        return _api_key_from_row(row)

    async def list_api_keys(self) -> list[StoredApiKey]:
        rows = await self.pool.fetch(
            """
            SELECT key_id, name, secret_hash, secret_prefix, subject, role,
                created_by, team_id, created_at, last_used_at, revoked_at
            FROM api_keys
            ORDER BY created_at DESC
            """
        )
        return [_api_key_from_row(row) for row in rows]

    async def get_api_key_by_secret_hash(self, secret_hash: str) -> StoredApiKey | None:
        row = await self.pool.fetchrow(
            """
            SELECT key_id, name, secret_hash, secret_prefix, subject, role,
                created_by, team_id, created_at, last_used_at, revoked_at
            FROM api_keys
            WHERE secret_hash = $1
              AND revoked_at IS NULL
            """,
            secret_hash,
        )
        return _api_key_from_row(row) if row else None

    async def touch_api_key(
        self, key_id: str, *, last_used_at: str | None = None
    ) -> StoredApiKey | None:
        row = await self.pool.fetchrow(
            """
            UPDATE api_keys
            SET last_used_at = $2::timestamptz
            WHERE key_id = $1
            RETURNING key_id, name, secret_hash, secret_prefix, subject, role,
                created_by, team_id, created_at, last_used_at, revoked_at
            """,
            key_id,
            _pg_timestamp(last_used_at or utc_now_iso()),
        )
        return _api_key_from_row(row) if row else None

    async def revoke_api_key(
        self, key_id: str, *, revoked_at: str | None = None
    ) -> StoredApiKey | None:
        row = await self.pool.fetchrow(
            """
            UPDATE api_keys
            SET revoked_at = COALESCE(revoked_at, $2::timestamptz)
            WHERE key_id = $1
            RETURNING key_id, name, secret_hash, secret_prefix, subject, role,
                created_by, team_id, created_at, last_used_at, revoked_at
            """,
            key_id,
            _pg_timestamp(revoked_at or utc_now_iso()),
        )
        return _api_key_from_row(row) if row else None

    async def upsert_user(
        self,
        subject: str,
        password_hash: str,
        role: str,
        created_by: str,
        *,
        display_name: str | None = None,
        now: str | None = None,
    ) -> StoredUser:
        row = await self.pool.fetchrow(
            """
            INSERT INTO auth_users (
                subject, display_name, password_hash, role, created_by,
                created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6::timestamptz, $6::timestamptz)
            ON CONFLICT (subject) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                password_hash = EXCLUDED.password_hash,
                role = EXCLUDED.role,
                updated_at = EXCLUDED.updated_at
            RETURNING subject, display_name, password_hash, role, created_by,
                created_at, updated_at, disabled_at
            """,
            subject,
            display_name,
            password_hash,
            role,
            created_by,
            _pg_timestamp(now or utc_now_iso()),
        )
        return _user_from_row(row)

    async def get_user(self, subject: str) -> StoredUser | None:
        row = await self.pool.fetchrow(
            """
            SELECT subject, display_name, password_hash, role, created_by,
                created_at, updated_at, disabled_at
            FROM auth_users
            WHERE subject = $1
            """,
            subject,
        )
        return _user_from_row(row) if row else None

    async def list_users(self) -> list[StoredUser]:
        rows = await self.pool.fetch(
            """
            SELECT subject, display_name, password_hash, role, created_by,
                created_at, updated_at, disabled_at
            FROM auth_users
            ORDER BY created_at DESC
            """
        )
        return [_user_from_row(row) for row in rows]

    async def update_user(
        self,
        subject: str,
        *,
        display_name: str | None = None,
        role: str | None = None,
        password_hash: str | None = None,
        updated_at: str | None = None,
    ) -> StoredUser | None:
        row = await self.pool.fetchrow(
            """
            UPDATE auth_users
            SET display_name = COALESCE($2, display_name),
                role = COALESCE($3, role),
                password_hash = COALESCE($4, password_hash),
                updated_at = $5::timestamptz
            WHERE subject = $1
            RETURNING subject, display_name, password_hash, role, created_by,
                created_at, updated_at, disabled_at
            """,
            subject,
            display_name,
            role,
            password_hash,
            _pg_timestamp(updated_at or utc_now_iso()),
        )
        return _user_from_row(row) if row else None

    async def disable_user(
        self, subject: str, *, disabled_at: str | None = None
    ) -> StoredUser | None:
        row = await self.pool.fetchrow(
            """
            UPDATE auth_users
            SET disabled_at = COALESCE(disabled_at, $2::timestamptz),
                updated_at = $2::timestamptz
            WHERE subject = $1
            RETURNING subject, display_name, password_hash, role, created_by,
                created_at, updated_at, disabled_at
            """,
            subject,
            _pg_timestamp(disabled_at or utc_now_iso()),
        )
        return _user_from_row(row) if row else None

    async def enable_user(
        self, subject: str, *, updated_at: str | None = None
    ) -> StoredUser | None:
        row = await self.pool.fetchrow(
            """
            UPDATE auth_users
            SET disabled_at = NULL, updated_at = $2::timestamptz
            WHERE subject = $1
            RETURNING subject, display_name, password_hash, role, created_by,
                created_at, updated_at, disabled_at
            """,
            subject,
            _pg_timestamp(updated_at or utc_now_iso()),
        )
        return _user_from_row(row) if row else None

    async def upsert_team(
        self,
        team_id: str,
        name: str,
        created_by: str,
        *,
        description: str | None = None,
        now: str | None = None,
    ) -> StoredTeam:
        row = await self.pool.fetchrow(
            """
            INSERT INTO teams (
                team_id, name, description, created_by, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5::timestamptz, $5::timestamptz)
            ON CONFLICT (team_id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                updated_at = EXCLUDED.updated_at
            RETURNING team_id, name, description, created_by, created_at, updated_at
            """,
            team_id,
            name,
            description,
            created_by,
            _pg_timestamp(now or utc_now_iso()),
        )
        return _team_from_row(row)

    async def list_teams(self) -> list[StoredTeam]:
        rows = await self.pool.fetch(
            """
            SELECT team_id, name, description, created_by, created_at, updated_at
            FROM teams
            ORDER BY created_at DESC
            """
        )
        return [_team_from_row(row) for row in rows]

    async def update_team(
        self,
        team_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        updated_at: str | None = None,
    ) -> StoredTeam | None:
        row = await self.pool.fetchrow(
            """
            UPDATE teams
            SET name = COALESCE($2, name),
                description = COALESCE($3, description),
                updated_at = $4::timestamptz
            WHERE team_id = $1
            RETURNING team_id, name, description, created_by, created_at, updated_at
            """,
            team_id,
            name,
            description,
            _pg_timestamp(updated_at or utc_now_iso()),
        )
        return _team_from_row(row) if row else None

    async def add_team_member(
        self,
        team_id: str,
        subject: str,
        role: str,
        created_by: str,
        *,
        now: str | None = None,
    ) -> StoredTeamMember:
        row = await self.pool.fetchrow(
            """
            INSERT INTO team_members (team_id, subject, role, created_by, created_at)
            VALUES ($1, $2, $3, $4, $5::timestamptz)
            ON CONFLICT (team_id, subject) DO UPDATE SET
                role = EXCLUDED.role
            RETURNING team_id, subject, role, created_by, created_at
            """,
            team_id,
            subject,
            role,
            created_by,
            _pg_timestamp(now or utc_now_iso()),
        )
        return _team_member_from_row(row)

    async def list_team_members(
        self, *, team_id: str | None = None, subject: str | None = None
    ) -> list[StoredTeamMember]:
        rows = await self.pool.fetch(
            """
            SELECT team_id, subject, role, created_by, created_at
            FROM team_members
            WHERE ($1::text IS NULL OR team_id = $1)
              AND ($2::text IS NULL OR subject = $2)
            ORDER BY created_at DESC
            """,
            team_id,
            subject,
        )
        return [_team_member_from_row(row) for row in rows]

    async def remove_team_member(
        self, team_id: str, subject: str
    ) -> StoredTeamMember | None:
        row = await self.pool.fetchrow(
            """
            DELETE FROM team_members
            WHERE team_id = $1 AND subject = $2
            RETURNING team_id, subject, role, created_by, created_at
            """,
            team_id,
            subject,
        )
        if row is None:
            return None
        return _team_member_from_row(row)

    async def find_stale_runs(
        self, *, before: str, statuses: Iterable[str] = HEARTBEAT_RUN_STATUSES
    ) -> list[StoredRun]:
        rows = await self.pool.fetch(
            """
            SELECT run_id::text, workload_name, user_subject, status, payload,
                result, error, session_id::text, created_at, updated_at,
                heartbeat_at, completed_at
            FROM runs
            WHERE status = ANY($1::text[])
                AND COALESCE(heartbeat_at, updated_at, created_at) < $2::timestamptz
            """,
            list(statuses),
            _pg_timestamp(before),
        )
        return [_run_from_row(row) for row in rows]


async def connect_postgres_control_plane(dsn: str) -> PostgresControlPlaneRepository:
    """Create and initialize a Postgres repository."""

    import asyncpg  # type: ignore[import-untyped]

    from moiraweave_shared.alembic_runner import upgrade_control_plane

    await upgrade_control_plane(dsn)
    pool = await asyncpg.create_pool(dsn)
    repo = PostgresControlPlaneRepository(pool)
    await repo.init()
    return repo


def _run_from_row(row: Any) -> StoredRun:
    return StoredRun(
        run_id=str(row["run_id"]),
        workload_name=str(row["workload_name"]),
        status=str(row["status"]),
        user=str(row["user_subject"]),
        created_at=str(_iso(row["created_at"])),
        updated_at=_iso(row["updated_at"]),
        heartbeat_at=_iso(row["heartbeat_at"]),
        completed_at=_iso(row["completed_at"]),
        session_id=str(row["session_id"]) if row["session_id"] is not None else None,
        payload=_json_dict(row["payload"]),
        result=_json_dict(row["result"]),
        error=row["error"],
    )


def _event_from_row(row: Any) -> StoredRunEvent:
    return StoredRunEvent(
        id=str(row["id"]),
        run_id=str(row["run_id"]),
        timestamp=str(_iso(row["timestamp"])),
        type=str(row["type"]),
        message=str(row["message"]),
        data=_json_dict(row["data"]) or {},
    )


def _artifact_from_dict(
    run_id: str, artifact: dict[str, Any], fallback_index: int
) -> StoredArtifact:
    return StoredArtifact(
        id=str(artifact.get("id") or f"{run_id}-{fallback_index}"),
        run_id=run_id,
        name=str(artifact.get("name") or f"artifact-{fallback_index}"),
        uri=str(artifact.get("uri") or ""),
        content_type=artifact.get("content_type"),
        size_bytes=artifact.get("size_bytes"),
        created_at=str(artifact.get("created_at") or utc_now_iso()),
        metadata=artifact.get("metadata") or {},
    )


def _artifact_from_row(row: Any) -> StoredArtifact:
    return StoredArtifact(
        id=str(row["id"]),
        run_id=str(row["run_id"]),
        name=str(row["name"]),
        uri=str(row["uri"]),
        content_type=row["content_type"],
        size_bytes=row["size_bytes"],
        created_at=str(_iso(row["created_at"])),
        metadata=_json_dict(row["metadata"]) or {},
    )


def _session_from_row(row: Any) -> StoredAgentSession:
    return StoredAgentSession(
        session_id=str(row["session_id"]),
        agent_name=str(row["agent_name"]),
        status=str(row["status"]),
        user=str(row["user_subject"]),
        created_at=str(_iso(row["created_at"])),
        updated_at=_iso(row["updated_at"]),
        metadata=_json_dict(row["metadata"]) or {},
    )


def _message_from_row(row: Any) -> StoredAgentMessage:
    return StoredAgentMessage(
        message_id=str(row["id"]),
        session_id=str(row["session_id"]),
        role=str(row["role"]),
        message=str(row["message"]),
        context=_json_dict(row["context"]) or {},
        created_at=str(_iso(row["created_at"])),
    )


def _deployment_from_row(row: Any) -> StoredDeployment:
    return StoredDeployment(
        deployment_id=str(row["deployment_id"]),
        workload_name=str(row["workload_name"]),
        target=str(row["target"]),
        env=str(row["environment"]),
        status=str(row["status"]),
        user=str(row["user_subject"]),
        endpoint=row["endpoint"],
        metadata=_json_dict(row["metadata"]) or {},
        created_at=str(_iso(row["created_at"])),
        updated_at=_iso(row["updated_at"]),
    )


def _deployment_operation_from_row(row: Any) -> StoredDeploymentOperation:
    return StoredDeploymentOperation(
        operation_id=str(row["operation_id"]),
        action=str(row["action"]),
        workload_name=str(row["workload_name"]),
        target=str(row["target"]),
        env=str(row["environment"]),
        status=str(row["status"]),
        user=str(row["user_subject"]),
        metadata=_json_dict(row["metadata"]) or {},
        created_at=str(_iso(row["created_at"])),
        updated_at=_iso(row["updated_at"]),
        completed_at=_iso(row["completed_at"]),
        lease_expires_at=_iso(row["lease_expires_at"]),
        controller_id=row["controller_id"],
        heartbeat_at=_iso(row["heartbeat_at"]),
        timeout_seconds=row["timeout_seconds"],
        stdout_summary=row["stdout_summary"],
        stderr_summary=row["stderr_summary"],
    )


def _deployment_operation_event_from_row(
    row: Any,
) -> StoredDeploymentOperationEvent:
    return StoredDeploymentOperationEvent(
        id=str(row["id"]),
        operation_id=str(row["operation_id"]),
        timestamp=str(_iso(row["timestamp"])),
        type=str(row["type"]),
        message=str(row["message"]),
        data=_json_dict(row["data"]) or {},
    )


def _channel_message_from_row(row: Any) -> StoredChannelMessage:
    return StoredChannelMessage(
        message_id=str(row["id"]),
        channel=str(row["channel"]),
        agent_name=str(row["agent_name"]),
        external_user_id=str(row["external_user_id"]),
        session_id=str(row["session_id"]),
        run_id=str(row["run_id"]) if row["run_id"] is not None else None,
        direction=str(row["direction"]),
        message=str(row["message"]),
        user=str(row["user_subject"]),
        metadata=_json_dict(row["metadata"]) or {},
        created_at=str(_iso(row["created_at"])),
    )


def _audit_event_from_row(row: Any) -> StoredAuditEvent:
    return StoredAuditEvent(
        event_id=str(row["id"]),
        timestamp=str(_iso(row["timestamp"])),
        actor=str(row["actor_subject"]),
        action=str(row["action"]),
        resource_type=str(row["resource_type"]),
        resource_id=str(row["resource_id"]),
        metadata=_json_dict(row["metadata"]) or {},
    )


def _api_key_from_row(row: Any) -> StoredApiKey:
    return StoredApiKey(
        key_id=str(row["key_id"]),
        name=str(row["name"]),
        secret_hash=str(row["secret_hash"]),
        secret_prefix=str(row["secret_prefix"]),
        subject=str(row["subject"]),
        role=str(row["role"]),
        created_by=str(row["created_by"]),
        created_at=str(_iso(row["created_at"])),
        team_id=str(_row_get(row, "team_id"))
        if _row_get(row, "team_id") is not None
        else None,
        last_used_at=_iso(row["last_used_at"]),
        revoked_at=_iso(row["revoked_at"]),
    )


def _user_from_row(row: Any) -> StoredUser:
    return StoredUser(
        subject=str(row["subject"]),
        display_name=str(_row_get(row, "display_name"))
        if _row_get(row, "display_name") is not None
        else None,
        password_hash=str(row["password_hash"]),
        role=str(row["role"]),
        created_by=str(row["created_by"]),
        created_at=str(_iso(row["created_at"])),
        updated_at=str(_iso(row["updated_at"])),
        disabled_at=_iso(row["disabled_at"]),
    )


def _team_from_row(row: Any) -> StoredTeam:
    return StoredTeam(
        team_id=str(row["team_id"]),
        name=str(row["name"]),
        description=str(_row_get(row, "description"))
        if _row_get(row, "description") is not None
        else None,
        created_by=str(row["created_by"]),
        created_at=str(_iso(row["created_at"])),
        updated_at=str(_iso(row["updated_at"])),
    )


def _team_member_from_row(row: Any) -> StoredTeamMember:
    return StoredTeamMember(
        team_id=str(row["team_id"]),
        subject=str(row["subject"]),
        role=str(row["role"]),
        created_by=str(row["created_by"]),
        created_at=str(_iso(row["created_at"])),
    )


def workloads_by_name(
    workloads: Sequence[WorkloadDefinition],
) -> dict[str, WorkloadDefinition]:
    return {workload.metadata.name: workload for workload in workloads}
