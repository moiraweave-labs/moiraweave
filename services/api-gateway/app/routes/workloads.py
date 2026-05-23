"""Workload control-plane API routes."""

from __future__ import annotations

import asyncio
import json
import logging
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
    AgentMessageRequest,
    AgentMessageResponse,
    AgentSessionHealthResponse,
    AgentSessionRequest,
    AgentSessionResponse,
    ChannelMessageRequest,
    DeploymentRequest,
    DeploymentResponse,
    RunArtifact,
    RunEvent,
    RunRequest,
    RunResponse,
    RunStatusResponse,
    WorkloadHealthResponse,
    WorkloadInfo,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1", tags=["workloads"])


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


def _message_payload(message: Any) -> dict[str, Any]:
    return {
        "message_id": message.message_id,
        "session_id": message.session_id,
        "role": message.role,
        "message": message.message,
        "context": message.context,
        "created_at": message.created_at,
    }


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


@router.get("/workloads", response_model=list[WorkloadInfo])
async def list_workloads(control_plane: ControlPlane) -> list[WorkloadInfo]:
    settings = get_settings()
    workloads = await _all_workloads(control_plane, settings)
    return [_workload_info(workload) for workload in workloads.values()]


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


@router.get("/agents/{name}/sessions/{session_id}/messages")
async def list_agent_session_messages(
    name: str,
    session_id: str,
    control_plane: ControlPlane,
    current_user: CurrentUser,
) -> list[dict[str, Any]]:
    await _authorize_agent_session(name, session_id, control_plane, current_user)

    messages = await control_plane.list_agent_messages(session_id)
    return [_message_payload(message) for message in messages]


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
