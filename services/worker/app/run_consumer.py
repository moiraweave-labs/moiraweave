"""Redis Stream consumer for generic MoiraWeave workload runs."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from moiraweave_shared.control_plane import (
    ControlPlaneRepository,
    utc_now_iso,
    workloads_by_name,
)
from moiraweave_shared.schemas import RunMessage
from moiraweave_shared.streams import CONSUMER_GROUP, DEAD_LETTER_STREAM, RUN_STREAM
from moiraweave_shared.workloads import (
    TERMINAL_RUN_STATUSES,
    WorkloadDefinition,
    ensure_run_transition,
    load_workloads,
)
from pydantic import ValidationError
from redis.exceptions import ResponseError

from app.agent_adapters import extract_assistant_message
from app.workload_executor import RunCancelledError, WorkloadExecutor

if TYPE_CHECKING:
    from pathlib import Path

    from redis.asyncio import Redis

logger = logging.getLogger(__name__)


async def _ensure_consumer_group(redis: Redis) -> None:
    try:
        await redis.xgroup_create(RUN_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        logger.info(
            "run_consumer_group_created group=%s stream=%s",
            CONSUMER_GROUP,
            RUN_STREAM,
        )
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def _record_artifacts(
    control_plane: ControlPlaneRepository, run_id: str, result: dict[str, Any]
) -> None:
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list):
        return
    for index, artifact in enumerate(artifacts):
        if isinstance(artifact, dict):
            await control_plane.record_artifact(
                run_id,
                artifact,
                fallback_index=index,
            )


async def _record_agent_response(
    control_plane: ControlPlaneRepository,
    workload: WorkloadDefinition,
    payload: dict[str, Any],
    result: dict[str, Any],
    run_id: str,
) -> None:
    if workload.spec.type != "agent-service":
        return
    session_id = payload.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        return
    message = extract_assistant_message(result)
    if message is None:
        return
    await control_plane.append_agent_message(
        session_id,
        "assistant",
        message,
        context={"run_id": run_id, "adapter": result.get("adapter", "unknown")},
        created_at=utc_now_iso(),
    )


async def _dead_letter(
    redis: Redis,
    msg_id: str,
    fields: dict[str, str],
    *,
    reason: str,
) -> None:
    await redis.xadd(
        DEAD_LETTER_STREAM,
        {
            "source_stream": RUN_STREAM,
            "source_id": msg_id,
            "reason": reason,
            "payload": json.dumps(fields),
            "created_at": utc_now_iso(),
        },
    )
    await redis.xack(RUN_STREAM, CONSUMER_GROUP, msg_id)


async def _load_workload_map(
    control_plane: ControlPlaneRepository,
    workloads_dir: str | Path,
    workload_manifest: str | None,
) -> dict[str, WorkloadDefinition]:
    workloads = workloads_by_name(load_workloads(workloads_dir))
    workloads.update(workloads_by_name(await control_plane.list_workloads()))

    if workload_manifest:
        with contextlib.suppress(Exception):
            workload = WorkloadDefinition.model_validate(json.loads(workload_manifest))
            workloads[workload.metadata.name] = workload

    return workloads


async def _heartbeat_loop(
    control_plane: ControlPlaneRepository,
    run_id: str,
    stop_event: asyncio.Event,
    *,
    interval_seconds: float,
) -> None:
    while not stop_event.is_set():
        now = utc_now_iso()
        try:
            await control_plane.update_run(
                run_id,
                heartbeat_at=now,
                updated_at=now,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("run_heartbeat_failed run_id=%s error=%s", run_id, exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue


async def _is_cancel_requested(
    control_plane: ControlPlaneRepository, run_id: str
) -> bool:
    run = await control_plane.get_run(run_id)
    return run is not None and run.status in {
        "cancel_requested",
        "cancelling",
        "canceled",
    }


async def mark_stale_runs(
    control_plane: ControlPlaneRepository,
    *,
    stale_after_seconds: float,
) -> None:
    threshold = datetime.now(UTC) - timedelta(seconds=stale_after_seconds)
    stale_runs = await control_plane.find_stale_runs(before=threshold.isoformat())
    now = datetime.now(UTC)
    for run in stale_runs:
        latest = await control_plane.get_run(run.run_id)
        if latest is None or latest.status in TERMINAL_RUN_STATUSES:
            continue
        heartbeat_raw = latest.heartbeat_at or latest.updated_at or latest.created_at
        with contextlib.suppress(ValueError):
            heartbeat = datetime.fromisoformat(heartbeat_raw.replace("Z", "+00:00"))
            if heartbeat.tzinfo is None:
                heartbeat = heartbeat.replace(tzinfo=UTC)
            if heartbeat >= threshold:
                continue
            age = int((now - heartbeat).total_seconds())
            completed_at = utc_now_iso()
            await control_plane.update_run(
                latest.run_id,
                status="lost",
                error=f"Heartbeat stale for {age}s",
                updated_at=completed_at,
                completed_at=completed_at,
            )
            await control_plane.append_run_event(
                latest.run_id,
                "run.lost",
                "Run marked lost after stale heartbeat",
                data={"age_seconds": age},
            )


async def run_consumer(
    redis: Redis,
    control_plane: ControlPlaneRepository,
    consumer_id: str,
    shutdown_event: asyncio.Event,
    *,
    workloads_dir: str,
    heartbeat_interval_seconds: float,
    stale_run_seconds: float,
    stale_check_interval_seconds: float,
) -> None:
    """Consume generic workload runs from Redis and execute them."""

    await _ensure_consumer_group(redis)
    logger.info("run_consumer_start consumer=%s stream=%s", consumer_id, RUN_STREAM)
    next_stale_check = 0.0

    while not shutdown_event.is_set():
        now_loop = asyncio.get_running_loop().time()
        if now_loop >= next_stale_check:
            await mark_stale_runs(
                control_plane,
                stale_after_seconds=stale_run_seconds,
            )
            next_stale_check = now_loop + stale_check_interval_seconds

        try:
            entries: Any = await redis.xreadgroup(
                CONSUMER_GROUP,
                consumer_id,
                {RUN_STREAM: ">"},
                count=1,
                block=1000,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("run_read_error error=%s", exc)
            await asyncio.sleep(1.0)
            continue

        if not entries:
            continue

        for _stream_name, messages in entries:
            for msg_id, fields in messages:
                await _process_message(
                    redis,
                    control_plane,
                    msg_id,
                    dict(fields),
                    workloads_dir=workloads_dir,
                    heartbeat_interval_seconds=heartbeat_interval_seconds,
                )


async def _process_message(
    redis: Redis,
    control_plane: ControlPlaneRepository,
    msg_id: str,
    fields: dict[str, str],
    *,
    workloads_dir: str,
    heartbeat_interval_seconds: float,
) -> None:
    try:
        msg = RunMessage.model_validate(fields)
    except ValidationError:
        logger.exception("run_message_invalid msg_id=%s fields=%s", msg_id, fields)
        await _dead_letter(redis, msg_id, fields, reason="invalid_run_message")
        return

    run_id = msg.run_id
    existing_run = await control_plane.get_run(run_id)
    if existing_run is None:
        logger.warning("run_missing run_id=%s msg_id=%s", run_id, msg_id)
        await _dead_letter(redis, msg_id, fields, reason="run_not_found")
        return
    if existing_run.status in TERMINAL_RUN_STATUSES:
        await redis.xack(RUN_STREAM, CONSUMER_GROUP, msg_id)
        return

    try:
        payload = json.loads(msg.payload)
        if not isinstance(payload, dict):
            raise ValueError("Run payload must be a JSON object")
    except Exception as exc:  # noqa: BLE001
        now = utc_now_iso()
        await _transition_run(
            control_plane,
            run_id,
            status="failed",
            error=f"Invalid payload: {exc}",
            updated_at=now,
            completed_at=now,
        )
        await control_plane.append_run_event(
            run_id,
            "run.failed",
            "Invalid run payload",
        )
        await redis.xack(RUN_STREAM, CONSUMER_GROUP, msg_id)
        return

    workloads = await _load_workload_map(
        control_plane,
        workloads_dir,
        msg.workload_manifest,
    )
    workload = workloads.get(msg.workload_name)
    if workload is None:
        now = utc_now_iso()
        await _transition_run(
            control_plane,
            run_id,
            status="failed",
            error=f"Workload {msg.workload_name!r} not found",
            updated_at=now,
            completed_at=now,
        )
        await control_plane.append_run_event(
            run_id,
            "run.failed",
            "Workload manifest was not found by worker",
            data={"workload_name": msg.workload_name},
        )
        await redis.xack(RUN_STREAM, CONSUMER_GROUP, msg_id)
        return

    if await _is_cancel_requested(control_plane, run_id):
        await _cancel(control_plane, run_id, "Run canceled before execution")
        await redis.xack(RUN_STREAM, CONSUMER_GROUP, msg_id)
        return

    now = utc_now_iso()
    await _transition_run(
        control_plane,
        run_id,
        status="starting",
        updated_at=now,
        heartbeat_at=now,
    )
    await control_plane.append_run_event(
        run_id,
        "run.starting",
        "Worker accepted run",
        data={"workload_name": msg.workload_name},
    )

    stop_heartbeat = asyncio.Event()
    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(
            control_plane,
            run_id,
            stop_heartbeat,
            interval_seconds=heartbeat_interval_seconds,
        )
    )

    try:
        await _transition_run(
            control_plane,
            run_id,
            status="running",
            updated_at=utc_now_iso(),
        )
        await control_plane.append_run_event(
            run_id,
            "run.running",
            "Run execution started",
        )

        async def emit(
            event_type: str,
            message: str,
            data: dict[str, Any] | None = None,
        ) -> None:
            await control_plane.append_run_event(
                run_id,
                event_type,
                message,
                data=data,
            )

        async def is_cancel_requested() -> bool:
            return await _is_cancel_requested(control_plane, run_id)

        async with asyncio.timeout(workload.spec.execution.timeoutSeconds):
            result = await WorkloadExecutor(workloads).execute(
                workload,
                payload,
                emit=emit,
                is_cancel_requested=is_cancel_requested,
            )
        if await _is_cancel_requested(control_plane, run_id):
            await _cancel(control_plane, run_id, "Run canceled after executor returned")
        else:
            await _record_artifacts(control_plane, run_id, result)
            await _record_agent_response(
                control_plane, workload, payload, result, run_id
            )
            completed_at = utc_now_iso()
            await _transition_run(
                control_plane,
                run_id,
                status="succeeded",
                result=result,
                updated_at=completed_at,
                completed_at=completed_at,
            )
            await control_plane.append_run_event(
                run_id,
                "run.succeeded",
                "Run completed",
            )
    except RunCancelledError as exc:
        await _cancel(control_plane, run_id, str(exc))
    except TimeoutError:
        completed_at = utc_now_iso()
        await _transition_run(
            control_plane,
            run_id,
            status="failed",
            error=f"Run timed out after {workload.spec.execution.timeoutSeconds}s",
            updated_at=completed_at,
            completed_at=completed_at,
        )
        await control_plane.append_run_event(
            run_id,
            "run.timeout",
            "Run exceeded workload execution timeout",
            data={"timeout_seconds": workload.spec.execution.timeoutSeconds},
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "run_failed run_id=%s workload=%s error=%s",
            run_id,
            msg.workload_name,
            exc,
        )
        completed_at = utc_now_iso()
        await _transition_run(
            control_plane,
            run_id,
            status="failed",
            error=str(exc),
            updated_at=completed_at,
            completed_at=completed_at,
        )
        await control_plane.append_run_event(
            run_id,
            "run.failed",
            "Run failed",
            data={"error": str(exc)},
        )
    finally:
        stop_heartbeat.set()
        await heartbeat_task
        await redis.xack(RUN_STREAM, CONSUMER_GROUP, msg_id)


async def _cancel(
    control_plane: ControlPlaneRepository, run_id: str, message: str
) -> None:
    completed_at = utc_now_iso()
    run = await control_plane.get_run(run_id)
    if run is not None and run.status not in {"cancel_requested", "cancelling"}:
        await _transition_run(
            control_plane,
            run_id,
            status="cancelling",
            updated_at=completed_at,
        )
    await _transition_run(
        control_plane,
        run_id,
        status="canceled",
        updated_at=completed_at,
        completed_at=completed_at,
    )
    await control_plane.append_run_event(run_id, "run.canceled", message)


async def _transition_run(
    control_plane: ControlPlaneRepository,
    run_id: str,
    *,
    status: str,
    result: dict[str, Any] | None = None,
    error: str | None = None,
    heartbeat_at: str | None = None,
    completed_at: str | None = None,
    updated_at: str | None = None,
) -> None:
    run = await control_plane.get_run(run_id)
    if run is not None:
        ensure_run_transition(run.status, status)
    await control_plane.update_run(
        run_id,
        status=status,
        result=result,
        error=error,
        heartbeat_at=heartbeat_at,
        completed_at=completed_at,
        updated_at=updated_at,
    )
