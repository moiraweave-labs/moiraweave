"""Tests for workload run consumption."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from moiraweave_shared.control_plane import (
    InMemoryControlPlaneRepository,
    StoredRun,
    utc_now_iso,
)
from moiraweave_shared.schemas import RunMessage
from moiraweave_shared.streams import CONSUMER_GROUP, DEAD_LETTER_STREAM, RUN_STREAM
from moiraweave_shared.workloads import WorkloadDefinition

from app.agent_adapters import HttpAgentAdapter
from app.run_consumer import (
    _ensure_consumer_group,
    _process_message,
    mark_stale_runs,
    reclaim_pending_runs,
)


def _agent_workload() -> WorkloadDefinition:
    return WorkloadDefinition.model_validate(
        {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {"name": "agent"},
            "spec": {
                "type": "agent-service",
                "image": "ghcr.io/example/agent:latest",
                "endpoint": "http://agent:8000",
                "execution": {"mode": "session", "timeoutSeconds": 5},
                "agent": {"adapter": "generic-http", "messagePath": "/messages"},
            },
        }
    )


async def _advance_run(
    control_plane: InMemoryControlPlaneRepository,
    run_id: str,
    status: str,
    **kwargs: Any,
) -> None:
    paths = {
        "starting": ["starting"],
        "running": ["starting", "running"],
        "cancel_requested": ["cancel_requested"],
        "cancelling": ["cancel_requested", "cancelling"],
        "succeeded": ["starting", "running", "succeeded"],
        "failed": ["starting", "running", "failed"],
        "canceled": ["cancel_requested", "canceled"],
        "lost": ["starting", "running", "lost"],
    }
    for step in paths[status][:-1]:
        await control_plane.update_run(run_id, status=step)
    await control_plane.update_run(run_id, status=paths[status][-1], **kwargs)


async def test_process_agent_message_records_assistant_response(
    fake_redis: Any,
    tmp_path,
    monkeypatch,
) -> None:
    async def fake_send_message(
        self: HttpAgentAdapter, payload: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            "response": f"received {payload['message']}",
            "adapter": self.name,
            "artifacts": [
                {
                    "id": "artifact-1",
                    "name": "trace.json",
                    "uri": "file:///artifacts/trace.json",
                }
            ],
        }

    monkeypatch.setattr(HttpAgentAdapter, "send_message", fake_send_message)
    await _ensure_consumer_group(fake_redis)
    control_plane = InMemoryControlPlaneRepository()
    workload = _agent_workload()
    await control_plane.upsert_workload(workload, "user")
    await control_plane.create_agent_session(
        "session-1",
        "agent",
        "user",
        metadata={},
        created_at=utc_now_iso(),
    )
    await control_plane.create_run(
        "run-1",
        "agent",
        {"session_id": "session-1", "message": "hello"},
        "user",
        created_at=utc_now_iso(),
        session_id="session-1",
    )
    msg = RunMessage(
        run_id="run-1",
        workload_name="agent",
        payload=json.dumps({"session_id": "session-1", "message": "hello"}),
        user="user",
    ).model_dump(mode="python", exclude_none=True)

    await _process_message(
        fake_redis,
        control_plane,
        "1-0",
        msg,
        workloads_dir=tmp_path,
        heartbeat_interval_seconds=0.01,
    )

    run = await control_plane.get_run("run-1")
    messages = await control_plane.list_agent_messages("session-1")
    artifacts = await control_plane.list_artifacts("run-1")

    assert run is not None
    assert run.status == "succeeded"
    assert messages[-1].role == "assistant"
    assert messages[-1].message == "received hello"
    assert artifacts[0].name == "trace.json"
    assert artifacts[0].metadata["workload_name"] == "agent"
    assert artifacts[0].metadata["session_id"] == "session-1"


async def test_process_message_retries_transient_executor_failure(
    fake_redis: Any,
    tmp_path,
    monkeypatch,
) -> None:
    calls = 0

    async def flaky_send_message(
        self: HttpAgentAdapter, payload: dict[str, Any]
    ) -> dict[str, Any]:
        nonlocal calls
        del self, payload
        calls += 1
        if calls == 1:
            raise RuntimeError("runtime warming up")
        return {"response": "ok"}

    monkeypatch.setattr(HttpAgentAdapter, "send_message", flaky_send_message)
    await _ensure_consumer_group(fake_redis)
    control_plane = InMemoryControlPlaneRepository()
    workload = _agent_workload()
    await control_plane.upsert_workload(workload, "user")
    await control_plane.create_run(
        "run-retry",
        "agent",
        {"message": "hello"},
        "user",
        created_at=utc_now_iso(),
    )
    msg = RunMessage(
        run_id="run-retry",
        workload_name="agent",
        payload=json.dumps({"message": "hello"}),
        user="user",
    ).model_dump(mode="python", exclude_none=True)

    await _process_message(
        fake_redis,
        control_plane,
        "1-0",
        msg,
        workloads_dir=tmp_path,
        heartbeat_interval_seconds=0.01,
        run_retry_attempts=2,
        run_retry_backoff_seconds=0.0,
    )

    run = await control_plane.get_run("run-retry")
    events = await control_plane.list_run_events("run-retry")

    assert calls == 2
    assert run is not None
    assert run.status == "succeeded"
    assert any(event.type == "run.retry" for event in events)
    assert any(event.type == "run.retrying" for event in events)


async def test_active_duplicate_message_is_acknowledged_without_reexecution(
    fake_redis: Any,
    tmp_path,
) -> None:
    await _ensure_consumer_group(fake_redis)
    control_plane = InMemoryControlPlaneRepository()
    await control_plane.create_run(
        "run-active-duplicate",
        "agent",
        {},
        "user",
        created_at=utc_now_iso(),
    )
    await _advance_run(control_plane, "run-active-duplicate", "running")
    msg = RunMessage(
        run_id="run-active-duplicate",
        workload_name="agent",
        payload="{}",
        user="user",
    ).model_dump(mode="python", exclude_none=True)

    await _process_message(
        fake_redis,
        control_plane,
        "1-0",
        msg,
        workloads_dir=tmp_path,
        heartbeat_interval_seconds=0.01,
    )

    run = await control_plane.get_run("run-active-duplicate")
    events = await control_plane.list_run_events("run-active-duplicate")

    assert run is not None
    assert run.status == "running"
    assert events[-1].type == "run.duplicate_ignored"


async def test_invalid_run_message_goes_to_dead_letter(
    fake_redis: Any, tmp_path
) -> None:
    await _ensure_consumer_group(fake_redis)
    control_plane = InMemoryControlPlaneRepository()

    await _process_message(
        fake_redis,
        control_plane,
        "1-0",
        {"run_id": "missing-fields"},
        workloads_dir=tmp_path,
        heartbeat_interval_seconds=0.01,
    )

    dead_letters = await fake_redis.xrange(DEAD_LETTER_STREAM)
    assert len(dead_letters) == 1
    assert dead_letters[0][1]["reason"] == "invalid_run_message"


async def test_invalid_payload_fails_run(fake_redis: Any, tmp_path) -> None:
    await _ensure_consumer_group(fake_redis)
    control_plane = InMemoryControlPlaneRepository()
    await control_plane.create_run(
        "run-invalid-payload",
        "agent",
        {},
        "user",
        created_at=utc_now_iso(),
    )
    msg = RunMessage(
        run_id="run-invalid-payload",
        workload_name="agent",
        payload="[]",
        user="user",
    ).model_dump(mode="python", exclude_none=True)

    await _process_message(
        fake_redis,
        control_plane,
        "1-0",
        msg,
        workloads_dir=tmp_path,
        heartbeat_interval_seconds=0.01,
    )

    run = await control_plane.get_run("run-invalid-payload")
    assert run is not None
    assert run.status == "failed"
    assert "Invalid payload" in str(run.error)


async def test_missing_workload_fails_run(fake_redis: Any, tmp_path) -> None:
    await _ensure_consumer_group(fake_redis)
    control_plane = InMemoryControlPlaneRepository()
    await control_plane.create_run(
        "run-missing-workload",
        "missing",
        {},
        "user",
        created_at=utc_now_iso(),
    )
    msg = RunMessage(
        run_id="run-missing-workload",
        workload_name="missing",
        payload="{}",
        user="user",
    ).model_dump(mode="python", exclude_none=True)

    await _process_message(
        fake_redis,
        control_plane,
        "1-0",
        msg,
        workloads_dir=tmp_path,
        heartbeat_interval_seconds=0.01,
    )

    run = await control_plane.get_run("run-missing-workload")
    assert run is not None
    assert run.status == "failed"
    assert "not found" in str(run.error)


async def test_cancel_requested_run_is_canceled_before_execution(
    fake_redis: Any, tmp_path
) -> None:
    await _ensure_consumer_group(fake_redis)
    control_plane = InMemoryControlPlaneRepository()
    workload = _agent_workload()
    await control_plane.upsert_workload(workload, "user")
    await control_plane.create_run(
        "run-cancel",
        "agent",
        {},
        "user",
        created_at=utc_now_iso(),
    )
    await control_plane.update_run("run-cancel", status="cancel_requested")
    msg = RunMessage(
        run_id="run-cancel",
        workload_name="agent",
        payload="{}",
        user="user",
    ).model_dump(mode="python", exclude_none=True)

    await _process_message(
        fake_redis,
        control_plane,
        "1-0",
        msg,
        workloads_dir=tmp_path,
        heartbeat_interval_seconds=0.01,
    )

    run = await control_plane.get_run("run-cancel")
    assert run is not None
    assert run.status == "canceled"


async def test_mark_stale_runs_marks_active_run_lost() -> None:
    control_plane = InMemoryControlPlaneRepository()
    old_heartbeat = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
    await control_plane.create_run(
        "run-stale",
        "agent",
        {},
        "user",
        created_at=old_heartbeat,
    )
    await _advance_run(
        control_plane,
        "run-stale",
        "running",
        heartbeat_at=old_heartbeat,
        updated_at=old_heartbeat,
    )

    await mark_stale_runs(control_plane, stale_after_seconds=120)

    run = await control_plane.get_run("run-stale")
    events = await control_plane.list_run_events("run-stale")

    assert run is not None
    assert run.status == "lost"
    assert run.error is not None
    assert "Heartbeat stale" in run.error
    assert run.completed_at is not None
    assert events[-1].type == "run.lost"


async def test_mark_stale_runs_ignores_recent_and_terminal_runs() -> None:
    control_plane = InMemoryControlPlaneRepository()
    now = utc_now_iso()
    old_heartbeat = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
    await control_plane.create_run(
        "run-recent",
        "agent",
        {},
        "user",
        created_at=now,
    )
    await _advance_run(
        control_plane,
        "run-recent",
        "running",
        heartbeat_at=now,
        updated_at=now,
    )
    await control_plane.create_run(
        "run-terminal",
        "agent",
        {},
        "user",
        created_at=old_heartbeat,
    )
    await _advance_run(
        control_plane,
        "run-terminal",
        "succeeded",
        heartbeat_at=old_heartbeat,
        updated_at=old_heartbeat,
        completed_at=old_heartbeat,
    )

    await mark_stale_runs(control_plane, stale_after_seconds=120)

    recent = await control_plane.get_run("run-recent")
    terminal = await control_plane.get_run("run-terminal")

    assert recent is not None
    assert terminal is not None
    assert recent.status == "running"
    assert terminal.status == "succeeded"


async def test_mark_stale_runs_skips_refreshed_heartbeat() -> None:
    class RefreshingRepository(InMemoryControlPlaneRepository):
        async def find_stale_runs(self, **kwargs: Any) -> list[StoredRun]:
            stale = await super().find_stale_runs(**kwargs)
            refreshed = utc_now_iso()
            await self.update_run(
                "run-refreshed",
                heartbeat_at=refreshed,
                updated_at=refreshed,
            )
            return stale

    control_plane = RefreshingRepository()
    old_heartbeat = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
    await control_plane.create_run(
        "run-refreshed",
        "agent",
        {},
        "user",
        created_at=old_heartbeat,
    )
    await _advance_run(
        control_plane,
        "run-refreshed",
        "running",
        heartbeat_at=old_heartbeat,
        updated_at=old_heartbeat,
    )

    await mark_stale_runs(control_plane, stale_after_seconds=120)

    run = await control_plane.get_run("run-refreshed")
    events = await control_plane.list_run_events("run-refreshed")

    assert run is not None
    assert run.status == "running"
    assert events == []


async def test_reclaim_pending_runs_processes_queued_run(
    fake_redis: Any,
    tmp_path,
    monkeypatch,
) -> None:
    async def fake_send_message(
        self: HttpAgentAdapter, payload: dict[str, Any]
    ) -> dict[str, Any]:
        del payload
        return {"response": "reclaimed", "adapter": self.name}

    monkeypatch.setattr(HttpAgentAdapter, "send_message", fake_send_message)
    await _ensure_consumer_group(fake_redis)
    control_plane = InMemoryControlPlaneRepository()
    workload = _agent_workload()
    await control_plane.upsert_workload(workload, "user")
    await control_plane.create_run(
        "run-pending",
        "agent",
        {},
        "user",
        created_at=utc_now_iso(),
    )
    stream_message = RunMessage(
        run_id="run-pending",
        workload_name="agent",
        payload="{}",
        user="user",
    ).model_dump(mode="python", exclude_none=True)
    await fake_redis.xadd(RUN_STREAM, stream_message)
    await fake_redis.xreadgroup(
        CONSUMER_GROUP,
        "old-worker",
        {RUN_STREAM: ">"},
        count=1,
    )
    await asyncio.sleep(0.01)

    reclaimed = await reclaim_pending_runs(
        fake_redis,
        control_plane,
        "new-worker",
        workloads_dir=tmp_path,
        heartbeat_interval_seconds=0.01,
        min_idle_seconds=0.001,
        count=10,
    )

    run = await control_plane.get_run("run-pending")
    pending = await fake_redis.xpending(RUN_STREAM, CONSUMER_GROUP)

    assert reclaimed == 1
    assert run is not None
    assert run.status == "succeeded"
    assert pending["pending"] == 0


async def test_reclaim_pending_runs_skips_active_heartbeating_run(
    fake_redis: Any,
    tmp_path,
) -> None:
    await _ensure_consumer_group(fake_redis)
    control_plane = InMemoryControlPlaneRepository()
    await control_plane.create_run(
        "run-active",
        "agent",
        {},
        "user",
        created_at=utc_now_iso(),
    )
    now = utc_now_iso()
    await _advance_run(
        control_plane,
        "run-active",
        "running",
        heartbeat_at=now,
        updated_at=now,
    )
    stream_message = RunMessage(
        run_id="run-active",
        workload_name="agent",
        payload="{}",
        user="user",
    ).model_dump(mode="python", exclude_none=True)
    await fake_redis.xadd(RUN_STREAM, stream_message)
    await fake_redis.xreadgroup(
        CONSUMER_GROUP,
        "old-worker",
        {RUN_STREAM: ">"},
        count=1,
    )
    await asyncio.sleep(0.01)

    reclaimed = await reclaim_pending_runs(
        fake_redis,
        control_plane,
        "new-worker",
        workloads_dir=tmp_path,
        heartbeat_interval_seconds=0.01,
        min_idle_seconds=0.001,
        count=10,
    )

    pending = await fake_redis.xpending(RUN_STREAM, CONSUMER_GROUP)

    assert reclaimed == 0
    assert pending["pending"] == 1
    assert pending["consumers"][0]["name"] == "old-worker"


async def test_reclaim_pending_runs_acks_terminal_run(
    fake_redis: Any,
    tmp_path,
) -> None:
    await _ensure_consumer_group(fake_redis)
    control_plane = InMemoryControlPlaneRepository()
    completed_at = utc_now_iso()
    await control_plane.create_run(
        "run-terminal-pending",
        "agent",
        {},
        "user",
        created_at=completed_at,
    )
    await _advance_run(
        control_plane,
        "run-terminal-pending",
        "succeeded",
        updated_at=completed_at,
        completed_at=completed_at,
    )
    stream_message = RunMessage(
        run_id="run-terminal-pending",
        workload_name="agent",
        payload="{}",
        user="user",
    ).model_dump(mode="python", exclude_none=True)
    await fake_redis.xadd(RUN_STREAM, stream_message)
    await fake_redis.xreadgroup(
        CONSUMER_GROUP,
        "old-worker",
        {RUN_STREAM: ">"},
        count=1,
    )
    await asyncio.sleep(0.01)

    reclaimed = await reclaim_pending_runs(
        fake_redis,
        control_plane,
        "new-worker",
        workloads_dir=tmp_path,
        heartbeat_interval_seconds=0.01,
        min_idle_seconds=0.001,
        count=10,
    )

    pending = await fake_redis.xpending(RUN_STREAM, CONSUMER_GROUP)

    assert reclaimed == 1
    assert pending["pending"] == 0
