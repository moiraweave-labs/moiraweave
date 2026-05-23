"""Tests for workload run consumption."""

from __future__ import annotations

import json
from typing import Any

from moiraweave_shared.control_plane import InMemoryControlPlaneRepository, utc_now_iso
from moiraweave_shared.schemas import RunMessage
from moiraweave_shared.streams import DEAD_LETTER_STREAM
from moiraweave_shared.workloads import WorkloadDefinition

from app.agent_adapters import HttpAgentAdapter
from app.run_consumer import _ensure_consumer_group, _process_message


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
