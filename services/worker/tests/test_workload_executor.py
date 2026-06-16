"""Tests for workload executors."""

from __future__ import annotations

from typing import Any

import pytest
from moiraweave_shared.workloads import WorkloadDefinition

from app.workload_executor import (
    AgentExecutor,
    ModelServiceExecutor,
    PipelineExecutor,
    RunCancelledError,
    WorkloadExecutor,
    _kserve_request,
    _model_response,
)


def _workload(
    name: str, workload_type: str, **spec_overrides: Any
) -> WorkloadDefinition:
    spec: dict[str, Any] = {"type": workload_type}
    if workload_type != "pipeline":
        spec["image"] = f"ghcr.io/example/{name}:latest"
    spec.update(spec_overrides)
    return WorkloadDefinition.model_validate(
        {
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {"name": name},
            "spec": spec,
        }
    )


async def _emit(
    event_type: str, message: str, data: dict[str, Any] | None = None
) -> None:
    del event_type, message, data


async def _not_canceled() -> bool:
    return False


async def _canceled() -> bool:
    return True


async def test_model_executor_without_endpoint_returns_mock_response() -> None:
    workload = _workload("model", "model-service")

    result = await ModelServiceExecutor(workload).execute(
        {"text": "hello"},
        emit=_emit,
        is_cancel_requested=_not_canceled,
    )

    assert result == {"workload": "model", "result": {"text": "hello"}}


def test_model_executor_uses_deployment_service_name() -> None:
    workload = _workload(
        "model",
        "model-service",
        ports=[{"name": "http", "port": 8080}],
        deployment={"serviceName": "model-runtime"},
    )

    assert (
        ModelServiceExecutor(workload)._endpoint()
        == "http://model-runtime:8080/v2/models/model/infer"
    )


async def test_workload_executor_cancels_before_dispatch() -> None:
    workload = _workload("model", "model-service")

    with pytest.raises(RunCancelledError):
        await WorkloadExecutor({"model": workload}).execute(
            workload,
            {},
            emit=_emit,
            is_cancel_requested=_canceled,
        )


async def test_agent_executor_uses_adapter_and_cancels_after_dispatch(
    monkeypatch,
) -> None:
    calls = {"cancel": 0, "check": 0}

    class FakeAdapter:
        name = "generic-http"

        async def send_message(self, payload: dict[str, Any]) -> dict[str, Any]:
            return {"response": payload["message"], "adapter": self.name}

        async def wait_for_completion(
            self,
            payload: dict[str, Any],
            accepted: dict[str, Any],
            *,
            emit: Any,
            is_cancel_requested: Any,
            timeout_seconds: float,
        ) -> dict[str, Any]:
            del payload, emit, is_cancel_requested, timeout_seconds
            return accepted

        async def cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
            del payload
            calls["cancel"] += 1
            return {"accepted": True}

    def fake_build_adapter(
        workload: WorkloadDefinition, *, timeout_seconds: float
    ) -> FakeAdapter:
        del workload, timeout_seconds
        return FakeAdapter()

    async def cancel_after_dispatch() -> bool:
        calls["check"] += 1
        return calls["check"] > 1

    monkeypatch.setitem(
        AgentExecutor.execute.__globals__, "build_agent_adapter", fake_build_adapter
    )
    workload = _workload("agent", "agent-service")

    with pytest.raises(RunCancelledError):
        await AgentExecutor(workload).execute(
            {"message": "hello"},
            emit=_emit,
            is_cancel_requested=cancel_after_dispatch,
        )

    assert calls["cancel"] == 1


async def test_pipeline_executor_runs_nodes_and_input_from() -> None:
    model_a = _workload("a", "model-service")
    model_b = _workload("b", "model-service")
    pipeline = _workload(
        "pipe",
        "pipeline",
        steps=[
            {"id": "first", "uses": "a", "payload": {"a": "1"}},
            {"id": "second", "uses": "b", "inputFrom": "first", "payload": {"b": "2"}},
        ],
    )

    result = await PipelineExecutor(pipeline, {"a": model_a, "b": model_b}).execute(
        {"text": "hello"},
        emit=_emit,
        is_cancel_requested=_not_canceled,
    )

    assert result["workload"] == "b"
    assert result["result"]["b"] == "2"


async def test_pipeline_executor_rejects_unknown_workload() -> None:
    pipeline = _workload(
        "pipe",
        "pipeline",
        steps=[{"id": "missing", "uses": "missing"}],
    )

    with pytest.raises(ValueError, match="unknown workload"):
        await PipelineExecutor(pipeline, {}).execute(
            {},
            emit=_emit,
            is_cancel_requested=_not_canceled,
        )


def test_kserve_helpers_parse_known_shapes() -> None:
    assert _kserve_request({"prompt": "hi"})["inputs"][0]["data"] == ["hi"]
    assert _model_response({"outputs": [{"name": "answer", "data": ["ok"]}]}) == {
        "answer": "ok"
    }
    assert _model_response(["raw"]) == {"response": ["raw"]}
