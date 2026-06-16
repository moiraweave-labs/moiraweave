"""Executors for MoiraWeave workload runs."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import httpx
from pydantic import BaseModel

from app.agent_adapters import build_agent_adapter
from app.config import get_settings

if TYPE_CHECKING:
    from moiraweave_shared.workloads import WorkloadDefinition

logger = logging.getLogger(__name__)

EventEmitter = Callable[[str, str, dict[str, Any] | None], Awaitable[None]]
CancelChecker = Callable[[], Awaitable[bool]]


class RunCancelledError(Exception):
    """Raised when a run sees a cooperative cancellation request."""


class _InferTensor(BaseModel):
    name: str
    data: list[Any]


class _InferResponse(BaseModel):
    outputs: list[_InferTensor] = []


class WorkloadExecutor:
    """Dispatch a workload run to the appropriate executor implementation."""

    def __init__(self, workloads: dict[str, WorkloadDefinition]) -> None:
        self._workloads = workloads

    async def execute(
        self,
        workload: WorkloadDefinition,
        payload: dict[str, Any],
        *,
        emit: EventEmitter,
        is_cancel_requested: CancelChecker,
    ) -> dict[str, Any]:
        if await is_cancel_requested():
            raise RunCancelledError("Run was canceled before it started")

        if workload.spec.type == "model-service":
            return await ModelServiceExecutor(workload).execute(
                payload, emit=emit, is_cancel_requested=is_cancel_requested
            )
        if workload.spec.type == "agent-service":
            return await AgentExecutor(workload).execute(
                payload, emit=emit, is_cancel_requested=is_cancel_requested
            )
        return await PipelineExecutor(workload, self._workloads).execute(
            payload, emit=emit, is_cancel_requested=is_cancel_requested
        )


class ModelServiceExecutor:
    """Call a model-service workload through a generic HTTP/KServe contract."""

    def __init__(self, workload: WorkloadDefinition) -> None:
        self._workload = workload

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        emit: EventEmitter,
        is_cancel_requested: CancelChecker,
    ) -> dict[str, Any]:
        if await is_cancel_requested():
            raise RunCancelledError("Run canceled before model call")

        endpoint = self._endpoint()
        if endpoint is None:
            await emit(
                "executor.model.mock",
                "No endpoint declared; returning mock model response",
                {"workload": self._workload.metadata.name},
            )
            return {"workload": self._workload.metadata.name, "result": payload}

        await emit(
            "executor.model.call",
            "Calling model-service endpoint",
            {"endpoint": endpoint},
        )
        async with httpx.AsyncClient(
            timeout=get_settings().call_timeout_seconds
        ) as client:
            body = payload if "inputs" in payload else _kserve_request(payload)
            response = await client.post(endpoint, json=body)
            response.raise_for_status()
            data = response.json()

        if await is_cancel_requested():
            raise RunCancelledError("Run canceled after model call")
        return _model_response(data)

    def _endpoint(self) -> str | None:
        if self._workload.spec.endpoint:
            return self._workload.spec.endpoint
        if not self._workload.spec.ports:
            return None
        port = self._workload.spec.ports[0].port
        name = self._workload.metadata.name
        service_name = self._workload.spec.deployment.serviceName or name
        return f"http://{service_name}:{port}/v2/models/{name}/infer"


class AgentExecutor:
    """Talk to Hermes/OpenClaw-style agent runtimes through adapter hooks."""

    def __init__(self, workload: WorkloadDefinition) -> None:
        self._workload = workload

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        emit: EventEmitter,
        is_cancel_requested: CancelChecker,
    ) -> dict[str, Any]:
        if await is_cancel_requested():
            raise RunCancelledError("Agent run canceled before dispatch")

        settings = get_settings()
        dispatch_timeout = min(
            settings.call_timeout_seconds,
            self._workload.spec.agent.dispatchTimeoutSeconds,
        )
        adapter = build_agent_adapter(self._workload, timeout_seconds=dispatch_timeout)

        await emit(
            "executor.agent.call",
            "Dispatching message to agent runtime",
            {
                "adapter": adapter.name,
                "dispatch_timeout_seconds": dispatch_timeout,
                "session_id": payload.get("session_id"),
            },
        )
        accepted = await adapter.send_message(payload)
        data = await adapter.wait_for_completion(
            payload,
            accepted,
            emit=emit,
            is_cancel_requested=is_cancel_requested,
            timeout_seconds=float(self._workload.spec.execution.timeoutSeconds),
        )

        if await is_cancel_requested():
            await adapter.cancel({**payload, **data})
            raise RunCancelledError("Agent run canceled after runtime call")
        return data if isinstance(data, dict) else {"response": data}


class PipelineExecutor:
    """Execute a pipeline workload whose nodes call other workloads."""

    def __init__(
        self,
        workload: WorkloadDefinition,
        workloads: dict[str, WorkloadDefinition],
    ) -> None:
        self._workload = workload
        self._workloads = workloads

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        emit: EventEmitter,
        is_cancel_requested: CancelChecker,
    ) -> dict[str, Any]:
        if not self._workload.spec.steps:
            await emit(
                "executor.pipeline.empty",
                "Pipeline has no steps; returning input payload",
                {"workload": self._workload.metadata.name},
            )
            return payload

        outputs: dict[str, dict[str, Any]] = {}
        last_output = payload
        dispatcher = WorkloadExecutor(self._workloads)
        for node in self._workload.spec.steps:
            if await is_cancel_requested():
                raise RunCancelledError(f"Pipeline canceled before node {node.id}")
            if node.uses not in self._workloads:
                raise ValueError(
                    f"Pipeline node {node.id!r} references unknown workload {node.uses!r}"
                )
            node_input = last_output
            if node.inputFrom is not None:
                if node.inputFrom not in outputs:
                    raise ValueError(
                        f"Pipeline node {node.id!r} inputFrom={node.inputFrom!r} "
                        "has not executed"
                    )
                node_input = outputs[node.inputFrom]
            merged_input = {**node_input, **node.payload}
            await emit(
                "executor.pipeline.node_start",
                "Starting pipeline node",
                {"node": node.id, "uses": node.uses},
            )
            output = await dispatcher.execute(
                self._workloads[node.uses],
                merged_input,
                emit=emit,
                is_cancel_requested=is_cancel_requested,
            )
            outputs[node.id] = output
            last_output = output
            await emit(
                "executor.pipeline.node_done",
                "Pipeline node completed",
                {"node": node.id, "uses": node.uses},
            )
        return last_output


def _kserve_request(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "inputs": [
            {"name": key, "shape": [1], "datatype": "BYTES", "data": [str(value)]}
            for key, value in payload.items()
        ],
    }


def _model_response(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {"response": data}
    try:
        parsed = _InferResponse.model_validate(data)
    except Exception:  # noqa: BLE001
        return data
    if not parsed.outputs:
        return data
    return {
        tensor.name: tensor.data[0] if len(tensor.data) == 1 else tensor.data
        for tensor in parsed.outputs
    }
