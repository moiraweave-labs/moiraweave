"""Generic pipeline runner — executes a PipelineDefinition by calling steps via HTTP.

Each step is called via the KServe V2 ``POST /v2/models/{id}/infer`` endpoint.
The runner converts the job payload dict to V2 input tensors, passes outputs
from one step as inputs to the next, and returns the final output dict.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from moiraweave_shared.pipeline import PipelineDefinition, StepConfig

logger = logging.getLogger(__name__)

_STEP_TIMEOUT_SECONDS = 300.0


class PipelineRunner:
    """Execute a :class:`~moiraweave_shared.pipeline.PipelineDefinition` by calling
    each step's ``/v2/models/{id}/infer`` endpoint in declaration order.

    :param pipeline: Validated pipeline definition.
    """

    def __init__(self, pipeline: PipelineDefinition) -> None:
        self._pipeline = pipeline

    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Execute the pipeline for a single job payload.

        The pipeline payload is passed as V2 tensors to the first step.
        Each subsequent step receives the previous step's output tensors
        as its inputs.

        :param payload: Flat ``{name: value}`` dict of job inputs.
        :returns: Final step output as a ``{tensor_name: value}`` dict.
        :raises httpx.HTTPStatusError: If any step returns a non-2xx response.
        """
        current: dict[str, Any] = payload
        async with httpx.AsyncClient(timeout=_STEP_TIMEOUT_SECONDS) as client:
            for step in self._pipeline.steps:
                current = await _call_step(step, current, client)
        return current

    async def check_ready(self, timeout: float = 5.0) -> bool:
        """Return ``True`` when every step's ``GET /v2/health/ready`` responds 200.

        :param timeout: Per-step request timeout in seconds.
        :returns: ``True`` only when all steps are healthy.
        """
        async with httpx.AsyncClient(timeout=timeout) as client:
            for step in self._pipeline.steps:
                try:
                    resp = await client.get(f"{step.url}/v2/health/ready")
                    if not resp.is_success:
                        logger.warning(
                            "step_not_ready step=%s status=%d",
                            step.id,
                            resp.status_code,
                        )
                        return False
                except httpx.RequestError:
                    logger.warning("step_unreachable step=%s url=%s", step.id, step.url)
                    return False
        return True


async def _call_step(
    step: StepConfig, payload: dict[str, Any], client: httpx.AsyncClient
) -> dict[str, Any]:
    """POST *payload* to a step's V2 infer endpoint and return the output dict.

    :param step: Step configuration (id, url).
    :param payload: ``{name: value}`` dict of input tensors.
    :param client: Shared async HTTP client (connection pool reused across steps).
    :returns: ``{tensor_name: value}`` dict from the step's response outputs.
    :raises httpx.HTTPStatusError: When the step returns a non-2xx status.
    """
    url = f"{step.url}/v2/models/{step.id}/infer"
    request_body = {
        "inputs": [
            {"name": k, "shape": [1], "datatype": "BYTES", "data": [str(v)]}
            for k, v in payload.items()
        ],
    }
    resp = await client.post(url, json=request_body)
    resp.raise_for_status()

    response_data: dict[str, Any] = resp.json()
    raw_outputs: list[Any] = response_data.get("outputs", [])
    return {
        t["name"]: t["data"][0] if len(t["data"]) == 1 else t["data"]
        for t in raw_outputs
    }
