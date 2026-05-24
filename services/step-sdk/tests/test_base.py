"""Tests for moiraweave_step_sdk BaseStep and KServe V2 models."""

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from moiraweave_step_sdk.base import BaseStep
from moiraweave_step_sdk.models import InferRequest, InferResponse

# ---------------------------------------------------------------------------
# Minimal concrete step used across all tests
# ---------------------------------------------------------------------------


class EchoStep(BaseStep):
    """Returns the first input tensor unchanged."""

    @property
    def name(self) -> str:
        return "echo"

    @property
    def version(self) -> str:
        return "1"

    async def predict(self, request: InferRequest) -> InferResponse:
        return InferResponse(
            model_name=self.name,
            id=request.id,
            outputs=list(request.inputs),
        )


@pytest.fixture
def app() -> FastAPI:
    return EchoStep().build_app()


async def _client_get(app: FastAPI, path: str) -> Response:
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        return await client.get(path)


async def _client_post(
    app: FastAPI,
    path: str,
    payload: dict[str, object],
) -> Response:
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        return await client.post(path, json=payload)


# ---------------------------------------------------------------------------
# Health endpoints
# ---------------------------------------------------------------------------


async def test_liveness_returns_live(app: FastAPI) -> None:
    resp = await _client_get(app, "/v2/health/live")
    assert resp.status_code == 200
    assert resp.json() == {"live": True}


async def test_server_ready_returns_live(app: FastAPI) -> None:
    resp = await _client_get(app, "/v2/health/ready")
    assert resp.status_code == 200
    assert resp.json() == {"live": True}


async def test_model_ready_returns_ready(app: FastAPI) -> None:
    resp = await _client_get(app, "/v2/models/echo/ready")
    assert resp.status_code == 200
    assert resp.json() == {"name": "echo", "ready": True}


async def test_model_ready_unknown_model_returns_404(app: FastAPI) -> None:
    resp = await _client_get(app, "/v2/models/unknown/ready")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Inference endpoint
# ---------------------------------------------------------------------------


async def test_infer_echoes_input(app: FastAPI) -> None:
    payload = {
        "id": "req-1",
        "inputs": [
            {"name": "text", "shape": [1], "datatype": "BYTES", "data": ["hello"]}
        ],
    }
    resp = await _client_post(app, "/v2/models/echo/infer", payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["model_name"] == "echo"
    assert body["id"] == "req-1"
    assert body["outputs"][0]["name"] == "text"
    assert body["outputs"][0]["data"] == ["hello"]


async def test_infer_unknown_model_returns_404(app: FastAPI) -> None:
    payload = {
        "inputs": [{"name": "x", "shape": [1], "datatype": "BYTES", "data": ["y"]}]
    }
    resp = await _client_post(app, "/v2/models/wrong/infer", payload)
    assert resp.status_code == 404


async def test_infer_without_id_omits_id_field(app: FastAPI) -> None:
    payload = {
        "inputs": [{"name": "x", "shape": [1], "datatype": "FP32", "data": [1.0]}]
    }
    resp = await _client_post(app, "/v2/models/echo/infer", payload)
    assert resp.status_code == 200
    assert resp.json()["id"] is None


# ---------------------------------------------------------------------------
# Custom is_ready override
# ---------------------------------------------------------------------------


class NotReadyStep(EchoStep):
    @property
    def name(self) -> str:
        return "not-ready"

    async def is_ready(self) -> bool:
        return False


async def test_not_ready_step_returns_false() -> None:
    app = NotReadyStep().build_app()
    assert (await _client_get(app, "/v2/health/ready")).json() == {"live": False}
    assert (await _client_get(app, "/v2/models/not-ready/ready")).json() == {
        "name": "not-ready",
        "ready": False,
    }


# ---------------------------------------------------------------------------
# Model metadata endpoint (KServe V2 GET /v2/models/{name})
# ---------------------------------------------------------------------------


async def test_model_metadata_returns_correct_response(app: FastAPI) -> None:
    resp = await _client_get(app, "/v2/models/echo")
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "echo"
    assert body["platform"] == "moiraweave"
    assert body["versions"] == ["1"]
    assert body["task"] == ""
    assert body["implementation"] == ""
    assert body["inputs"] == []
    assert body["outputs"] == []


async def test_model_metadata_unknown_model_returns_404(app: FastAPI) -> None:
    resp = await _client_get(app, "/v2/models/unknown")
    assert resp.status_code == 404
