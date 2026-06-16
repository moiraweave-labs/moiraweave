"""Tests for /health and /ready endpoints."""

from unittest.mock import AsyncMock, MagicMock

from fakeredis.aioredis import FakeRedis
from httpx import AsyncClient
from moiraweave_shared.control_plane import InMemoryControlPlaneRepository
from moiraweave_shared.streams import CONSUMER_GROUP, RUN_STREAM
from pytest_mock import MockerFixture


async def _attach_worker_consumer(redis: FakeRedis) -> None:
    await redis.xgroup_create(RUN_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    msg_id = await redis.xadd(RUN_STREAM, {"run_id": "ready-worker-check"})
    await redis.xreadgroup(
        CONSUMER_GROUP,
        "worker-ready-test",
        {RUN_STREAM: ">"},
        count=1,
    )
    await redis.xack(RUN_STREAM, CONSUMER_GROUP, msg_id)


async def test_health_returns_ok(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["uptime_seconds"] >= 0


async def test_ready_all_ok(
    client: AsyncClient, fake_redis: FakeRedis, mock_qdrant: MagicMock
) -> None:
    del mock_qdrant
    await _attach_worker_consumer(fake_redis)

    response = await client.get("/ready")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["checks"]["redis"]["status"] == "ok"
    assert body["checks"]["run_queue"]["status"] == "ok"
    assert body["checks"]["run_queue"]["metadata"]["consumers"] == 1
    assert body["checks"]["postgres"]["status"] == "ok"
    assert body["checks"]["qdrant"]["status"] == "ok"


async def test_ready_run_queue_degraded_without_worker(client: AsyncClient) -> None:
    response = await client.get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["checks"]["run_queue"]["status"] == "degraded"
    assert body["checks"]["run_queue"]["metadata"]["consumer_group"] == CONSUMER_GROUP


async def test_ready_redis_degraded(
    client: AsyncClient, fake_redis: FakeRedis, mocker: MockerFixture
) -> None:
    # Given: Redis ping raises ConnectionError
    # ``app.state.redis`` IS ``fake_redis`` — patch ping on the same object.
    mocker.patch.object(
        fake_redis, "ping", AsyncMock(side_effect=ConnectionError("down"))
    )

    # When
    response = await client.get("/ready")

    # Then
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["checks"]["redis"]["status"] == "error"
    assert "down" in body["checks"]["redis"]["message"]


async def test_ready_qdrant_degraded(
    client: AsyncClient, mock_qdrant: MagicMock
) -> None:
    # Given: Qdrant raises ConnectionError on collections check
    mock_qdrant.get_collections.side_effect = ConnectionError("qdrant-down")

    # When
    response = await client.get("/ready")

    # Then
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["checks"]["qdrant"]["status"] == "error"


async def test_ready_postgres_degraded(
    client: AsyncClient,
    control_plane: InMemoryControlPlaneRepository,
    mocker: MockerFixture,
) -> None:
    mocker.patch.object(
        control_plane,
        "ping",
        AsyncMock(side_effect=ConnectionError("postgres-down")),
    )

    response = await client.get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["checks"]["postgres"]["status"] == "error"


async def test_ready_latency_ms_present(client: AsyncClient) -> None:
    response = await client.get("/ready")
    body = response.json()
    assert body["checks"]["redis"]["latency_ms"] >= 0
    assert body["checks"]["postgres"]["latency_ms"] >= 0
    assert body["checks"]["qdrant"]["latency_ms"] >= 0


async def test_metrics_endpoint_exposes_http_counters(client: AsyncClient) -> None:
    await client.get("/health")

    response = await client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "moiraweave_api_http_requests_total" in response.text
    assert 'path="/health"' in response.text
