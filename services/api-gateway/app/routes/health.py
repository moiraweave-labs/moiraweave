import time
from typing import Any

from fastapi import APIRouter, Request
from moiraweave_shared.streams import CONSUMER_GROUP, RUN_STREAM
from redis.exceptions import ResponseError

from app.models.health import CheckResult, HealthResponse, ReadyResponse

router = APIRouter(tags=["health"])

_START_TIME = time.monotonic()


@router.get("/health", response_model=HealthResponse, summary="Liveness probe")
async def health() -> HealthResponse:
    """Return 200 as long as the process is running."""
    return HealthResponse(status="ok", uptime_seconds=time.monotonic() - _START_TIME)


@router.get("/ready", response_model=ReadyResponse, summary="Readiness probe")
async def ready(request: Request) -> ReadyResponse:
    """Check downstream dependencies and return readiness status.

    Returns 200 even when degraded so Kubernetes keeps routing traffic;
    the ``status`` field signals the actual health to consumers.
    """
    checks: dict[str, CheckResult] = {}

    # Redis check
    t0 = time.monotonic()
    try:
        await request.app.state.redis.ping()
        checks["redis"] = CheckResult(
            status="ok",
            latency_ms=round((time.monotonic() - t0) * 1000, 2),
        )
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = CheckResult(
            status="error",
            latency_ms=round((time.monotonic() - t0) * 1000, 2),
            message=str(exc),
        )

    checks["run_queue"] = await _run_queue_check(request.app.state.redis)

    # Postgres control-plane check
    t0 = time.monotonic()
    try:
        await request.app.state.control_plane.ping()
        checks["postgres"] = CheckResult(
            status="ok",
            latency_ms=round((time.monotonic() - t0) * 1000, 2),
        )
    except Exception as exc:  # noqa: BLE001
        checks["postgres"] = CheckResult(
            status="error",
            latency_ms=round((time.monotonic() - t0) * 1000, 2),
            message=str(exc),
        )

    # Qdrant check
    t0 = time.monotonic()
    try:
        await request.app.state.qdrant.get_collections()
        checks["qdrant"] = CheckResult(
            status="ok",
            latency_ms=round((time.monotonic() - t0) * 1000, 2),
        )
    except Exception as exc:  # noqa: BLE001
        checks["qdrant"] = CheckResult(
            status="error",
            latency_ms=round((time.monotonic() - t0) * 1000, 2),
            message=str(exc),
        )

    all_ok = all(c.status == "ok" for c in checks.values())
    return ReadyResponse(
        status="ready" if all_ok else "not_ready",
        checks=checks,
    )


async def _run_queue_check(redis: Any) -> CheckResult:
    t0 = time.monotonic()
    try:
        groups = await redis.xinfo_groups(RUN_STREAM)
    except ResponseError as exc:
        return CheckResult(
            status="degraded",
            latency_ms=round((time.monotonic() - t0) * 1000, 2),
            message=(
                f"Worker consumer group {CONSUMER_GROUP!r} is not registered on "
                f"{RUN_STREAM!r}: {exc}"
            ),
            metadata={"stream": RUN_STREAM, "consumer_group": CONSUMER_GROUP},
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            status="error",
            latency_ms=round((time.monotonic() - t0) * 1000, 2),
            message=f"Run queue state could not be inspected: {exc}",
            metadata={"stream": RUN_STREAM, "consumer_group": CONSUMER_GROUP},
        )

    group = next(
        (item for item in groups if _redis_group_name(item) == CONSUMER_GROUP),
        None,
    )
    if group is None:
        return CheckResult(
            status="degraded",
            latency_ms=round((time.monotonic() - t0) * 1000, 2),
            message=f"Worker consumer group {CONSUMER_GROUP!r} is not registered.",
            metadata={
                "stream": RUN_STREAM,
                "consumer_group": CONSUMER_GROUP,
                "known_groups": [_redis_group_name(item) for item in groups],
            },
        )

    consumers = int(_redis_mapping_value(group, "consumers", 0) or 0)
    pending = int(_redis_mapping_value(group, "pending", 0) or 0)
    metadata = {
        "stream": RUN_STREAM,
        "consumer_group": CONSUMER_GROUP,
        "consumers": consumers,
        "pending": pending,
        "lag": _redis_mapping_value(group, "lag"),
    }
    if consumers <= 0:
        return CheckResult(
            status="degraded",
            latency_ms=round((time.monotonic() - t0) * 1000, 2),
            message="Run queue exists, but no worker consumer is attached.",
            metadata=metadata,
        )
    return CheckResult(
        status="ok",
        latency_ms=round((time.monotonic() - t0) * 1000, 2),
        message=f"{consumers} worker consumer(s) are attached.",
        metadata=metadata,
    )


def _redis_mapping_value(item: dict[Any, Any], key: str, default: Any = None) -> Any:
    return item.get(key, item.get(key.encode(), default))


def _redis_group_name(item: dict[Any, Any]) -> str:
    value = _redis_mapping_value(item, "name", "")
    if isinstance(value, bytes):
        return value.decode()
    return str(value)
