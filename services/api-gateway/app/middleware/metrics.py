from __future__ import annotations

from time import perf_counter
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from prometheus_client.registry import CollectorRegistry

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

_registry = CollectorRegistry()

_request_count = Counter(
    "moiraweave_api_http_requests_total",
    "Total HTTP requests served by the API gateway.",
    ["method", "path", "status"],
    registry=_registry,
)

_request_duration = Histogram(
    "moiraweave_api_http_request_duration_seconds",
    "HTTP request duration in seconds for the API gateway.",
    ["method", "path", "status"],
    registry=_registry,
)


def _route_path(request: Request) -> str:
    route: Any = request.scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str):
        return path
    return request.url.path


def setup_metrics(app: FastAPI) -> None:
    @app.middleware("http")
    async def prometheus_metrics(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if request.url.path == "/metrics":
            return await call_next(request)

        start = perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            labels = {
                "method": request.method,
                "path": _route_path(request),
                "status": str(status_code),
            }
            elapsed = perf_counter() - start
            _request_count.labels(**labels).inc()
            _request_duration.labels(**labels).observe(elapsed)

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(_registry), media_type=CONTENT_TYPE_LATEST)
