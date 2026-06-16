from typing import Literal

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: Literal["ok"]
    uptime_seconds: float


class CheckResult(BaseModel):
    status: Literal["ok", "degraded", "unavailable", "error"]
    latency_ms: float | None = None
    message: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class ReadyResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    checks: dict[str, CheckResult]
