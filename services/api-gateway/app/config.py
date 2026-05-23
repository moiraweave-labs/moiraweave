from functools import lru_cache
from typing import Literal

from pydantic import AnyHttpUrl, RedisDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # tolerate unrecognised env vars (common in Docker)
    )

    app_name: str = "MoiraWeave API Gateway"
    app_version: str = "0.1.0"

    # JWT
    jwt_secret_key: SecretStr
    jwt_algorithm: Literal["HS256", "RS256", "ES256"] = "HS256"
    jwt_access_token_expire_minutes: int = 30

    # Rate limiting (slowapi format: "N/period")
    rate_limit_default: str = "100/minute"
    rate_limit_auth: str = "10/minute"

    # Redis — queues and short-lived coordination.
    redis_url: RedisDsn = RedisDsn("redis://redis:6379/0")
    run_ttl_seconds: int = 604800
    job_ttl_seconds: int = 3600

    # Control-plane storage. Postgres is the source of truth for workloads,
    # runs, sessions, events, and artifact metadata.
    postgres_dsn: str = (
        "postgresql://moiraweave:moiraweave-dev@postgres:5432/moiraweave"
    )

    # Qdrant — vector store for RAG/search workloads
    qdrant_url: AnyHttpUrl = AnyHttpUrl("http://qdrant:6333")
    embedding_model: str = "BAAI/bge-small-en-v1.5"

    # CORS — override with a comma-separated list via CORS_ORIGINS env var
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:8000"]

    # Demo auth credentials — override via DEMO_USERNAME / DEMO_PASSWORD env vars.
    # Replace with a database-backed user store for production.
    demo_username: str = "admin"
    demo_password: SecretStr = SecretStr("demo-password")

    # OpenTelemetry
    otel_enabled: bool = True
    otel_service_name: str = "api-gateway"
    otel_otlp_endpoint: AnyHttpUrl = AnyHttpUrl("http://jaeger:4318")  # OTLP/HTTP
    otel_sample_rate: float = 1.0  # 1.0 = 100%; reduce in high-volume prod

    # Workload manifests and artifact storage.
    workloads_dir: str = "workloads"
    artifacts_dir: str = "artifacts"


@lru_cache
def get_settings() -> Settings:
    """Return the cached Settings instance.

    Using ``@lru_cache`` keeps Settings as a singleton while allowing tests
    to override it via ``get_settings.cache_clear()`` + dependency override.
    """
    return Settings()
