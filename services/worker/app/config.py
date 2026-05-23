from functools import lru_cache

from pydantic import RedisDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    redis_url: RedisDsn = RedisDsn("redis://redis:6379/0")
    postgres_dsn: str = (
        "postgresql://moiraweave:moiraweave-dev@postgres:5432/moiraweave"
    )
    log_level: str = "INFO"

    # MLflow — inference metrics tracking
    mlflow_tracking_uri: str = "http://mlflow.mlflow.svc.cluster.local:80"
    mlflow_model_name: str = "moiraweave-pipeline"
    mlflow_model_version: str = "1"
    mlflow_experiment_name: str = "moiraweave-inference"

    # Retained for old env compatibility. Run metadata lives in Postgres.
    run_ttl_seconds: int = 604800

    # HTTP timeout for model/agent/pipeline calls (seconds)
    call_timeout_seconds: float = 300.0
    step_timeout_seconds: float = 300.0

    # Workload manifests and artifacts
    workloads_dir: str = "workloads"
    artifacts_dir: str = "artifacts"

    # Worker liveness and stale-run recovery.
    heartbeat_interval_seconds: float = 10.0
    stale_run_seconds: float = 120.0
    stale_check_interval_seconds: float = 30.0


@lru_cache
def get_settings() -> Settings:
    """Return the cached Settings singleton."""
    return Settings()
