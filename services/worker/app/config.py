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
    log_level: str = "INFO"

    # MLflow — inference metrics tracking
    mlflow_tracking_uri: str = "http://mlflow.mlflow.svc.cluster.local:80"
    mlflow_model_name: str = "moiraweave-pipeline"
    mlflow_model_version: str = "1"
    mlflow_experiment_name: str = "moiraweave-inference"

    # Pipeline-as-code — directory containing per-pipeline subdirectories
    pipelines_dir: str = "pipelines"


@lru_cache
def get_settings() -> Settings:
    """Return the cached Settings singleton."""
    return Settings()
