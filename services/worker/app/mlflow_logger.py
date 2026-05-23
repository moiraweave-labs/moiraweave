"""Inference metrics logger — persists per-run metrics to MLflow.

Each completed (or failed) inference run logs a run under the configured
experiment.  The run is tagged with the model name and version so that
runs can be filtered by model in the MLflow UI.
"""

import logging

import mlflow

from app.config import Settings

logger = logging.getLogger(__name__)


def _log_run_sync(
    settings: Settings,
    run_id: str,
    duration_seconds: float,
    status: str,
    *,
    tokens_per_second: float | None = None,
    error_type: str | None = None,
) -> None:
    """Synchronous MLflow logging — runs inside the thread executor."""
    try:
        mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
        mlflow.set_experiment(settings.mlflow_experiment_name)
        with mlflow.start_run(run_name=f"run-{run_id}"):
            mlflow.set_tags(
                {
                    "model.name": settings.mlflow_model_name,
                    "model.version": settings.mlflow_model_version,
                    "run.id": run_id,
                    "run.status": status,
                }
            )
            mlflow.log_metric("duration_seconds", duration_seconds)
            mlflow.log_metric("success", 1.0 if status == "succeeded" else 0.0)
            if tokens_per_second is not None:
                mlflow.log_metric("tokens_per_second", tokens_per_second)
            if error_type:
                mlflow.log_param("error_type", error_type)
    except Exception:  # noqa: BLE001
        # MLflow logging is best-effort and never fails the workload run.
        logger.warning("mlflow_log_failed run_id=%s", run_id, exc_info=True)


async def log_inference_metrics(
    settings: Settings,
    run_id: str,
    duration_seconds: float,
    status: str,
    *,
    tokens_per_second: float | None = None,
    error_type: str | None = None,
) -> None:
    """Async wrapper for the synchronous MLflow call.

    :param settings: Application settings (tracking URI, model name/version).
    :param run_id: Unique identifier of the workload run.
    :param duration_seconds: Wall-clock time for the run.
    :param status: ``"succeeded"`` or ``"failed"``.
    :param tokens_per_second: Optional throughput estimate from the processor.
    :param error_type: Exception class name when *status* is ``"failed"``.
    """
    _log_run_sync(
        settings,
        run_id,
        duration_seconds,
        status,
        tokens_per_second=tokens_per_second,
        error_type=error_type,
    )
