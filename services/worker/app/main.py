"""MoiraWeave async inference worker.

Consumes pipeline jobs from Redis Streams, dispatches each step to its
KServe V2 inference endpoint, and updates job status in Redis.

Usage:
    python -m app.main
"""

import asyncio
import logging
import signal
import uuid

from moiraweave_shared.pipeline import load_pipelines
from prometheus_client import start_http_server
from redis.asyncio import Redis

from app.config import get_settings
from app.pipeline_consumer import run_pipeline_consumer

_METRICS_PORT = 9090

logging.basicConfig(
    level=getattr(logging, get_settings().log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


async def _main() -> None:
    settings = get_settings()
    consumer_id = f"worker-{uuid.uuid4().hex[:8]}"

    logger.info(
        "worker_start consumer=%s redis=%s metrics_port=%d",
        consumer_id,
        settings.redis_url,
        _METRICS_PORT,
    )

    # Expose Prometheus metrics for scraping by PodMonitor
    start_http_server(_METRICS_PORT)

    redis: Redis = Redis.from_url(str(settings.redis_url), decode_responses=True)

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("signal_received — initiating graceful shutdown")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    # Start one pipeline consumer task per pipeline definition found in pipelines_dir.
    pipeline_tasks: list[asyncio.Task[None]] = []
    try:
        pipelines = load_pipelines(settings.pipelines_dir)
        for pipeline in pipelines:
            task = asyncio.create_task(
                run_pipeline_consumer(
                    redis,
                    consumer_id,
                    pipeline,
                    shutdown_event,
                    job_ttl_seconds=settings.job_ttl_seconds
                    if hasattr(settings, "job_ttl_seconds")
                    else 3600,
                )
            )
            pipeline_tasks.append(task)
            logger.info("pipeline_consumer_registered pipeline=%s", pipeline.name)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "pipeline_load_error dir=%s error=%s", settings.pipelines_dir, exc
        )

    # Block until a SIGINT/SIGTERM arrives.
    await shutdown_event.wait()

    for t in pipeline_tasks:
        t.cancel()
    await asyncio.gather(*pipeline_tasks, return_exceptions=True)
    await redis.aclose()

    logger.info("worker_stopped consumer=%s", consumer_id)


if __name__ == "__main__":
    asyncio.run(_main())
