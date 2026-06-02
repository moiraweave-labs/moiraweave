"""MoiraWeave workload worker.

Consumes generic workload runs from Redis Streams, dispatches them through
model-service, pipeline, or agent-service executors, and updates run state.

Usage:
    python -m app.main
"""

import asyncio
import logging
import signal
import uuid

from moiraweave_shared.control_plane import connect_postgres_control_plane
from prometheus_client import start_http_server
from redis.asyncio import Redis

from app.config import get_settings
from app.run_consumer import run_consumer

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
        "worker_start consumer=%s redis=%s postgres=%s metrics_port=%d",
        consumer_id,
        settings.redis_url,
        settings.postgres_dsn,
        _METRICS_PORT,
    )

    # Expose Prometheus metrics for scraping by PodMonitor
    start_http_server(_METRICS_PORT)

    redis: Redis = Redis.from_url(str(settings.redis_url), decode_responses=True)
    control_plane = await connect_postgres_control_plane(settings.postgres_dsn)

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("signal_received — initiating graceful shutdown")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    run_task = asyncio.create_task(
        run_consumer(
            redis,
            control_plane,
            consumer_id,
            shutdown_event,
            workloads_dir=settings.workloads_dir,
            heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
            stale_run_seconds=settings.stale_run_seconds,
            stale_check_interval_seconds=settings.stale_check_interval_seconds,
            pending_reclaim_idle_seconds=settings.pending_reclaim_idle_seconds,
            pending_reclaim_interval_seconds=settings.pending_reclaim_interval_seconds,
            pending_reclaim_count=settings.pending_reclaim_count,
        )
    )
    logger.info("run_consumer_registered workloads_dir=%s", settings.workloads_dir)

    # Block until a SIGINT/SIGTERM arrives.
    await shutdown_event.wait()

    run_task.cancel()
    await asyncio.gather(run_task, return_exceptions=True)
    await control_plane.close()
    await redis.aclose()

    logger.info("worker_stopped consumer=%s", consumer_id)


if __name__ == "__main__":
    asyncio.run(_main())
