"""Pipeline consumer — reads jobs from a pipeline's Redis Stream and executes them.

One instance of :func:`run_pipeline_consumer` runs per pipeline loaded from
``pipelines/``.  Jobs are deserialized from the stream, routed through the
:class:`~app.pipeline_runner.PipelineRunner`, and the result is stored in a
Redis Hash for polling by the api-gateway.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from moiraweave_shared.schemas import PipelineJobMessage
from moiraweave_shared.streams import CONSUMER_GROUP, JOB_KEY_PREFIX
from pydantic import ValidationError
from redis.exceptions import ResponseError

from app.pipeline_runner import PipelineRunner

if TYPE_CHECKING:
    from moiraweave_shared.pipeline import PipelineDefinition
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)


async def _ensure_consumer_group(redis: Redis, stream: str) -> None:
    try:
        await redis.xgroup_create(stream, _CONSUMER_GROUP, id="0", mkstream=True)
        logger.info(
            "pipeline_consumer_group_created group=%s stream=%s",
            _CONSUMER_GROUP,
            stream,
        )
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def run_pipeline_consumer(
    redis: Redis,
    consumer_id: str,
    pipeline: PipelineDefinition,
    shutdown_event: asyncio.Event,
    *,
    job_key_prefix: str = JOB_KEY_PREFIX,
    job_ttl_seconds: int = 3600,
) -> None:
    """Consume jobs from the pipeline's Redis Stream and route them to steps.

    :param redis: Async Redis client.
    :param consumer_id: Unique consumer identifier for the consumer group.
    :param pipeline: Pipeline definition describing the steps to execute.
    :param shutdown_event: When set, the loop exits gracefully.
    :param job_key_prefix: Prefix for Redis Hash keys storing job results.
    :param job_ttl_seconds: TTL applied to each job Hash after completion.
    """
    stream = pipeline.trigger.stream
    runner = PipelineRunner(pipeline)
    await _ensure_consumer_group(redis, stream)
    logger.info(
        "pipeline_consumer_start consumer=%s pipeline=%s stream=%s",
        consumer_id,
        pipeline.name,
        stream,
    )

    while not shutdown_event.is_set():
        try:
            entries: Any = await redis.xreadgroup(
                _CONSUMER_GROUP,
                consumer_id,
                {stream: ">"},
                count=1,
                block=1000,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "pipeline_read_error pipeline=%s error=%s", pipeline.name, exc
            )
            await asyncio.sleep(1.0)
            continue

        if not entries:
            continue

        for _stream_name, messages in entries:
            for msg_id, fields in messages:
                await _process_message(
                    redis,
                    runner,
                    stream,
                    msg_id,
                    fields,
                    pipeline.name,
                    job_key_prefix,
                    job_ttl_seconds,
                )


async def _process_message(
    redis: Redis,
    runner: PipelineRunner,
    stream: str,
    msg_id: str,
    fields: dict[str, str],
    pipeline_name: str,
    job_key_prefix: str,
    job_ttl_seconds: int,
) -> None:
    try:
        msg = PipelineJobMessage.model_validate(fields)
    except ValidationError:
        logger.exception("pipeline_message_invalid msg_id=%s fields=%s", msg_id, fields)
        await redis.xack(stream, _CONSUMER_GROUP, msg_id)
        return

    job_id = msg.job_id
    job_key = f"{job_key_prefix}:{job_id}"
    payload: dict[str, Any] = json.loads(msg.payload)

    logger.info("pipeline_job_start job_id=%s pipeline=%s", job_id, pipeline_name)
    await redis.hset(job_key, "status", "processing")  # type: ignore[misc]

    try:
        result = await runner.run(payload)
        completed_at = datetime.now(UTC).isoformat()
        await redis.hset(  # type: ignore[misc]
            job_key,
            mapping={
                "status": "completed",
                "result": json.dumps(result),
                "completed_at": completed_at,
            },
        )
        await redis.expire(job_key, job_ttl_seconds)
        logger.info("pipeline_job_done job_id=%s pipeline=%s", job_id, pipeline_name)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "pipeline_job_failed job_id=%s pipeline=%s error=%s",
            job_id,
            pipeline_name,
            exc,
        )
        await redis.hset(  # type: ignore[misc]
            job_key,
            mapping={
                "status": "failed",
                "error": str(exc),
                "completed_at": datetime.now(UTC).isoformat(),
            },
        )
        await redis.expire(job_key, job_ttl_seconds)

    await redis.xack(stream, _CONSUMER_GROUP, msg_id)
