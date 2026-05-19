"""Single source of truth for Redis Streams topology constants.

Both api-gateway (producer) and worker (consumer) import from here to
guarantee they always agree on consumer group and job key names,
preventing silent job loss.

Note: each pipeline declares its own trigger stream key in ``pipeline.yaml``
(``trigger.stream``), so there is no single shared ``STREAM_KEY``.
"""

#: Consumer group name for the worker fleet.
#: Must match the value used in ``pipeline_consumer._ensure_consumer_group``.
CONSUMER_GROUP: str = "moiraweave-pipeline"

#: Prefix for per-job Redis Hash keys  (full key: ``f"{JOB_KEY_PREFIX}:{job_id}"``).
#: Must match the prefix used by both api-gateway (producer) and worker (consumer).
JOB_KEY_PREFIX: str = "pipeline:job"
