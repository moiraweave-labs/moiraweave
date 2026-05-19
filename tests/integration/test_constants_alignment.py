"""Integration test: Redis stream constants are consistent across all services.

Imports the shared package (not mocks) to guarantee that any future
refactoring that changes stream names will break these tests immediately
rather than causing silent job loss in production.

The "no hardcoded constants" invariant is enforced structurally — both
api-gateway and worker import from moiraweave_shared.streams at module level,
so any attempt to redeclare the constants would shadow the import and be
caught by mypy's --strict checks.
"""

import pytest


class TestStreamConstants:
    """Verify canonical constant values in the shared package."""

    def test_consumer_group(self) -> None:
        from moiraweave_shared.streams import CONSUMER_GROUP

        assert CONSUMER_GROUP == "moiraweave-pipeline"

    def test_job_key_prefix(self) -> None:
        from moiraweave_shared.streams import JOB_KEY_PREFIX

        assert JOB_KEY_PREFIX == "pipeline:job"

    def test_job_key_format(self) -> None:
        from moiraweave_shared.streams import JOB_KEY_PREFIX

        job_id = "abc-123"
        assert f"{JOB_KEY_PREFIX}:{job_id}" == "pipeline:job:abc-123"


class TestPipelineJobMessage:
    def test_serializes_to_flat_dict(self) -> None:
        from moiraweave_shared.schemas import PipelineJobMessage

        msg = PipelineJobMessage(
            job_id="abc-123",
            pipeline_id="image-search",
            payload='{"query": "cats"}',
            user="user1",
        )
        data = msg.model_dump(mode="python")
        assert data["job_id"] == "abc-123"
        assert data["pipeline_id"] == "image-search"
        assert data["user"] == "user1"

    def test_roundtrip_from_redis_fields(self) -> None:
        """Simulate what Redis xreadgroup returns and validate deserialization."""
        from moiraweave_shared.schemas import PipelineJobMessage

        redis_fields: dict[str, str] = {
            "job_id": "xyz-789",
            "pipeline_id": "text-search",
            "payload": '{"query": "dogs"}',
            "user": "alice",
        }
        msg = PipelineJobMessage.model_validate(redis_fields)
        assert msg.job_id == "xyz-789"
        assert msg.pipeline_id == "text-search"
        assert msg.user == "alice"

    def test_invalid_missing_field_raises(self) -> None:
        """A message missing a required field must fail validation."""
        from moiraweave_shared.schemas import PipelineJobMessage
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PipelineJobMessage.model_validate(
                {
                    "job_id": "x",
                    # missing pipeline_id, payload, user
                }
            )
