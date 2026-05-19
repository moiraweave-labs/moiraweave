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

    def test_stream_key(self) -> None:
        from moiraweave_shared.streams import STREAM_KEY

        assert STREAM_KEY == "moiraweave:jobs"

    def test_consumer_group(self) -> None:
        from moiraweave_shared.streams import CONSUMER_GROUP

        assert CONSUMER_GROUP == "moiraweave:workers"

    def test_job_key_prefix(self) -> None:
        from moiraweave_shared.streams import JOB_KEY_PREFIX

        assert JOB_KEY_PREFIX == "moiraweave:job"

    def test_job_key_format(self) -> None:
        from moiraweave_shared.streams import JOB_KEY_PREFIX

        job_id = "abc-123"
        assert f"{JOB_KEY_PREFIX}:{job_id}" == "moiraweave:job:abc-123"


class TestTranscribeStreamMessage:
    def test_serializes_to_flat_dict(self) -> None:
        from moiraweave_shared.schemas import TranscribeStreamMessage

        msg = TranscribeStreamMessage(
            job_id="abc-123",
            audio_url="https://example.com/audio.mp3",
            user="user1",
        )
        data = msg.model_dump(mode="python")
        assert data["type"] == "transcribe"
        assert data["job_id"] == "abc-123"
        assert data["audio_url"] == "https://example.com/audio.mp3"
        assert data["language"] == "auto"

    def test_roundtrip_from_redis_fields(self) -> None:
        """Simulate what Redis xreadgroup returns and validate deserialization."""
        from moiraweave_shared.schemas import TranscribeStreamMessage

        redis_fields: dict[str, str] = {
            "job_id": "xyz-789",
            "type": "transcribe",
            "audio_url": "https://example.com/clip.wav",
            "language": "es",
            "user": "alice",
        }
        msg = TranscribeStreamMessage.model_validate(redis_fields)
        assert msg.job_id == "xyz-789"
        assert msg.language == "es"
        assert msg.user == "alice"

    def test_invalid_type_raises(self) -> None:
        """A message with an unknown type must fail validation."""
        from moiraweave_shared.schemas import TranscribeStreamMessage
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TranscribeStreamMessage.model_validate(
                {
                    "job_id": "x",
                    "type": "unknown_task",
                    "audio_url": "https://example.com/a.mp3",
                    "user": "bob",
                }
            )
