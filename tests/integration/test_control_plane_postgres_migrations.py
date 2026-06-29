from __future__ import annotations

import os
from importlib import import_module
from typing import Any

import asyncpg  # type: ignore[import-untyped]
import pytest
from moiraweave_shared.alembic.control_plane_schema import (
    CONTROL_PLANE_ALEMBIC_BASELINE,
    CONTROL_PLANE_MIGRATIONS,
)
from moiraweave_shared.alembic_runner import upgrade_control_plane
from moiraweave_shared.control_plane import PostgresControlPlaneRepository

baseline = import_module(
    "moiraweave_shared.alembic.versions.20260612_0001_control_plane_baseline"
)

_POSTGRES_MIGRATION_DSN = os.getenv("MOIRAWEAVE_POSTGRES_MIGRATION_DSN")
_POSTGRES_MIGRATION_DSN_IS_DISPOSABLE = (
    os.getenv("MOIRAWEAVE_POSTGRES_MIGRATION_DSN_IS_DISPOSABLE") == "1"
)

pytestmark = pytest.mark.skipif(
    not _POSTGRES_MIGRATION_DSN or not _POSTGRES_MIGRATION_DSN_IS_DISPOSABLE,
    reason=(
        "Set MOIRAWEAVE_POSTGRES_MIGRATION_DSN and "
        "MOIRAWEAVE_POSTGRES_MIGRATION_DSN_IS_DISPOSABLE=1 to run destructive "
        "Postgres migration tests."
    ),
)


async def _connect() -> asyncpg.Connection[Any]:
    assert _POSTGRES_MIGRATION_DSN is not None
    return await asyncpg.connect(_POSTGRES_MIGRATION_DSN)


async def _reset_public_schema() -> None:
    conn = await _connect()
    try:
        await conn.execute("DROP SCHEMA IF EXISTS public CASCADE")
        await conn.execute("CREATE SCHEMA public")
    finally:
        await conn.close()


async def _apply_legacy_control_plane_schema() -> None:
    conn = await _connect()
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS control_plane_migrations (
                version integer PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        for version, sql in CONTROL_PLANE_MIGRATIONS:
            for statement in baseline._split_sql_statements(sql):
                await conn.execute(statement)
            await conn.execute(
                """
                INSERT INTO control_plane_migrations (version)
                VALUES ($1)
                ON CONFLICT (version) DO NOTHING
                """,
                version,
            )
    finally:
        await conn.close()


async def _fetchval(query: str, *args: object) -> Any:
    conn = await _connect()
    try:
        return await conn.fetchval(query, *args)
    finally:
        await conn.close()


async def _fetch_table_names() -> set[str]:
    conn = await _connect()
    try:
        rows = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
        )
        return {str(row["table_name"]) for row in rows}
    finally:
        await conn.close()


async def _fetch_index_names() -> set[str]:
    conn = await _connect()
    try:
        rows = await conn.fetch(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
            """
        )
        return {str(row["indexname"]) for row in rows}
    finally:
        await conn.close()


async def test_postgres_repository_rejects_unmigrated_database() -> None:
    assert _POSTGRES_MIGRATION_DSN is not None
    await _reset_public_schema()
    pool = await asyncpg.create_pool(_POSTGRES_MIGRATION_DSN)
    assert pool is not None
    repo = PostgresControlPlaneRepository(pool)

    with pytest.raises(RuntimeError, match="Control-plane schema is not migrated"):
        await repo.init()

    await repo.close()


async def test_postgres_upgrade_from_empty_database_creates_expected_schema() -> None:
    assert _POSTGRES_MIGRATION_DSN is not None
    await _reset_public_schema()

    await upgrade_control_plane(_POSTGRES_MIGRATION_DSN)

    assert (
        await _fetchval("SELECT version_num FROM alembic_version LIMIT 1")
        == CONTROL_PLANE_ALEMBIC_BASELINE
    )
    assert {
        "workloads",
        "runs",
        "run_events",
        "artifacts",
        "agent_sessions",
        "agent_messages",
        "deployments",
        "deployment_operations",
        "deployment_operation_events",
        "audit_events",
        "api_keys",
        "auth_users",
        "teams",
        "team_members",
        "control_plane_migrations",
        "alembic_version",
    }.issubset(await _fetch_table_names())
    assert {
        "runs_user_created_idx",
        "runs_status_heartbeat_idx",
        "deployment_operations_status_lease_idx",
        "deployment_operations_controller_heartbeat_idx",
    }.issubset(await _fetch_index_names())

    pool = await asyncpg.create_pool(_POSTGRES_MIGRATION_DSN)
    assert pool is not None
    repo = PostgresControlPlaneRepository(pool)
    await repo.init()
    await repo.close()


async def test_postgres_upgrade_from_existing_baseline_schema_stamps_alembic() -> None:
    assert _POSTGRES_MIGRATION_DSN is not None
    await _reset_public_schema()
    await _apply_legacy_control_plane_schema()

    conn = await _connect()
    try:
        await conn.execute(
            """
            INSERT INTO workloads (name, manifest, user_subject)
            VALUES ('legacy-agent', '{}'::jsonb, 'legacy-user')
            """
        )
    finally:
        await conn.close()

    await upgrade_control_plane(_POSTGRES_MIGRATION_DSN)

    assert (
        await _fetchval("SELECT version_num FROM alembic_version LIMIT 1")
        == CONTROL_PLANE_ALEMBIC_BASELINE
    )
    assert (
        await _fetchval(
            "SELECT manifest::text FROM workloads WHERE name = $1", "legacy-agent"
        )
        == "{}"
    )
    assert await _fetchval("SELECT count(*) FROM control_plane_migrations") == len(
        CONTROL_PLANE_MIGRATIONS
    )


async def test_postgres_upgrade_is_idempotent() -> None:
    assert _POSTGRES_MIGRATION_DSN is not None
    await _reset_public_schema()

    await upgrade_control_plane(_POSTGRES_MIGRATION_DSN)
    await upgrade_control_plane(_POSTGRES_MIGRATION_DSN)

    assert (
        await _fetchval("SELECT version_num FROM alembic_version LIMIT 1")
        == CONTROL_PLANE_ALEMBIC_BASELINE
    )
    assert await _fetchval("SELECT count(*) FROM control_plane_migrations") == len(
        CONTROL_PLANE_MIGRATIONS
    )
