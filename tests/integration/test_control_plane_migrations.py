from __future__ import annotations

from importlib import import_module

import pytest
from alembic.script import ScriptDirectory
from moiraweave_shared.alembic_runner import alembic_config, to_sqlalchemy_async_url
from moiraweave_shared.control_plane import (
    CONTROL_PLANE_ALEMBIC_BASELINE,
    PostgresControlPlaneRepository,
)

baseline = import_module(
    "moiraweave_shared.alembic.versions.20260612_0001_control_plane_baseline"
)


def test_postgres_dsn_is_converted_to_async_sqlalchemy_url() -> None:
    assert (
        to_sqlalchemy_async_url("postgresql://user:pass@db/moira")
        == "postgresql+asyncpg://user:pass@db/moira"
    )
    assert (
        to_sqlalchemy_async_url("postgres://user:pass@db/moira")
        == "postgresql+asyncpg://user:pass@db/moira"
    )
    assert (
        to_sqlalchemy_async_url("postgresql+asyncpg://user:pass@db/moira")
        == "postgresql+asyncpg://user:pass@db/moira"
    )


def test_alembic_script_directory_exposes_control_plane_head() -> None:
    script = ScriptDirectory.from_config(
        alembic_config("postgresql://user:pass@localhost/moira")
    )

    assert script.get_current_head() == CONTROL_PLANE_ALEMBIC_BASELINE


def test_baseline_revision_runs_all_legacy_control_plane_migrations() -> None:
    assert baseline.revision == CONTROL_PLANE_ALEMBIC_BASELINE
    assert [version for version, _ in baseline.CONTROL_PLANE_MIGRATIONS] == [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
    ]


def test_baseline_sql_splitter_keeps_dollar_quoted_blocks_intact() -> None:
    statements = list(
        baseline._split_sql_statements(
            """
            CREATE TABLE example (id integer);
            DO $$
            BEGIN
                RAISE NOTICE 'semicolon; inside block';
            END $$;
            CREATE INDEX example_id_idx ON example (id);
            """
        )
    )

    assert len(statements) == 3
    assert "semicolon; inside block" in statements[1]
    assert statements[1].startswith("DO $$")


def test_control_plane_migration_sql_is_split_into_single_driver_statements() -> None:
    for _, sql in baseline.CONTROL_PLANE_MIGRATIONS:
        for statement in baseline._split_sql_statements(sql):
            assert statement
            assert not statement.rstrip().endswith(";")


class _FakeAcquire:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self.conn

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FakeConn:
    def __init__(self, revision: str | None) -> None:
        self.revision = revision
        self.executed: list[str] = []

    async def fetchval(self, query: str) -> str | None:
        self.executed.append(query)
        return self.revision

    async def execute(self, query: str, *args: object) -> None:
        raise AssertionError(
            f"PostgresControlPlaneRepository.init must not run DDL: {query!r}"
        )

    async def fetch(self, query: str, *args: object) -> list[object]:
        raise AssertionError(
            f"PostgresControlPlaneRepository.init must not run legacy fetches: {query!r}"
        )


class _FakePool:
    def __init__(self, revision: str | None) -> None:
        self.conn = _FakeConn(revision)

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


class _AuditQueryPool:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, query: str, *args: object) -> list[object]:
        self.calls.append((query, args))
        return []

    async def fetchrow(self, query: str, *args: object) -> None:
        self.calls.append((query, args))
        return None


async def test_postgres_repository_init_only_verifies_alembic_revision() -> None:
    pool = _FakePool(CONTROL_PLANE_ALEMBIC_BASELINE)
    repo = PostgresControlPlaneRepository(pool)

    await repo.init()

    assert pool.conn.executed == ["SELECT version_num FROM alembic_version LIMIT 1"]


async def test_postgres_repository_init_fails_without_alembic_baseline() -> None:
    pool = _FakePool(None)
    repo = PostgresControlPlaneRepository(pool)

    with pytest.raises(RuntimeError, match="Control-plane schema is not migrated"):
        await repo.init()


async def test_postgres_audit_query_filters_environment_metadata() -> None:
    pool = _AuditQueryPool()
    repo = PostgresControlPlaneRepository(pool)

    events = await repo.list_audit_events(
        None,
        action="deployment_operation.apply",
        env="prod",
        limit=25,
        offset=10,
    )

    assert events == []
    assert len(pool.calls) == 1
    query, args = pool.calls[0]
    assert "COALESCE(metadata ->> 'env', metadata ->> 'environment')" in query
    assert args == (
        None,
        "deployment_operation.apply",
        None,
        None,
        "prod",
        25,
        10,
    )


async def test_postgres_workload_record_queries_include_persistent_owner() -> None:
    pool = _AuditQueryPool()
    repo = PostgresControlPlaneRepository(pool)

    records = await repo.list_workload_records()
    record = await repo.get_workload_record("team-a-agent")

    assert records == []
    assert record is None
    assert len(pool.calls) == 2
    assert (
        "SELECT manifest, user_subject FROM workloads ORDER BY name ASC"
        in pool.calls[0][0]
    )
    assert pool.calls[0][1] == ()
    assert (
        "SELECT manifest, user_subject FROM workloads WHERE name = $1"
        in pool.calls[1][0]
    )
    assert pool.calls[1][1] == ("team-a-agent",)
