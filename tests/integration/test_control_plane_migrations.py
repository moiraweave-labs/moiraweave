from __future__ import annotations

from importlib import import_module

from alembic.script import ScriptDirectory
from moiraweave_shared.alembic_runner import alembic_config, to_sqlalchemy_async_url
from moiraweave_shared.control_plane import CONTROL_PLANE_ALEMBIC_BASELINE

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
