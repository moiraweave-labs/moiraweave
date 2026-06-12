"""Baseline control-plane schema.

Revision ID: 20260612_0001
Revises:
Create Date: 2026-06-12
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

from moiraweave_shared.control_plane import CONTROL_PLANE_MIGRATIONS

if TYPE_CHECKING:
    from collections.abc import Iterator

revision = "20260612_0001"
down_revision = None
branch_labels = None
depends_on = None


def _split_sql_statements(sql: str) -> Iterator[str]:
    statement: list[str] = []
    single_quote = False
    double_quote = False
    dollar_quote: str | None = None
    index = 0

    while index < len(sql):
        char = sql[index]

        if dollar_quote is not None:
            if sql.startswith(dollar_quote, index):
                statement.append(dollar_quote)
                index += len(dollar_quote)
                dollar_quote = None
                continue
            statement.append(char)
            index += 1
            continue

        if single_quote:
            statement.append(char)
            if char == "'":
                if index + 1 < len(sql) and sql[index + 1] == "'":
                    statement.append("'")
                    index += 2
                    continue
                single_quote = False
            index += 1
            continue

        if double_quote:
            statement.append(char)
            if char == '"':
                double_quote = False
            index += 1
            continue

        if char == "'":
            single_quote = True
            statement.append(char)
            index += 1
            continue

        if char == '"':
            double_quote = True
            statement.append(char)
            index += 1
            continue

        if char == "$":
            match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[index:])
            if match:
                dollar_quote = match.group(0)
                statement.append(dollar_quote)
                index += len(dollar_quote)
                continue

        if char == ";":
            stripped = "".join(statement).strip()
            if stripped:
                yield stripped
            statement = []
            index += 1
            continue

        statement.append(char)
        index += 1

    stripped = "".join(statement).strip()
    if stripped:
        yield stripped


def upgrade() -> None:
    connection = op.get_bind()
    connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS control_plane_migrations (
            version integer PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )
    for version, sql in CONTROL_PLANE_MIGRATIONS:
        for statement in _split_sql_statements(sql):
            connection.exec_driver_sql(statement)
        connection.execute(
            sa.text(
                """
                INSERT INTO control_plane_migrations (version)
                VALUES (:version)
                ON CONFLICT (version) DO NOTHING
                """
            ),
            {"version": version},
        )


def downgrade() -> None:
    raise NotImplementedError("MoiraWeave control-plane migrations are forward-only")
