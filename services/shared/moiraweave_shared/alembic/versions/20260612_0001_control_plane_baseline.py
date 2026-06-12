"""Baseline control-plane schema.

Revision ID: 20260612_0001
Revises:
Create Date: 2026-06-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from moiraweave_shared.control_plane import CONTROL_PLANE_MIGRATIONS

revision = "20260612_0001"
down_revision = None
branch_labels = None
depends_on = None


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
        connection.exec_driver_sql(sql)
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
