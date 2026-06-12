"""Alembic runner for the MoiraWeave control-plane schema."""

from __future__ import annotations

import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config


def to_sqlalchemy_async_url(dsn: str) -> str:
    """Convert asyncpg-compatible Postgres URLs to SQLAlchemy's async dialect."""

    if dsn.startswith("postgresql+asyncpg://"):
        return dsn
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    if dsn.startswith("postgres://"):
        return dsn.replace("postgres://", "postgresql+asyncpg://", 1)
    return dsn


def alembic_config(dsn: str) -> Config:
    script_location = Path(__file__).resolve().parent / "alembic"
    config = Config()
    config.set_main_option("script_location", str(script_location))
    config.set_main_option("sqlalchemy.url", to_sqlalchemy_async_url(dsn))
    return config


async def upgrade_control_plane(dsn: str, revision: str = "head") -> None:
    """Upgrade the control-plane schema to the requested Alembic revision."""

    config = alembic_config(dsn)
    await asyncio.to_thread(command.upgrade, config, revision)
