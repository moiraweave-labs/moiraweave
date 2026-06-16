# MoiraWeave Control-Plane Migrations

This Alembic environment owns the Postgres control-plane schema used by the API
gateway and worker. The first revision is a baseline for the schema that existed
before Alembic was introduced. It is written with idempotent DDL so existing
databases can be stamped safely during startup.
