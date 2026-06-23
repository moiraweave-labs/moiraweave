"""Control-plane schema metadata owned by Alembic migrations."""

from __future__ import annotations

CONTROL_PLANE_ALEMBIC_BASELINE = "20260612_0001"

CONTROL_PLANE_MIGRATIONS: tuple[tuple[int, str], ...] = (
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS workloads (
            name text PRIMARY KEY,
            manifest jsonb NOT NULL,
            user_subject text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS runs (
            run_id uuid PRIMARY KEY,
            workload_name text NOT NULL,
            user_subject text NOT NULL,
            status text NOT NULL,
            payload jsonb NOT NULL DEFAULT '{}'::jsonb,
            result jsonb,
            error text,
            session_id uuid,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz,
            heartbeat_at timestamptz,
            completed_at timestamptz
        );

        CREATE INDEX IF NOT EXISTS runs_user_created_idx
            ON runs (user_subject, created_at DESC);
        CREATE INDEX IF NOT EXISTS runs_status_heartbeat_idx
            ON runs (status, heartbeat_at);

        CREATE TABLE IF NOT EXISTS run_events (
            id bigserial PRIMARY KEY,
            run_id uuid NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
            timestamp timestamptz NOT NULL DEFAULT now(),
            type text NOT NULL,
            message text NOT NULL,
            data jsonb NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS artifacts (
            id text PRIMARY KEY,
            run_id uuid NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
            name text NOT NULL,
            uri text NOT NULL,
            content_type text,
            size_bytes bigint,
            created_at timestamptz NOT NULL DEFAULT now(),
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE TABLE IF NOT EXISTS agent_sessions (
            session_id uuid PRIMARY KEY,
            agent_name text NOT NULL,
            user_subject text NOT NULL,
            status text NOT NULL,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz
        );

        CREATE INDEX IF NOT EXISTS agent_sessions_user_created_idx
            ON agent_sessions (user_subject, agent_name, created_at DESC);

        CREATE TABLE IF NOT EXISTS agent_messages (
            id bigserial PRIMARY KEY,
            session_id uuid NOT NULL
                REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
            role text NOT NULL,
            message text NOT NULL,
            context jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now()
        );
        """,
    ),
    (
        2,
        """
        CREATE TABLE IF NOT EXISTS deployments (
            deployment_id uuid PRIMARY KEY,
            workload_name text NOT NULL,
            target text NOT NULL,
            environment text NOT NULL DEFAULT 'local',
            status text NOT NULL,
            endpoint text,
            user_subject text NOT NULL,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT deployments_workload_target_env_user_key
                UNIQUE (workload_name, target, environment, user_subject)
        );

        CREATE INDEX IF NOT EXISTS deployments_user_workload_idx
            ON deployments (user_subject, workload_name, environment, updated_at DESC);

        CREATE TABLE IF NOT EXISTS channel_messages (
            id bigserial PRIMARY KEY,
            channel text NOT NULL,
            agent_name text NOT NULL,
            external_user_id text NOT NULL,
            session_id uuid NOT NULL
                REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
            run_id uuid REFERENCES runs(run_id) ON DELETE SET NULL,
            direction text NOT NULL,
            message text NOT NULL,
            user_subject text NOT NULL,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS channel_messages_session_idx
            ON channel_messages (session_id, created_at ASC);
        """,
    ),
    (
        3,
        """
        CREATE TABLE IF NOT EXISTS deployment_operations (
            operation_id uuid PRIMARY KEY,
            action text NOT NULL,
            workload_name text NOT NULL,
            target text NOT NULL,
            environment text NOT NULL DEFAULT 'dev',
            status text NOT NULL,
            user_subject text NOT NULL,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            completed_at timestamptz
        );

        CREATE INDEX IF NOT EXISTS deployment_operations_user_created_idx
            ON deployment_operations (user_subject, environment, created_at DESC);

        CREATE TABLE IF NOT EXISTS deployment_operation_events (
            id bigserial PRIMARY KEY,
            operation_id uuid NOT NULL
                REFERENCES deployment_operations(operation_id) ON DELETE CASCADE,
            timestamp timestamptz NOT NULL DEFAULT now(),
            type text NOT NULL,
            message text NOT NULL,
            data jsonb NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE INDEX IF NOT EXISTS deployment_operation_events_operation_idx
            ON deployment_operation_events (operation_id, id ASC);
        """,
    ),
    (
        4,
        """
        CREATE TABLE IF NOT EXISTS audit_events (
            id bigserial PRIMARY KEY,
            timestamp timestamptz NOT NULL DEFAULT now(),
            actor_subject text NOT NULL,
            action text NOT NULL,
            resource_type text NOT NULL,
            resource_id text NOT NULL,
            metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        );

        CREATE INDEX IF NOT EXISTS audit_events_actor_timestamp_idx
            ON audit_events (actor_subject, timestamp DESC);
        CREATE INDEX IF NOT EXISTS audit_events_resource_idx
            ON audit_events (resource_type, resource_id, timestamp DESC);
        """,
    ),
    (
        5,
        """
        ALTER TABLE deployments
            ADD COLUMN IF NOT EXISTS environment text NOT NULL DEFAULT 'local';

        ALTER TABLE deployment_operations
            ADD COLUMN IF NOT EXISTS environment text NOT NULL DEFAULT 'dev';

        ALTER TABLE deployments
            DROP CONSTRAINT IF EXISTS deployments_workload_name_target_user_subject_key;

        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'deployments_workload_target_env_user_key'
            ) THEN
                ALTER TABLE deployments
                    ADD CONSTRAINT deployments_workload_target_env_user_key
                    UNIQUE (workload_name, target, environment, user_subject);
            END IF;
        END $$;

        CREATE INDEX IF NOT EXISTS deployments_user_workload_env_idx
            ON deployments (user_subject, workload_name, environment, updated_at DESC);

        CREATE INDEX IF NOT EXISTS deployment_operations_user_env_created_idx
            ON deployment_operations (user_subject, environment, created_at DESC);
        """,
    ),
    (
        6,
        """
        CREATE TABLE IF NOT EXISTS api_keys (
            key_id text PRIMARY KEY,
            name text NOT NULL,
            secret_hash text NOT NULL UNIQUE,
            secret_prefix text NOT NULL,
            subject text NOT NULL,
            role text NOT NULL,
            created_by text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            last_used_at timestamptz,
            revoked_at timestamptz
        );

        CREATE INDEX IF NOT EXISTS api_keys_active_subject_idx
            ON api_keys (subject, role, created_at DESC)
            WHERE revoked_at IS NULL;
        """,
    ),
    (
        7,
        """
        CREATE TABLE IF NOT EXISTS auth_users (
            subject text PRIMARY KEY,
            display_name text,
            password_hash text NOT NULL,
            role text NOT NULL,
            created_by text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            disabled_at timestamptz
        );

        CREATE TABLE IF NOT EXISTS teams (
            team_id text PRIMARY KEY,
            name text NOT NULL UNIQUE,
            description text,
            created_by text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS team_members (
            team_id text NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
            subject text NOT NULL REFERENCES auth_users(subject) ON DELETE CASCADE,
            role text NOT NULL,
            created_by text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (team_id, subject)
        );

        ALTER TABLE api_keys
            ADD COLUMN IF NOT EXISTS team_id text REFERENCES teams(team_id)
                ON DELETE SET NULL;

        CREATE INDEX IF NOT EXISTS auth_users_role_idx
            ON auth_users (role, created_at DESC)
            WHERE disabled_at IS NULL;
        CREATE INDEX IF NOT EXISTS team_members_subject_idx
            ON team_members (subject, team_id);
        CREATE INDEX IF NOT EXISTS api_keys_active_team_idx
            ON api_keys (team_id, created_at DESC)
            WHERE revoked_at IS NULL AND team_id IS NOT NULL;
        """,
    ),
    (
        8,
        """
        ALTER TABLE deployment_operations
            ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz,
            ADD COLUMN IF NOT EXISTS controller_id text,
            ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz,
            ADD COLUMN IF NOT EXISTS timeout_seconds integer,
            ADD COLUMN IF NOT EXISTS stdout_summary text,
            ADD COLUMN IF NOT EXISTS stderr_summary text;

        CREATE INDEX IF NOT EXISTS deployment_operations_status_lease_idx
            ON deployment_operations (status, lease_expires_at);
        CREATE INDEX IF NOT EXISTS deployment_operations_controller_heartbeat_idx
            ON deployment_operations (controller_id, heartbeat_at DESC)
            WHERE controller_id IS NOT NULL;
        """,
    ),
    (
        9,
        """
        CREATE INDEX IF NOT EXISTS run_events_run_id_id_idx
            ON run_events (run_id, id ASC);
        """,
    ),
)
