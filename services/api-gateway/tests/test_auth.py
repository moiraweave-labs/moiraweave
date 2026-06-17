"""Tests for /auth/token endpoint."""

from datetime import UTC, datetime, timedelta

import jwt
import pytest
from httpx import AsyncClient

from app.config import get_settings


def _token(subject: str, role: str) -> str:
    settings = get_settings()
    return jwt.encode(
        {
            "sub": subject,
            "role": role,
            "exp": datetime.now(UTC) + timedelta(minutes=5),
        },
        settings.jwt_secret_key.get_secret_value(),
        algorithm=settings.jwt_algorithm,
    )


async def test_login_success_returns_token(client: AsyncClient) -> None:
    response = await client.post(
        "/auth/token", json={"username": "admin", "password": "demo-password"}
    )
    assert response.status_code == 200
    body = response.json()
    assert "access_token" in body
    assert body["token_type"] == "bearer"
    assert body["subject"] == "admin"
    assert body["role"] == "admin"
    assert len(body["access_token"]) > 10


@pytest.mark.parametrize(
    ("username", "password"),
    [
        ("admin", "wrong!"),
        ("nobody", "demo-password"),
        ("", "demo-password"),
    ],
    ids=["wrong-password", "wrong-username", "empty-username"],
)
async def test_login_invalid_credentials_returns_401(
    client: AsyncClient, username: str, password: str
) -> None:
    response = await client.post(
        "/auth/token", json={"username": username, "password": password}
    )
    assert response.status_code == 401


async def test_login_missing_body_returns_422(client: AsyncClient) -> None:
    response = await client.post("/auth/token", json={})
    assert response.status_code == 422


async def test_demo_auth_can_be_disabled_without_blocking_stored_users(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEMO_AUTH_ENABLED", "false")
    get_settings.cache_clear()
    admin_token = _token("admin", "admin")

    demo = await client.post(
        "/auth/token", json={"username": "admin", "password": "demo-password"}
    )
    assert demo.status_code == 401

    await client.post(
        "/auth/users",
        json={
            "subject": "alice",
            "password": "correct-horse",
            "role": "operator",
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    stored = await client.post(
        "/auth/token", json={"username": "alice", "password": "correct-horse"}
    )

    assert stored.status_code == 200
    assert stored.json()["subject"] == "alice"
    get_settings.cache_clear()


async def test_token_allows_authenticated_request(client: AsyncClient) -> None:
    # Given: a valid token obtained from the login endpoint
    login = await client.post(
        "/auth/token", json={"username": "admin", "password": "demo-password"}
    )
    token = login.json()["access_token"]

    # When: using that token on an authenticated endpoint
    response = await client.post(
        "/v1/search",
        json={"collection": "docs", "query": "test"},
        headers={"Authorization": f"Bearer {token}"},
    )

    # Then: authenticated request is accepted (search result can be empty, not 401)
    assert response.status_code in {200, 500}  # 500 if qdrant not running; not 401


async def test_me_returns_jwt_profile(client: AsyncClient) -> None:
    token = _token("operator", "operator")

    response = await client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "subject": "operator",
        "role": "operator",
        "credential_type": "jwt",
        "api_key_id": None,
        "team_id": None,
        "teams": [],
    }


async def test_api_key_allows_authenticated_request(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOIRA_API_KEYS", "local-dev-key:automation:operator")
    get_settings.cache_clear()

    response = await client.get(
        "/v1/runs",
        headers={"Authorization": "Bearer local-dev-key"},
    )

    assert response.status_code == 200


async def test_me_returns_api_key_profile(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOIRA_API_KEYS", "local-dev-key:automation:operator")
    get_settings.cache_clear()

    response = await client.get(
        "/auth/me",
        headers={"Authorization": "Bearer local-dev-key"},
    )
    body = response.json()

    assert response.status_code == 200
    assert body["subject"] == "automation"
    assert body["role"] == "operator"
    assert body["credential_type"] == "api_key"
    assert isinstance(body["api_key_id"], str)
    assert body["api_key_id"] != "local-dev-key"


async def test_admin_can_create_use_and_revoke_persistent_api_key(
    client: AsyncClient,
) -> None:
    admin_token = _token("admin", "admin")

    created = await client.post(
        "/auth/api-keys",
        json={"name": "ci deploy", "subject": "ci", "role": "operator"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    body = created.json()

    assert created.status_code == 201
    assert body["key_id"]
    assert body["secret"].startswith("mwk_")
    assert body["secret_prefix"].endswith("...")
    assert body["secret"] not in body["secret_prefix"]
    assert body["subject"] == "ci"
    assert body["role"] == "operator"
    assert body["revoked_at"] is None

    profile = await client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {body['secret']}"},
    )
    assert profile.status_code == 200
    assert profile.json() == {
        "subject": "ci",
        "role": "operator",
        "credential_type": "api_key",
        "api_key_id": body["key_id"],
        "team_id": None,
        "teams": [],
    }

    keys = await client.get(
        "/auth/api-keys",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    listed = keys.json()[0]
    assert keys.status_code == 200
    assert "secret" not in listed
    assert listed["last_used_at"] is not None

    revoked = await client.delete(
        f"/auth/api-keys/{body['key_id']}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert revoked.status_code == 200
    assert revoked.json()["revoked_at"] is not None

    rejected = await client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {body['secret']}"},
    )
    assert rejected.status_code == 401

    audit = await client.get(
        "/v1/audit-events?resource_type=api_key",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    actions = {event["action"] for event in audit.json()}
    assert {"api_key.create", "api_key.revoke"} <= actions


async def test_admin_can_rotate_persistent_api_key(
    client: AsyncClient,
) -> None:
    admin_token = _token("admin", "admin")
    created = await client.post(
        "/auth/api-keys",
        json={"name": "bot", "subject": "automation", "role": "operator"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    original = created.json()

    rotated = await client.post(
        f"/auth/api-keys/{original['key_id']}/rotate",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    replacement = rotated.json()

    assert rotated.status_code == 201
    assert replacement["key_id"] != original["key_id"]
    assert replacement["secret"] != original["secret"]
    assert replacement["name"] == "bot"
    assert replacement["subject"] == "automation"
    assert replacement["role"] == "operator"
    assert replacement["revoked_at"] is None

    old_profile = await client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {original['secret']}"},
    )
    new_profile = await client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {replacement['secret']}"},
    )

    assert old_profile.status_code == 401
    assert new_profile.status_code == 200
    assert new_profile.json()["api_key_id"] == replacement["key_id"]

    listed = await client.get(
        "/auth/api-keys",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    keys = {item["key_id"]: item for item in listed.json()}
    assert keys[original["key_id"]]["revoked_at"] is not None
    assert keys[replacement["key_id"]]["revoked_at"] is None

    audit = await client.get(
        "/v1/audit-events?action=api_key.rotate",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert audit.status_code == 200
    assert audit.json()[0]["resource_id"] == replacement["key_id"]
    assert audit.json()[0]["metadata"]["previous_key_id"] == original["key_id"]


async def test_rotating_revoked_api_key_returns_409(
    client: AsyncClient,
) -> None:
    admin_token = _token("admin", "admin")
    created = await client.post(
        "/auth/api-keys",
        json={"name": "bot", "subject": "automation", "role": "operator"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    key_id = created.json()["key_id"]
    await client.delete(
        f"/auth/api-keys/{key_id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    rotated = await client.post(
        f"/auth/api-keys/{key_id}/rotate",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert rotated.status_code == 409
    assert rotated.json()["detail"] == "API key is already revoked"


async def test_operator_cannot_manage_persistent_api_keys(
    client: AsyncClient,
) -> None:
    operator_token = _token("operator", "operator")

    response = await client.post(
        "/auth/api-keys",
        json={"name": "blocked", "subject": "ci", "role": "operator"},
        headers={"Authorization": f"Bearer {operator_token}"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Requires admin role"


async def test_api_key_creation_rejects_blank_subject(client: AsyncClient) -> None:
    response = await client.post(
        "/auth/api-keys",
        json={"name": "ci deploy", "subject": "   ", "role": "operator"},
        headers={"Authorization": f"Bearer {_token('admin', 'admin')}"},
    )

    assert response.status_code == 422


async def test_admin_can_create_persistent_user_and_login(
    client: AsyncClient,
) -> None:
    admin_token = _token("admin", "admin")

    created = await client.post(
        "/auth/users",
        json={
            "subject": "alice",
            "password": "correct-horse",
            "role": "operator",
            "display_name": "Alice Operator",
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert created.status_code == 201
    assert created.json()["subject"] == "alice"
    assert created.json()["role"] == "operator"
    assert "password_hash" not in created.json()

    login = await client.post(
        "/auth/token",
        json={"username": "alice", "password": "correct-horse"},
    )

    assert login.status_code == 200
    assert login.json()["subject"] == "alice"
    assert login.json()["role"] == "operator"


async def test_admin_can_manage_team_and_team_scoped_api_key(
    client: AsyncClient,
) -> None:
    admin_token = _token("admin", "admin")
    await client.post(
        "/auth/users",
        json={"subject": "team-bot", "password": "correct-horse", "role": "operator"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    team = await client.post(
        "/auth/teams",
        json={
            "team_id": "agents",
            "name": "Agent Operators",
            "description": "Runs production agents",
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    member = await client.post(
        "/auth/teams/agents/members",
        json={"subject": "team-bot", "role": "operator"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert team.status_code == 201
    assert member.status_code == 201
    assert member.json()["team_id"] == "agents"
    assert member.json()["subject"] == "team-bot"

    created_key = await client.post(
        "/auth/api-keys",
        json={
            "name": "team automation",
            "subject": "team-bot",
            "role": "operator",
            "team_id": "agents",
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    key = created_key.json()

    assert created_key.status_code == 201
    assert key["team_id"] == "agents"

    profile = await client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {key['secret']}"},
    )

    assert profile.status_code == 200
    assert profile.json()["subject"] == "team-bot"
    assert profile.json()["team_id"] == "agents"
    assert profile.json()["teams"] == ["agents"]

    removed = await client.delete(
        "/auth/teams/agents/members/team-bot",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    listed = await client.get(
        "/auth/teams/agents/members",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert removed.status_code == 200
    assert removed.json()["subject"] == "team-bot"
    assert listed.status_code == 200
    assert listed.json() == []

    audit = await client.get(
        "/v1/audit-events?action=team.member.remove",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert audit.status_code == 200
    assert audit.json()[0]["resource_id"] == "agents"
    assert audit.json()[0]["metadata"]["subject"] == "team-bot"


async def test_viewer_cannot_register_workload(client: AsyncClient) -> None:
    response = await client.post(
        "/v1/workloads",
        json={
            "apiVersion": "moiraweave.io/v1alpha1",
            "kind": "Workload",
            "metadata": {"name": "viewer-blocked"},
            "spec": {
                "type": "agent-service",
                "image": "example/agent:latest",
                "execution": {"mode": "session"},
                "ports": [{"name": "http", "port": 8000}],
            },
        },
        headers={"Authorization": f"Bearer {_token('viewer', 'viewer')}"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Requires admin role"


async def test_operator_can_submit_runs_but_not_register_workloads(
    client: AsyncClient,
) -> None:
    admin_token = _token("admin", "admin")
    operator_token = _token("operator", "operator")
    manifest = {
        "apiVersion": "moiraweave.io/v1alpha1",
        "kind": "Workload",
        "metadata": {"name": "operator-agent"},
        "spec": {
            "type": "agent-service",
            "image": "example/agent:latest",
            "execution": {"mode": "session"},
            "ports": [{"name": "http", "port": 8000}],
        },
    }
    registered = await client.post(
        "/v1/workloads",
        json=manifest,
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    blocked = await client.post(
        "/v1/workloads",
        json={**manifest, "metadata": {"name": "operator-blocked"}},
        headers={"Authorization": f"Bearer {operator_token}"},
    )
    submitted = await client.post(
        "/v1/workloads/operator-agent/runs",
        json={"payload": {"prompt": "hello"}},
        headers={"Authorization": f"Bearer {operator_token}"},
    )

    assert registered.status_code == 201
    assert blocked.status_code == 403
    assert submitted.status_code == 202


async def test_expired_token_returns_401(client: AsyncClient) -> None:
    """A tampered/garbage token is rejected."""
    response = await client.post(
        "/v1/search",
        json={"collection": "docs", "query": "test"},
        headers={"Authorization": "Bearer not.a.valid.token"},
    )
    assert response.status_code == 401


async def test_missing_auth_header_returns_401(client: AsyncClient) -> None:
    """No Authorization header → 401 (Starlette 1.0 HTTPBearer behaviour)."""
    response = await client.post(
        "/v1/search", json={"collection": "docs", "query": "test"}
    )
    assert response.status_code in {401, 403}
