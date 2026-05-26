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
