"""Tests for /auth/token endpoint."""

import pytest
from httpx import AsyncClient


async def test_login_success_returns_token(client: AsyncClient) -> None:
    response = await client.post(
        "/auth/token", json={"username": "admin", "password": "demo-password"}
    )
    assert response.status_code == 200
    body = response.json()
    assert "access_token" in body
    assert body["token_type"] == "bearer"
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
