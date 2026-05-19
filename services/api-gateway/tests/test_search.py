"""Tests for POST /search endpoint."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import AsyncClient
from qdrant_client.fastembed_common import QueryResponse
from qdrant_client.http.exceptions import UnexpectedResponse


def _make_query_response(
    job_id: str = "job-123",
    score: float = 0.9,
    document: str = "test document",
    metadata: dict | None = None,
) -> QueryResponse:
    return QueryResponse(
        id=job_id,
        score=score,
        document=document,
        metadata=metadata
        or {
            "user": "testuser",
            "text": "test document",
            "created_at": "2026-01-01T00:00:00",
        },
        embedding=None,
    )


async def test_search_returns_results(
    auth_client: AsyncClient, mock_qdrant: MagicMock
) -> None:
    # Given
    mock_qdrant.query = AsyncMock(
        return_value=[_make_query_response(job_id="job-1", score=0.95)]
    )

    # When
    response = await auth_client.post(
        "/v1/search", json={"collection": "docs", "query": "hello world", "limit": 5}
    )

    # Then
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["results"][0]["id"] == "job-1"
    assert body["results"][0]["score"] == 0.95
    assert body["results"][0]["payload"]["text"] == "test document"


async def test_search_empty_collection_returns_empty(
    auth_client: AsyncClient, mock_qdrant: MagicMock
) -> None:
    # Given: Qdrant returns 404 (collection not found)
    err = UnexpectedResponse(
        status_code=404,
        reason_phrase="Not Found",
        content=b"Collection not found",
        headers={},
    )
    mock_qdrant.query = AsyncMock(side_effect=err)

    # When
    response = await auth_client.post("/v1/search", json={"collection": "docs", "query": "anything"})
    assert response.status_code == 200
    body = response.json()
    assert body["results"] == []
    assert body["total"] == 0


async def test_search_qdrant_server_error_propagates(
    auth_client: AsyncClient, mock_qdrant: MagicMock
) -> None:
    # Given: Qdrant returns 5xx — not caught by the search route
    err = UnexpectedResponse(
        status_code=500,
        reason_phrase="Internal Server Error",
        content=b"oops",
        headers={},
    )
    mock_qdrant.query = AsyncMock(side_effect=err)

    # When / Then: ASGITransport with raise_app_exceptions=True propagates it
    with pytest.raises(UnexpectedResponse):
        await auth_client.post("/v1/search", json={"collection": "docs", "query": "anything"})


async def test_search_no_auth_returns_4xx(client: AsyncClient) -> None:
    response = await client.post("/v1/search", json={"collection": "docs", "query": "test"})
    assert response.status_code in {401, 403}


async def test_search_query_too_long_returns_422(auth_client: AsyncClient) -> None:
    response = await auth_client.post("/v1/search", json={"collection": "docs", "query": "x" * 501})
    assert response.status_code == 422


async def test_search_empty_query_returns_422(auth_client: AsyncClient) -> None:
    response = await auth_client.post("/v1/search", json={"collection": "docs", "query": ""})
    assert response.status_code == 422


async def test_search_limit_out_of_range_returns_422(
    auth_client: AsyncClient,
) -> None:
    response = await auth_client.post("/v1/search", json={"collection": "docs", "query": "test", "limit": 25})
    assert response.status_code == 422


async def test_search_filters_by_current_user(
    auth_client: AsyncClient, mock_qdrant: MagicMock
) -> None:
    # Given
    mock_qdrant.query = AsyncMock(return_value=[])

    # When
    await auth_client.post("/v1/search", json={"collection": "docs", "query": "test"})

    # Then: user filter must be passed to Qdrant
    call_kwargs = mock_qdrant.query.call_args.kwargs
    assert call_kwargs["query_filter"] is not None
    field_cond = call_kwargs["query_filter"].must[0]
    assert field_cond.key == "user"
    assert field_cond.match.value == "testuser"


async def test_search_multiple_results_ordered(
    auth_client: AsyncClient, mock_qdrant: MagicMock
) -> None:
    # Given: Qdrant returns two results in score order
    mock_qdrant.query = AsyncMock(
        return_value=[
            _make_query_response(job_id="job-a", score=0.9),
            _make_query_response(job_id="job-b", score=0.7),
        ]
    )

    # When
    response = await auth_client.post("/v1/search", json={"collection": "docs", "query": "test", "limit": 2})

    # Then
    body = response.json()
    assert body["total"] == 2
    assert body["results"][0]["id"] == "job-a"
    assert body["results"][1]["id"] == "job-b"
