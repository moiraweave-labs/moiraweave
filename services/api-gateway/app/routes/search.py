import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.models import FieldCondition, Filter, MatchValue

from app.config import Settings, get_settings
from app.dependencies.auth import CurrentUser
from app.dependencies.qdrant import QdrantDep
from app.middleware.rate_limit import limiter
from app.models.search import SearchHit, SearchRequest, SearchResponse

router = APIRouter(tags=["search"])
logger = logging.getLogger(__name__)


@router.post(
    "/search",
    response_model=SearchResponse,
    summary="Semantic search over a Qdrant collection",
)
@limiter.limit("30/minute")
async def search(
    request: Request,
    body: SearchRequest,
    qdrant: QdrantDep,
    current_user: CurrentUser,
    settings: Annotated[Settings, Depends(get_settings)],
) -> SearchResponse:
    """Query a Qdrant collection by natural language.

    Results are scoped to the authenticated user via a metadata filter on the
    ``user`` field. The ``collection`` field in the request body determines which
    Qdrant collection is queried.
    """
    query_filter = Filter(
        must=[
            FieldCondition(
                key="user",
                match=MatchValue(value=current_user.subject),
            )
        ]
    )
    try:
        hits = await qdrant.query(
            collection_name=body.collection,
            query_text=body.query,
            query_filter=query_filter,
            limit=body.limit,
        )
    except UnexpectedResponse as exc:
        if exc.status_code == 404:
            # Collection doesn't exist yet — no data indexed.
            return SearchResponse(results=[], total=0)
        raise

    results = [
        SearchHit(
            id=str(hit.id),
            score=round(hit.score, 4),
            payload=hit.metadata or {},
        )
        for hit in hits
    ]
    return SearchResponse(results=results, total=len(results))
