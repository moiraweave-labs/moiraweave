from typing import Any

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    collection: str = Field(..., description="Qdrant collection to search against")
    query: str = Field(
        ..., min_length=1, max_length=500, description="Natural language search query"
    )
    limit: int = Field(default=5, ge=1, le=20, description="Max results (1-20)")


class SearchHit(BaseModel):
    id: str
    score: float
    payload: dict[str, Any]


class SearchResponse(BaseModel):
    results: list[SearchHit]
    total: int
    total: int
